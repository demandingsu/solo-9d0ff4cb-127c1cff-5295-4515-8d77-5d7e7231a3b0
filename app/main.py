"""沿海潮位卫星短报文采集服务（FastAPI + SQLite）。

接口：
  GET  /                                采集网页（上传 / 清单与补传 / 报文状态 / 观测）
  POST /api/upload                      上传 NDJSON（multipart 文件或裸文本）
  GET  /api/messages                    报文状态：已收/缺段/待核原因
  GET  /api/observations                已确认观测（JSON 预览）
  GET  /api/observations.csv            已确认观测下载（可选 ?station_id=）
  DELETE /api/messages/{station}/{boot}/{msg}  待核处理后复位报文，允许重新采集
  POST /api/manifests/{station}/{boot}  上传应收清单（同站同启动号整版替换）
  GET  /api/manifests                   全部清单版本（条目数、更新时间）
  GET  /api/manifests/{station}/{boot}  当前清单明细
  GET  /api/retransmit.csv              补传请求 CSV（?station_id=&boot_id=）
"""

from __future__ import annotations

import os
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, Response

from . import core, db

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


def create_app(db_path: str) -> FastAPI:
    write_lock = threading.Lock()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.db = db.connect(db_path)
        yield
        app.state.db.close()

    app = FastAPI(title="潮位短报文采集", lifespan=lifespan)

    @app.get("/", include_in_schema=False)
    async def index():
        return FileResponse(os.path.join(STATIC_DIR, "index.html"))

    @app.post("/api/upload")
    async def upload(request: Request, file: UploadFile | None = File(default=None)):
        """接收 NDJSON：multipart 文件字段 `file`，或直接 POST 裸文本。"""
        if file is not None:
            raw = await file.read()
        else:
            raw = await request.body()
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            raise HTTPException(status_code=400, detail="内容必须是 UTF-8 编码的 NDJSON")
        if not text.strip():
            raise HTTPException(status_code=400, detail="空的 NDJSON")
        # 串行化写入；单事务保证一批上传要么全部落库要么全部回滚
        with write_lock:
            summary = core.ingest_ndjson(request.app.state.db, text)
        return JSONResponse(summary)

    @app.get("/api/messages")
    async def messages(request: Request):
        return {"messages": core.list_messages(request.app.state.db)}

    @app.get("/api/observations")
    async def observations(request: Request, station_id: int | None = None,
                           limit: int = 200):
        limit = max(1, min(limit, 5000))
        sql = ("SELECT station_id, observed_at, level_m, boot_id, msg_id"
               " FROM observations")
        args: list = []
        if station_id is not None:
            sql += " WHERE station_id=?"
            args.append(station_id)
        sql += " ORDER BY station_id, observed_at LIMIT ?"
        args.append(limit)
        rows = request.app.state.db.execute(sql, args).fetchall()
        return {"observations": [dict(r) for r in rows]}

    @app.get("/api/observations.csv")
    async def observations_csv(request: Request, station_id: int | None = None):
        sql = "SELECT station_id, observed_at, level_m FROM observations"
        args: list = []
        if station_id is not None:
            sql += " WHERE station_id=?"
            args.append(station_id)
        sql += " ORDER BY station_id, observed_at"
        rows = request.app.state.db.execute(sql, args).fetchall()
        lines = ["station_id,observed_at,level_m"]
        lines += [f"{r['station_id']},{r['observed_at']},{r['level_m']}" for r in rows]
        name = f"observations_{station_id}.csv" if station_id is not None else "observations.csv"
        return Response(
            "\n".join(lines) + "\n",
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{name}"'},
        )

    @app.delete("/api/messages/{station_id}/{boot_id}/{msg_id}")
    async def delete_message(request: Request, station_id: int, boot_id: int, msg_id: int):
        with write_lock:
            removed = core.reset_message(request.app.state.db, station_id, boot_id, msg_id)
        if not removed:
            raise HTTPException(status_code=404, detail="报文不存在")
        return {"deleted": True}

    @app.post("/api/manifests/{station_id}/{boot_id}")
    async def upload_manifest(request: Request, station_id: int, boot_id: int,
                              file: UploadFile | None = File(default=None)):
        """上传应收清单（每行 `报文号,总段数,整报SHA-256`），同站同启动号整版替换。

        先整体校验再落库：校验或保存失败都保留旧清单。
        """
        if file is not None:
            raw = await file.read()
        else:
            raw = await request.body()
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            raise HTTPException(status_code=400, detail="清单必须是 UTF-8 编码的文本")
        try:
            entries = core.validate_manifest(text.lstrip("\ufeff"))
        except core.ManifestError as exc:
            raise HTTPException(status_code=400,
                                detail=f"清单校验失败，旧清单已保留: {exc}") from None
        # 串行化写入；单事务整版替换，失败回滚即保留旧清单
        with write_lock:
            return core.replace_manifest(request.app.state.db, station_id, boot_id, entries)

    @app.get("/api/manifests")
    async def manifests(request: Request):
        return {"manifests": core.list_manifests(request.app.state.db)}

    @app.get("/api/manifests/{station_id}/{boot_id}")
    async def manifest_detail(request: Request, station_id: int, boot_id: int):
        manifest = core.get_manifest(request.app.state.db, station_id, boot_id)
        if manifest is None:
            raise HTTPException(status_code=404, detail="该站/启动号尚无清单")
        return manifest

    @app.get("/api/retransmit.csv")
    async def retransmit_csv(request: Request, station_id: int, boot_id: int):
        # 写锁 + 单读事务：下载只反映某一已提交快照，不混入未提交数据或不同清单版本
        with write_lock:
            rows = core.retransmit_requests(request.app.state.db, station_id, boot_id)
        if rows is None:
            raise HTTPException(status_code=404, detail="该站/启动号尚无清单，请先上传")
        lines = ["station_id,boot_id,msg_id,request,segments,seg_total,sha256,reason"]
        for r in rows:
            segs = ";".join(str(s) for s in r["segments"])
            lines.append(
                f"{r['station_id']},{r['boot_id']},{r['msg_id']},{r['request']},"
                f"{segs},{r['seg_total']},{r['sha256']},{r['reason']}")
        return Response(
            "\n".join(lines) + "\n",
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition":
                     f'attachment; filename="retransmit_{station_id}_{boot_id}.csv"'},
        )

    return app


# uvicorn app.main:app 使用的默认实例；数据库路径可用 DB_PATH 覆盖
app = create_app(os.environ.get("DB_PATH", os.path.join("data", "tide.db")))
