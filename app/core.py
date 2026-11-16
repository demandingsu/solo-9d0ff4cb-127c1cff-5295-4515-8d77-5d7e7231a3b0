"""报文段归组、组装、校验与观测解析的核心逻辑。

归组键：(station_id 站号, boot_id 设备启动号, msg_id 报文号)。

每条 NDJSON 记录字段：
    station_id  站号（整数）
    boot_id     设备启动号（整数）
    msg_id      报文号（整数）
    seg_no      段号，1..seg_total（整数）
    seg_total   总段数（整数 >= 1）
    payload_b64 本段正文的 Base64
    sha256      整报（全部段按段号拼接后的字节）的 SHA-256 十六进制

一致性规则：
  * 完全相同的段重投 -> 幂等忽略（duplicate），只入库一次；
  * 同一报文的段内容 / 总段数 / 整报摘要出现冲突，或段号越界，
    或组装后摘要与登记摘要不符，或 CSV 解析失败
    -> 报文标记 pending_review（待核），并立即撤下该报文已发布的观测；
  * 待核报文后续的段一律忽略，直到监测员复位（DELETE）后重新采集。
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
import sqlite3
from datetime import datetime

REQUIRED_FIELDS = (
    "station_id",
    "boot_id",
    "msg_id",
    "seg_no",
    "seg_total",
    "payload_b64",
    "sha256",
)

MSG_KEY = "station_id=? AND boot_id=? AND msg_id=?"


class LineError(ValueError):
    """单行记录本身不合法（无法归属到某个报文）。"""


def _as_int(value, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LineError(f"字段 {name} 必须是整数")
    return value


def validate_record(obj) -> dict:
    """校验并规范化一条 NDJSON 记录，返回字典（payload 为 bytes）。"""
    if not isinstance(obj, dict):
        raise LineError("记录必须是 JSON 对象")
    missing = [f for f in REQUIRED_FIELDS if f not in obj]
    if missing:
        raise LineError(f"缺少字段: {', '.join(missing)}")

    station_id = _as_int(obj["station_id"], "station_id")
    boot_id = _as_int(obj["boot_id"], "boot_id")
    msg_id = _as_int(obj["msg_id"], "msg_id")
    seg_no = _as_int(obj["seg_no"], "seg_no")
    seg_total = _as_int(obj["seg_total"], "seg_total")
    if seg_total < 1:
        raise LineError("seg_total 必须 >= 1")
    if seg_no < 1:
        raise LineError("seg_no 必须 >= 1")

    payload_b64 = obj["payload_b64"]
    if not isinstance(payload_b64, str):
        raise LineError("payload_b64 必须是字符串")
    try:
        payload = base64.b64decode(payload_b64, validate=True)
    except (binascii.Error, ValueError):
        raise LineError("payload_b64 不是合法的 Base64")

    sha256 = obj["sha256"]
    if not isinstance(sha256, str):
        raise LineError("sha256 必须是字符串")
    sha256 = sha256.strip().lower()
    if len(sha256) != 64 or any(c not in "0123456789abcdef" for c in sha256):
        raise LineError("sha256 必须是 64 位十六进制")

    return {
        "station_id": station_id,
        "boot_id": boot_id,
        "msg_id": msg_id,
        "seg_no": seg_no,
        "seg_total": seg_total,
        "payload": payload,
        "sha256": sha256,
    }


def parse_tide_csv(data: bytes) -> list[tuple[str, float]]:
    """解析整报正文：UTC 分钟潮位 CSV。

    每行：`YYYY-MM-DDTHH:MM:00Z,<潮位米>`；允许空行与 `#` 开头的注释行。
    返回 [(observed_at, level_m), ...]，任一数据行非法即抛 ValueError。
    """
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"正文不是合法 UTF-8: {exc}") from exc

    obs: list[tuple[str, float]] = []
    for lineno, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError(f"第 {lineno} 行格式错误（应为 时间,潮位）: {line!r}")
        try:
            dt = datetime.strptime(parts[0], "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            raise ValueError(
                f"第 {lineno} 行时间格式错误（应为 YYYY-MM-DDTHH:MM:00Z）: {parts[0]!r}"
            ) from None
        if dt.second != 0:
            raise ValueError(f"第 {lineno} 行不是整分钟: {parts[0]!r}")
        try:
            level = float(parts[1])
        except ValueError:
            raise ValueError(f"第 {lineno} 行潮位不是数值: {parts[1]!r}") from None
        obs.append((dt.strftime("%Y-%m-%dT%H:%M:00Z"), level))

    if not obs:
        raise ValueError("报文不含任何观测行")
    return obs


def _flag_pending(con: sqlite3.Connection, key: tuple, reason: str) -> None:
    """标记报文待核，并撤下该报文已发布的全部观测。"""
    con.execute(
        f"UPDATE messages SET status='pending_review', reason=?, "
        f"updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE {MSG_KEY}",
        (reason, *key),
    )
    con.execute(f"DELETE FROM observations WHERE {MSG_KEY}", key)


def ingest_record(con: sqlite3.Connection, rec: dict) -> dict:
    """在调用方的事务内处理一条已校验记录，返回该行的处理结果。"""
    key = (rec["station_id"], rec["boot_id"], rec["msg_id"])
    base = {
        "station_id": key[0],
        "boot_id": key[1],
        "msg_id": key[2],
        "seg_no": rec["seg_no"],
    }

    row = con.execute(
        f"SELECT seg_total, sha256, status FROM messages WHERE {MSG_KEY}", key
    ).fetchone()
    if row is None:
        con.execute(
            "INSERT INTO messages (station_id, boot_id, msg_id, seg_total, sha256)"
            " VALUES (?, ?, ?, ?, ?)",
            (*key, rec["seg_total"], rec["sha256"]),
        )
        row = {"seg_total": rec["seg_total"], "sha256": rec["sha256"], "status": "collecting"}

    if row["status"] == "pending_review":
        return {**base, "status": "ignored", "detail": "报文待核，段未入库"}

    # ---- 冲突检测：总段数 / 整报摘要 / 段号越界 / 同段号不同内容 ----
    if rec["seg_total"] != row["seg_total"]:
        reason = f"总段数冲突：已登记 {row['seg_total']}，新记录为 {rec['seg_total']}"
        _flag_pending(con, key, reason)
        return {**base, "status": "conflict", "detail": reason}
    if rec["sha256"] != row["sha256"]:
        reason = "整报 SHA-256 摘要冲突：同报文出现不同摘要"
        _flag_pending(con, key, reason)
        return {**base, "status": "conflict", "detail": reason}
    if rec["seg_no"] > row["seg_total"]:
        reason = f"段号越界：段 {rec['seg_no']} 超出总段数 {row['seg_total']}"
        _flag_pending(con, key, reason)
        return {**base, "status": "conflict", "detail": reason}

    existing = con.execute(
        f"SELECT payload FROM segments WHERE {MSG_KEY} AND seg_no=?",
        (*key, rec["seg_no"]),
    ).fetchone()
    if existing is not None:
        if existing["payload"] == rec["payload"]:
            return {**base, "status": "duplicate", "detail": "相同段重投，幂等忽略"}
        reason = f"段 {rec['seg_no']} 内容冲突：同段号不同正文"
        _flag_pending(con, key, reason)
        return {**base, "status": "conflict", "detail": reason}

    con.execute(
        "INSERT INTO segments (station_id, boot_id, msg_id, seg_no, payload)"
        " VALUES (?, ?, ?, ?, ?)",
        (*key, rec["seg_no"], rec["payload"]),
    )

    # ---- 收齐则组装、校验、解析、发布 ----
    rows = con.execute(
        f"SELECT seg_no, payload FROM segments WHERE {MSG_KEY} ORDER BY seg_no", key
    ).fetchall()
    if len(rows) < row["seg_total"]:
        return {**base, "status": "stored", "detail": "段已入库"}

    data = b"".join(r["payload"] for r in rows)
    digest = hashlib.sha256(data).hexdigest()
    if digest != row["sha256"]:
        reason = f"组装后摘要校验不符：登记 {row['sha256'][:12]}…，实算 {digest[:12]}…"
        _flag_pending(con, key, reason)
        return {**base, "status": "conflict", "detail": reason}

    try:
        obs = parse_tide_csv(data)
    except ValueError as exc:
        reason = f"潮位 CSV 解析失败：{exc}"
        _flag_pending(con, key, reason)
        return {**base, "status": "conflict", "detail": reason}

    con.executemany(
        "INSERT OR IGNORE INTO observations"
        " (station_id, observed_at, level_m, boot_id, msg_id)"
        " VALUES (?, ?, ?, ?, ?)",
        [(key[0], ts, level, key[1], key[2]) for ts, level in obs],
    )
    con.execute(
        f"UPDATE messages SET status='published', reason=NULL, "
        f"updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE {MSG_KEY}",
        key,
    )
    return {**base, "status": "published", "detail": f"报文完整，发布 {len(obs)} 条观测"}


def ingest_ndjson(con: sqlite3.Connection, text: str) -> dict:
    """处理一整段 NDJSON 文本。调用方须持有写锁；本函数自成一个事务。"""
    results = []
    counts = {"stored": 0, "published": 0, "duplicate": 0,
              "conflict": 0, "ignored": 0, "error": 0}
    con.execute("BEGIN IMMEDIATE")
    try:
        for lineno, raw in enumerate(text.splitlines(), 1):
            line = raw.strip()
            if not line:
                continue
            try:
                rec = validate_record(json.loads(line))
                outcome = ingest_record(con, rec)
            except LineError as exc:
                outcome = {"status": "error", "detail": str(exc)}
            except Exception as exc:  # 单行异常不拖垮整批
                outcome = {"status": "error", "detail": f"记录解析失败: {exc}"}
            outcome["line"] = lineno
            results.append(outcome)
            counts[outcome["status"]] = counts.get(outcome["status"], 0) + 1
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    return {"lines": len(results), "counts": counts, "results": results}


def list_messages(con: sqlite3.Connection) -> list[dict]:
    """全部报文的状态，含已收段数、缺段清单、待核原因、观测数。"""
    msgs = con.execute(
        "SELECT m.*, "
        " (SELECT COUNT(*) FROM observations o WHERE o.station_id=m.station_id"
        "   AND o.boot_id=m.boot_id AND o.msg_id=m.msg_id) AS obs_count "
        "FROM messages m ORDER BY m.station_id, m.boot_id, m.msg_id"
    ).fetchall()
    out = []
    for m in msgs:
        key = (m["station_id"], m["boot_id"], m["msg_id"])
        have = {
            r["seg_no"]
            for r in con.execute(
                f"SELECT seg_no FROM segments WHERE {MSG_KEY}", key
            )
        }
        missing = sorted(set(range(1, m["seg_total"] + 1)) - have)
        out.append({
            "station_id": m["station_id"],
            "boot_id": m["boot_id"],
            "msg_id": m["msg_id"],
            "seg_total": m["seg_total"],
            "received": len(have),
            "missing_segments": missing,
            "status": m["status"],
            "reason": m["reason"],
            "observations": m["obs_count"],
            "updated_at": m["updated_at"],
        })
    return out


def reset_message(con: sqlite3.Connection, station_id: int, boot_id: int, msg_id: int) -> bool:
    """删除一个报文及其段与观测（待核处理后重新采集）。"""
    key = (station_id, boot_id, msg_id)
    con.execute("BEGIN IMMEDIATE")
    try:
        cur = con.execute(f"DELETE FROM messages WHERE {MSG_KEY}", key)
        con.execute(f"DELETE FROM segments WHERE {MSG_KEY}", key)
        con.execute(f"DELETE FROM observations WHERE {MSG_KEY}", key)
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    return cur.rowcount > 0


# ---------------------------------------------------------------------------
# 岸台应收报文清单与补传请求
# ---------------------------------------------------------------------------

_INT_RE = re.compile(r"^-?[0-9]+$")
_MANIFEST_HEADER = ("msg_id", "seg_total", "sha256")


class ManifestError(ValueError):
    """清单内容不合法（校验失败，调用方必须保留旧清单）。"""


def validate_manifest(text: str) -> list[dict]:
    """解析并校验清单文本，每行 `报文号,总段数,整报SHA-256`。

    允许空行、`#` 注释行与表头行 `msg_id,seg_total,sha256`（大小写不限）。
    全部行校验通过才返回条目列表；任一行非法或报文号重复即抛
    ManifestError —— 调用方据此拒绝整份清单并保留旧版。
    """
    entries: list[dict] = []
    seen: dict[int, int] = {}
    for lineno, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = [p.strip() for p in line.split(",")]
        if tuple(p.lower() for p in parts) == _MANIFEST_HEADER:
            continue  # 表头行
        if len(parts) != 3:
            raise ManifestError(
                f"第 {lineno} 行应有 3 列（报文号,总段数,整报SHA-256）: {line!r}")
        msg_s, total_s, sha = parts
        if not _INT_RE.match(msg_s):
            raise ManifestError(f"第 {lineno} 行报文号不是整数: {msg_s!r}")
        if not _INT_RE.match(total_s):
            raise ManifestError(f"第 {lineno} 行总段数不是整数: {total_s!r}")
        msg_id, seg_total = int(msg_s), int(total_s)
        if seg_total < 1:
            raise ManifestError(f"第 {lineno} 行总段数必须 >= 1: {seg_total}")
        sha = sha.lower()
        if len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha):
            raise ManifestError(f"第 {lineno} 行 SHA-256 必须是 64 位十六进制: {sha!r}")
        if msg_id in seen:
            raise ManifestError(
                f"第 {lineno} 行与第 {seen[msg_id]} 行的报文号重复: {msg_id}")
        seen[msg_id] = lineno
        entries.append({"msg_id": msg_id, "seg_total": seg_total, "sha256": sha})
    if not entries:
        raise ManifestError("清单不含任何有效条目")
    entries.sort(key=lambda e: e["msg_id"])
    return entries


def replace_manifest(con: sqlite3.Connection, station_id: int, boot_id: int,
                     entries: list[dict]) -> dict:
    """整版替换 (站号, 启动号) 的清单。调用方须持有写锁。

    单事务内先删旧条目再写新条目；任何失败整体回滚，旧清单原样保留，
    读者不会看到删了一半或混入两版的清单。
    """
    key = (station_id, boot_id)
    con.execute("BEGIN IMMEDIATE")
    try:
        cur = con.execute(
            "DELETE FROM manifest_entries WHERE station_id=? AND boot_id=?", key)
        con.executemany(
            "INSERT INTO manifest_entries (station_id, boot_id, msg_id, seg_total, sha256)"
            " VALUES (?, ?, ?, ?, ?)",
            [(station_id, boot_id, e["msg_id"], e["seg_total"], e["sha256"])
             for e in entries],
        )
        con.execute(
            "INSERT INTO manifests (station_id, boot_id, entries, updated_at)"
            " VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))"
            " ON CONFLICT (station_id, boot_id) DO UPDATE SET"
            "   entries=excluded.entries, updated_at=excluded.updated_at",
            (station_id, boot_id, len(entries)),
        )
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    return {"station_id": station_id, "boot_id": boot_id,
            "entries": len(entries), "replaced": cur.rowcount > 0}


def list_manifests(con: sqlite3.Connection) -> list[dict]:
    """全部清单版本（站号、启动号、条目数、更新时间）。"""
    rows = con.execute(
        "SELECT station_id, boot_id, entries, updated_at FROM manifests"
        " ORDER BY station_id, boot_id"
    ).fetchall()
    return [dict(r) for r in rows]


def get_manifest(con: sqlite3.Connection, station_id: int,
                 boot_id: int) -> dict | None:
    """某站/启动号的当前清单明细；无清单返回 None。"""
    head = con.execute(
        "SELECT station_id, boot_id, entries, updated_at FROM manifests"
        " WHERE station_id=? AND boot_id=?", (station_id, boot_id)).fetchone()
    if head is None:
        return None
    items = con.execute(
        "SELECT msg_id, seg_total, sha256 FROM manifest_entries"
        " WHERE station_id=? AND boot_id=? ORDER BY msg_id",
        (station_id, boot_id)).fetchall()
    return {**dict(head), "items": [dict(r) for r in items]}


def retransmit_requests(con: sqlite3.Connection, station_id: int,
                        boot_id: int) -> list[dict] | None:
    """计算该站/启动号的补传请求。调用方须持有写锁。

    本函数自成一个读事务：清单条目与报文状态来自同一快照，并发上传、
    替换或复位时不会混入未提交数据或不同清单版本。只按清单条目做主键
    核查，不扫描历史报文。返回 None 表示该站/启动号尚无清单。

    规则（对清单中每个报文号）：
      * 报文未到 / 待核 / 登记的总段数或摘要与清单不符 -> 请求整报；
      * 登记一致但尚缺段 -> 只请求缺段；
      * 已发布且登记与清单一致（完整匹配）-> 不请求。
    """
    key = (station_id, boot_id)
    con.execute("BEGIN")
    try:
        entries = con.execute(
            "SELECT msg_id, seg_total, sha256 FROM manifest_entries"
            " WHERE station_id=? AND boot_id=? ORDER BY msg_id", key).fetchall()
        if not entries:
            con.execute("COMMIT")
            return None
        out: list[dict] = []
        for e in entries:
            mkey = (station_id, boot_id, e["msg_id"])
            row = con.execute(
                f"SELECT seg_total, sha256, status FROM messages WHERE {MSG_KEY}",
                mkey).fetchone()
            base = {"station_id": station_id, "boot_id": boot_id,
                    "msg_id": e["msg_id"], "seg_total": e["seg_total"],
                    "sha256": e["sha256"]}
            if row is None:
                out.append({**base, "request": "message", "segments": [],
                            "reason": "报文未到"})
                continue
            if row["status"] == "pending_review":
                out.append({**base, "request": "message", "segments": [],
                            "reason": "报文待核"})
                continue
            if row["seg_total"] != e["seg_total"] or row["sha256"] != e["sha256"]:
                out.append({**base, "request": "message", "segments": [],
                            "reason": "登记总段数/摘要与清单不符"})
                continue
            if row["status"] == "published":
                continue  # 完整匹配，不请求
            have = {r["seg_no"] for r in con.execute(
                f"SELECT seg_no FROM segments WHERE {MSG_KEY}", mkey)}
            missing = sorted(set(range(1, e["seg_total"] + 1)) - have)
            if missing:
                out.append({**base, "request": "segments", "segments": missing,
                            "reason": "缺段"})
        con.execute("COMMIT")
        return out
    except BaseException:
        con.execute("ROLLBACK")
        raise
