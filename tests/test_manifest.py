"""应收清单上传与补传 CSV：整版替换、校验失败保留旧清单、按状态请求、并发一致性。"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import create_app  # noqa: E402

SAMPLES = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "samples")
ST, BOOT = 101, 7
CSV_HEADER = "station_id,boot_id,msg_id,request,segments,seg_total,sha256,reason"


def make_client(tmp_path, name="t.db"):
    return TestClient(create_app(str(tmp_path / name)))


def upload_file(client, path):
    with open(path, "rb") as fh:
        return client.post("/api/upload", files={"file": (os.path.basename(path), fh)})


def make_segments(station, boot, msg, data: bytes, n_seg: int):
    """把整报正文切成 n_seg 段，返回 (记录列表, 整报摘要)。"""
    digest = hashlib.sha256(data).hexdigest()
    per = max(1, -(-len(data) // n_seg))
    chunks = [data[i:i + per] for i in range(0, len(data), per)]
    while len(chunks) < n_seg:
        chunks.append(b"")
    recs = [{"station_id": station, "boot_id": boot, "msg_id": msg,
             "seg_no": i + 1, "seg_total": n_seg,
             "payload_b64": base64.b64encode(c).decode(), "sha256": digest}
            for i, c in enumerate(chunks)]
    return recs, digest


def post_records(client, recs):
    text = "".join(json.dumps(r) + "\n" for r in recs)
    return client.post("/api/upload", content=text)


def put_manifest(client, text, station=ST, boot=BOOT):
    return client.post(f"/api/manifests/{station}/{boot}",
                       files={"file": ("manifest.csv", text)})


def retransmit(client, station=ST, boot=BOOT):
    return client.get("/api/retransmit.csv",
                      params={"station_id": station, "boot_id": boot})


def csv_rows(resp):
    lines = resp.text.strip().splitlines()
    assert lines[0] == CSV_HEADER
    header = CSV_HEADER.split(",")
    return [dict(zip(header, line.split(","))) for line in lines[1:]]


def manifest_text(items):
    return "msg_id,seg_total,sha256\n" + "".join(
        f"{m},{t},{s}\n" for m, t, s in items)


# ---------- 基本流：上传清单 -> 补传 CSV ----------

def test_manifest_upload_and_retransmit_flow(tmp_path):
    with make_client(tmp_path) as c:
        upload_file(c, os.path.join(SAMPLES, "sample_upload.ndjson"))
        r = put_manifest(c, manifest_text([
            (1001, 4, "6917df940b6c3ef1ab00da214277d9428f78e91e46c61214c794ad9914209fac"),
            (1002, 3, "7dd5e620f8653144be097b509891923f0f4ec44f0f46577db6f38317fcb4c2aa"),
            (1003, 3, "a5734af22db4a9d87ac69ea9459476beb02fe9a2dff8b56abf766c8f18f7e583"),
            (1004, 2, hashlib.sha256(b"not arrived").hexdigest()),
        ]))
        assert r.status_code == 200
        body = r.json()
        assert body["entries"] == 4 and body["replaced"] is False

        resp = retransmit(c)
        assert resp.status_code == 200
        assert "attachment" in resp.headers["content-disposition"]
        rows = {row["msg_id"]: row for row in csv_rows(resp)}
        assert set(rows) == {"1003", "1004"}          # 1001/1002 完整匹配，不请求
        assert rows["1003"]["request"] == "segments"  # 登记一致只缺段 -> 只请求缺段
        assert rows["1003"]["segments"] == "2"
        assert rows["1003"]["reason"] == "缺段"
        assert rows["1004"]["request"] == "message"   # 报文未到 -> 请求整报
        assert rows["1004"]["segments"] == ""
        assert rows["1004"]["reason"] == "报文未到"

        mf = c.get("/api/manifests").json()["manifests"]
        assert [(m["station_id"], m["boot_id"], m["entries"]) for m in mf] == [(ST, BOOT, 4)]
        detail = c.get(f"/api/manifests/{ST}/{BOOT}").json()
        assert [i["msg_id"] for i in detail["items"]] == [1001, 1002, 1003, 1004]


def test_manifest_uppercase_sha_and_comments_accepted(tmp_path):
    data = b"2026-11-15T00:00:00Z,1.5\n"
    recs, digest = make_segments(ST, BOOT, 7001, data, 1)
    with make_client(tmp_path) as c:
        post_records(c, recs)                        # 完整发布
        text = "# 岸台清单\n\n7001,1," + digest.upper() + "\n"
        r = put_manifest(c, text)
        assert r.status_code == 200, r.text
        assert r.json()["entries"] == 1
        assert csv_rows(retransmit(c)) == []         # 大写摘要归一化后完整匹配


def test_manifest_raw_body_upload(tmp_path):
    with make_client(tmp_path) as c:
        r = c.post(f"/api/manifests/{ST}/{BOOT}",
                   content="8001,2," + "a" * 64 + "\n")
        assert r.status_code == 200
        assert [row["msg_id"] for row in csv_rows(retransmit(c))] == ["8001"]


# ---------- 状态变化实时反映到补传 CSV ----------

def test_late_segment_completion_updates_csv(tmp_path):
    data = b"2026-11-15T01:00:00Z,2.1\n2026-11-15T01:01:00Z,2.2\n"
    recs, digest = make_segments(ST, BOOT, 3001, data, 3)
    with make_client(tmp_path) as c:
        post_records(c, recs[:2])                    # 缺第 3 段
        put_manifest(c, manifest_text([(3001, 3, digest)]))
        rows = csv_rows(retransmit(c))
        assert [(r["request"], r["segments"]) for r in rows] == [("segments", "3")]

        post_records(c, recs[2:])                    # 迟到段到达 -> 发布
        assert csv_rows(retransmit(c)) == []         # 不再请求


def test_pending_then_reset_then_recollect(tmp_path):
    data = b"2026-11-15T02:00:00Z,1.1\n"
    recs, digest = make_segments(ST, BOOT, 3002, data, 2)
    forged = dict(recs[0])
    forged["payload_b64"] = base64.b64encode(b"# tampered\n").decode()
    with make_client(tmp_path) as c:
        post_records(c, [recs[0], forged])           # 段内容冲突 -> 待核
        put_manifest(c, manifest_text([(3002, 2, digest)]))
        rows = csv_rows(retransmit(c))
        assert [(r["request"], r["reason"]) for r in rows] == [("message", "报文待核")]

        assert c.delete(f"/api/messages/{ST}/{BOOT}/3002").status_code == 200
        rows = csv_rows(retransmit(c))               # 复位后视为未到 -> 仍请求整报
        assert [(r["request"], r["reason"]) for r in rows] == [("message", "报文未到")]

        post_records(c, recs)                        # 重新采集 -> 发布
        assert csv_rows(retransmit(c)) == []


def test_registered_mismatch_requests_whole_message(tmp_path):
    data = b"2026-11-15T03:00:00Z,0.9\n"
    recs, digest = make_segments(ST, BOOT, 3003, data, 2)
    other = hashlib.sha256(b"shore side copy").hexdigest()
    with make_client(tmp_path) as c:
        post_records(c, recs[:1])                    # 登记 seg_total=2, sha=digest
        # 清单总段数不符
        put_manifest(c, manifest_text([(3003, 3, digest)]))
        rows = csv_rows(retransmit(c))
        assert [(r["request"], r["reason"]) for r in rows] == [("message", "登记总段数/摘要与清单不符")]
        # 清单摘要不符
        put_manifest(c, manifest_text([(3003, 2, other)]))
        rows = csv_rows(retransmit(c))
        assert [(r["request"], r["reason"]) for r in rows] == [("message", "登记总段数/摘要与清单不符")]


def test_conflicts_sample_all_pending_requested_whole(tmp_path):
    with make_client(tmp_path) as c:
        upload_file(c, os.path.join(SAMPLES, "sample_conflicts.ndjson"))
        items = [(m, 2, hashlib.sha256(f"m{m}".encode()).hexdigest())
                 for m in (2001, 2002, 2003, 2004, 2005)]
        put_manifest(c, manifest_text(items))
        rows = csv_rows(retransmit(c))
        assert all(r["request"] == "message" for r in rows)   # 全部待核 -> 整报
        assert {r["msg_id"] for r in rows} == {"2001", "2002", "2003", "2004", "2005"}


# ---------- 整版替换 ----------

def test_replace_manifest_whole_version(tmp_path):
    sha = lambda m: hashlib.sha256(f"m{m}".encode()).hexdigest()
    with make_client(tmp_path) as c:
        put_manifest(c, manifest_text([(4001, 1, sha(4001)), (4002, 1, sha(4002))]))
        r = put_manifest(c, manifest_text([(4002, 1, sha(4002)), (4003, 1, sha(4003))]))
        assert r.json()["replaced"] is True
        detail = c.get(f"/api/manifests/{ST}/{BOOT}").json()
        assert [i["msg_id"] for i in detail["items"]] == [4002, 4003]  # 旧条目 4001 已移除
        rows = csv_rows(retransmit(c))
        assert {r["msg_id"] for r in rows} == {"4002", "4003"}         # 只按新版核查


def test_manifests_are_per_station_boot(tmp_path):
    sha = lambda m: hashlib.sha256(f"m{m}".encode()).hexdigest()
    with make_client(tmp_path) as c:
        put_manifest(c, manifest_text([(5001, 1, sha(5001))]), station=101, boot=7)
        put_manifest(c, manifest_text([(5002, 1, sha(5002))]), station=101, boot=8)
        put_manifest(c, manifest_text([(5003, 1, sha(5003))]), station=102, boot=7)
        mf = c.get("/api/manifests").json()["manifests"]
        assert [(m["station_id"], m["boot_id"]) for m in mf] == [(101, 7), (101, 8), (102, 7)]
        assert [r["msg_id"] for r in csv_rows(retransmit(c, 101, 8))] == ["5002"]


# ---------- 校验失败保留旧清单 ----------

@pytest.mark.parametrize("bad_text,needle", [
    ("6001,2,xyz\n", "SHA-256"),
    ("6001,0," + "a" * 64 + "\n", ">= 1"),
    ("6001,2," + "a" * 64 + "\n6001,2," + "b" * 64 + "\n", "重复"),
    ("6001,2\n", "3 列"),
    ("abc,2," + "a" * 64 + "\n", "报文号不是整数"),
    ("6001,二," + "a" * 64 + "\n", "总段数不是整数"),
    ("", "有效条目"),
    ("# 只有注释\n\n", "有效条目"),
])
def test_invalid_manifest_rejected_and_old_kept(tmp_path, bad_text, needle):
    good = manifest_text([(6001, 2, hashlib.sha256(b"good").hexdigest())])
    with make_client(tmp_path) as c:
        assert put_manifest(c, good).status_code == 200
        r = put_manifest(c, bad_text)
        assert r.status_code == 400
        assert "旧清单已保留" in r.json()["detail"] and needle in r.json()["detail"]
        # 旧清单原样保留
        detail = c.get(f"/api/manifests/{ST}/{BOOT}").json()
        assert [i["msg_id"] for i in detail["items"]] == [6001]
        assert [row["msg_id"] for row in csv_rows(retransmit(c))] == ["6001"]


def test_non_utf8_manifest_rejected(tmp_path):
    with make_client(tmp_path) as c:
        r = c.post(f"/api/manifests/{ST}/{BOOT}",
                   files={"file": ("m.csv", b"6001,2,\xff\xfe")})
        assert r.status_code == 400
        assert retransmit(c).status_code == 404      # 从未保存成功 -> 仍无清单


# ---------- 无清单 / 只核查清单内报文 ----------

def test_no_manifest_returns_404(tmp_path):
    with make_client(tmp_path) as c:
        assert retransmit(c).status_code == 404
        assert c.get(f"/api/manifests/{ST}/{BOOT}").status_code == 404
        assert c.get("/api/manifests").json()["manifests"] == []


def test_only_manifest_messages_checked(tmp_path):
    with make_client(tmp_path) as c:
        upload_file(c, os.path.join(SAMPLES, "sample_upload.ndjson"))  # 1001-1003 在库
        # 清单只含 1003：其余历史报文（含缺段的、已发布的）一概不出现在补传 CSV
        put_manifest(c, manifest_text([
            (1003, 3, "a5734af22db4a9d87ac69ea9459476beb02fe9a2dff8b56abf766c8f18f7e583"),
        ]))
        rows = csv_rows(retransmit(c))
        assert [(r["msg_id"], r["request"], r["segments"]) for r in rows] == [("1003", "segments", "2")]


def test_many_historical_messages_not_scanned(tmp_path):
    # 大量历史报文在库，清单只涉及其中一个：结果与历史报文无关
    sha = lambda m: hashlib.sha256(f"m{m}".encode()).hexdigest()
    with make_client(tmp_path) as c:
        recs = []
        for m in range(9000, 9050):                  # 50 个各缺一段的历史报文
            r, _ = make_segments(ST, BOOT, m, f"2026-11-15T04:{m % 60:02d}:00Z,1.0\n".encode(), 2)
            recs.append(r[0])
        post_records(c, recs)
        put_manifest(c, manifest_text([(9049, 2, sha(9049))]))  # 摘要故意不符
        rows = csv_rows(retransmit(c))
        assert [(r["msg_id"], r["request"]) for r in rows] == [("9049", "message")]


# ---------- 并发：替换与下载不混版本 ----------

def test_concurrent_replace_and_download_never_mix_versions(tmp_path):
    sha_a = hashlib.sha256(b"version A").hexdigest()
    sha_b = hashlib.sha256(b"version B").hexdigest()
    text_a = manifest_text([(9101, 1, sha_a)])
    text_b = manifest_text([(9102, 1, sha_b)])
    expect = {
        "a": f"{CSV_HEADER}\n{ST},{BOOT},9101,message,,1,{sha_a},报文未到\n",
        "b": f"{CSV_HEADER}\n{ST},{BOOT},9102,message,,1,{sha_b},报文未到\n",
    }
    with make_client(tmp_path) as c:
        put_manifest(c, text_a)

        def upload_version(i):
            r = put_manifest(c, text_a if i % 2 == 0 else text_b)
            assert r.status_code == 200

        def download(_):
            r = retransmit(c)
            assert r.status_code == 200
            assert r.text in (expect["a"], expect["b"])  # 只能是某一整版，不得混杂

        with ThreadPoolExecutor(max_workers=12) as pool:
            list(pool.map(upload_version, range(12)))
            list(pool.map(download, range(24)))
            list(pool.map(lambda i: (upload_version(i), download(i)), range(12)))


def test_concurrent_ingest_and_download_no_errors(tmp_path):
    data = b"2026-11-15T05:00:00Z,3.3\n"
    recs, digest = make_segments(ST, BOOT, 9201, data, 4)
    with make_client(tmp_path) as c:
        put_manifest(c, manifest_text([(9201, 4, digest)]))

        def send(i):
            assert post_records(c, [recs[i % 4]]).status_code == 200

        def download(_):
            r = retransmit(c)
            assert r.status_code == 200
            for row in csv_rows(r):                  # 下载只反映已提交状态
                assert row["msg_id"] == "9201"
                if row["request"] == "segments":
                    assert all(1 <= int(s) <= 4 for s in row["segments"].split(";"))

        with ThreadPoolExecutor(max_workers=10) as pool:
            list(pool.map(lambda i: (send(i), download(i)), range(20)))
        assert csv_rows(retransmit(c)) == []         # 最终收齐发布，不再请求
