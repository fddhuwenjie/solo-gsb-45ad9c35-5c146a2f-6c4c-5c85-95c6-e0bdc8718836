"""多来源读数仲裁测试。

覆盖：
- 上报携带 source_id 的留存与读数 ID 幂等（含来源不同判 conflict）；
- 无配置（legacy）保持旧行为；未列入来源最低档；同值合并、异值冲突；
- 来源优先级版本：expected_version 乐观并发（409）、同生效时刻 422、
  版本只从生效时刻起影响重算，历史读数与收据不改写；
- 权威时间线查询（authoritative / conflict / legacy）；
- 冲突时刻不进入剂量与诊断时间线；
- 归并：优先级配置冲突必须裁决、不得静默覆盖，别名访问命中规范档案配置；
- 作废读数立即退出仲裁，收据/历史不改写；calendar=display 与仲裁叠加。
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

BASE = 1_760_000_000  # 固定基准时刻


def iso(offset_seconds: float = 0) -> str:
    # 统一为 UTC、Z 结尾（与 API 输出口径一致，便于直接比较）
    return datetime.fromtimestamp(BASE + offset_seconds, tz=timezone.utc) \
        .isoformat().replace("+00:00", "Z")


def weekday(offset_seconds: float = 0) -> int:
    return datetime.fromtimestamp(BASE + offset_seconds, tz=timezone.utc).isoweekday()


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    from app.main import app

    with TestClient(app) as c:
        yield c


def make_exhibit(client: TestClient, eid: str = "E", limit: float = 1_000_000.0,
                 interval: float = 3600.0) -> str:
    resp = client.post("/exhibits", json={
        "id": eid, "name": eid, "dose_limit_luxh": limit, "max_interval_seconds": interval,
    })
    assert resp.status_code == 201, resp.text
    return resp.json()["canonical_id"]


def ingest(client: TestClient, eid: str, readings: list[dict]) -> dict:
    resp = client.post(f"/exhibits/{eid}/readings", json={"readings": readings})
    assert resp.status_code == 200, resp.text
    return resp.json()


def put_priorities(client: TestClient, eid: str, expected: int, effective: str,
                   priorities: list[dict]) -> tuple[int, dict]:
    resp = client.put(f"/exhibits/{eid}/source-priorities", json={
        "expected_version": expected, "effective_at": effective, "priorities": priorities,
    })
    return resp.status_code, resp.json()


def timeline(client: TestClient, eid: str, **params) -> dict:
    resp = client.get(f"/exhibits/{eid}/authoritative-timeline", params=params)
    assert resp.status_code == 200, resp.text
    return resp.json()


def diagnose(client: TestClient, eid: str, start: float, end: float,
             rate: float = 1000.0, stuck_tol: float = 5.0, min_stuck: float = 1.0) -> dict:
    resp = client.post(f"/exhibits/{eid}/readings/diagnose", json={
        "start": iso(start), "end": iso(end),
        "max_rate_lux_per_second": rate,
        "stuck_tolerance_lux": stuck_tol,
        "min_stuck_seconds": min_stuck,
    })
    assert resp.status_code == 200, resp.text
    return resp.json()


def dose(client: TestClient, eid: str, start: float, end: float, **params) -> dict:
    resp = client.get(f"/exhibits/{eid}/dose",
                      params={"start": iso(start), "end": iso(end), **params})
    assert resp.status_code == 200, resp.text
    return resp.json()


# ---------- 上报留存与幂等 ----------

def test_source_id_is_stored_and_returned_on_raw_readings(client):
    eid = make_exhibit(client)
    ingest(client, eid, [
        {"id": "a", "ts": iso(0), "lux": 100, "source_id": "A"},
        {"id": "b", "ts": iso(0), "lux": 120, "source_id": "B"},
        {"id": "n", "ts": iso(60), "lux": 100},
    ])
    rows = client.get(f"/exhibits/{eid}/readings").json()["readings"]
    by_id = {r["id"]: r for r in rows}
    assert by_id["a"]["source_id"] == "A"
    assert by_id["b"]["source_id"] == "B"
    assert by_id["n"]["source_id"] is None
    # 同一时刻不同来源读数全部留存（无覆盖）
    same_ts = [r for r in rows if r["ts"] == iso(0)]
    assert len(same_ts) == 2


def test_idempotency_considers_source_id(client):
    eid = make_exhibit(client)
    ingest(client, eid, [{"id": "r", "ts": iso(0), "lux": 100, "source_id": "A"}])
    # 同 ID 同 ts/lux/source → duplicate
    dup = ingest(client, eid, [{"id": "r", "ts": iso(0), "lux": 100, "source_id": "A"}])
    assert dup["results"][0]["status"] == "duplicate"
    assert dup["inserted"] == 0 and dup["duplicates"] == 1
    # 同 ID 但来源不同 → conflict（不覆盖）
    conf = ingest(client, eid, [{"id": "r", "ts": iso(0), "lux": 100, "source_id": "B"}])
    assert conf["results"][0]["status"] == "conflict"
    # 缺省 source_id（旧请求）与已有来源读数也判 conflict
    conf2 = ingest(client, eid, [{"id": "r", "ts": iso(0), "lux": 100}])
    assert conf2["results"][0]["status"] == "conflict"


def test_receipt_records_source_id_only_when_present_and_hash_recomputable(client):
    from app.receipts import content_hash_of

    eid = make_exhibit(client)
    out = ingest(client, eid, [
        {"id": "a", "ts": iso(0), "lux": 100, "source_id": "A"},
        {"id": "b", "ts": iso(60), "lux": 100},
    ])
    receipt = out["receipt"]
    assert receipt["content_hash"] == content_hash_of(receipt["content"])
    req_readings = receipt["content"]["request"]["readings"]
    assert req_readings[0]["source_id"] == "A"
    assert "source_id" not in req_readings[1]  # 未标注来源时收据中无该键


# ---------- legacy：无配置保持旧行为 ----------

def test_legacy_no_config_keeps_old_behaviour(client):
    eid = make_exhibit(client)
    ingest(client, eid, [
        {"id": "a", "ts": iso(0), "lux": 100, "source_id": "A"},
        {"id": "b", "ts": iso(0), "lux": 120, "source_id": "B"},
        {"id": "c", "ts": iso(60), "lux": 110, "source_id": "A"},
    ])
    tl = timeline(client, eid)["points"]
    assert len(tl) == 2
    t0 = next(p for p in tl if p["reading_ids"] == ["a", "b"] or set(p["reading_ids"]) == {"a", "b"})
    assert t0["status"] == "legacy" and t0["lux"] is None and t0["version"] is None
    # 诊断沿用 timestamp_conflict
    events = diagnose(client, eid, 0, 120)["events"]
    assert [e["type"] for e in events].count("timestamp_conflict") == 1
    conflict = next(e for e in events if e["type"] == "timestamp_conflict")
    assert set(conflict["reading_ids"]) == {"a", "b"}


def test_legacy_same_lux_same_ts_merges_like_before(client):
    eid = make_exhibit(client)
    ingest(client, eid, [
        {"id": "a", "ts": iso(0), "lux": 100, "source_id": "A"},
        {"id": "b", "ts": iso(0), "lux": 100, "source_id": "B"},
        {"id": "c", "ts": iso(60), "lux": 100, "source_id": "A"},
    ])
    tl = timeline(client, eid)["points"]
    t0 = next(p for p in tl if p["ts"] == iso(0))
    assert t0["status"] == "legacy" and t0["lux"] == 100.0
    # reading_count 旧口径：统计原始读数条数
    body = dose(client, eid, 0, 60)
    assert body["reading_count"] == 3


# ---------- 配置：版本与乐观并发 ----------

def test_put_requires_expected_version_and_returns_409_when_stale(client):
    eid = make_exhibit(client)
    status, body = put_priorities(client, eid, 0, iso(0),
                                  [{"source_id": "A", "priority": 10}])
    assert status == 200 and body["current_version"] == 1
    # 仍带 0 → 409
    status2, body2 = put_priorities(client, eid, 0, iso(3600),
                                    [{"source_id": "A", "priority": 1}])
    assert status2 == 409
    assert "expected_version" in body2["detail"] and "1" in body2["detail"]
    # 正确版本号成功
    status3, body3 = put_priorities(client, eid, 1, iso(3600),
                                    [{"source_id": "A", "priority": 1}])
    assert status3 == 200 and body3["current_version"] == 2


def test_put_validation_duplicate_effective_and_sources(client):
    eid = make_exhibit(client)
    put_priorities(client, eid, 0, iso(0), [{"source_id": "A", "priority": 10}])
    # 同一生效时刻不能重复建版本（版本只增不改）
    status, _ = put_priorities(client, eid, 1, iso(0), [{"source_id": "A", "priority": 1}])
    assert status == 422
    # 空清单 / 同版本 source_id 重复
    assert client.put(f"/exhibits/{eid}/source-priorities", json={
        "expected_version": 1, "effective_at": iso(60), "priorities": [],
    }).status_code == 422
    assert client.put(f"/exhibits/{eid}/source-priorities", json={
        "expected_version": 1, "effective_at": iso(60),
        "priorities": [{"source_id": "A", "priority": 1},
                       {"source_id": "A", "priority": 2}],
    }).status_code == 422


def test_get_config_unknown_exhibit_404_and_empty_initial(client):
    assert client.get("/exhibits/nope/source-priorities").status_code == 404
    eid = make_exhibit(client)
    body = client.get(f"/exhibits/{eid}/source-priorities").json()
    assert body["current_version"] == 0 and body["versions"] == []
    assert body["canonical_id"] == eid


# ---------- 仲裁：最高优先级、未列入最低档、同值合并 / 异值冲突 ----------

def test_highest_priority_wins_and_unlisted_source_is_lowest(client):
    eid = make_exhibit(client)
    ingest(client, eid, [
        {"id": "hi", "ts": iso(0), "lux": 100, "source_id": "A"},
        {"id": "mid", "ts": iso(0), "lux": 200, "source_id": "B"},
        {"id": "lo", "ts": iso(0), "lux": 999, "source_id": "UNKNOWN"},
        {"id": "null", "ts": iso(0), "lux": 998},
    ])
    put_priorities(client, eid, 0, iso(0),
                   [{"source_id": "A", "priority": 10}, {"source_id": "B", "priority": 5}])
    p = timeline(client, eid)["points"][0]
    assert p["status"] == "authoritative"
    assert p["lux"] == 100.0 and p["source_ids"] == ["A"] and p["reading_ids"] == ["hi"]
    assert p["priority"] == 10
    assert len(p["candidates"]) == 4  # 全部留存读数都在权威时间线中可核查


def test_top_priority_tie_same_lux_merges_into_one_point(client):
    eid = make_exhibit(client)
    ingest(client, eid, [
        {"id": "a", "ts": iso(0), "lux": 100, "source_id": "A"},
        {"id": "b", "ts": iso(0), "lux": 100, "source_id": "B"},
        {"id": "c", "ts": iso(0), "lux": 200, "source_id": "LOW"},
        {"id": "d", "ts": iso(60), "lux": 100, "source_id": "A"},
    ])
    put_priorities(client, eid, 0, iso(0),
                   [{"source_id": "A", "priority": 10}, {"source_id": "B", "priority": 10}])
    points = timeline(client, eid)["points"]
    p0 = next(p for p in points if p["ts"] == iso(0))
    assert p0["status"] == "authoritative" and p0["lux"] == 100.0
    assert p0["source_ids"] == ["A", "B"] and set(p0["reading_ids"]) == {"a", "b"}
    body = dose(client, eid, 0, 60)
    # 权威点每时刻计 1（并列读数合并）
    assert body["reading_count"] == 2
    assert body["known_dose_luxh"] == pytest.approx(100.0 * 60 / 3600.0)
    assert body["covered_seconds"] == 60.0


def test_top_priority_tie_different_lux_is_conflict_excluded_from_dose_and_diagnose(client):
    eid = make_exhibit(client)
    ingest(client, eid, [
        {"id": "a", "ts": iso(60), "lux": 100, "source_id": "A"},
        {"id": "b", "ts": iso(60), "lux": 120, "source_id": "B"},
        {"id": "c", "ts": iso(120), "lux": 100, "source_id": "A"},
    ])
    put_priorities(client, eid, 0, iso(0),
                   [{"source_id": "A", "priority": 10}, {"source_id": "B", "priority": 10}])
    points = timeline(client, eid)["points"]
    p1 = next(p for p in points if p["ts"] == iso(60))
    assert p1["status"] == "conflict" and p1["lux"] is None
    assert p1["lux_span"] == 20.0
    assert set(p1["source_ids"]) == {"A", "B"} and set(p1["reading_ids"]) == {"a", "b"}

    # 剂量：冲突时刻两侧不跨接积分 → [0,180] 无有效覆盖
    body = dose(client, eid, 0, 180)
    assert body["known_dose_luxh"] == 0.0
    assert body["covered_seconds"] == 0.0
    assert body["unknown_seconds"] == 180.0
    assert body["reading_count"] == 1  # 仅 120s 权威点

    # 诊断：仲裁冲突不产生 timestamp_conflict 事件，但截断连续链；
    # 60s→120s 间隔 60s 合规，因此也没有 gap（冲突时刻根本不在时间线上）
    events = diagnose(client, eid, 0, 180, stuck_tol=0)["events"]
    assert all(e["type"] != "timestamp_conflict" for e in events)
    assert all(e["type"] != "gap" for e in events)


def test_conflict_breaks_first_exceedance_history(client):
    # 全历史首次越线也不得跨过冲突时刻累计
    eid = make_exhibit(client, limit=10.0, interval=3600)
    ingest(client, eid, [
        {"id": "a", "ts": iso(0), "lux": 100000, "source_id": "A"},
        {"id": "b", "ts": iso(0), "lux": 90000, "source_id": "B"},  # 并列异值 → 冲突
        {"id": "c", "ts": iso(3600), "lux": 0, "source_id": "A"},
    ])
    put_priorities(client, eid, 0, iso(0),
                   [{"source_id": "A", "priority": 10}, {"source_id": "B", "priority": 10}])
    body = dose(client, eid, 0, 3600)
    assert body["known_dose_luxh"] == 0.0
    assert body["exceeded"] is False and body["first_exceeded_at"] is None
    assert body["remaining_luxh"] == 10.0


# ---------- 生效时刻：只从生效起影响重算，历史不改写 ----------

def test_priority_change_only_affects_from_effective_time(client):
    eid = make_exhibit(client)
    ingest(client, eid, [
        {"id": "a1", "ts": iso(0), "lux": 100, "source_id": "A"},
        {"id": "b1", "ts": iso(0), "lux": 200, "source_id": "B"},
        {"id": "a2", "ts": iso(120), "lux": 100, "source_id": "A"},
        {"id": "b2", "ts": iso(120), "lux": 200, "source_id": "B"},
    ])
    # v1: B 权威，生效 00:00
    put_priorities(client, eid, 0, iso(0),
                   [{"source_id": "B", "priority": 10}, {"source_id": "A", "priority": 5}])
    p = timeline(client, eid)["points"]
    assert all(x["lux"] == 200.0 and x["source_ids"] == ["B"] for x in p)

    # v2: A 权威，生效 01:00（早于 120s 采样、晚于 0s 采样）
    put_priorities(client, eid, 1, iso(60),
                   [{"source_id": "A", "priority": 10}, {"source_id": "B", "priority": 5}])
    p = timeline(client, eid)["points"]
    early = next(x for x in p if x["ts"] == iso(0))
    late = next(x for x in p if x["ts"] == iso(120))
    assert early["lux"] == 200.0 and early["version"] == 1   # 历史不被新优先级改写
    assert late["lux"] == 100.0 and late["version"] == 2


def test_before_first_version_samples_remain_legacy(client):
    eid = make_exhibit(client)
    ingest(client, eid, [
        {"id": "a", "ts": iso(0), "lux": 100, "source_id": "A"},
        {"id": "b", "ts": iso(0), "lux": 200, "source_id": "B"},
        {"id": "a2", "ts": iso(120), "lux": 100, "source_id": "A"},
    ])
    put_priorities(client, eid, 0, iso(60),
                   [{"source_id": "A", "priority": 10}, {"source_id": "B", "priority": 5}])
    p = timeline(client, eid)["points"]
    early = next(x for x in p if x["ts"] == iso(0))
    assert early["status"] == "legacy"  # 采样早于首个生效版本
    late = next(x for x in p if x["ts"] == iso(120))
    assert late["status"] == "authoritative" and late["lux"] == 100.0
    # legacy 异值仍出 timestamp_conflict；权威时刻不产生
    events = diagnose(client, eid, 0, 120)["events"]
    tc = [e for e in events if e["type"] == "timestamp_conflict"]
    assert len(tc) == 1 and set(tc[0]["reading_ids"]) == {"a", "b"}


def test_config_change_does_not_rewrite_raw_readings_or_receipts(client):
    eid = make_exhibit(client)
    out = ingest(client, eid, [
        {"id": "a", "ts": iso(0), "lux": 100, "source_id": "A"},
        {"id": "b", "ts": iso(0), "lux": 200, "source_id": "B"},
    ])
    first_hash = out["receipt"]["chain_hash"]
    put_priorities(client, eid, 0, iso(0),
                   [{"source_id": "A", "priority": 10}, {"source_id": "B", "priority": 5}])
    put_priorities(client, eid, 1, iso(60),
                   [{"source_id": "B", "priority": 10}, {"source_id": "A", "priority": 5}])
    verify = client.get(f"/exhibits/{eid}/receipts/verify").json()
    assert verify["ok"] is True
    got = client.get(f"/exhibits/{eid}/receipts/1").json()
    assert got["chain_hash"] == first_hash
    raw = client.get(f"/exhibits/{eid}/readings").json()["readings"]
    assert {r["id"]: r["lux"] for r in raw} == {"a": 100.0, "b": 200.0}


# ---------- 权威时间线查询 ----------

def test_timeline_window_filters_and_limit(client):
    eid = make_exhibit(client)
    ingest(client, eid, [
        {"id": f"r{i}", "ts": iso(i * 60), "lux": 100, "source_id": "A"}
        for i in range(5)
    ])
    body = timeline(client, eid, start=iso(60), end=iso(180))
    assert [p["ts"] for p in body["points"]] == [iso(60), iso(120), iso(180)]
    body = timeline(client, eid, limit=2)
    assert len(body["points"]) == 2
    assert client.get(f"/exhibits/{eid}/authoritative-timeline",
                      params={"start": iso(10), "end": iso(5)}).status_code == 422


def test_timeline_candidates_show_all_sources_for_conflict_audit(client):
    eid = make_exhibit(client)
    ingest(client, eid, [
        {"id": "a", "ts": iso(0), "lux": 100, "source_id": "A"},
        {"id": "b", "ts": iso(0), "lux": 120, "source_id": "B"},
        {"id": "z", "ts": iso(0), "lux": 500, "source_id": "UNLISTED"},
    ])
    put_priorities(client, eid, 0, iso(0),
                   [{"source_id": "A", "priority": 10}, {"source_id": "B", "priority": 10}])
    p = timeline(client, eid)["points"][0]
    candidate_ids = {c["id"] for c in p["candidates"]}
    assert candidate_ids == {"a", "b", "z"}  # 含未列入来源的读数


# ---------- 别名访问命中规范档案配置 + 归并 ----------

def test_alias_access_hits_canonical_priority_config(client):
    tgt = make_exhibit(client, eid="T")
    src = make_exhibit(client, eid="S")
    ingest(client, "T", [{"id": "t", "ts": iso(0), "lux": 10, "source_id": "X"}])
    ingest(client, "S", [{"id": "s", "ts": iso(0), "lux": 10, "source_id": "X"}])
    # 目标配置 X 高优先级；来源无配置（legacy）——无优先级分歧
    put_priorities(client, "T", 0, iso(0), [{"source_id": "X", "priority": 10}])
    pv = client.post("/merges/preview", json={"source_id": "S", "target_id": "T"}).json()
    assert pv["priority_collisions"] == []
    assert pv["ready_to_commit"] is True  # 读数 ID 不碰撞、配置单边自动并入 → 可直接提交
    resp = client.post("/merges/commit", json={"token": pv["token"], "resolutions": []})
    assert resp.status_code == 200, resp.text
    # 用来源别名访问优先级配置 → 命中目标档案（归并按并集重写配置行，
    # 版本号重新分配，但生效时刻与优先级内容不变）
    cfg = client.get("/exhibits/S/source-priorities").json()
    assert cfg["canonical_id"] == "T" and cfg["current_version"] >= 1
    assert cfg["versions"][0]["effective_at"] == iso(0)
    assert cfg["versions"][0]["priorities"] == [{"source_id": "X", "priority": 10}]
    tl = timeline(client, "S")["points"][0]
    assert tl["status"] == "authoritative" and tl["lux"] == 10.0  # 两条 X 同档同来源... 同时刻
    # 同 source X 同优先级、同值 → 合并
    assert set(tl["reading_ids"]) == {"t", "s"}


def test_merge_priority_conflicts_must_be_resolved_no_silent_override(client):
    tgt = make_exhibit(client, eid="T")
    src = make_exhibit(client, eid="S")
    ingest(client, "T", [{"id": "t1", "ts": iso(0), "lux": 10, "source_id": "X"}])
    ingest(client, "S", [{"id": "s1", "ts": iso(0), "lux": 20, "source_id": "Y"}])
    # 目标：X=10 从 00:00；来源：Y=10/X=1 从 00:00 → X 在该时刻分歧（10 vs 1）
    put_priorities(client, "T", 0, iso(0), [{"source_id": "X", "priority": 10}])
    put_priorities(client, "S", 0, iso(0),
                   [{"source_id": "Y", "priority": 10}, {"source_id": "X", "priority": 1}])
    pv = client.post("/merges/preview", json={"source_id": "S", "target_id": "T"}).json()
    assert pv["ready_to_commit"] is False
    x_collisions = [c for c in pv["priority_collisions"] if c["source_id"] == "X"]
    assert x_collisions and x_collisions[0]["source_priority"] == 1
    assert x_collisions[0]["target_priority"] == 10

    # 不裁决 → 422
    r = client.post("/merges/commit", json={"token": pv["token"], "resolutions": []})
    assert r.status_code == 422

    # 裁决不存在的分歧 → 422
    r = client.post("/merges/commit", json={
        "token": pv["token"], "resolutions": [],
        "priority_resolutions": [
            {"source_id": "NOPE", "effective_at": iso(0), "action": "keep_target"}],
    })
    assert r.status_code == 422

    # keep_target：归并后 X 仍为 10，X(10) 压过 Y（未列入该目标历史配置 →
    # 合并后 Y 也由来源配置带入 10）——该时刻 X、Y 同档 10、异值 → 冲突
    resolutions = [
        {"source_id": c["source_id"], "effective_at": c["effective_at"], "action": "keep_target"}
        for c in pv["priority_collisions"]
    ]
    r = client.post("/merges/commit", json={
        "token": pv["token"], "resolutions": [], "priority_resolutions": resolutions,
    })
    assert r.status_code == 200, r.text
    applied = {a["source_id"]: a["priority"] for a in r.json()["priority_resolutions_applied"]}
    assert applied["X"] == 10
    tl = timeline(client, "S")["points"][0]
    assert tl["status"] == "conflict"  # 没有静默选边，分歧被显式裁决后仍按仲裁规则冲突

    # 令牌重放
    replay = client.post("/merges/commit", json={
        "token": pv["token"], "resolutions": [], "priority_resolutions": resolutions,
    })
    assert replay.status_code == 200 and replay.json()["already_committed"] is True


def test_merge_keep_source_priority_takes_effect_and_chains_intact(client):
    tgt = make_exhibit(client, eid="T")
    src = make_exhibit(client, eid="S")
    ingest(client, "T", [{"id": "t1", "ts": iso(0), "lux": 10, "source_id": "X"}])
    ingest(client, "S", [{"id": "s1", "ts": iso(0), "lux": 20, "source_id": "Y"}])
    put_priorities(client, "T", 0, iso(0), [{"source_id": "X", "priority": 10}])
    put_priorities(client, "S", 0, iso(0), [{"source_id": "Y", "priority": 10}])
    pv = client.post("/merges/preview", json={"source_id": "S", "target_id": "T"}).json()
    # X 仅目标配置、Y 仅来源配置：单边条目自动并入并集，不需要裁决
    assert pv["priority_collisions"] == []
    assert pv["ready_to_commit"] is True
    r = client.post("/merges/commit", json={"token": pv["token"], "resolutions": []})
    assert r.status_code == 200
    # 合并配置为 {X:10, Y:10}：两个来源同档 10、照度 10 vs 20 → 仲裁冲突
    tl = timeline(client, "T")["points"][0]
    assert tl["status"] == "conflict"
    assert set(tl["source_ids"]) == {"X", "Y"}
    # 双方收据链保留
    for chain in ("S", "T"):
        v = client.get(f"/receipts/chains/{chain}/verify").json()
        assert v["ok"] is True


def test_merge_preview_token_invalidated_by_priority_config_change(client):
    tgt = make_exhibit(client, eid="T")
    src = make_exhibit(client, eid="S")
    ingest(client, "T", [{"id": "t1", "ts": iso(0), "lux": 10, "source_id": "X"}])
    put_priorities(client, "T", 0, iso(0), [{"source_id": "X", "priority": 10}])
    pv = client.post("/merges/preview", json={"source_id": "S", "target_id": "T"}).json()
    # 预演后修改目标优先级配置
    put_priorities(client, "T", 1, iso(60), [{"source_id": "X", "priority": 1}])
    r = client.post("/merges/commit", json={"token": pv["token"], "resolutions": []})
    assert r.status_code == 409


# ---------- 作废 / 日历叠加 ----------

def test_voided_reading_exits_arbitration(client):
    eid = make_exhibit(client)
    # A 组读数单独成批，便于只作废 A 组、保留 B
    out = ingest(client, eid, [
        {"id": "a", "ts": iso(0), "lux": 100, "source_id": "A"},
        {"id": "c", "ts": iso(60), "lux": 100, "source_id": "A"},
    ])
    ingest(client, eid, [{"id": "b", "ts": iso(0), "lux": 200, "source_id": "B"}])
    put_priorities(client, eid, 0, iso(0),
                   [{"source_id": "A", "priority": 10}, {"source_id": "B", "priority": 5}])
    receipt_id = out["receipt"]["receipt_id"]
    r = client.post(f"/exhibits/{eid}/receipts/corrections", json={
        "receipt_id": receipt_id, "request_key": "fix-a", "reason": "A 传感器故障",
    })
    assert r.status_code == 200 and set(v["id"] for v in r.json()["voided_readings"]) == {"a", "c"}
    p = timeline(client, eid)["points"]
    t0 = next(x for x in p if x["ts"] == iso(0))
    # a 已作废，只剩 B → B 权威 200
    assert t0["status"] == "authoritative" and t0["lux"] == 200.0 and t0["reading_ids"] == ["b"]
    # c 作废后 60s 时刻无任何读数
    assert all(x["ts"] != iso(60) for x in p)


def test_arbitration_conflict_with_calendar_display(client):
    # 全日历（00:00–01:00 本地全天开放，UTC）下冲突时刻仍不积分
    eid = make_exhibit(client)
    client.put(f"/exhibits/{eid}/calendar", json={
        "timezone": "UTC",
        "weekly": [{"weekday": weekday(0), "start": "00:00", "end": "23:59:59"}],
        "exceptions": [],
    })
    ingest(client, eid, [
        {"id": "a", "ts": iso(0), "lux": 100, "source_id": "A"},
        {"id": "b", "ts": iso(0), "lux": 120, "source_id": "B"},
        {"id": "c", "ts": iso(60), "lux": 100, "source_id": "A"},
    ])
    put_priorities(client, eid, 0, iso(0),
                   [{"source_id": "A", "priority": 10}, {"source_id": "B", "priority": 10}])
    body = dose(client, eid, 0, 120, calendar="display")
    assert body["known_dose_luxh"] == 0.0
    assert body["covered_seconds"] == 0.0
    assert body["closed_seconds"] == 0.0  # 全开放
    assert body["unknown_seconds"] == 120.0
