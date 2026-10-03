"""多来源读数仲裁：source_id 上报留存、来源优先级版本（expected_version /
生效时刻）、权威时间线、并列冲突不进入剂量与诊断、归并合并来源配置。"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app.receipts import content_hash_of

BASE = 1_700_000_000  # 2023-11-14T22:13:20Z


def iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    from app.main import app

    with TestClient(app) as c:
        yield c


def make_exhibit(client: TestClient, limit: float = 1000.0, interval: float = 3600.0,
                 eid: str | None = None, name: str = "唐代绢画") -> str:
    body = {"name": name, "dose_limit_luxh": limit, "max_interval_seconds": interval}
    if eid is not None:
        body["id"] = eid
    resp = client.post("/exhibits", json=body)
    assert resp.status_code == 201, resp.text
    return resp.json()["canonical_id"]


def send(client: TestClient, exhibit_id: str, readings: list[dict]) -> dict:
    resp = client.post(f"/exhibits/{exhibit_id}/readings", json={"readings": readings})
    assert resp.status_code == 200, resp.text
    return resp.json()


def put_priority(client: TestClient, exhibit_id: str, priorities: list[str],
                 effective_at: float, expected_version: int) -> dict:
    resp = client.put(f"/exhibits/{exhibit_id}/source-priority", json={
        "effective_at": iso(effective_at),
        "priorities": priorities,
        "expected_version": expected_version,
    })
    assert resp.status_code == 201, resp.text
    return resp.json()


def dose(client: TestClient, exhibit_id: str, start: float, end: float,
         extra: dict | None = None) -> dict:
    params = {"start": iso(start), "end": iso(end)}
    if extra:
        params.update(extra)
    resp = client.get(f"/exhibits/{exhibit_id}/dose", params=params)
    assert resp.status_code == 200, resp.text
    return resp.json()


def timeline(client: TestClient, exhibit_id: str, start: float, end: float) -> dict:
    resp = client.get(f"/exhibits/{exhibit_id}/timeline",
                      params={"start": iso(start), "end": iso(end)})
    assert resp.status_code == 200, resp.text
    return resp.json()


def diagnose(client: TestClient, exhibit_id: str, start: float, end: float,
             max_rate: float = 1000.0, stuck_tol: float = 0.0,
             min_stuck: float = 10_000_000.0) -> dict:
    resp = client.post(f"/exhibits/{exhibit_id}/readings/diagnose", json={
        "start": iso(start), "end": iso(end),
        "max_rate_lux_per_second": max_rate,
        "stuck_tolerance_lux": stuck_tol,
        "min_stuck_seconds": min_stuck,
    })
    assert resp.status_code == 200, resp.text
    return resp.json()


def merge_commit(client: TestClient, source_id: str, target_id: str,
                 resolutions: list[dict] | None = None) -> dict:
    preview = client.post("/merges/preview",
                          json={"source_id": source_id, "target_id": target_id})
    assert preview.status_code == 200, preview.text
    resp = client.post("/merges/commit", json={
        "token": preview.json()["token"], "resolutions": resolutions or [],
    })
    assert resp.status_code == 200, resp.text
    return resp.json()


# ---------- 上报：source_id 留存与旧行为兼容 ----------

def test_ingest_sources_kept_and_listed(client):
    eid = make_exhibit(client)
    out = send(client, eid, [
        {"id": "r1", "ts": iso(BASE), "lux": 100, "source_id": "sensor-a"},
        {"id": "r2", "ts": iso(BASE), "lux": 500, "source_id": "sensor-b"},
        {"id": "r3", "ts": iso(BASE + 3600), "lux": 100, "source_id": "sensor-a"},
    ])
    assert out["inserted"] == 3
    # 同一时刻不同来源全部留存
    resp = client.get(f"/exhibits/{eid}/readings")
    body = resp.json()["readings"]
    assert [(r["id"], r["source_id"]) for r in body] == [
        ("r1", "sensor-a"), ("r2", "sensor-b"), ("r3", "sensor-a"),
    ]


def test_legacy_ingest_without_source_unchanged(client):
    eid = make_exhibit(client)
    out = send(client, eid, [{"id": "r1", "ts": iso(BASE), "lux": 100}])
    # 旧式请求的收据内容不含 source_id 键，内容哈希可重算复现
    content = out["receipt"]["content"]
    assert "source_id" not in content["request"]["readings"][0]
    assert out["receipt"]["content_hash"] == content_hash_of(content)
    # 幂等语义不变：同 ID 同数据 duplicate、不同数据 conflict
    out2 = send(client, eid, [
        {"id": "r1", "ts": iso(BASE), "lux": 100},
        {"id": "r1", "ts": iso(BASE), "lux": 101},
    ])
    assert [r["status"] for r in out2["results"]] == ["duplicate", "conflict"]
    # 读数列表带 source_id 字段（未声明来源为 null）
    rows = client.get(f"/exhibits/{eid}/readings").json()["readings"]
    assert rows[0]["source_id"] is None


def test_same_id_different_source_is_conflict(client):
    eid = make_exhibit(client)
    send(client, eid, [{"id": "r1", "ts": iso(BASE), "lux": 100, "source_id": "a"}])
    out = send(client, eid, [
        {"id": "r1", "ts": iso(BASE), "lux": 100, "source_id": "b"},   # 来源不同
        {"id": "r1", "ts": iso(BASE), "lux": 100, "source_id": "a"},   # 完全一致
        {"id": "r1", "ts": iso(BASE), "lux": 100},                     # 缺来源
    ])
    assert [r["status"] for r in out["results"]] == ["conflict", "duplicate", "conflict"]


def test_receipt_records_source_id_and_hash_recomputable(client):
    eid = make_exhibit(client)
    out = send(client, eid, [
        {"id": "r1", "ts": iso(BASE), "lux": 100, "source_id": "sensor-a"},
    ])
    content = out["receipt"]["content"]
    assert content["request"]["readings"][0]["source_id"] == "sensor-a"
    assert out["receipt"]["content_hash"] == content_hash_of(content)
    verify = client.get(f"/exhibits/{eid}/receipts/verify").json()
    assert verify["ok"] is True


# ---------- 来源优先级版本：发布、expected_version、校验 ----------

def test_put_and_get_priority_versions(client):
    eid = make_exhibit(client)
    v1 = put_priority(client, eid, ["sensor-a", "sensor-b"], BASE, 0)
    assert v1["version"] == 1
    assert v1["priorities"] == ["sensor-a", "sensor-b"]
    assert v1["canonical_id"] == eid

    got = client.get(f"/exhibits/{eid}/source-priority")
    assert got.status_code == 200
    body = got.json()
    assert body["current_version"] == 1
    assert [v["version"] for v in body["versions"]] == [1]

    # 过期 expected_version → 409
    stale = client.put(f"/exhibits/{eid}/source-priority", json={
        "effective_at": iso(BASE + 100), "priorities": ["sensor-b"],
        "expected_version": 0,
    })
    assert stale.status_code == 409

    v2 = put_priority(client, eid, ["sensor-b", "sensor-a"], BASE + 3600, 1)
    assert v2["version"] == 2
    body = client.get(f"/exhibits/{eid}/source-priority").json()
    assert body["current_version"] == 2
    assert [v["version"] for v in body["versions"]] == [1, 2]


def test_get_priority_empty_when_unconfigured(client):
    eid = make_exhibit(client)
    body = client.get(f"/exhibits/{eid}/source-priority").json()
    assert body["current_version"] == 0
    assert body["versions"] == []


def test_put_priority_validation_and_404(client):
    eid = make_exhibit(client)
    # 优先级列表来源重复 → 422
    resp = client.put(f"/exhibits/{eid}/source-priority", json={
        "effective_at": iso(BASE), "priorities": ["a", "a"], "expected_version": 0,
    })
    assert resp.status_code == 422
    # 缺 expected_version → 422
    resp = client.put(f"/exhibits/{eid}/source-priority", json={
        "effective_at": iso(BASE), "priorities": ["a"],
    })
    assert resp.status_code == 422
    # 展品不存在 → 404
    assert client.put("/exhibits/nope/source-priority", json={
        "effective_at": iso(BASE), "priorities": ["a"], "expected_version": 0,
    }).status_code == 404
    assert client.get("/exhibits/nope/source-priority").status_code == 404
    assert client.get("/exhibits/nope/timeline",
                      params={"start": iso(BASE), "end": iso(BASE + 1)}).status_code == 404


def test_concurrent_put_priority_only_one_wins(client):
    eid = make_exhibit(client)

    def put():
        return client.put(f"/exhibits/{eid}/source-priority", json={
            "effective_at": iso(BASE), "priorities": ["a"], "expected_version": 0,
        }).status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        codes = list(pool.map(lambda _: put(), range(2)))
    assert sorted(codes) == [201, 409]
    assert client.get(f"/exhibits/{eid}/source-priority").json()["current_version"] == 1


# ---------- 仲裁：权威值、并列冲突、生效时刻 ----------

def test_dose_arbitrated_by_highest_priority(client):
    eid = make_exhibit(client, interval=3600.0)
    send(client, eid, [
        {"id": "r1", "ts": iso(BASE), "lux": 100, "source_id": "a"},
        {"id": "r2", "ts": iso(BASE), "lux": 500, "source_id": "b"},
        {"id": "r3", "ts": iso(BASE + 3600), "lux": 100, "source_id": "a"},
    ])
    put_priority(client, eid, ["a", "b"], BASE - 10, 0)
    got = dose(client, eid, BASE, BASE + 3600)
    # 权威值取高优先级来源 a：100 lux 恒定 1 小时 = 100 lux·h
    assert got["known_dose_luxh"] == 100.0
    assert got["covered_seconds"] == 3600.0
    assert got["reading_count"] == 2  # 权威点数量，而非原始读数条数

    tl = timeline(client, eid, BASE, BASE + 3600)
    assert tl["arbitrated"] is True
    assert [(e["status"], e.get("lux")) for e in tl["entries"]] == [
        ("ok", 100.0), ("ok", 100.0),
    ]
    first = tl["entries"][0]
    assert first["reading_ids"] == ["r1"]          # 只有胜出集合进入时间线
    assert first["source_ids"] == ["a"]
    assert first["priority_version"] == 1


def test_tied_unlisted_sources_conflict_excluded(client):
    """未列入优先级的来源并列最低：照度不同时该时刻保持冲突，不进入剂量/诊断。"""
    eid = make_exhibit(client, interval=1800.0)
    send(client, eid, [
        {"id": "r1", "ts": iso(BASE), "lux": 100, "source_id": "a"},
        {"id": "r2", "ts": iso(BASE + 1800), "lux": 100, "source_id": "c"},
        {"id": "r3", "ts": iso(BASE + 1800), "lux": 200, "source_id": "d"},
        {"id": "r4", "ts": iso(BASE + 3600), "lux": 100, "source_id": "a"},
    ])
    put_priority(client, eid, ["a"], BASE - 10, 0)

    got = dose(client, eid, BASE, BASE + 3600)
    # 冲突时刻被移除后，BASE 与 BASE+3600 间隔 3600 > 1800 → 整段缺测
    assert got["covered_seconds"] == 0.0
    assert got["unknown_seconds"] == 3600.0
    assert got["known_dose_luxh"] == 0.0

    events = diagnose(client, eid, BASE, BASE + 3600)["events"]
    conflict = [e for e in events if e["type"] == "timestamp_conflict"]
    assert len(conflict) == 1
    assert conflict[0]["reading_ids"] == ["r2", "r3"]
    assert conflict[0]["lux_span"] == 100.0
    # 冲突时刻从两侧中断连续链（与既有诊断语义一致），不形成跨冲突的 gap 事件；
    # 剂量口径的缺测由上面的 unknown_seconds 体现
    assert [e for e in events if e["type"] == "gap"] == []

    tl = timeline(client, eid, BASE, BASE + 3600)
    statuses = [e["status"] for e in tl["entries"]]
    assert statuses == ["ok", "conflict", "ok"]
    mid = tl["entries"][1]
    assert mid["lux_span"] == 100.0
    assert "lux" not in mid  # 冲突时刻无权威值


def test_tied_same_lux_merged(client):
    """并列最高但照度相同：合并为一个权威点（读数 ID 全部保留）。"""
    eid = make_exhibit(client, interval=3600.0)
    send(client, eid, [
        {"id": "r1", "ts": iso(BASE), "lux": 100, "source_id": "c"},
        {"id": "r2", "ts": iso(BASE), "lux": 100, "source_id": "d"},
        {"id": "r3", "ts": iso(BASE + 3600), "lux": 100, "source_id": "a"},
    ])
    put_priority(client, eid, ["a"], BASE - 10, 0)
    got = dose(client, eid, BASE, BASE + 3600)
    assert got["known_dose_luxh"] == 100.0
    assert got["covered_seconds"] == 3600.0
    entry = timeline(client, eid, BASE, BASE + 3600)["entries"][0]
    assert entry["status"] == "ok"
    assert entry["reading_ids"] == ["r1", "r2"]
    assert entry["source_ids"] == ["c", "d"]


def test_same_source_same_ts_different_lux_conflicts(client):
    """同一来源在同一时刻上报不同照度（不同读数 ID）：同样保持冲突。"""
    eid = make_exhibit(client, interval=3600.0)
    send(client, eid, [
        {"id": "r1", "ts": iso(BASE), "lux": 100, "source_id": "a"},
        {"id": "r2", "ts": iso(BASE), "lux": 200, "source_id": "a"},
    ])
    put_priority(client, eid, ["a"], BASE - 10, 0)
    entry = timeline(client, eid, BASE, BASE + 1)["entries"][0]
    assert entry["status"] == "conflict"
    assert entry["reading_ids"] == ["r1", "r2"]


def test_unlisted_and_null_source_are_lowest(client):
    """未列入优先级的来源与未声明来源的旧式读数并列最低。"""
    eid = make_exhibit(client, interval=3600.0)
    send(client, eid, [
        {"id": "r1", "ts": iso(BASE), "lux": 100, "source_id": "a"},
        {"id": "r2", "ts": iso(BASE), "lux": 500, "source_id": "unlisted"},
        {"id": "r3", "ts": iso(BASE), "lux": 900},  # 未声明来源
        {"id": "r4", "ts": iso(BASE + 3600), "lux": 100, "source_id": "a"},
    ])
    put_priority(client, eid, ["a"], BASE - 10, 0)
    assert dose(client, eid, BASE, BASE + 3600)["known_dose_luxh"] == 100.0
    entry = timeline(client, eid, BASE, BASE + 1)["entries"][0]
    assert entry["reading_ids"] == ["r1"]


def test_effective_at_selects_version_per_instant(client):
    """每个采样时刻按 effective_at <= ts 的最新版本仲裁；生效前平权。"""
    eid = make_exhibit(client, interval=7200.0)
    put_priority(client, eid, ["a", "b"], BASE, 0)
    put_priority(client, eid, ["b", "a"], BASE + 3600, 1)
    send(client, eid, [
        # 首个版本生效之前：平权，同值合并
        {"id": "r1", "ts": iso(BASE - 100), "lux": 50, "source_id": "a"},
        {"id": "r2", "ts": iso(BASE - 100), "lux": 50, "source_id": "b"},
        # v1 生效：a 优先
        {"id": "r3", "ts": iso(BASE + 1800), "lux": 100, "source_id": "a"},
        {"id": "r4", "ts": iso(BASE + 1800), "lux": 200, "source_id": "b"},
        # v2 生效：b 优先
        {"id": "r5", "ts": iso(BASE + 7200), "lux": 100, "source_id": "a"},
        {"id": "r6", "ts": iso(BASE + 7200), "lux": 200, "source_id": "b"},
    ])
    entries = timeline(client, eid, BASE - 100, BASE + 7200)["entries"]
    assert [(e["status"], e.get("lux"), e["priority_version"]) for e in entries] == [
        ("ok", 50.0, None),
        ("ok", 100.0, 1),
        ("ok", 200.0, 2),
    ]


def test_priority_adjustment_recomputes_from_effective_at_only(client):
    """调整优先级只从生效时刻起影响重算；历史收据与原始读数不改写。"""
    eid = make_exhibit(client, interval=3600.0)
    send(client, eid, [
        {"id": "r1", "ts": iso(BASE), "lux": 100, "source_id": "a"},
        {"id": "r2", "ts": iso(BASE), "lux": 500, "source_id": "b"},
        {"id": "r3", "ts": iso(BASE + 3600), "lux": 100, "source_id": "a"},
        {"id": "r4", "ts": iso(BASE + 3600), "lux": 500, "source_id": "b"},
        {"id": "r5", "ts": iso(BASE + 7200), "lux": 100, "source_id": "a"},
        {"id": "r6", "ts": iso(BASE + 7200), "lux": 500, "source_id": "b"},
    ])
    put_priority(client, eid, ["a", "b"], BASE - 10, 0)
    before = dose(client, eid, BASE, BASE + 7200)
    assert before["known_dose_luxh"] == 200.0  # a 全程优先：100 lux × 2h

    # 自 BASE+3600 起 b 优先：只影响生效时刻之后的重算
    put_priority(client, eid, ["b", "a"], BASE + 3600, 1)
    after = dose(client, eid, BASE, BASE + 7200)
    # (100+500)/2×1h + 500×1h = 800
    assert after["known_dose_luxh"] == 800.0

    # 历史收据链不改写，整链校验仍通过
    verify = client.get(f"/exhibits/{eid}/receipts/verify").json()
    assert verify["ok"] is True
    # 原始读数不改写：b 的低优先级读数原样保留
    rows = {r["id"]: r for r in client.get(f"/exhibits/{eid}/readings").json()["readings"]}
    assert rows["r2"]["lux"] == 500.0 and rows["r2"]["source_id"] == "b"


def test_empty_priorities_means_flat(client):
    """空优先级列表：全部来源平权（同值合并、异值冲突）。"""
    eid = make_exhibit(client, interval=3600.0)
    send(client, eid, [
        {"id": "r1", "ts": iso(BASE), "lux": 100, "source_id": "a"},
        {"id": "r2", "ts": iso(BASE), "lux": 200, "source_id": "b"},
    ])
    put_priority(client, eid, [], BASE - 10, 0)
    tl = timeline(client, eid, BASE, BASE + 1)
    assert tl["arbitrated"] is True
    assert tl["entries"][0]["status"] == "conflict"


# ---------- 权威时间线查询 ----------

def test_timeline_flat_without_config(client):
    """未配置任何版本：平权口径，与旧行为一致（同值合并、异值冲突）。"""
    eid = make_exhibit(client)
    send(client, eid, [
        {"id": "r1", "ts": iso(BASE), "lux": 100},
        {"id": "r2", "ts": iso(BASE), "lux": 200},
        {"id": "r3", "ts": iso(BASE + 3600), "lux": 150},
    ])
    tl = timeline(client, eid, BASE, BASE + 3600)
    assert tl["arbitrated"] is False
    assert [e["status"] for e in tl["entries"]] == ["conflict", "ok"]
    assert tl["entries"][0]["priority_version"] is None
    assert tl["entries"][0]["lux_span"] == 100.0
    assert tl["entries"][1]["lux"] == 150.0


def test_timeline_window_and_validation(client):
    eid = make_exhibit(client)
    send(client, eid, [
        {"id": "r1", "ts": iso(BASE), "lux": 100},
        {"id": "r2", "ts": iso(BASE + 3600), "lux": 100},
    ])
    tl = timeline(client, eid, BASE, BASE + 1)
    assert [e["ts"] for e in tl["entries"]] == [iso(BASE).replace("+00:00", "Z")]
    # end <= start → 422
    resp = client.get(f"/exhibits/{eid}/timeline",
                      params={"start": iso(BASE + 1), "end": iso(BASE)})
    assert resp.status_code == 422


def test_dose_after_point_skips_conflict(client):
    """窗口终点之后最近权威点：保持冲突的时刻被跳过。"""
    eid = make_exhibit(client, interval=3600.0)
    send(client, eid, [
        {"id": "r1", "ts": iso(BASE), "lux": 100, "source_id": "a"},
        {"id": "r2", "ts": iso(BASE + 2700), "lux": 100, "source_id": "c"},
        {"id": "r3", "ts": iso(BASE + 2700), "lux": 200, "source_id": "d"},
        {"id": "r4", "ts": iso(BASE + 3600), "lux": 100, "source_id": "a"},
    ])
    put_priority(client, eid, ["a"], BASE - 10, 0)
    got = dose(client, eid, BASE, BASE + 1900)
    # 冲突时刻（BASE+2700）不进入时间线；窗口末端向 BASE+3600 的权威点裁剪
    assert got["covered_seconds"] == 1900.0
    assert got["unknown_seconds"] == 0.0


# ---------- 诊断基于权威时间线 ----------

def test_diagnose_uses_authoritative_timeline(client):
    """低优先级来源的尖峰被仲裁丢弃，不产生 jump；未配置时则是时间冲突。"""
    eid = make_exhibit(client, interval=3600.0)
    batch = []
    for i in range(3):
        batch.append({"id": f"a{i}", "ts": iso(BASE + 1800 * i), "lux": 100,
                      "source_id": "a"})
        batch.append({"id": f"b{i}", "ts": iso(BASE + 1800 * i), "lux": 999,
                      "source_id": "b"})
    send(client, eid, batch)

    # 未配置优先级：同时刻异值 → 每个时刻都是 timestamp_conflict
    events = diagnose(client, eid, BASE, BASE + 7200)["events"]
    assert len([e for e in events if e["type"] == "timestamp_conflict"]) == 3

    # 配置 a 优先：b 的尖峰被丢弃，时间线平稳，无任何事件
    put_priority(client, eid, ["a", "b"], BASE - 10, 0)
    events = diagnose(client, eid, BASE, BASE + 7200)["events"]
    assert events == []


# ---------- 日历 + 仲裁 ----------

def test_calendar_dose_with_arbitration(client):
    eid = make_exhibit(client, interval=3600.0)
    send(client, eid, [
        {"id": "r1", "ts": iso(BASE), "lux": 100, "source_id": "a"},
        {"id": "r2", "ts": iso(BASE), "lux": 500, "source_id": "b"},
        {"id": "r3", "ts": iso(BASE + 3600), "lux": 100, "source_id": "a"},
    ])
    put_priority(client, eid, ["a", "b"], BASE - 10, 0)
    # 每周 7 天全天开放
    resp = client.put(f"/exhibits/{eid}/calendar", json={
        "timezone": "Asia/Shanghai",
        "weekly": [{"weekday": d, "start": "00:00", "end": "00:00"} for d in range(1, 8)],
    })
    assert resp.status_code == 200, resp.text
    got = dose(client, eid, BASE, BASE + 3600, extra={"calendar": "display"})
    assert got["known_dose_luxh"] == 100.0
    assert got["closed_seconds"] == 0.0


# ---------- 批次更正与仲裁 ----------

def test_correction_voids_sourced_readings_and_recomputes(client):
    """作废高优先级来源的批次后，剩余时间线按低优先级来源重算。"""
    eid = make_exhibit(client, interval=3600.0)
    out1 = send(client, eid, [
        {"id": "r1", "ts": iso(BASE), "lux": 100, "source_id": "a"},
        {"id": "r3", "ts": iso(BASE + 3600), "lux": 100, "source_id": "a"},
    ])
    send(client, eid, [
        {"id": "r2", "ts": iso(BASE), "lux": 500, "source_id": "b"},
        {"id": "r4", "ts": iso(BASE + 3600), "lux": 500, "source_id": "b"},
    ])
    put_priority(client, eid, ["a", "b"], BASE - 10, 0)
    assert dose(client, eid, BASE, BASE + 3600)["known_dose_luxh"] == 100.0

    resp = client.post(f"/exhibits/{eid}/receipts/corrections", json={
        "receipt_id": out1["receipt"]["receipt_id"],
        "request_key": "void-a-batch",
        "reason": "a 传感器漂移，批次作废",
    })
    assert resp.status_code == 200, resp.text
    assert resp.json()["voided_count"] == 2
    # a 的读数退出时间线后，b 成为权威来源
    assert dose(client, eid, BASE, BASE + 3600)["known_dose_luxh"] == 500.0
    tl = timeline(client, eid, BASE, BASE + 3600)
    assert [e["source_ids"] for e in tl["entries"]] == [["b"], ["b"]]


# ---------- 别名访问与归并合并配置 ----------

def test_alias_access_hits_canonical_config(client):
    make_exhibit(client, eid="EX-S", name="重复档案")
    make_exhibit(client, eid="EX-T", name="规范档案")
    put_priority(client, "EX-T", ["a", "b"], BASE, 0)
    merge_commit(client, "EX-S", "EX-T")

    # 别名读配置：命中规范档案
    got = client.get("/exhibits/EX-S/source-priority")
    assert got.status_code == 200
    body = got.json()
    assert body["requested_id"] == "EX-S"
    assert body["canonical_id"] == "EX-T"
    assert body["current_version"] == 1

    # 别名发版本：写入规范档案
    v2 = put_priority(client, "EX-S", ["b", "a"], BASE + 3600, 1)
    assert v2["canonical_id"] == "EX-T" and v2["version"] == 2

    # 别名查时间线：命中规范档案
    send(client, "EX-S", [{"id": "r1", "ts": iso(BASE + 7200), "lux": 42,
                           "source_id": "b"}])
    tl = timeline(client, "EX-S", BASE + 7200, BASE + 7201)
    assert tl["canonical_id"] == "EX-T"
    assert tl["entries"][0]["lux"] == 42.0
    assert tl["entries"][0]["priority_version"] == 2


def test_merge_moves_source_config_when_target_has_none(client):
    make_exhibit(client, eid="EX-S")
    make_exhibit(client, eid="EX-T")
    put_priority(client, "EX-S", ["a", "b"], BASE, 0)

    preview = client.post("/merges/preview",
                          json={"source_id": "EX-S", "target_id": "EX-T"})
    assert preview.status_code == 200
    body = preview.json()
    assert body["source_priority_conflict"] is False
    assert body["ready_to_commit"] is True
    assert "source_priority" in body["metadata"]  # 差异明示：来源有、目标无

    commit = client.post("/merges/commit",
                         json={"token": body["token"], "resolutions": []})
    assert commit.status_code == 200, commit.text
    got = client.get("/exhibits/EX-T/source-priority").json()
    assert got["current_version"] == 1
    assert got["versions"][0]["priorities"] == ["a", "b"]


def test_merge_identical_configs_commit_ok(client):
    make_exhibit(client, eid="EX-S")
    make_exhibit(client, eid="EX-T")
    put_priority(client, "EX-S", ["a", "b"], BASE, 0)
    put_priority(client, "EX-T", ["a", "b"], BASE, 0)

    preview = client.post("/merges/preview",
                          json={"source_id": "EX-S", "target_id": "EX-T"})
    body = preview.json()
    assert body["source_priority_conflict"] is False
    assert "source_priority" not in body["metadata"]  # 配置一致，无差异
    commit = client.post("/merges/commit",
                         json={"token": body["token"], "resolutions": []})
    assert commit.status_code == 200, commit.text
    # 目标保留一份配置
    got = client.get("/exhibits/EX-T/source-priority").json()
    assert got["current_version"] == 1


def test_merge_conflicting_configs_rejected(client):
    """双方都有配置且不一致：冲突不得静默覆盖，归并 409。"""
    make_exhibit(client, eid="EX-S")
    make_exhibit(client, eid="EX-T")
    put_priority(client, "EX-S", ["a", "b"], BASE, 0)
    put_priority(client, "EX-T", ["b", "a"], BASE, 0)

    preview = client.post("/merges/preview",
                          json={"source_id": "EX-S", "target_id": "EX-T"})
    body = preview.json()
    assert body["source_priority_conflict"] is True
    assert body["ready_to_commit"] is False
    diff = body["metadata"]["source_priority"]
    assert diff["source"][0]["priorities"] == ["a", "b"]
    assert diff["target"][0]["priorities"] == ["b", "a"]

    commit = client.post("/merges/commit",
                         json={"token": body["token"], "resolutions": []})
    assert commit.status_code == 409
    # 双方配置原样保留，未被覆盖
    assert client.get("/exhibits/EX-S/source-priority").json()["current_version"] == 1
    assert (client.get("/exhibits/EX-T/source-priority")
            .json()["versions"][0]["priorities"] == ["b", "a"])


def test_merge_preview_token_invalidated_by_config_change(client):
    """预演后任一侧发布新优先级版本：版本令牌失效，提交 409。"""
    make_exhibit(client, eid="EX-S")
    make_exhibit(client, eid="EX-T")
    preview = client.post("/merges/preview",
                          json={"source_id": "EX-S", "target_id": "EX-T"})
    token = preview.json()["token"]
    put_priority(client, "EX-T", ["a"], BASE, 0)
    commit = client.post("/merges/commit", json={"token": token, "resolutions": []})
    assert commit.status_code == 409


def test_merge_preserves_reading_source_id(client):
    """归并搬迁读数时保留来源标识，归并后仲裁继续生效。"""
    make_exhibit(client, eid="EX-S")
    make_exhibit(client, eid="EX-T")
    send(client, "EX-S", [
        {"id": "r1", "ts": iso(BASE), "lux": 100, "source_id": "a"},
        {"id": "r2", "ts": iso(BASE), "lux": 500, "source_id": "b"},
        {"id": "r3", "ts": iso(BASE + 3600), "lux": 100, "source_id": "a"},
    ])
    put_priority(client, "EX-S", ["a", "b"], BASE - 10, 0)
    merge_commit(client, "EX-S", "EX-T")

    rows = {r["id"]: r for r in client.get("/exhibits/EX-T/readings").json()["readings"]}
    assert rows["r2"]["source_id"] == "b"
    # 归并后配置已搬迁：剂量按权威时间线核算
    assert dose(client, "EX-T", BASE, BASE + 3600)["known_dose_luxh"] == 100.0
