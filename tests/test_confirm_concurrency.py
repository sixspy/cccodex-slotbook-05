"""收银确认并发互斥失败测：双正式并存 / 两边同时确认争用。

口径（docs/收银确认说明.md，甲路径：持档锁 + 同事务双写）：
- 并发确认必须互斥落库：一成一争用，后到只留争用痕，不写业务。
- 修完一占位只进一正式：正式行/占位态/占用数/互证/审计五处同态。
夹具跑本机文件库（tmp_path 文件库，非内存库）。
"""
from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from app import app  # noqa: E402
import confirm_svc  # noqa: E402
import db as dbmod  # noqa: E402
from db import init_db  # noqa: E402

SLOT = "S-AM-01"
LOCK_KEY = f"slot:{SLOT}"
PARTY = "P-RACE"
SEATS = 2


@pytest.fixture(autouse=True)
def _fresh_db(tmp_path, monkeypatch):
    db = tmp_path / "t.db"  # 本机文件库
    monkeypatch.setattr(dbmod, "DB_PATH", db)
    if db.exists():
        db.unlink()
    init_db()
    yield


@pytest.fixture()
def _wide_race_window(monkeypatch):
    """拉宽确认写口临界区，让两个并发确认稳定同时处于确认窗口内。"""
    real_audit = dbmod.write_audit

    def sleepy_audit(*a, **kw):
        time.sleep(0.25)
        return real_audit(*a, **kw)

    monkeypatch.setattr(confirm_svc, "write_audit", sleepy_audit)


def _login(c: TestClient, user: str):
    r = c.post("/api/login", json={"username": user, "password": "123456"})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


def _make_hold(c: TestClient, headers) -> int:
    r = c.post(
        "/api/holds",
        headers=headers,
        json={"slot_code": SLOT, "party_key": PARTY, "seats": SEATS},
    )
    assert r.status_code == 200, r.text
    assert r.json()["disposition"] == "占位"
    return r.json()["hold_id"]


def _fire_concurrent(hold_id: int, shots: list[tuple[str, dict]]) -> dict:
    """并发打同一确认口。shots: [(标签, 请求头)]，返回 {标签: response}。"""
    barrier = threading.Barrier(len(shots))
    out: dict = {}

    def hit(label: str, headers: dict) -> None:
        cc = TestClient(app, raise_server_exceptions=False)
        barrier.wait()
        out[label] = cc.post(
            "/api/bookings/confirm", headers=headers, json={"hold_id": hold_id}
        )

    threads = [threading.Thread(target=hit, args=(label, h)) for label, h in shots]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return out


def _bookings_of(c: TestClient, headers, hold_id: int) -> list:
    return [
        b
        for b in c.get("/api/bookings", headers=headers).json()["items"]
        if b["hold_id"] == hold_id
    ]


def _contention_traces(c: TestClient, headers) -> list:
    return [
        t
        for t in c.get("/api/contention", headers=headers).json()["items"]
        if t["lock_key"] == LOCK_KEY and t["action"] == "争用"
    ]


def _confirm_audits(c: TestClient, headers) -> list:
    return [
        a
        for a in c.get("/api/audit", headers=headers, params={"limit": 500}).json()["items"]
        if a["action"] == "booking_confirm" and a["target"] == PARTY
    ]


@pytest.mark.usefixtures("_wide_race_window")
def test_double_confirm_never_two_bookings():
    """双正式并存失败测：收银连点确认同一占位，一占位只进一正式。"""
    with TestClient(app) as c:
        h = _login(c, "clerk")
        hid = _make_hold(c, h)
        before = c.get(f"/api/slots/{SLOT}", headers=h).json()["booked"]

        resps = _fire_concurrent(hid, [("click1", h), ("click2", h)])

        codes = sorted(r.status_code for r in resps.values())
        detail = {k: (r.status_code, r.text[:120]) for k, r in resps.items()}
        assert codes == [200, 409], f"连点确认须一成一争用，实得 {detail}"

        # 正式行：一占位只进一正式
        books = _bookings_of(c, h, hid)
        assert len(books) == 1, f"一占位只进一正式，实得 {len(books)} 笔"

        # 占位态
        holds = [x for x in c.get("/api/holds", headers=h).json()["items"] if x["id"] == hid]
        assert holds[0]["status"] == "已确认"

        # 占用数：只加一次
        after = c.get(f"/api/slots/{SLOT}", headers=h).json()["booked"]
        assert after - before == SEATS, f"占用数只加一次，实加 {after - before}"

        # 互证：按当事人同源可读，一占一正
        proof = c.get(f"/api/proof/{PARTY}", headers=h).json()
        assert len(proof["bookings"]) == 1
        assert proof["holds"][0]["status"] == "已确认"

        # 审计：确认痕只有一条
        assert len(_confirm_audits(c, h)) == 1

        # 对账：booked 与正式册同源
        assert c.get(f"/api/reconcile/{SLOT}", headers=h).json()["ok"] is True

        # 争用痕：后到只留一条
        traces = _contention_traces(c, h)
        assert len(traces) == 1, f"后到只留一条争用痕，实得 {len(traces)}"


@pytest.mark.usefixtures("_wide_race_window")
def test_concurrent_confirm_loser_only_contention_trace():
    """两边同时确认争用失败测：两边抢同一占位须抢档锁，后到只留争用痕。"""
    with TestClient(app) as c:
        h_clerk = _login(c, "clerk")
        h_admin = _login(c, "admin")
        hid = _make_hold(c, h_clerk)
        before = c.get(f"/api/slots/{SLOT}", headers=h_admin).json()["booked"]

        resps = _fire_concurrent(hid, [("clerk", h_clerk), ("admin", h_admin)])

        codes = sorted(r.status_code for r in resps.values())
        detail = {k: (r.status_code, r.text[:120]) for k, r in resps.items()}
        assert codes == [200, 409], f"两边确认须一成一争用，实得 {detail}"

        loser = next(u for u, r in resps.items() if r.status_code == 409)
        assert "争用" in resps[loser].json()["detail"]

        # 争用痕：后到者留名，且痕迹已落库（不随后续回滚）
        traces = _contention_traces(c, h_admin)
        assert len(traces) == 1, f"后到只留一条争用痕，实得 {len(traces)}"
        assert traces[0]["actor"] == loser, "争用痕须记后到者"

        # 后到者零业务写入：正式行/占用数/确认审计均只出一次
        assert len(_bookings_of(c, h_admin, hid)) == 1
        after = c.get(f"/api/slots/{SLOT}", headers=h_admin).json()["booked"]
        assert after - before == SEATS, f"占用数只加一次，实加 {after - before}"
        audits = _confirm_audits(c, h_admin)
        assert len(audits) == 1
        assert audits[0]["actor"] != loser, "后到者不得写确认审计"
