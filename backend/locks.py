"""档期锁与争用登记。"""
from __future__ import annotations

import sqlite3
from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from auth import AuthHeader, require_user
from db import connect, write_audit

router = APIRouter(tags=["locks"])


class LockIn(BaseModel):
    lock_key: str


def try_acquire(conn, lock_key: str, holder: str) -> bool:
    """抢档锁：零等待 INSERT 即抢锁，抢到立即落库可见、排他持有。

    锁不认同人：同人第二请求与异账号同等待遇。INSERT 撞锁（锁行已在或
    他人写事务进行中）即败，只留争用痕；争用痕当场落库，不随后续业务
    回滚。抢锁 INSERT 零等待；落库 commit 恢复正常等待，不被瞬时读锁
    误伤（否则赢家 commit 会被后到者的读快照顶掉，双方便宜都抢不到）。
    """
    now = datetime.now().isoformat(timespec="seconds")
    conn.execute("PRAGMA busy_timeout=0")
    try:
        conn.execute(
            "INSERT INTO slot_locks(lock_key,holder,acquired_at) VALUES (?,?,?)",
            (lock_key, holder, now),
        )
    except sqlite3.Error:
        conn.rollback()  # 未抢到：锁行已落库或他人写事务进行中
        conn.execute("PRAGMA busy_timeout=30000")
        row = conn.execute(
            "SELECT holder FROM slot_locks WHERE lock_key=?", (lock_key,)
        ).fetchone()
        conn.execute(
            """INSERT INTO contention_log(lock_key,actor,action,detail,created_at)
               VALUES (?,?,?,?,?)""",
            (lock_key, holder, "争用", f"持有者={row['holder'] if row else '未知'}", now),
        )
        conn.commit()  # 争用痕落库，不随后续业务回滚
        return False
    conn.execute("PRAGMA busy_timeout=30000")
    conn.commit()  # 锁行立即落库可见，排他持有至 release
    return True


def release(conn, lock_key: str, holder: str) -> None:
    conn.execute(
        "DELETE FROM slot_locks WHERE lock_key=? AND holder=?",
        (lock_key, holder),
    )


def require_lock(conn, lock_key: str, holder: str) -> None:
    if not try_acquire(conn, lock_key, holder):
        raise HTTPException(409, "争用：后到只记争用，不写业务")


@router.get("/api/locks")
def list_locks(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM slot_locks ORDER BY acquired_at DESC"
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.get("/api/contention")
def list_contention(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM contention_log ORDER BY id DESC LIMIT 100"
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.post("/api/locks/acquire")
def acquire_lock(body: LockIn, authorization: AuthHeader = None):
    user = require_user(authorization)
    with connect() as conn:
        ok = try_acquire(conn, body.lock_key, user)
        write_audit(
            conn,
            user,
            "lock_acquire" if ok else "lock_contention",
            body.lock_key,
            {"ok": ok},
        )
        conn.commit()
        if not ok:
            raise HTTPException(409, "争用：后到只记争用")
        return {"ok": True, "lock_key": body.lock_key, "holder": user}


@router.post("/api/locks/release")
def release_lock(body: LockIn, authorization: AuthHeader = None):
    user = require_user(authorization)
    with connect() as conn:
        release(conn, body.lock_key, user)
        write_audit(conn, user, "lock_release", body.lock_key, {})
        conn.commit()
    return {"ok": True}
