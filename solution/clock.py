"""
Simulated clock synchronization and calling window verification.
All calculations use IST (UTC+05:30) as required by the regulatory spec.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

IST = timezone(timedelta(hours=5, minutes=30))
DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


def hm(s: str) -> tuple[int, int]:
    parts = s.split(":")
    return int(parts[0]), int(parts[1])


def in_window(
    t: datetime,
    start: str,
    end: str,
    days: Optional[list[str]] = None,
    safety_margin_s: float = 0.0,
) -> bool:
    """
    Check if datetime `t` falls within [start, end - safety_margin_s) on an allowed day in IST.
    A positive `safety_margin_s` prevents dispatching right at window closure.
    """
    lt = t.astimezone(IST)
    if days is not None and DAYS[lt.weekday()] not in days:
        return False
    sh, sm = hm(start)
    eh, em = hm(end)
    s = lt.replace(hour=sh, minute=sm, second=0, microsecond=0)
    e = lt.replace(hour=eh, minute=em, second=0, microsecond=0)
    if safety_margin_s > 0:
        e = e - timedelta(seconds=safety_margin_s)
        if e <= s:
            return False
    return s <= lt < e


def is_dispatch_legal(
    sim_now: datetime,
    camp_window: dict,
    reg_window: dict,
    safety_margin_s: float = 300.0,
) -> bool:
    """
    True if sim_now is legal to dispatch under BOTH:
      1. Regulatory window (e.g. 09:00-21:00, all days)
      2. Campaign window (start, end, days)
    with safety margin applied before closing.
    """
    if not in_window(sim_now, reg_window["start"], reg_window["end"], None, safety_margin_s):
        return False
    if not in_window(sim_now, camp_window["start"], camp_window["end"], camp_window.get("days"), safety_margin_s):
        return False
    return True


def next_open(t: datetime, start: str, end: str, days: list[str]) -> datetime:
    """
    Earliest instant >= t in IST that is inside [start, end) on an allowed day.
    """
    lt = t.astimezone(IST)
    sh, sm = hm(start)
    eh, em = hm(end)
    for d in range(0, 14):
        day = (lt + timedelta(days=d)).replace(second=0, microsecond=0)
        if DAYS[day.weekday()] not in days:
            continue
        s = day.replace(hour=sh, minute=sm)
        e = day.replace(hour=eh, minute=em)
        if d == 0:
            if lt < s:
                return s
            if lt < e:
                return lt
            continue
        return s
    return lt


def next_combined_open(t: datetime, camp_window: dict, reg_window: dict) -> datetime:
    """
    Find earliest instant >= t that satisfies both campaign and regulatory windows.
    """
    candidate = next_open(t, camp_window["start"], camp_window["end"], camp_window["days"])
    reg_days = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    for _ in range(28):
        if is_dispatch_legal(candidate, camp_window, reg_window, safety_margin_s=0.0):
            return candidate
        # advance candidate to either next campaign open or next reg open
        next_c = next_open(candidate + timedelta(minutes=1), camp_window["start"], camp_window["end"], camp_window["days"])
        next_r = next_open(candidate + timedelta(minutes=1), reg_window["start"], reg_window["end"], reg_days)
        candidate = max(next_c, next_r)
    return candidate


class SimClockClient:
    """
    Client for the simulated clock exposed by mock telephony.
    Computes sim_now locally using sim_epoch + (time.time() - real_epoch) * speed.
    """

    def __init__(
        self,
        sim_epoch: Optional[datetime] = None,
        real_epoch: Optional[float] = None,
        speed: float = 600.0,
    ):
        self.sim_epoch: datetime = sim_epoch or datetime(2026, 11, 12, 17, 30, 0, tzinfo=IST)
        self.real_epoch: float = real_epoch if real_epoch is not None else time.time()
        self.speed: float = speed
        self.synced: bool = False

    def now(self) -> datetime:
        elapsed_real = time.time() - self.real_epoch
        return self.sim_epoch + timedelta(seconds=elapsed_real * self.speed)

    async def sync(self, client: httpx.AsyncClient, base_url: str) -> bool:
        try:
            resp = await client.get(f"{base_url.rstrip('/')}/v1/clock", timeout=2.0)
            if resp.status_code == 200:
                data = resp.json()
                self.sim_epoch = datetime.fromisoformat(data["sim_epoch"]).astimezone(IST)
                self.real_epoch = float(data["real_epoch"])
                self.speed = float(data["speed"])
                self.synced = True
                return True
        except Exception:
            pass
        return False

