"""
Unit tests for calling windows, boundary math, and regulatory compliance.
All tests use a simulated clock and deterministic timestamps.
"""
from datetime import datetime, timedelta

import pytest

from solution.clock import DAYS, IST, hm, in_window, is_dispatch_legal, next_combined_open, next_open


def test_hm_parser():
    assert hm("09:00") == (9, 0)
    assert hm("20:30") == (20, 30)


def test_in_window_basic():
    # Thursday, 12 Nov 2026, 11:30 IST
    t = datetime(2026, 11, 12, 11, 30, tzinfo=IST)
    assert in_window(t, "10:00", "19:00", ["mon", "tue", "wed", "thu", "fri", "sat"]) is True
    assert in_window(t, "12:00", "19:00", ["mon", "tue", "wed", "thu", "fri", "sat"]) is False


def test_in_window_days_filter():
    # Sunday, 15 Nov 2026, 12:00 IST
    t_sun = datetime(2026, 11, 15, 12, 0, tzinfo=IST)
    # Acme Renewals is Mon-Sat only
    assert in_window(t_sun, "10:00", "19:00", ["mon", "tue", "wed", "thu", "fri", "sat"]) is False
    # Acme Feedback is Mon-Sun
    assert in_window(t_sun, "11:00", "20:30", ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]) is True


def test_in_window_safety_margin():
    # Window 10:00 - 19:00 with 30s safety margin
    # 18:59:20 (40s before close) -> within safety margin
    t_safe = datetime(2026, 11, 12, 18, 59, 20, tzinfo=IST)
    assert in_window(t_safe, "10:00", "19:00", ["thu"], safety_margin_s=30.0) is True

    # 18:59:45 (15s before close) -> outside safety margin
    t_unsafe = datetime(2026, 11, 12, 18, 59, 45, tzinfo=IST)
    assert in_window(t_unsafe, "10:00", "19:00", ["thu"], safety_margin_s=30.0) is False

    # 19:00:00 -> outside window
    t_closed = datetime(2026, 11, 12, 19, 0, 0, tzinfo=IST)
    assert in_window(t_closed, "10:00", "19:00", ["thu"], safety_margin_s=0.0) is False


def test_is_dispatch_legal_regulatory():
    camp_window = {"start": "08:00", "end": "22:00", "days": ["thu"]}
    reg_window = {"start": "09:00", "end": "21:00"}

    # 08:30 IST: inside campaign window, but outside regulatory window
    t_early = datetime(2026, 11, 12, 8, 30, tzinfo=IST)
    assert is_dispatch_legal(t_early, camp_window, reg_window, safety_margin_s=0.0) is False

    # 09:30 IST: inside both windows
    t_legal = datetime(2026, 11, 12, 9, 30, tzinfo=IST)
    assert is_dispatch_legal(t_legal, camp_window, reg_window, safety_margin_s=0.0) is True

    # 21:15 IST: inside campaign window, outside regulatory window
    t_late = datetime(2026, 11, 12, 21, 15, tzinfo=IST)
    assert is_dispatch_legal(t_late, camp_window, reg_window, safety_margin_s=0.0) is False


def test_next_open_same_day():
    # Thursday 08:00 IST, window starts 10:00
    t = datetime(2026, 11, 12, 8, 0, tzinfo=IST)
    nxt = next_open(t, "10:00", "19:00", ["thu", "fri"])
    assert nxt == datetime(2026, 11, 12, 10, 0, tzinfo=IST)


def test_next_open_next_day():
    # Thursday 20:00 IST (after close 19:00), window opens Friday 10:00
    t = datetime(2026, 11, 12, 20, 0, tzinfo=IST)
    nxt = next_open(t, "10:00", "19:00", ["thu", "fri"])
    assert nxt == datetime(2026, 11, 13, 10, 0, tzinfo=IST)


def test_next_open_over_weekend():
    # Friday 19:30 IST (after close), campaign runs Mon-Fri only
    t = datetime(2026, 11, 13, 19, 30, tzinfo=IST)
    nxt = next_open(t, "09:30", "18:00", ["mon", "tue", "wed", "thu", "fri"])
    # Next open is Monday 16 Nov 09:30 IST
    assert nxt == datetime(2026, 11, 16, 9, 30, tzinfo=IST)

