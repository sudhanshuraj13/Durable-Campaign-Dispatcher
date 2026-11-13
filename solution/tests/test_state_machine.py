"""
Unit tests for the Lead State Machine and SQLite WAL transitions.
Verifies two-phase reservations, concurrency limits, DND propagation, retries, and callbacks.
"""
import os
import tempfile
from datetime import datetime, timedelta

import pytest

from solution.clock import IST
from solution.db import (
    confirm_call_placed,
    get_connection,
    init_db,
    process_call_result,
    reserve_lead,
    rollback_reservation,
)


@pytest.fixture
def test_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    init_db(path)
    conn = get_connection(path)
    try:
        conn.execute(
            "INSERT INTO tenants (tenant_id, max_concurrent_calls) VALUES ('test_tenant', 2);"
        )
        conn.execute(
            """
            INSERT INTO campaigns (campaign_id, tenant_id, leads_file, window_start, window_end, window_days, max_attempts)
            VALUES ('camp_1', 'test_tenant', 'dummy.csv', '09:00', '21:00', '["thu"]', 3);
            """
        )
        conn.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, customer_name, state, attempts, priority, updated_at)
            VALUES ('camp_1', 'lead_1', 'test_tenant', '+919999999991', 'Alice', 'PENDING', 0, 10, '2026-11-12T17:30:00+05:30');
            """
        )
        conn.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, customer_name, state, attempts, priority, updated_at)
            VALUES ('camp_1', 'lead_2', 'test_tenant', '+919999999992', 'Bob', 'PENDING', 0, 10, '2026-11-12T17:30:00+05:30');
            """
        )
        conn.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, customer_name, state, attempts, priority, updated_at)
            VALUES ('camp_1', 'lead_3', 'test_tenant', '+919999999993', 'Charlie', 'PENDING', 0, 10, '2026-11-12T17:30:00+05:30');
            """
        )
    finally:
        conn.close()
    yield path
    try:
        os.remove(path)
        for ext in ("-wal", "-shm"):
            if os.path.exists(path + ext):
                os.remove(path + ext)
    except Exception:
        pass


def test_two_phase_reservation_success(test_db):
    conn = get_connection(test_db)
    # Step 1: Reserve
    req_id = reserve_lead(conn, "camp_1", "lead_1", "test_tenant", "+919999999991", 1, 2)
    assert req_id == "camp_1:lead_1:1"

    row = conn.execute("SELECT state, active_request_id FROM leads WHERE lead_id = 'lead_1';").fetchone()
    assert row["state"] == "RESERVING"
    assert row["active_request_id"] == "camp_1:lead_1:1"

    # Step 2: Confirm call placed
    confirmed = confirm_call_placed(conn, "camp_1", "lead_1", req_id, "CA123456", 1)
    assert confirmed is True

    row = conn.execute("SELECT state, active_call_sid, attempts FROM leads WHERE lead_id = 'lead_1';").fetchone()
    assert row["state"] == "IN_FLIGHT"
    assert row["active_call_sid"] == "CA123456"
    assert row["attempts"] == 1
    conn.close()


def test_tenant_concurrency_cap(test_db):
    conn = get_connection(test_db)
    # Tenant limit is 2
    r1 = reserve_lead(conn, "camp_1", "lead_1", "test_tenant", "+919999999991", 1, 2)
    assert r1 is not None

    r2 = reserve_lead(conn, "camp_1", "lead_2", "test_tenant", "+919999999992", 1, 2)
    assert r2 is not None

    # Third reservation must be denied due to concurrency cap
    r3 = reserve_lead(conn, "camp_1", "lead_3", "test_tenant", "+919999999993", 1, 2)
    assert r3 is None
    conn.close()


def test_phone_concurrency_lock(test_db):
    conn = get_connection(test_db)
    # Add another campaign with a lead sharing the same phone as lead_1
    conn.execute(
        """
        INSERT INTO campaigns (campaign_id, tenant_id, leads_file, window_start, window_end, window_days, max_attempts)
        VALUES ('camp_2', 'test_tenant', 'dummy2.csv', '09:00', '21:00', '["thu"]', 3);
        """
    )
    conn.execute(
        """
        INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, customer_name, state, attempts, priority, updated_at)
        VALUES ('camp_2', 'lead_cross', 'test_tenant', '+919999999991', 'Alice Dual', 'PENDING', 0, 10, '2026-11-12T17:30:00+05:30');
        """
    )

    r1 = reserve_lead(conn, "camp_1", "lead_1", "test_tenant", "+919999999991", 1, 2)
    assert r1 is not None

    # Same phone across camp_2 must be rejected
    r_conflict = reserve_lead(conn, "camp_2", "lead_cross", "test_tenant", "+919999999991", 1, 2)
    assert r_conflict is None
    conn.close()


def test_terminal_disposition_completed(test_db):
    conn = get_connection(test_db)
    req_id = reserve_lead(conn, "camp_1", "lead_1", "test_tenant", "+919999999991", 1, 2)
    confirm_call_placed(conn, "camp_1", "lead_1", req_id, "CA_TERM", 1)

    result = {
        "call_sid": "CA_TERM",
        "request_id": req_id,
        "campaign_id": "camp_1",
        "lead_id": "lead_1",
        "tenant_id": "test_tenant",
        "phone": "+919999999991",
        "status": "completed",
        "disposition": "interested",
        "started_at": "2026-11-12T17:35:00+05:30",
        "ended_at": "2026-11-12T17:38:00+05:30",
        "duration_sec": 180,
    }
    retry_policy = {"no_answer_minutes": 60, "busy_minutes": 60, "failed_minutes": 15}
    processed = process_call_result(conn, result, retry_policy)
    assert processed is True

    row = conn.execute("SELECT state, attempts, last_status, last_disposition FROM leads WHERE lead_id = 'lead_1';").fetchone()
    assert row["state"] == "completed"
    assert row["last_disposition"] == "interested"
    conn.close()


def test_dnd_propagation_blocks_other_leads(test_db):
    conn = get_connection(test_db)
    # Add cross-campaign lead sharing phone with lead_1
    conn.execute(
        """
        INSERT INTO campaigns (campaign_id, tenant_id, leads_file, window_start, window_end, window_days, max_attempts)
        VALUES ('camp_2', 'test_tenant', 'dummy2.csv', '09:00', '21:00', '["thu"]', 3);
        """
    )
    conn.execute(
        """
        INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, customer_name, state, attempts, priority, updated_at)
        VALUES ('camp_2', 'lead_shared', 'test_tenant', '+919999999991', 'Alice Shared', 'PENDING', 0, 10, '2026-11-12T17:30:00+05:30');
        """
    )

    req_id = reserve_lead(conn, "camp_1", "lead_1", "test_tenant", "+919999999991", 1, 2)
    confirm_call_placed(conn, "camp_1", "lead_1", req_id, "CA_DND", 1)

    result = {
        "call_sid": "CA_DND",
        "request_id": req_id,
        "campaign_id": "camp_1",
        "lead_id": "lead_1",
        "tenant_id": "test_tenant",
        "phone": "+919999999991",
        "status": "completed",
        "disposition": "dnd",
        "started_at": "2026-11-12T17:35:00+05:30",
        "ended_at": "2026-11-12T17:36:00+05:30",
        "duration_sec": 60,
    }
    retry_policy = {"no_answer_minutes": 60, "busy_minutes": 60, "failed_minutes": 15}
    process_call_result(conn, result, retry_policy)

    # Lead 1 is completed (it had the terminal DND disposition itself)
    row1 = conn.execute("SELECT state FROM leads WHERE lead_id = 'lead_1';").fetchone()
    assert row1["state"] == "completed"

    # Shared lead in camp_2 is automatically transitioned to dnd_blocked!
    row_shared = conn.execute("SELECT state FROM leads WHERE lead_id = 'lead_shared';").fetchone()
    assert row_shared["state"] == "dnd_blocked"

    # Global dnd_phones table contains the phone
    dnd_entry = conn.execute("SELECT 1 FROM dnd_phones WHERE phone = '+919999999991';").fetchone()
    assert dnd_entry is not None
    conn.close()


def test_retry_cooldown_calculation(test_db):
    conn = get_connection(test_db)
    req_id = reserve_lead(conn, "camp_1", "lead_1", "test_tenant", "+919999999991", 1, 2)
    confirm_call_placed(conn, "camp_1", "lead_1", req_id, "CA_NOANS", 1)

    ended_at = "2026-11-12T17:40:00+05:30"
    result = {
        "call_sid": "CA_NOANS",
        "request_id": req_id,
        "campaign_id": "camp_1",
        "lead_id": "lead_1",
        "tenant_id": "test_tenant",
        "phone": "+919999999991",
        "status": "no_answer",
        "disposition": None,
        "started_at": "2026-11-12T17:39:00+05:30",
        "ended_at": ended_at,
        "duration_sec": 0,
    }
    retry_policy = {"no_answer_minutes": 60, "busy_minutes": 60, "failed_minutes": 15}
    process_call_result(conn, result, retry_policy)

    row = conn.execute("SELECT state, priority, next_eligible_at, attempts FROM leads WHERE lead_id = 'lead_1';").fetchone()
    assert row["state"] == "RETRY_WAIT"
    assert row["priority"] == 50
    assert row["attempts"] == 1
    # 17:40 + 60 minutes + 15 seconds buffer = 18:40:15
    expected_eligible = (datetime.fromisoformat(ended_at) + timedelta(minutes=60, seconds=15)).isoformat()
    assert row["next_eligible_at"] == expected_eligible
    conn.close()


def test_callback_requested_priority_and_time(test_db):
    conn = get_connection(test_db)
    req_id = reserve_lead(conn, "camp_1", "lead_1", "test_tenant", "+919999999991", 1, 2)
    confirm_call_placed(conn, "camp_1", "lead_1", req_id, "CA_CB", 1)

    callback_at = "2026-11-12T19:15:00+05:30"
    result = {
        "call_sid": "CA_CB",
        "request_id": req_id,
        "campaign_id": "camp_1",
        "lead_id": "lead_1",
        "tenant_id": "test_tenant",
        "phone": "+919999999991",
        "status": "completed",
        "disposition": "callback_requested",
        "callback_at": callback_at,
        "started_at": "2026-11-12T17:35:00+05:30",
        "ended_at": "2026-11-12T17:38:00+05:30",
        "duration_sec": 180,
    }
    retry_policy = {"no_answer_minutes": 60, "busy_minutes": 60, "failed_minutes": 15}
    process_call_result(conn, result, retry_policy)

    row = conn.execute("SELECT state, priority, next_eligible_at FROM leads WHERE lead_id = 'lead_1';").fetchone()
    assert row["state"] == "CALLBACK_WAIT"
    assert row["priority"] == 100
    expected_eligible = (datetime.fromisoformat(callback_at) + timedelta(seconds=15)).isoformat()
    assert row["next_eligible_at"] == expected_eligible
    conn.close()


def test_max_attempts_exhausted(test_db):
    conn = get_connection(test_db)
    # Simulate attempt 3 (max_attempts = 3)
    conn.execute("UPDATE leads SET attempts = 2 WHERE lead_id = 'lead_1';")
    req_id = reserve_lead(conn, "camp_1", "lead_1", "test_tenant", "+919999999991", 3, 2)
    confirm_call_placed(conn, "camp_1", "lead_1", req_id, "CA_ATT3", 3)

    result = {
        "call_sid": "CA_ATT3",
        "request_id": req_id,
        "campaign_id": "camp_1",
        "lead_id": "lead_1",
        "tenant_id": "test_tenant",
        "phone": "+919999999991",
        "status": "no_answer",
        "disposition": None,
        "ended_at": "2026-11-12T17:40:00+05:30",
    }
    retry_policy = {"no_answer_minutes": 60, "busy_minutes": 60, "failed_minutes": 15}
    process_call_result(conn, result, retry_policy)

    row = conn.execute("SELECT state, attempts FROM leads WHERE lead_id = 'lead_1';").fetchone()
    assert row["state"] == "exhausted"
    assert row["attempts"] == 3
    conn.close()

