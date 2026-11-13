"""
Tests for Idempotency, 503/504 Handling, Crash Recovery, and Background Reconciliation.
"""
import os
import tempfile
import time

import pytest

from solution.db import (
    confirm_call_placed,
    get_connection,
    get_in_flight_leads,
    get_stale_reserving_leads,
    init_db,
    process_call_result,
    reserve_lead,
    rollback_reservation,
)


@pytest.fixture
def recovery_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    init_db(path)
    conn = get_connection(path)
    try:
        conn.execute("INSERT INTO tenants (tenant_id, max_concurrent_calls) VALUES ('acme', 2);")
        conn.execute(
            """
            INSERT INTO campaigns (campaign_id, tenant_id, leads_file, window_start, window_end, window_days, max_attempts)
            VALUES ('c_renew', 'acme', 'dummy.csv', '09:00', '21:00', '["thu"]', 3);
            """
        )
        conn.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, tenant_id, phone, customer_name, state, attempts, priority, updated_at)
            VALUES ('c_renew', 'ren_01', 'acme', '+919876543210', 'Rahul', 'PENDING', 0, 10, '2026-11-12T17:30:00+05:30');
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


def test_duplicate_webhook_idempotency(recovery_db):
    conn = get_connection(recovery_db)
    req_id = reserve_lead(conn, "c_renew", "ren_01", "acme", "+919876543210", 1, 2)
    confirm_call_placed(conn, "c_renew", "ren_01", req_id, "CA_DUP", 1)

    result = {
        "call_sid": "CA_DUP",
        "request_id": req_id,
        "campaign_id": "c_renew",
        "lead_id": "ren_01",
        "tenant_id": "acme",
        "phone": "+919876543210",
        "status": "completed",
        "disposition": "interested",
        "started_at": "2026-11-12T17:35:00+05:30",
        "ended_at": "2026-11-12T17:38:00+05:30",
        "duration_sec": 180,
    }
    retry_policy = {"no_answer_minutes": 60, "busy_minutes": 60, "failed_minutes": 15}

    # First delivery
    first_ok = process_call_result(conn, result, retry_policy)
    assert first_ok is True

    # Duplicate delivery
    second_ok = process_call_result(conn, result, retry_policy)
    assert second_ok is True

    # Only one record in call_attempts
    count = conn.execute("SELECT COUNT(*) as cnt FROM call_attempts WHERE call_sid = 'CA_DUP';").fetchone()["cnt"]
    assert count == 1
    conn.close()


def test_503_rollback_restores_wait_state(recovery_db):
    conn = get_connection(recovery_db)
    req_id = reserve_lead(conn, "c_renew", "ren_01", "acme", "+919876543210", 1, 2)
    assert req_id is not None

    # Verify reservation is holding slot
    row = conn.execute("SELECT state FROM leads WHERE lead_id = 'ren_01';").fetchone()
    assert row["state"] == "RESERVING"

    # Simulate 503 response -> rollback
    rolled_back = rollback_reservation(conn, "c_renew", "ren_01", req_id)
    assert rolled_back is True

    row_after = conn.execute("SELECT state, active_request_id FROM leads WHERE lead_id = 'ren_01';").fetchone()
    assert row_after["state"] == "PENDING"
    assert row_after["active_request_id"] is None

    # Slot is freed: another reservation succeeds
    req_id_retry = reserve_lead(conn, "c_renew", "ren_01", "acme", "+919876543210", 1, 2)
    assert req_id_retry is not None
    conn.close()


def test_stale_reserving_lead_detection_for_crash_recovery(recovery_db):
    conn = get_connection(recovery_db)
    # Reserve lead with a past timestamp to simulate process dying mid-flight
    req_id = reserve_lead(conn, "c_renew", "ren_01", "acme", "+919876543210", 1, 2)
    conn.execute(
        "UPDATE leads SET reserving_since_real_time = ? WHERE lead_id = 'ren_01';",
        (time.time() - 10.0,),
    )

    stale = get_stale_reserving_leads(conn, max_age_seconds=2.0)
    assert len(stale) == 1
    assert stale[0]["lead_id"] == "ren_01"
    assert stale[0]["active_request_id"] == req_id
    conn.close()


def test_in_flight_leads_polling_recovery(recovery_db):
    conn = get_connection(recovery_db)
    req_id = reserve_lead(conn, "c_renew", "ren_01", "acme", "+919876543210", 1, 2)
    confirm_call_placed(conn, "c_renew", "ren_01", req_id, "CA_IN_FLIGHT_RECOVER", 1)

    in_flight = get_in_flight_leads(conn)
    assert len(in_flight) == 1
    assert in_flight[0]["lead_id"] == "ren_01"
    assert in_flight[0]["active_call_sid"] == "CA_IN_FLIGHT_RECOVER"
    conn.close()

