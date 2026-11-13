"""
SQLite WAL State Store for Durable Campaign Dispatcher.
Enforces atomic transitions, two-phase reservations, concurrency tracking,
and global cross-campaign DND locks.
"""
from __future__ import annotations

import csv
import json
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

IST = timezone(timedelta(hours=5, minutes=30))
TERMINAL_DISPOSITIONS = {"interested", "not_interested", "wrong_number", "dnd"}


def get_connection(db_path: str) -> sqlite3.Connection:
    """Create a thread-safe connection configured for SQLite WAL mode."""
    conn = sqlite3.connect(db_path, timeout=10.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(db_path: str) -> None:
    """Initialize SQLite tables and indices."""
    conn = get_connection(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tenants (
                tenant_id TEXT PRIMARY KEY,
                max_concurrent_calls INTEGER NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS campaigns (
                campaign_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                leads_file TEXT NOT NULL,
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                window_days TEXT NOT NULL,
                max_attempts INTEGER NOT NULL,
                FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dnd_phones (
                phone TEXT PRIMARY KEY,
                created_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS leads (
                campaign_id TEXT NOT NULL,
                lead_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                phone TEXT NOT NULL,
                customer_name TEXT,
                state TEXT NOT NULL DEFAULT 'PENDING',
                attempts INTEGER NOT NULL DEFAULT 0,
                priority INTEGER NOT NULL DEFAULT 10,
                next_eligible_at TEXT,
                active_request_id TEXT,
                active_call_sid TEXT,
                reserving_since_real_time REAL,
                last_status TEXT,
                last_disposition TEXT,
                last_ended_at TEXT,
                last_callback_at TEXT,
                last_duration_sec INTEGER,
                updated_at TEXT,
                PRIMARY KEY (campaign_id, lead_id),
                FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id),
                FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS call_attempts (
                call_sid TEXT PRIMARY KEY,
                request_id TEXT UNIQUE,
                campaign_id TEXT NOT NULL,
                lead_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                phone TEXT NOT NULL,
                attempt_no INTEGER NOT NULL,
                status TEXT NOT NULL,
                disposition TEXT,
                callback_at TEXT,
                started_at TEXT,
                ended_at TEXT,
                duration_sec INTEGER,
                created_at TEXT NOT NULL
            );
            """
        )
        # Performance & integrity indices
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_state_prio ON leads (state, priority DESC, next_eligible_at);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_phone ON leads (phone);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_active_req ON leads (active_request_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_active_sid ON leads (active_call_sid);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_tenant ON leads (tenant_id, state);")
        conn.execute("COMMIT;")
    except Exception:
        conn.execute("ROLLBACK;")
        raise
    finally:
        conn.close()


def load_fixtures(db_path: str, fixtures_path: str) -> dict:
    """
    Load tenants, campaigns, and lead fixtures into SQLite.
    Idempotent: safe to run repeatedly or after crash restart.
    Returns the parsed fixtures config.
    """
    with open(fixtures_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    base_dir = os.path.dirname(os.path.abspath(fixtures_path))
    conn = get_connection(db_path)
    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        conn.execute("BEGIN IMMEDIATE;")
        # Tenants
        for t in cfg.get("tenants", []):
            conn.execute(
                """
                INSERT INTO tenants (tenant_id, max_concurrent_calls)
                VALUES (?, ?)
                ON CONFLICT(tenant_id) DO UPDATE SET max_concurrent_calls = excluded.max_concurrent_calls;
                """,
                (t["tenant_id"], t["max_concurrent_calls"]),
            )

        # Campaigns
        for c in cfg.get("campaigns", []):
            conn.execute(
                """
                INSERT INTO campaigns (campaign_id, tenant_id, leads_file, window_start, window_end, window_days, max_attempts)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(campaign_id) DO UPDATE SET
                    tenant_id = excluded.tenant_id,
                    leads_file = excluded.leads_file,
                    window_start = excluded.window_start,
                    window_end = excluded.window_end,
                    window_days = excluded.window_days,
                    max_attempts = excluded.max_attempts;
                """,
                (
                    c["campaign_id"],
                    c["tenant_id"],
                    c["leads_file"],
                    c["window"]["start"],
                    c["window"]["end"],
                    json.dumps(c["window"]["days"]),
                    c.get("max_attempts", 3),
                ),
            )

            # Leads CSV
            csv_path = os.path.join(base_dir, c["leads_file"])
            if os.path.exists(csv_path):
                with open(csv_path, "r", encoding="utf-8") as cf:
                    reader = csv.DictReader(cf)
                    for row in reader:
                        conn.execute(
                            """
                            INSERT INTO leads (
                                campaign_id, lead_id, tenant_id, phone, customer_name,
                                state, attempts, priority, updated_at
                            )
                            VALUES (?, ?, ?, ?, ?, 'PENDING', 0, 10, ?)
                            ON CONFLICT(campaign_id, lead_id) DO NOTHING;
                            """,
                            (
                                c["campaign_id"],
                                row["lead_id"],
                                c["tenant_id"],
                                row["phone"],
                                row.get("customer_name", ""),
                                now_iso,
                            ),
                        )

        # Sync any existing DND phones
        conn.execute(
            """
            UPDATE leads
            SET state = 'dnd_blocked', updated_at = ?
            WHERE phone IN (SELECT phone FROM dnd_phones)
              AND state IN ('PENDING', 'RETRY_WAIT', 'CALLBACK_WAIT');
            """,
            (now_iso,),
        )
        conn.execute("COMMIT;")
    except Exception:
        conn.execute("ROLLBACK;")
        raise
    finally:
        conn.close()

    return cfg


def get_active_tenant_counts(conn: sqlite3.Connection) -> Dict[str, int]:
    """Return count of active calls (RESERVING or IN_FLIGHT) per tenant."""
    rows = conn.execute(
        """
        SELECT tenant_id, COUNT(*) as cnt
        FROM leads
        WHERE state IN ('RESERVING', 'IN_FLIGHT')
        GROUP BY tenant_id;
        """
    ).fetchall()
    return {row["tenant_id"]: row["cnt"] for row in rows}


def get_active_phones(conn: sqlite3.Connection) -> Set[str]:
    """Return set of phones currently active (RESERVING or IN_FLIGHT)."""
    rows = conn.execute(
        """
        SELECT DISTINCT phone
        FROM leads
        WHERE state IN ('RESERVING', 'IN_FLIGHT');
        """
    ).fetchall()
    return {row["phone"] for row in rows}


def get_candidate_leads_for_tenant(
    conn: sqlite3.Connection,
    tenant_id: str,
    open_campaign_ids: List[str],
    sim_now_iso: str,
    limit: int,
) -> List[sqlite3.Row]:
    """
    Fetch eligible leads for a tenant across currently open campaigns.
    Ordered by priority DESC (callbacks > retries > pending), next_eligible_at ASC, lead_id ASC.
    """
    if not open_campaign_ids or limit <= 0:
        return []

    placeholders = ",".join("?" for _ in open_campaign_ids)
    query = f"""
        SELECT l.campaign_id, l.lead_id, l.tenant_id, l.phone, l.attempts, l.priority, l.next_eligible_at, l.state
        FROM leads l
        JOIN campaigns c ON l.campaign_id = c.campaign_id
        WHERE l.tenant_id = ?
          AND l.campaign_id IN ({placeholders})
          AND l.state IN ('CALLBACK_WAIT', 'RETRY_WAIT', 'PENDING')
          AND l.attempts < c.max_attempts
          AND (l.next_eligible_at IS NULL OR l.next_eligible_at <= ?)
          AND l.phone NOT IN (SELECT phone FROM dnd_phones)
          AND l.phone NOT IN (SELECT phone FROM leads WHERE state IN ('RESERVING', 'IN_FLIGHT'))
        ORDER BY l.priority DESC,
                 CASE WHEN l.next_eligible_at IS NULL THEN 1 ELSE 0 END,
                 l.next_eligible_at ASC,
                 l.lead_id ASC
        LIMIT ?;
    """
    params = [tenant_id] + open_campaign_ids + [sim_now_iso, limit]
    return conn.execute(query, params).fetchall()


def reserve_lead(
    conn: sqlite3.Connection,
    campaign_id: str,
    lead_id: str,
    tenant_id: str,
    phone: str,
    attempt_no: int,
    max_concurrent: int,
) -> Optional[str]:
    """
    Phase 1 of Two-Phase Reservation:
    Atomically verify tenant concurrency and phone lock, then commit RESERVING state.
    Returns request_id if reserved, or None if concurrency/phone lock check fails.
    """
    request_id = f"{campaign_id}:{lead_id}:{attempt_no}"
    now_real = time.time()
    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        conn.execute("BEGIN IMMEDIATE;")
        # 1. Verify tenant limit
        tenant_active = conn.execute(
            """
            SELECT COUNT(*) as cnt FROM leads
            WHERE tenant_id = ? AND state IN ('RESERVING', 'IN_FLIGHT');
            """,
            (tenant_id,),
        ).fetchone()["cnt"]

        if tenant_active >= max_concurrent:
            conn.execute("ROLLBACK;")
            return None

        # 2. Verify phone lock and DND
        phone_conflict = conn.execute(
            """
            SELECT COUNT(*) as cnt FROM leads
            WHERE phone = ? AND state IN ('RESERVING', 'IN_FLIGHT');
            """,
            (phone,),
        ).fetchone()["cnt"]

        if phone_conflict > 0:
            conn.execute("ROLLBACK;")
            return None

        is_dnd = conn.execute(
            "SELECT 1 FROM dnd_phones WHERE phone = ?;", (phone,)
        ).fetchone()
        if is_dnd:
            # Mark lead as dnd_blocked immediately
            conn.execute(
                """
                UPDATE leads
                SET state = 'dnd_blocked', updated_at = ?
                WHERE campaign_id = ? AND lead_id = ?;
                """,
                (now_iso, campaign_id, lead_id),
            )
            conn.execute("COMMIT;")
            return None

        # 3. Commit reservation
        cursor = conn.execute(
            """
            UPDATE leads
            SET state = 'RESERVING',
                active_request_id = ?,
                reserving_since_real_time = ?,
                updated_at = ?
            WHERE campaign_id = ? AND lead_id = ?
              AND state IN ('CALLBACK_WAIT', 'RETRY_WAIT', 'PENDING');
            """,
            (request_id, now_real, now_iso, campaign_id, lead_id),
        )

        if cursor.rowcount == 1:
            conn.execute("COMMIT;")
            return request_id
        else:
            conn.execute("ROLLBACK;")
            return None
    except Exception:
        conn.execute("ROLLBACK;")
        raise


def confirm_call_placed(
    conn: sqlite3.Connection,
    campaign_id: str,
    lead_id: str,
    request_id: str,
    call_sid: str,
    attempt_no: int,
) -> bool:
    """
    Phase 2 of Two-Phase Reservation:
    Transition lead from RESERVING to IN_FLIGHT with call_sid.
    Safe if webhook already arrived: preserves terminal/wait state if already processed.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        conn.execute("BEGIN IMMEDIATE;")
        cursor = conn.execute(
            """
            UPDATE leads
            SET state = 'IN_FLIGHT',
                active_call_sid = ?,
                attempts = MAX(attempts, ?),
                updated_at = ?
            WHERE campaign_id = ? AND lead_id = ?
              AND state = 'RESERVING' AND active_request_id = ?;
            """,
            (call_sid, attempt_no, now_iso, campaign_id, lead_id, request_id),
        )
        conn.execute("COMMIT;")
        return cursor.rowcount == 1
    except Exception:
        conn.execute("ROLLBACK;")
        raise


def rollback_reservation(
    conn: sqlite3.Connection,
    campaign_id: str,
    lead_id: str,
    request_id: str,
) -> bool:
    """
    Roll back a lead from RESERVING to its prior wait state on 503 or confirmed 404.
    Frees tenant slot and phone lock.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        conn.execute("BEGIN IMMEDIATE;")
        lead = conn.execute(
            "SELECT last_status, last_disposition FROM leads WHERE campaign_id = ? AND lead_id = ?;",
            (campaign_id, lead_id),
        ).fetchone()

        if not lead:
            conn.execute("ROLLBACK;")
            return False

        if lead["last_disposition"] == "callback_requested":
            target_state = "CALLBACK_WAIT"
            priority = 100
        elif lead["last_status"] in ("no_answer", "busy", "failed"):
            target_state = "RETRY_WAIT"
            priority = 50
        else:
            target_state = "PENDING"
            priority = 10

        cursor = conn.execute(
            """
            UPDATE leads
            SET state = ?,
                priority = ?,
                active_request_id = NULL,
                reserving_since_real_time = NULL,
                updated_at = ?
            WHERE campaign_id = ? AND lead_id = ?
              AND state = 'RESERVING' AND active_request_id = ?;
            """,
            (target_state, priority, now_iso, campaign_id, lead_id, request_id),
        )
        conn.execute("COMMIT;")
        return cursor.rowcount == 1
    except Exception:
        conn.execute("ROLLBACK;")
        raise


def process_call_result(
    conn: sqlite3.Connection,
    result: Dict[str, Any],
    retry_policy: Dict[str, int],
) -> bool:
    """
    Process call completion payload from webhook or background reconciler.
    Idempotent: duplicate deliveries return True without re-transitioning.
    Handles terminal dispositions, DND propagation, retries, and callback scheduling.
    """
    call_sid = result.get("call_sid")
    request_id = result.get("request_id")
    campaign_id = result.get("campaign_id")
    lead_id = result.get("lead_id")
    tenant_id = result.get("tenant_id")
    phone = result.get("phone")
    status = result.get("status")
    disposition = result.get("disposition")
    callback_at_str = result.get("callback_at")
    started_at = result.get("started_at")
    ended_at_str = result.get("ended_at")
    duration_sec = result.get("duration_sec", 0)

    now_utc_iso = datetime.now(timezone.utc).isoformat()

    try:
        conn.execute("BEGIN IMMEDIATE;")
        # Check if call already recorded
        existing = conn.execute(
            "SELECT 1 FROM call_attempts WHERE call_sid = ?;", (call_sid,)
        ).fetchone()
        if existing:
            conn.execute("COMMIT;")
            return True

        # Extract attempt_no from request_id if available, e.g. "cid:lid:3"
        attempt_no = 1
        if request_id and ":" in request_id:
            try:
                attempt_no = int(request_id.split(":")[-1])
            except ValueError:
                pass

        # Insert call record
        conn.execute(
            """
            INSERT OR IGNORE INTO call_attempts (
                call_sid, request_id, campaign_id, lead_id, tenant_id, phone,
                attempt_no, status, disposition, callback_at, started_at, ended_at,
                duration_sec, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                call_sid,
                request_id,
                campaign_id,
                lead_id,
                tenant_id,
                phone,
                attempt_no,
                status,
                disposition,
                callback_at_str,
                started_at,
                ended_at_str,
                duration_sec,
                now_utc_iso,
            ),
        )

        # Fetch current lead state and campaign config
        lead = conn.execute(
            "SELECT attempts, state FROM leads WHERE campaign_id = ? AND lead_id = ?;",
            (campaign_id, lead_id),
        ).fetchone()

        camp = conn.execute(
            "SELECT max_attempts FROM campaigns WHERE campaign_id = ?;",
            (campaign_id,),
        ).fetchone()
        max_attempts = camp["max_attempts"] if camp else 3

        lead_attempts = max(lead["attempts"] if lead else 0, attempt_no)

        # Parse ended_at for retry cooldown
        ended_at_dt = None
        if ended_at_str:
            try:
                ended_at_dt = datetime.fromisoformat(ended_at_str).astimezone(IST)
            except Exception:
                pass

        # Determine terminal state or next wait state
        next_eligible_at_str: Optional[str] = None
        priority = 10

        # R7 & R8: Terminal dispositions (interested, not_interested, wrong_number, dnd)
        if disposition in TERMINAL_DISPOSITIONS:
            new_state = "completed"
            if disposition == "dnd":
                # R8: Global DND propagation
                conn.execute(
                    "INSERT OR IGNORE INTO dnd_phones (phone, created_at) VALUES (?, ?);",
                    (phone, now_utc_iso),
                )
                # Mark other non-terminal leads with this phone as dnd_blocked
                conn.execute(
                    """
                    UPDATE leads
                    SET state = 'dnd_blocked', updated_at = ?
                    WHERE phone = ?
                      AND NOT (campaign_id = ? AND lead_id = ?)
                      AND state IN ('PENDING', 'RETRY_WAIT', 'CALLBACK_WAIT');
                    """,
                    (now_utc_iso, phone, campaign_id, lead_id),
                )
        # R4 & R9: Max attempts exceeded cap
        elif lead_attempts >= max_attempts:
            new_state = "exhausted"
        else:
            # Under max attempts: check if phone was marked DND by another call
            is_dnd = conn.execute(
                "SELECT 1 FROM dnd_phones WHERE phone = ?;", (phone,)
            ).fetchone()
            if is_dnd:
                new_state = "dnd_blocked"
            elif disposition == "callback_requested" and callback_at_str:
                # R6 & R9: Callback requested
                new_state = "CALLBACK_WAIT"
                priority = 100
                cb_dt = datetime.fromisoformat(callback_at_str).astimezone(IST)
                # +15 second buffer to guarantee call is never placed before callback_at
                next_eligible_at_str = (cb_dt + timedelta(seconds=15)).isoformat()
            elif status in ("no_answer", "busy", "failed"):
                # R5: Retry cooldowns
                new_state = "RETRY_WAIT"
                priority = 50
                delay_mins = retry_policy.get(f"{status}_minutes", 60)
                base_dt = ended_at_dt or datetime.now(IST)
                next_eligible_at_str = (base_dt + timedelta(minutes=delay_mins, seconds=15)).isoformat()
            else:
                new_state = "exhausted"

        conn.execute(
            """
            UPDATE leads
            SET state = ?,
                attempts = ?,
                priority = ?,
                next_eligible_at = ?,
                active_request_id = NULL,
                active_call_sid = NULL,
                reserving_since_real_time = NULL,
                last_status = ?,
                last_disposition = ?,
                last_ended_at = ?,
                last_callback_at = ?,
                last_duration_sec = ?,
                updated_at = ?
            WHERE campaign_id = ? AND lead_id = ?;
            """,
            (
                new_state,
                lead_attempts,
                priority,
                next_eligible_at_str,
                status,
                disposition,
                ended_at_str,
                callback_at_str,
                duration_sec,
                now_utc_iso,
                campaign_id,
                lead_id,
            ),
        )
        conn.execute("COMMIT;")
        return True
    except Exception:
        conn.execute("ROLLBACK;")
        raise


def get_stale_reserving_leads(
    conn: sqlite3.Connection,
    max_age_seconds: float = 3.0,
) -> List[sqlite3.Row]:
    """Fetch leads in RESERVING state older than max_age_seconds for reconciliation."""
    threshold = time.time() - max_age_seconds
    return conn.execute(
        """
        SELECT campaign_id, lead_id, tenant_id, phone, attempts, active_request_id, reserving_since_real_time
        FROM leads
        WHERE state = 'RESERVING' AND reserving_since_real_time <= ?;
        """,
        (threshold,),
    ).fetchall()


def get_in_flight_leads(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    """Fetch all leads currently IN_FLIGHT with active_call_sid."""
    return conn.execute(
        """
        SELECT campaign_id, lead_id, tenant_id, phone, attempts, active_request_id, active_call_sid
        FROM leads
        WHERE state = 'IN_FLIGHT' AND active_call_sid IS NOT NULL;
        """
    ).fetchall()


def are_all_leads_terminal(conn: sqlite3.Connection) -> Tuple[bool, int, int]:
    """
    Check if all leads are in terminal state (completed, exhausted, dnd_blocked).
    Returns (all_done, active_count, non_terminal_count).
    """
    active_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM leads WHERE state IN ('RESERVING', 'IN_FLIGHT');"
    ).fetchone()["cnt"]

    non_terminal_count = conn.execute(
        """
        SELECT COUNT(*) as cnt FROM leads
        WHERE state NOT IN ('completed', 'exhausted', 'dnd_blocked');
        """
    ).fetchone()["cnt"]

    return (active_count == 0 and non_terminal_count == 0), active_count, non_terminal_count


def export_final_state_csv(conn: sqlite3.Connection, output_path: str) -> None:
    """
    Export final_state.csv sorted by campaign_id, lead_id.
    Matches the exact specification contract.
    """
    rows = conn.execute(
        """
        SELECT campaign_id, lead_id, state as final_state, attempts,
               COALESCE(last_status, '') as last_status,
               COALESCE(last_disposition, '') as last_disposition
        FROM leads
        ORDER BY campaign_id ASC, lead_id ASC;
        """
    ).fetchall()

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["campaign_id", "lead_id", "final_state", "attempts", "last_status", "last_disposition"])
        for row in rows:
            writer.writerow([
                row["campaign_id"],
                row["lead_id"],
                row["final_state"],
                row["attempts"],
                row["last_status"],
                row["last_disposition"],
            ])

