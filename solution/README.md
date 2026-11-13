# Solution: Durable Campaign Dispatcher

Production-grade implementation of **Assignment B (Durable Campaign Dispatcher)**.

---

## Architecture Overview

The system is designed as a resilient, event-driven dispatcher backed by **SQLite in WAL mode** (`PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;`). It provides strict ACID durability, at-most-once dialling guarantees, and zero-loss crash recovery without requiring external database or message broker containers.

```
solution/
├── __init__.py           # Package marker
├── config.py             # Config dataclass, CLI args & environment variables
├── clock.py              # SimClock client, IST (UTC+05:30) extrapolation & window math
├── db.py                 # SQLite schema, WAL connection pool, atomic state transitions & DND
├── dispatcher.py         # Scheduler tick loop, Two-Phase reservation & background reconciler
├── server.py             # FastAPI webhook receiver & HMAC-SHA256 signature verification
├── main.py               # CLI entrypoint, live progress monitor & automated parity checker
└── tests/                # 23 automated unit & integration tests
    ├── __init__.py
    ├── test_windows.py
    ├── test_state_machine.py
    ├── test_webhook_hmac.py
    └── test_idempotency_and_recovery.py
```

---

## Key Invariants Enforced

| Invariant | Implementation Mechanism | Violation Avoided |
|---|---|---|
| **Simulated Clock (IST)** | Polls `/v1/clock`, maintains local linear extrapolation `sim_epoch + (wall_now - real_epoch) * speed` in `UTC+05:30`. | Time drift / out-of-order execution |
| **Dual Calling Windows** | Must satisfy both regulatory (`09:00–21:00`) and campaign window/days. Enforces a **300.0s pre-close safety margin** and atomic pre-flight check inside the semaphore right before `client.post()`. | `outside_calling_window` (HTTP 422) |
| **Global Concurrency Lock** | Single SQLite lock table (`active_phone`) prevents concurrent dials to the same phone number across all tenants and campaigns. | `concurrent_dial_same_phone` (HTTP 409) |
| **Tenant Concurrency Cap** | Sum of `RESERVING` + `IN_FLIGHT` calls is bounded by tenant's `max_concurrent_calls`. Slots are never released until confirmed ended or 404. | `tenant_concurrency_exceeded` (HTTP 429) |
| **Cross-Campaign DND** | When any call finishes with disposition `dnd`, phone is recorded in `dnd_phones` and all other non-terminal leads sharing that phone are transitioned to `dnd_blocked`. | `dnd_violation` (HTTP 422) |
| **Terminal Dispositions** | `interested`, `not_interested`, `wrong_number`, `dnd` immediately transition lead to `completed`. | `dial_after_terminal` (HTTP 422) |
| **Max Attempts Cap** | Leads reaching `attempts >= max_attempts` without a terminal disposition immediately transition to `exhausted`. | `max_attempts_exceeded` (HTTP 422) |
| **Retry & Callback Math** | Retry cooldowns calculated from provider `ended_at + policy + 15s buffer`. Callbacks scheduled from provider `callback_at + 15s buffer` with priority 100. | `retry_too_early` / `callback_too_early` (HTTP 422) |

---

## Two-Phase Reservation Pattern (At-Most-Once Dialling)

To guarantee that no lead is dialed twice across network failures, timeouts, or sudden `kill -9` termination:

```
[Candidate Lead]
       │
       ▼
[Phase 1: DB Reservation]
  • Verify tenant headroom & phone availability atomically
  • Transition state to RESERVING
  • Commit active_request_id = "{campaign_id}:{lead_id}:{attempt_no}" to disk
       │
       ▼
[Pre-Flight Window Check]
  • Immediately before network call, verify window is still open (with safety margin)
  • If window closed, rollback reservation cleanly
       │
       ▼
[Phase 2: HTTP POST /v1/calls]
  ├── 201 Created / 200 OK  ──► Transition to IN_FLIGHT, record call_sid
  ├── 503 Unavailable       ──► Rollback reservation to wait state (call was not placed)
  └── 504 Timeout / Crash   ──► Keep reservation held!
                                  Async GET /v1/calls?request_id=...
                                  ├── 200 OK  ──► Call was placed! Move to IN_FLIGHT
                                  └── 404     ──► Call not placed! Safe rollback
```

---

## Background Reconciler

The reconciler loop runs every 0.5s:
1. **Ambiguity Resolution:** Detects stale `RESERVING` leads without `call_sid` and resolves them via `GET /v1/calls?request_id=...`.
2. **Lost Webhook Recovery:** Polls `GET /v1/calls/{call_sid}` for any `IN_FLIGHT` call, recovering from the 3% dropped webhooks or webhooks dropped while the dispatcher was down during a `kill -9` crash.

---

## Reproduction & Verification Guide

### 1. Run Automated Unit Tests
```bash
pytest -v
```
*(All 23 tests pass in < 1 second)*

### 2. Run Full End-to-End Simulation
```bash
# Terminal 1: Start Mock Telephony Server (Port 8099, Speed 600)
# Windows:
$env:PYTHONPATH='assignment-b-workflow'
python -m mock_telephony --speed 600 --port 8099
# Linux/macOS:
PYTHONPATH=assignment-b-workflow python -m mock_telephony --speed 600 --port 8099

# Terminal 2: Start Dispatcher
python -m solution.main --telephony-url http://127.0.0.1:8099 --webhook-port 8200 --db-path dispatcher.db
```

### 3. Chaos Crash-Recovery Test (`kill -9`)
1. While the dispatcher is actively placing calls (e.g., at call 40–50), terminate the dispatcher process abruptly (`Ctrl+C` or `kill -9`).
2. Query `dispatcher.db` to observe that in-flight calls and reservations survived cleanly in SQLite WAL mode.
3. Restart the dispatcher against the same database:
   ```bash
   python -m solution.main --telephony-url http://127.0.0.1:8099 --webhook-port 8200 --db-path dispatcher.db
   ```
4. The dispatcher will instantly reconcile in-flight calls on boot and continue through to 100% completion with 0 violations and 100% lead parity.
