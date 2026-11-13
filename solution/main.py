"""
CLI Entrypoint for Durable Campaign Dispatcher.
Runs the FastAPI webhook receiver and dispatcher engine until all leads reach terminal state.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time

import httpx
import uvicorn

from .config import Config
from .db import export_final_state_csv, get_connection
from .dispatcher import Dispatcher
from .server import create_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("solution.main")


async def monitor_completion(dispatcher: Dispatcher, config: Config, server: uvicorn.Server):
    """
    Monitor dispatcher progress. Once all leads reach terminal states:
    - Query /admin/report and /admin/leads from mock telephony server
    - Validate violation_count == 0
    - Verify lead parity against mock telephony view
    - Trigger graceful server shutdown
    """
    print("\n--- Dispatcher running. Waiting for campaigns to complete... ---")
    start_real = time.time()

    while not dispatcher.is_finished:
        await asyncio.sleep(1.0)
        conn = get_connection(config.db_path)
        try:
            counts = conn.execute(
                "SELECT state, COUNT(*) as cnt FROM leads GROUP BY state;"
            ).fetchall()
            status_map = {row["state"]: row["cnt"] for row in counts}
            active = status_map.get("RESERVING", 0) + status_map.get("IN_FLIGHT", 0)
            completed = status_map.get("completed", 0)
            exhausted = status_map.get("exhausted", 0)
            dnd_blocked = status_map.get("dnd_blocked", 0)
            pending = status_map.get("PENDING", 0)
            retry_wait = status_map.get("RETRY_WAIT", 0)
            cb_wait = status_map.get("CALLBACK_WAIT", 0)
            total_terminal = completed + exhausted + dnd_blocked
            total_waiting = pending + retry_wait + cb_wait
            print(
                f"[{time.strftime('%H:%M:%S')}] SimNow: {dispatcher.clock.now().strftime('%Y-%m-%d %H:%M:%S')} | "
                f"Terminal: {total_terminal}/200 (comp:{completed}, exh:{exhausted}, dnd:{dnd_blocked}) | "
                f"Waiting: {total_waiting} (pend:{pending}, ret:{retry_wait}, cb:{cb_wait}) | "
                f"Active: {active}"
            )
        finally:
            conn.close()

    elapsed = time.time() - start_real
    print(f"\n==================================================================")
    print(f" ALL LEADS REACHED TERMINAL STATE IN {elapsed:.2f} REAL SECONDS! ")
    print(f"==================================================================\n")

    conn = get_connection(config.db_path)
    try:
        export_final_state_csv(conn, "final_state.csv")
        print("Generated final_state.csv successfully.")
    finally:
        conn.close()

    # Query mock telephony admin reports
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            report_resp = await client.get(f"{config.telephony_base_url}/admin/report")
            if report_resp.status_code == 200:
                report = report_resp.json()
                print("\n--- /admin/report from Mock Telephony Provider ---")
                print(f"Violation Count: {report.get('violation_count')}")
                if report.get("violation_count", 0) > 0:
                    print(f"Violations by type: {report.get('violations_by_type')}")
                    for v in report.get("violations", []):
                        print(f"  - {v}")
                else:
                    print("SUCCESS: 0 VIOLATIONS RECORDED BY PROVIDER!")
                print(f"Total Calls: {report.get('calls_total')}")
                print(f"Calls by Status: {report.get('calls_by_status')}")
                print(f"Max Concurrency Seen: {report.get('max_concurrency_seen')}")
                print(f"Callback Lateness (minutes): {report.get('callback_lateness_minutes')}")
                print(f"Webhook Stats: {report.get('webhooks')}")

            leads_resp = await client.get(f"{config.telephony_base_url}/admin/leads")
            if leads_resp.status_code == 200:
                admin_leads = leads_resp.json()
                conn = get_connection(config.db_path)
                try:
                    local_rows = {
                        (row["campaign_id"], row["lead_id"]): row
                        for row in conn.execute(
                            "SELECT campaign_id, lead_id, state, attempts, last_status, last_disposition FROM leads;"
                        ).fetchall()
                    }
                finally:
                    conn.close()

                mismatches = []
                for al in admin_leads:
                    key = (al["campaign_id"], al["lead_id"])
                    local = local_rows.get(key)
                    if not local:
                        mismatches.append(f"Missing lead {key} in local database")
                    elif local["state"] != al["expected_state"]:
                        mismatches.append(
                            f"State mismatch on {key}: local={local['state']} vs admin={al['expected_state']}"
                        )

                print("\n--- Lead Parity Verification against /admin/leads ---")
                print(f"Total Leads Evaluated: {len(admin_leads)}")
                if not mismatches:
                    print("SUCCESS: 100% PARITY! All leads match provider expected final states.")
                else:
                    print(f"FAILED: {len(mismatches)} mismatches detected:")
                    for m in mismatches[:10]:
                        print(f"  - {m}")
        except Exception as e:
            print(f"Could not query /admin/report: {e}")

    server.should_exit = True


def main():
    parser = argparse.ArgumentParser(description="Agentic Universe - Durable Campaign Dispatcher")
    parser.add_argument("--telephony-url", default="http://127.0.0.1:8099", help="Mock telephony base URL")
    parser.add_argument("--webhook-host", default="127.0.0.1", help="Webhook receiver bind host")
    parser.add_argument("--webhook-port", type=int, default=8200, help="Webhook receiver bind port")
    parser.add_argument("--db-path", default="dispatcher.db", help="SQLite database path")
    parser.add_argument(
        "--fixtures",
        default=os.path.join(
            os.path.dirname(__file__), "..", "assignment-b-workflow", "fixtures", "campaigns.json"
        ),
        help="Path to campaigns.json fixtures",
    )
    parser.add_argument("--safety-margin", type=float, default=300.0, help="Simulated seconds before window close")
    parser.add_argument("--poll-interval", type=float, default=0.05, help="Scheduler tick interval in seconds")
    parser.add_argument("--serve-only", action="store_true", help="Run webhook receiver without auto-stop monitor")

    args = parser.parse_args()

    cfg = Config(
        telephony_base_url=args.telephony_url,
        webhook_host=args.webhook_host,
        webhook_port=args.webhook_port,
        callback_url=f"http://{args.webhook_host}:{args.webhook_port}/webhooks/call",
        db_path=args.db_path,
        fixtures_path=args.fixtures,
        poll_interval=args.poll_interval,
        pre_close_safety_margin_s=args.safety_margin,
    )

    disp = Dispatcher(cfg)
    app = create_app(cfg, disp)

    server = uvicorn.Server(
        uvicorn.Config(
            app=app,
            host=cfg.webhook_host,
            port=cfg.webhook_port,
            log_level="warning",
        )
    )

    async def run_both():
        server_task = asyncio.create_task(server.serve())
        if not args.serve_only:
            asyncio.create_task(monitor_completion(disp, cfg, server))
        await server_task

    asyncio.run(run_both())


if __name__ == "__main__":
    main()

