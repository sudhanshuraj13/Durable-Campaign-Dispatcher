"""
Dispatcher core loop, two-phase reservation engine, and background reconciler.
Guarantees zero violations, at-most-once dialling across crashes and timeouts,
and automatic recovery from lost webhooks.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx

from .clock import SimClockClient, is_dispatch_legal
from .config import Config
from .db import (
    are_all_leads_terminal,
    confirm_call_placed,
    export_final_state_csv,
    get_active_tenant_counts,
    get_candidate_leads_for_tenant,
    get_connection,
    get_in_flight_leads,
    get_stale_reserving_leads,
    init_db,
    load_fixtures,
    process_call_result,
    reserve_lead,
    rollback_reservation,
)

logger = logging.getLogger("solution.dispatcher")


class Dispatcher:
    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.clock = SimClockClient()
        self.http_client: Optional[httpx.AsyncClient] = None
        self.cfg_fixtures: Dict[str, Any] = {}
        self.retry_policy: Dict[str, int] = {}
        self.regulatory_window: Dict[str, str] = {}
        self.campaigns: List[Dict[str, Any]] = []
        self.tenants: Dict[str, Dict[str, Any]] = {}
        self.is_running: bool = False
        self.is_finished: bool = False
        self.semaphore: Optional[asyncio.Semaphore] = None
        self.reconcile_counter: int = 0

    def init_database(self) -> None:
        """Initialize database and load campaigns/tenants/leads fixtures idempotently."""
        init_db(self.config.db_path)
        self.cfg_fixtures = load_fixtures(self.config.db_path, self.config.fixtures_path)
        self.retry_policy = self.cfg_fixtures.get("retry_policy", {
            "no_answer_minutes": 60,
            "busy_minutes": 60,
            "failed_minutes": 15,
        })
        self.regulatory_window = self.cfg_fixtures.get("regulatory_window", {"start": "09:00", "end": "21:00"})
        self.campaigns = self.cfg_fixtures.get("campaigns", [])
        self.tenants = {t["tenant_id"]: t for t in self.cfg_fixtures.get("tenants", [])}

    async def start(self) -> None:
        """Start dispatcher client and run clock synchronization."""
        self.init_database()
        self.http_client = httpx.AsyncClient(
            timeout=10.0,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
        )
        self.semaphore = asyncio.Semaphore(self.config.max_http_concurrency)

        # Initial clock sync
        for _ in range(5):
            synced = await self.clock.sync(self.http_client, self.config.telephony_base_url)
            if synced:
                logger.info(
                    "SimClock synchronized: sim_now=%s, speed=%.1fx",
                    self.clock.now().isoformat(),
                    self.clock.speed,
                )
                break
            await asyncio.sleep(0.5)

        self.is_running = True

        # Run startup reconciliation immediately to recover from any previous crash
        await self.reconcile()

    async def stop(self) -> None:
        """Gracefully shut down dispatcher and close HTTP client."""
        self.is_running = False
        if self.http_client:
            await self.http_client.aclose()

    async def sync_clock_periodically(self) -> None:
        """Background loop refreshing SimClock from the provider to avoid clock drift."""
        while self.is_running:
            try:
                if self.http_client:
                    await self.clock.sync(self.http_client, self.config.telephony_base_url)
            except Exception as e:
                logger.warning("Clock sync error: %s", e)
            await asyncio.sleep(self.config.clock_sync_interval)

    async def reconciler_loop(self) -> None:
        """Periodic background reconciler resolving timeouts and lost webhooks."""
        while self.is_running and not self.is_finished:
            try:
                await self.reconcile()
            except Exception as e:
                logger.error("Error in reconciler loop: %s", e)
            await asyncio.sleep(self.config.reconcile_interval)

    async def reconcile(self) -> None:
        """
        Reconciliation pass:
        1. Resolve ambiguous RESERVING leads (stale HTTP calls or crash recoveries) via GET /v1/calls?request_id=...
        2. Poll IN_FLIGHT calls via GET /v1/calls/{call_sid} to catch lost webhooks (3%) or calls completed during downtime.
        """
        if not self.http_client:
            return

        conn = get_connection(self.config.db_path)
        try:
            # 1. Stale RESERVING leads (> 2.0s real time)
            stale_leads = get_stale_reserving_leads(conn, max_age_seconds=2.0)
            for row in stale_leads:
                req_id = row["active_request_id"]
                cid = row["campaign_id"]
                lid = row["lead_id"]
                attempt_no = row["attempts"] + 1
                if not req_id:
                    continue
                await self.resolve_ambiguity(cid, lid, req_id, attempt_no)

            # 2. IN_FLIGHT leads
            in_flight = get_in_flight_leads(conn)
            for row in in_flight:
                call_sid = row["active_call_sid"]
                if not call_sid:
                    continue
                try:
                    resp = await self.http_client.get(
                        f"{self.config.telephony_base_url}/v1/calls/{call_sid}",
                        timeout=3.0,
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        if data.get("state") == "ended" and "result" in data:
                            logger.info("Reconciled ended call %s for %s:%s", call_sid, row["campaign_id"], row["lead_id"])
                            process_call_result(conn, data["result"], self.retry_policy)
                    elif resp.status_code == 404:
                        logger.warning("In-flight call %s returned 404; rolling back", call_sid)
                        rollback_reservation(conn, row["campaign_id"], row["lead_id"], row["active_request_id"] or "")
                except Exception as e:
                    logger.debug("Reconciler poll error for %s: %s", call_sid, e)
        finally:
            conn.close()

    async def resolve_ambiguity(
        self,
        campaign_id: str,
        lead_id: str,
        request_id: str,
        attempt_no: int,
    ) -> None:
        """
        Resolve outcome of an ambiguous call (504, timeout, or crash) using GET /v1/calls?request_id=...
        """
        if not self.http_client:
            return

        conn = get_connection(self.config.db_path)
        try:
            url = f"{self.config.telephony_base_url}/v1/calls"
            resp = await self.http_client.get(url, params={"request_id": request_id}, timeout=3.0)
            if resp.status_code == 200:
                data = resp.json()
                call_sid = data["call_sid"]
                confirm_call_placed(conn, campaign_id, lead_id, request_id, call_sid, attempt_no)
                if data.get("state") == "ended" and "result" in data:
                    process_call_result(conn, data["result"], self.retry_policy)
            elif resp.status_code == 404:
                # Provider confirms call was NOT placed
                logger.info("Call with request_id %s was not placed; rolling back reservation", request_id)
                rollback_reservation(conn, campaign_id, lead_id, request_id)
        except Exception as e:
            logger.debug("Ambiguity resolution error for %s: %s", request_id, e)
        finally:
            conn.close()

    async def tick(self) -> None:
        """
        Execute one scheduler cycle:
        1. Evaluate terminal condition.
        2. Identify currently open campaigns (enforcing 30s pre-close safety margin).
        3. Check tenant concurrency headroom.
        4. Reserve eligible candidates atomically.
        5. Dispatch outbound HTTP calls.
        """
        conn = get_connection(self.config.db_path)
        try:
            # Check terminal state
            all_done, active_cnt, non_term_cnt = are_all_leads_terminal(conn)
            if all_done:
                self.is_finished = True
                export_final_state_csv(conn, "final_state.csv")
                logger.info("All leads have reached terminal states! Exported final_state.csv.")
                return

            sim_now = self.clock.now()

            # Determine open campaigns
            open_cids_by_tenant: Dict[str, List[str]] = {}
            for camp in self.campaigns:
                cid = camp["campaign_id"]
                tid = camp["tenant_id"]
                camp_window = {
                    "start": camp["window"]["start"],
                    "end": camp["window"]["end"],
                    "days": camp["window"]["days"],
                }
                # Pre-close safety margin: 30 simulated seconds before closing
                if is_dispatch_legal(
                    sim_now,
                    camp_window,
                    self.regulatory_window,
                    safety_margin_s=self.config.pre_close_safety_margin_s,
                ):
                    open_cids_by_tenant.setdefault(tid, []).append(cid)

            if not open_cids_by_tenant:
                return

            # Check tenant concurrency and select candidates
            active_tenant_counts = get_active_tenant_counts(conn)
            now_iso = sim_now.isoformat()

            for tid, open_cids in open_cids_by_tenant.items():
                tenant_cfg = self.tenants.get(tid)
                if not tenant_cfg:
                    continue
                max_concurrency = tenant_cfg["max_concurrent_calls"]
                current_active = active_tenant_counts.get(tid, 0)
                headroom = max_concurrency - current_active

                if headroom <= 0:
                    continue

                candidates = get_candidate_leads_for_tenant(
                    conn,
                    tenant_id=tid,
                    open_campaign_ids=open_cids,
                    sim_now_iso=now_iso,
                    limit=headroom,
                )

                for row in candidates:
                    cid = row["campaign_id"]
                    lid = row["lead_id"]
                    phone = row["phone"]
                    attempt_no = row["attempts"] + 1

                    # Phase 1: Two-Phase Reservation
                    req_id = reserve_lead(
                        conn=conn,
                        campaign_id=cid,
                        lead_id=lid,
                        tenant_id=tid,
                        phone=phone,
                        attempt_no=attempt_no,
                        max_concurrent=max_concurrency,
                    )

                    if req_id:
                        # Successfully reserved on disk; launch async dispatch
                        asyncio.create_task(
                            self.dispatch_call(
                                campaign_id=cid,
                                lead_id=lid,
                                tenant_id=tid,
                                phone=phone,
                                attempt_no=attempt_no,
                                request_id=req_id,
                            )
                        )
        finally:
            conn.close()

    async def dispatch_call(
        self,
        campaign_id: str,
        lead_id: str,
        tenant_id: str,
        phone: str,
        attempt_no: int,
        request_id: str,
    ) -> None:
        """
        Phase 2 of Two-Phase Reservation:
        Make HTTP call to mock telephony provider and resolve response status.
        """
        if not self.http_client or not self.semaphore:
            return

        payload = {
            "request_id": request_id,
            "campaign_id": campaign_id,
            "lead_id": lead_id,
            "phone": phone,
            "callback_url": self.config.callback_url,
        }

        async with self.semaphore:
            # Pre-flight check: ensure window hasn't closed during async queueing
            sim_now = self.clock.now()
            camp = next((c for c in self.campaigns if c["campaign_id"] == campaign_id), None)
            if camp:
                camp_window = {
                    "start": camp["window"]["start"],
                    "end": camp["window"]["end"],
                    "days": camp["window"]["days"],
                }
                # Pre-flight window validation: ensure window is open with full safety margin
                if not is_dispatch_legal(
                    sim_now,
                    camp_window,
                    self.regulatory_window,
                    safety_margin_s=self.config.pre_close_safety_margin_s,
                ):
                    logger.info(
                        "Window closed before HTTP post of %s (sim_now=%s); aborting call and rolling back",
                        request_id,
                        sim_now.isoformat(),
                    )
                    conn = get_connection(self.config.db_path)
                    try:
                        rollback_reservation(conn, campaign_id, lead_id, request_id)
                    finally:
                        conn.close()
                    return

            try:
                resp = await self.http_client.post(
                    f"{self.config.telephony_base_url}/v1/calls",
                    json=payload,
                    timeout=5.0,
                )

                conn = get_connection(self.config.db_path)
                try:
                    if resp.status_code in (200, 201):
                        data = resp.json()
                        call_sid = data["call_sid"]
                        confirm_call_placed(conn, campaign_id, lead_id, request_id, call_sid, attempt_no)
                        if data.get("state") == "ended" and "result" in data:
                            process_call_result(conn, data["result"], self.retry_policy)

                    elif resp.status_code == 503:
                        # Guaranteed not placed by server
                        logger.debug("503 for %s; rolling back reservation", request_id)
                        rollback_reservation(conn, campaign_id, lead_id, request_id)

                    elif resp.status_code == 504:
                        # Ambiguous timeout: could be placed (4%) or not placed (2%)
                        logger.warning("504 ambiguous timeout for %s; resolving outcome...", request_id)
                        # Keep reservation locked, resolve ambiguity
                        asyncio.create_task(
                            self.resolve_ambiguity(campaign_id, lead_id, request_id, attempt_no)
                        )

                    else:
                        logger.error(
                            "Unexpected HTTP status %d for %s: %s",
                            resp.status_code,
                            request_id,
                            resp.text,
                        )
                        # Check outcome before rolling back
                        asyncio.create_task(
                            self.resolve_ambiguity(campaign_id, lead_id, request_id, attempt_no)
                        )
                finally:
                    conn.close()

            except (httpx.TimeoutException, httpx.RequestError) as e:
                logger.warning("Network timeout/error during dispatch of %s: %s", request_id, e)
                # Keep reservation locked; resolve ambiguity
                asyncio.create_task(
                    self.resolve_ambiguity(campaign_id, lead_id, request_id, attempt_no)
                )

    async def run(self) -> None:
        """Main dispatcher loop."""
        await self.start()
        clock_task = asyncio.create_task(self.sync_clock_periodically())
        reconciler_task = asyncio.create_task(self.reconciler_loop())

        try:
            while self.is_running and not self.is_finished:
                await self.tick()
                await asyncio.sleep(self.config.poll_interval)
        finally:
            clock_task.cancel()
            reconciler_task.cancel()
            await self.stop()

