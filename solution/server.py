"""
FastAPI Webhook Receiver & Dispatcher Lifespan Manager.
Verifies X-AU-Signature HMAC, processes incoming call completions idempotently,
and manages the background dispatcher and reconciler loops.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse

from .config import Config
from .db import are_all_leads_terminal, get_connection, process_call_result
from .dispatcher import Dispatcher

logger = logging.getLogger("solution.server")


def create_app(
    config: Optional[Config] = None,
    dispatcher: Optional[Dispatcher] = None,
) -> FastAPI:
    cfg = config or Config()
    disp = dispatcher or Dispatcher(cfg)

    # Pre-load fixtures so webhook_secret is ready
    disp.init_database()
    webhook_secret = disp.cfg_fixtures.get("webhook_secret", "whsec_au_assignment_local")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Startup: start dispatcher tasks
        await disp.start()
        clock_task = asyncio.create_task(disp.sync_clock_periodically())
        reconciler_task = asyncio.create_task(disp.reconciler_loop())

        async def scheduler_runner():
            while disp.is_running and not disp.is_finished:
                await disp.tick()
                await asyncio.sleep(cfg.poll_interval)

        scheduler_task = asyncio.create_task(scheduler_runner())

        yield

        # Shutdown
        disp.is_running = False
        clock_task.cancel()
        reconciler_task.cancel()
        scheduler_task.cancel()
        await disp.stop()

    app = FastAPI(
        title="Agentic Universe - Durable Campaign Dispatcher",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.dispatcher = disp
    app.state.config = cfg

    @app.post(cfg.webhook_path)
    async def handle_call_webhook(request: Request):
        raw_body = await request.body()
        sig_header = request.headers.get("X-AU-Signature")

        # Verify HMAC-SHA256 signature
        if not sig_header:
            logger.warning("Missing X-AU-Signature header on webhook request")
            raise HTTPException(status_code=401, detail="missing_signature")

        expected_sig = hmac.new(
            webhook_secret.encode("utf-8"),
            raw_body,
            hashlib.sha256,
        ).hexdigest()

        if not hmac.compare_digest(sig_header, expected_sig):
            logger.warning("Invalid X-AU-Signature on webhook request")
            raise HTTPException(status_code=401, detail="invalid_signature")

        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except Exception:
            raise HTTPException(status_code=400, detail="invalid_json")

        conn = get_connection(cfg.db_path)
        try:
            process_call_result(conn, payload, disp.retry_policy)
        finally:
            conn.close()

        # Always return 200 OK for valid webhooks
        return JSONResponse({"status": "accepted", "call_sid": payload.get("call_sid")}, status_code=200)

    @app.get("/health")
    async def health():
        return {"status": "ok", "synced": disp.clock.synced}

    @app.get("/status")
    async def status():
        conn = get_connection(cfg.db_path)
        try:
            all_done, active_cnt, non_term_cnt = are_all_leads_terminal(conn)
            state_counts = conn.execute(
                "SELECT state, count(*) as count FROM leads GROUP BY state;"
            ).fetchall()
            return {
                "all_done": all_done,
                "active_calls": active_cnt,
                "non_terminal_leads": non_term_cnt,
                "states": {row["state"]: row["count"] for row in state_counts},
                "sim_now": disp.clock.now().isoformat(),
            }
        finally:
            conn.close()

    return app

