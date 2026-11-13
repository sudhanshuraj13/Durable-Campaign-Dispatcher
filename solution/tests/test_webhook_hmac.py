"""
Tests for Webhook Receiver security: HMAC-SHA256 signature verification and payload validation.
"""
import hashlib
import hmac
import json
import os
import tempfile

import httpx
import pytest

from solution.config import Config
from solution.db import get_connection, init_db, load_fixtures
from solution.dispatcher import Dispatcher
from solution.server import create_app


@pytest.fixture
def webhook_app():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    fixtures_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "assignment-b-workflow", "fixtures", "campaigns.json"
    )
    init_db(path)
    cfg = Config(db_path=path, fixtures_path=fixtures_path)
    disp = Dispatcher(cfg)
    app = create_app(cfg, disp)
    yield app, cfg, path
    try:
        os.remove(path)
        for ext in ("-wal", "-shm"):
            if os.path.exists(path + ext):
                os.remove(path + ext)
    except Exception:
        pass


@pytest.mark.asyncio
async def test_valid_hmac_signature_accepted(webhook_app):
    app, cfg, _ = webhook_app
    payload = {
        "event": "call.completed",
        "call_sid": "CA_TEST_HMAC_VALID",
        "request_id": "c_acme_renewals:ren_0001:1",
        "tenant_id": "acme_insurance",
        "campaign_id": "c_acme_renewals",
        "lead_id": "ren_0001",
        "phone": "+919876543210",
        "status": "completed",
        "disposition": "interested",
        "started_at": "2026-11-12T17:35:00+05:30",
        "ended_at": "2026-11-12T17:38:00+05:30",
        "duration_sec": 180,
    }
    raw_body = json.dumps(payload).encode("utf-8")
    secret = "whsec_au_assignment_local"
    sig = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/webhooks/call",
            content=raw_body,
            headers={"Content-Type": "application/json", "X-AU-Signature": sig},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "accepted"
        assert data["call_sid"] == "CA_TEST_HMAC_VALID"


@pytest.mark.asyncio
async def test_invalid_hmac_signature_rejected(webhook_app):
    app, cfg, _ = webhook_app
    payload = {"call_sid": "CA_FAKE", "status": "completed"}
    raw_body = json.dumps(payload).encode("utf-8")
    bad_sig = "deadbeef" * 8

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/webhooks/call",
            content=raw_body,
            headers={"Content-Type": "application/json", "X-AU-Signature": bad_sig},
        )
        assert resp.status_code == 401
        assert resp.json()["detail"] == "invalid_signature"


@pytest.mark.asyncio
async def test_missing_hmac_signature_rejected(webhook_app):
    app, cfg, _ = webhook_app
    payload = {"call_sid": "CA_FAKE"}
    raw_body = json.dumps(payload).encode("utf-8")

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/webhooks/call",
            content=raw_body,
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 401
        assert resp.json()["detail"] == "missing_signature"

