"""
Configuration settings for the Durable Campaign Dispatcher.
Supports environment variables and programmatic overrides.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class Config:
    telephony_base_url: str = os.getenv("TELEPHONY_BASE_URL", "http://127.0.0.1:8099")
    webhook_host: str = os.getenv("WEBHOOK_HOST", "127.0.0.1")
    webhook_port: int = int(os.getenv("WEBHOOK_PORT", "8200"))
    webhook_path: str = os.getenv("WEBHOOK_PATH", "/webhooks/call")
    callback_url: str = os.getenv("CALLBACK_URL", "http://127.0.0.1:8200/webhooks/call")
    db_path: str = os.getenv("DB_PATH", "dispatcher.db")
    fixtures_path: str = os.getenv(
        "FIXTURES_PATH",
        os.path.join(os.path.dirname(__file__), "..", "assignment-b-workflow", "fixtures", "campaigns.json")
    )
    poll_interval: float = float(os.getenv("POLL_INTERVAL", "0.05"))
    reconcile_interval: float = float(os.getenv("RECONCILE_INTERVAL", "0.5"))
    clock_sync_interval: float = float(os.getenv("CLOCK_SYNC_INTERVAL", "1.0"))
    pre_close_safety_margin_s: float = float(os.getenv("PRE_CLOSE_SAFETY_MARGIN_S", "300.0"))
    max_http_concurrency: int = int(os.getenv("MAX_HTTP_CONCURRENCY", "16"))

