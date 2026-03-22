"""
data_layer/notifier.py

Thin Telegram notification utility.
All sends are best-effort — failures are logged but never raised to callers.
"""
from __future__ import annotations

import logging

import requests

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)

_TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
_TIMEOUT_SECONDS = 8


def send_telegram_msg(message: str) -> bool:
    """
    Send a Telegram message to the configured chat.

    Returns True on success, False on any failure (missing config, network
    error, non-2xx response). Never raises.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning(
            "Telegram send skipped — TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set"
        )
        return False

    url = _TELEGRAM_API.format(token=TELEGRAM_BOT_TOKEN)
    payload = {
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       message,
        "parse_mode": "Markdown",
    }

    try:
        resp = requests.post(url, json=payload, timeout=_TIMEOUT_SECONDS)
        if resp.status_code == 200:
            return True
        logger.warning(
            "Telegram send failed — HTTP %d: %s",
            resp.status_code,
            resp.text[:200],
        )
        return False
    except Exception as exc:
        logger.warning("Telegram send error — %s", exc)
        return False
