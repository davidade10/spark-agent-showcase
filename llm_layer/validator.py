from __future__ import annotations

import json
from typing import Any, Dict, Optional


REQUIRED_TOP_LEVEL = {
    "recommendation",
    "confidence",
    "summary",
    "market_environment",
    "rationale",
    "setup_specific_risks",
    "numbers_used",
    "conditions_if_conditional",
    "red_flags",
}

ALLOWED_RECOMMENDATIONS = {"yes", "no", "conditional"}


def _extract_json_object(raw: str) -> str:
    """
    Ollama sometimes returns leading/trailing whitespace. In worst cases it can
    wrap JSON with text; this tries to extract the first {...} block.
    """
    raw = raw.strip()
    if raw.startswith("{") and raw.endswith("}"):
        return raw

    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("No JSON object found in model output")
    return raw[start : end + 1]


def validate_trade_card(raw_text: str) -> Dict[str, Any]:
    js = _extract_json_object(raw_text)
    obj = json.loads(js)

    if not isinstance(obj, dict):
        raise ValueError("Trade card must be a JSON object")

    missing = REQUIRED_TOP_LEVEL - set(obj.keys())
    if missing:
        raise ValueError(f"Missing required fields: {sorted(missing)}")

    rec = obj.get("recommendation")
    if rec not in ALLOWED_RECOMMENDATIONS:
        raise ValueError(f"Invalid recommendation: {rec}")

    conf = obj.get("confidence")
    if not isinstance(conf, (int, float)) or not (0.0 <= float(conf) <= 1.0):
        raise ValueError("confidence must be a number in [0,1]")

    # Basic type checks
    for k in ("rationale", "setup_specific_risks", "conditions_if_conditional", "red_flags"):
        if not isinstance(obj.get(k), list):
            raise ValueError(f"{k} must be a list")

    if not isinstance(obj.get("numbers_used"), dict):
        raise ValueError("numbers_used must be an object")

    return obj