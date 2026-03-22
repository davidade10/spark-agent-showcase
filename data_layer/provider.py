from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# schwab-py
from schwab.auth import easy_client

from config import (
    SCHWAB_API_KEY,
    SCHWAB_APP_SECRET,
    SCHWAB_CALLBACK_URL,
    SCHWAB_TOKEN_PATH,
    SCHWAB_AUTH_INTERACTIVE,
)

load_dotenv()


class AuthenticationRequiredError(RuntimeError):
    """
    Raised when Schwab auth would require an interactive/browser flow but the
    caller explicitly disallows interactive auth (e.g. unattended startup).
    """


def _should_allow_interactive(interactive: Optional[bool]) -> bool:
    if interactive is True:
        return True
    if interactive is False:
        return False

    # interactive is None → decide from env/config + whether stdin is usable
    mode = (SCHWAB_AUTH_INTERACTIVE or "auto").strip().lower()
    if mode in ("1", "true", "yes", "y", "on"):
        return True
    if mode in ("0", "false", "no", "n", "off"):
        return False
    # auto: only allow if stdin is a TTY (so input() won't EOF) and not explicitly suppressed
    try:
        return bool(sys.stdin) and sys.stdin.isatty()
    except Exception:
        return False


def get_schwab_client(*, interactive: Optional[bool] = None):
    """
    Create (or reuse) a Schwab API client using OAuth.

    - If interactive auth is allowed, schwab-py's browser-assisted login flow
      may be used when needed.
    - If interactive auth is not allowed, this will never prompt or open a browser.
      If a token refresh is impossible without user interaction, raises
      AuthenticationRequiredError (instead of triggering input() → EOF).
    """
    api_key = SCHWAB_API_KEY or os.getenv("SCHWAB_API_KEY")
    app_secret = SCHWAB_APP_SECRET or os.getenv("SCHWAB_APP_SECRET")
    if not api_key or not app_secret:
        raise RuntimeError("Missing SCHWAB_API_KEY or SCHWAB_APP_SECRET in environment")

    callback_url = SCHWAB_CALLBACK_URL or os.getenv("SCHWAB_CALLBACK_URL", "https://127.0.0.1:8182/")
    token_path = SCHWAB_TOKEN_PATH or os.getenv("SCHWAB_TOKEN_PATH", "token.json")

    allow_interactive = _should_allow_interactive(interactive)

    # Pre-check: if we're non-interactive and there's no token file, fail fast
    # with a clear operator action instead of letting schwab-py attempt input().
    if not allow_interactive and not Path(token_path).exists():
        raise AuthenticationRequiredError(
            f"Schwab token missing at {token_path!r}. Manual re-auth required: "
            "run `python -m data_layer.provider` in an interactive terminal."
        )

    try:
        return easy_client(
            api_key=api_key,
            app_secret=app_secret,
            callback_url=callback_url,
            token_path=token_path,
            interactive=allow_interactive,
        )
    except EOFError as e:
        # Defensive: if any code path still tries to read stdin, convert it to a
        # clear "auth required" error so background startup doesn't crash.
        raise AuthenticationRequiredError(
            "Schwab authentication requires interactive re-auth, but stdin is not available. "
            "Run `python -m data_layer.provider` manually to refresh token.json."
        ) from e

def smoke_test():
    c = get_schwab_client(interactive=True)
    r1 = c.get_instruments(
        symbols=["SPY"],
        projection=c.Instrument.Projection.FUNDAMENTAL
    )
    r1.raise_for_status()
    print("Fundamentals OK")
    print("token.json created — Schwab authentication successful")

if __name__ == "__main__":
    smoke_test()