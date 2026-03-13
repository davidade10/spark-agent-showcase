import os
from dotenv import load_dotenv

# schwab-py
from schwab.auth import easy_client

load_dotenv()

def get_schwab_client():
    """
    Creates (or reuses) a Schwab API client using OAuth.
    On first run it opens a browser for login/consent and writes token.json.
    On later runs it reuses token.json.
    """

    api_key = os.getenv("SCHWAB_API_KEY")
    app_secret = os.getenv("SCHWAB_APP_SECRET")

    if not api_key or not app_secret:
        raise RuntimeError("Missing SCHWAB_API_KEY or SCHWAB_APP_SECRET in .env")

    # These must match what you registered in Schwab Developer Portal
    callback_url = os.getenv("SCHWAB_CALLBACK_URL", "https://127.0.0.1:8182/")

    # Token file location (keep it out of git; you already ignore token.json)
    token_path = os.getenv("SCHWAB_TOKEN_PATH", "token.json")

    return easy_client(
        api_key=api_key,
        app_secret=app_secret,
        callback_url=callback_url,
        token_path=token_path,
    )

def smoke_test():
    c = get_schwab_client()
    r1 = c.get_instruments(
        symbols=["SPY"],
        projection=c.Instrument.Projection.FUNDAMENTAL
    )
    r1.raise_for_status()
    print("Fundamentals OK")
    print("token.json created — Schwab authentication successful")

if __name__ == "__main__":
    smoke_test()