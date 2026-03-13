"""
Connection test — run once after Schwab authentication to verify
the full data pipeline: .env → config → schwab-py → live API → DB.
"""

from datetime import datetime
from config import SCHWAB_API_KEY, SCHWAB_APP_SECRET, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
import schwab
from sqlalchemy import create_engine, text

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def test_database_connection():
    print("\n── Test 1: Database Connection ──────────────────")
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM events")).scalar()
            tables = conn.execute(text(
                "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename"
            )).fetchall()
        print(f"✓ Database connected")
        print(f"  Tables: {[t[0] for t in tables]}")
        print(f"  Events rows: {result}")
    except Exception as e:
        print(f"✗ Database connection failed: {e}")

def test_ollama_connection():
    print("\n── Test 2: Ollama LLM ───────────────────────────")
    try:
        import ollama
        response = ollama.chat(
            model="llama3.2:3b",
            messages=[{"role": "user", "content": "Reply with only the word: READY"}]
        )
        reply = response['message']['content'].strip()
        print(f"✓ Ollama responding — model says: {reply}")
    except Exception as e:
        print(f"✗ Ollama connection failed: {e}")

def test_schwab_connection():
    print("\n── Test 3: Schwab Authentication ────────────────")
    try:
        client = schwab.auth.client_from_token_file(
            token_path="token.json",
            api_key=SCHWAB_API_KEY,
            app_secret=SCHWAB_APP_SECRET,
        )
        print("✓ Token file loaded successfully")
        return client
    except Exception as e:
        print(f"✗ Authentication failed: {e}")
        return None

def test_account_data(client):
    print("\n── Test 4: Account Data ─────────────────────────")
    try:
        response = client.get_accounts()
        accounts = response.json()
        print(f"✓ Account data received — {len(accounts)} account(s) found")
        for acc in accounts:
            acct_num = acc.get('securitiesAccount', {}).get('accountNumber', 'unknown')
            nav = acc.get('securitiesAccount', {}).get('currentBalances', {}).get('liquidationValue', 0)
            print(f"  Account: ...{str(acct_num)[-4:]} | NAV: ${nav:,.2f}")
        return accounts
    except Exception as e:
        print(f"✗ Account data failed: {e}")
        return None

def test_options_chain(client):
    print("\n── Test 5: Live Options Chain (SPY) ─────────────")
    try:
        response = client.get_option_chain(
            symbol="SPY",
            contract_type=client.Options.ContractType.ALL,
            include_underlying_quote=True,
        )
        chain = response.json()
        puts  = chain.get('putExpDateMap',  {})
        calls = chain.get('callExpDateMap', {})
        total_puts  = sum(len(v) for v in puts.values())
        total_calls = sum(len(v) for v in calls.values())
        underlying_price = chain.get('underlyingPrice', 'N/A')
        print(f"✓ SPY options chain received")
        print(f"  Underlying price: ${underlying_price}")
        print(f"  Put expirations:  {total_puts}")
        print(f"  Call expirations: {total_calls}")
    except Exception as e:
        print(f"✗ Options chain failed: {e}")

if __name__ == "__main__":
    print("=" * 52)
    print("  Spark Agent — Connection Test")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 52)

    test_database_connection()
    test_ollama_connection()
    client = test_schwab_connection()

    if client:
        test_account_data(client)
        test_options_chain(client)

    print("\n" + "=" * 52)
    print("  If all 5 tests show ✓ your pipeline is live.")
    print("=" * 52)