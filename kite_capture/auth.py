import os
import sys
import re
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from kiteconnect import KiteConnect
from dotenv import load_dotenv, set_key

ENV_PATH = Path(__file__).parent / ".env"
load_dotenv(ENV_PATH)

def get_login_url(api_key: str) -> str:
    kite = KiteConnect(api_key=api_key)
    return kite.login_url()

def extract_request_token(redirect_url: str) -> str:
    if redirect_url.startswith("http"):
        parsed = urlparse(redirect_url)
        params = parse_qs(parsed.query)
        if "request_token" in params:
            return params["request_token"][0]
        # Sometimes the token is in the fragment
        params = parse_qs(parsed.fragment)
        if "request_token" in params:
            return params["request_token"][0]
        raise ValueError(f"No request_token found in URL: {redirect_url}")
    else:
        # Assume they pasted the token directly
        token = redirect_url.strip()
        if len(token) > 10 and re.match(r'^[a-zA-Z0-9]+$', token):
            return token
        raise ValueError(f"Doesn't look like a valid request_token: {token}")

def exchange_token(api_key: str, api_secret: str, request_token: str) -> str:
    kite = KiteConnect(api_key=api_key)
    data = kite.generate_session(request_token, api_secret=api_secret)
    return data["access_token"]

def save_access_token(access_token: str):
    set_key(str(ENV_PATH), "KITE_ACCESS_TOKEN", access_token)
    print(f"\n✓ Access token saved to {ENV_PATH}")

def verify_token(api_key: str, access_token: str) -> bool:
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    try:
        profile = kite.profile()
        print(f"✓ Logged in as: {profile['user_name']} ({profile['user_id']})")
        print(f"  Email: {profile.get('email', 'N/A')}")
        print(f"  Broker: {profile.get('broker', 'N/A')}")
        return True
    except Exception as e:
        print(f"✗ Token verification failed: {e}")
        return False

def main():
    api_key = os.getenv("KITE_API_KEY", "")
    api_secret = os.getenv("KITE_API_SECRET", "")

    if not api_key or api_key == "your_api_key_here":
        print("ERROR: Set KITE_API_KEY in .env first")
        print(f"  Edit: {ENV_PATH}")
        sys.exit(1)

    if not api_secret or api_secret == "your_api_secret_here":
        print("ERROR: Set KITE_API_SECRET in .env first")
        print(f"  Edit: {ENV_PATH}")
        sys.exit(1)

    login_url = get_login_url(api_key)
    print("=" * 60)
    print("  KITE CONNECT DAILY LOGIN")
    print("=" * 60)
    print()
    print("1. Open this URL in your browser:")
    print()
    print(f"   {login_url}")
    print()
    print("2. Log in to Zerodha (enter credentials + TOTP)")
    print()
    print("3. After login, you'll be redirected.")
    print("   Copy the FULL URL from your browser's address bar")
    print("   and paste it below.")
    print()

    redirect_input = input("Paste redirect URL (or request_token): ").strip()

    if not redirect_input:
        print("No input provided. Exiting.")
        sys.exit(1)

    try:
        request_token = extract_request_token(redirect_input)
        print(f"\n✓ Request token: {request_token[:8]}...")
    except ValueError as e:
        print(f"\n✗ {e}")
        sys.exit(1)

    try:
        access_token = exchange_token(api_key, api_secret, request_token)
        print(f"✓ Access token: {access_token[:8]}...")
    except Exception as e:
        print(f"\n✗ Token exchange failed: {e}")
        print("  This usually means the request_token expired (valid for ~60 seconds).")
        print("  Try again — be quick with the paste.")
        sys.exit(1)

    save_access_token(access_token)

    print()
    verify_token(api_key, access_token)
    print()
    print("You can now run: python main.py")

if __name__ == "__main__":
    main()
