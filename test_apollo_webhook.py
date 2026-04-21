"""Phase 1 validation — fire ONE Apollo call with reveal_phone_number + webhook_url
and wait for the async payload to hit webhook.site.

Usage:
    python test_apollo_webhook.py

Prerequisites:
    - APOLLO_API_KEY set in .env
    - APOLLO_WEBHOOK_URL set in .env (point to your webhook.site URL)
    - webhook.site tab open in browser to watch for async payload
"""
import json
import os
import sys
import requests
from dotenv import load_dotenv

load_dotenv()
APOLLO_API_KEY = os.getenv("APOLLO_API_KEY", "")
APOLLO_WEBHOOK_URL = os.getenv("APOLLO_WEBHOOK_URL", "")

# Real BLR founders with India-only career histories — best test for whether
# Apollo has genuine Indian mobile coverage.
TEST_LEADS = [
    {
        "name": "Prajith Nair",
        "organization_name": "NoBlink",
        "linkedin_url": "http://www.linkedin.com/in/prajithnair22",
    },
    {
        "name": "Pulkit Agrawal",
        "organization_name": "H2LooP.ai",
        "linkedin_url": "http://www.linkedin.com/in/pulkit-agrawal-1b73224",
    },
    {
        "name": "Mayank Sachan",
        "organization_name": "Zanskar",
        "linkedin_url": "http://www.linkedin.com/in/mayank-sachan-44945a29",
    },
    {
        "name": "Sahil Ludhani",
        "organization_name": "Helium",
        "linkedin_url": "http://www.linkedin.com/in/sahilludhani",
    },
]


def main() -> None:
    if not APOLLO_API_KEY:
        print("ERROR: APOLLO_API_KEY not set in .env")
        sys.exit(1)
    if not APOLLO_WEBHOOK_URL:
        print("ERROR: APOLLO_WEBHOOK_URL not set in .env")
        print("       Get one at https://webhook.site and paste into .env")
        sys.exit(1)

    print(f"Apollo key: ...{APOLLO_API_KEY[-4:]}")
    print(f"Webhook:    {APOLLO_WEBHOOK_URL}")
    print(f"Firing {len(TEST_LEADS)} requests\n")

    for i, lead in enumerate(TEST_LEADS, 1):
        print(f"--- Lead {i}/{len(TEST_LEADS)}: {lead['name']} @ {lead['organization_name']} ---")
        resp = requests.post(
            "https://api.apollo.io/api/v1/people/match",
            headers={
                "Content-Type": "application/json",
                "Cache-Control": "no-cache",
                "accept": "application/json",
                "x-api-key": APOLLO_API_KEY,
            },
            params={
                "reveal_phone_number": "true",
                "webhook_url": APOLLO_WEBHOOK_URL,
            },
            json=lead,
            timeout=20,
        )
        print(f"  HTTP {resp.status_code}")
        if resp.status_code == 200:
            body = resp.json()
            person = body.get("person") or {}
            print(f"  Matched: {person.get('name')} — {person.get('title')}")
            print(f"  Apollo ID: {person.get('id')}")
        else:
            print(f"  Error: {resp.text[:200]}")
        print()

    print("=" * 60)
    print("ALL REQUESTS FIRED — watch webhook.site for up to 15 min")
    print("You should see 4 POSTs land (one per lead)")
    print("Paste all payloads back when they arrive")
    print("=" * 60)


if __name__ == "__main__":
    main()
