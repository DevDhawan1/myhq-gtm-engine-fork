"""Throwaway test: directly hit Apollo /people/match for real leads and print
full response so we can see why phones aren't coming back.

Usage:
    python test_apollo_phone_fix.py results/sdr_call_list_2026-04-19_BLR.json
"""
import json
import os
import sys
import requests
from dotenv import load_dotenv

load_dotenv()
APOLLO_API_KEY = os.getenv("APOLLO_API_KEY", "")


def apollo_call(name: str, company: str, linkedin: str) -> dict:
    payload: dict = {}
    if name:
        payload["name"] = name
    if company:
        payload["organization_name"] = company
    if linkedin:
        payload["linkedin_url"] = linkedin

    resp = requests.post(
        "https://api.apollo.io/api/v1/people/match",
        headers={
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            "accept": "application/json",
            "x-api-key": APOLLO_API_KEY,
        },
        params={"reveal_phone_number": "true"},
        json=payload,
        timeout=20,
    )
    return {
        "status": resp.status_code,
        "body": resp.json() if resp.content else {},
    }


def main(path: str) -> None:
    if not APOLLO_API_KEY:
        print("ERROR: APOLLO_API_KEY not set in .env")
        return

    print(f"APOLLO_API_KEY loaded (last 4 chars): ...{APOLLO_API_KEY[-4:]}\n")

    with open(path, encoding="utf-8") as f:
        leads = json.load(f)

    for i, lead in enumerate(leads, 1):
        name = lead.get("name") or lead.get("contact_name", "")
        company = lead.get("company_name", "")
        linkedin = lead.get("linkedin_url") or lead.get("contact_linkedin", "")

        print(f"\n=== Lead {i}: {name} @ {company} ===")
        print(f"LinkedIn: {linkedin}")

        try:
            r = apollo_call(name, company, linkedin)
        except Exception as e:
            print(f"EXCEPTION: {e}")
            continue

        print(f"HTTP {r['status']}")
        body = r["body"]

        if r["status"] != 200:
            print(f"Error body: {json.dumps(body, indent=2)[:400]}")
            continue

        person = body.get("person") or {}
        if not person:
            print(f"No person matched. Top-level keys: {list(body.keys())}")
            continue

        phones = person.get("phone_numbers") or []
        print(f"Matched: {person.get('name')} — {person.get('title')}")
        print(f"Email: {person.get('email')}")
        print(f"Phones ({len(phones)}): {phones}")
        if person.get("organization"):
            print(f"Org: {person['organization'].get('name')} — {person['organization'].get('primary_domain')}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "results/sdr_call_list_2026-04-19_BLR.json")
