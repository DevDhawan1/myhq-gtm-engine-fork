"""Safe single-email sender for testing outreach end-to-end.

Reads one lead from a results JSON file, pulls its AI-generated email_subject
and email_body, and sends via Gmail SMTP to a specified TO address.

Usage:
    # Send lead #1 (1-indexed) to your own email — SAFE test
    python test_email_send.py --file results/sdr_call_list_2026-04-19_BLR.json --to you@myhq.in --lead 1

    # Send lead #3 to its real prospect email — LIVE, only after self-test works
    python test_email_send.py --file results/sdr_call_list_2026-04-19_BLR.json --lead 3 --live

Safety:
  - By default requires --to (override). Without --live flag, won't send to
    the lead's real email.
  - Prints a preview + confirmation prompt before sending.
  - Honours PKM gate — refuses to send if lead has no defense_mode.
"""
from __future__ import annotations

import argparse
import json
import os
import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")


def main() -> None:
    parser = argparse.ArgumentParser(description="Safe single-email test send")
    parser.add_argument("--file", required=True, help="Path to results JSON")
    parser.add_argument("--lead", type=int, default=1, help="1-indexed lead position")
    parser.add_argument("--to", help="Override recipient (recommended for first test)")
    parser.add_argument("--live", action="store_true",
                        help="Send to the lead's actual email (use only after self-test works)")
    args = parser.parse_args()

    if not SMTP_USER or not SMTP_PASS:
        print("ERROR: SMTP_USER or SMTP_PASS missing from .env")
        sys.exit(1)

    with open(args.file, encoding="utf-8") as f:
        leads = json.load(f)

    idx = args.lead - 1
    if idx < 0 or idx >= len(leads):
        print(f"ERROR: --lead {args.lead} out of range (file has {len(leads)} leads)")
        sys.exit(1)

    lead = leads[idx]
    msgs = lead.get("messages") or {}
    pkm = lead.get("pkm") or {}

    # PKM gate — match production behavior
    if not pkm.get("defense_mode"):
        print("BLOCKED: lead has no PKM defense_mode. Email not sent.")
        sys.exit(1)

    subject = msgs.get("email_subject") or ""
    body = msgs.get("email_body") or ""
    if not subject or not body:
        print("ERROR: lead is missing email_subject or email_body. Regenerate outreach first.")
        sys.exit(1)

    # Decide recipient
    real_email = lead.get("contact_email") or lead.get("email") or ""
    if args.to:
        recipient = args.to
        mode = f"OVERRIDE → {recipient}"
    elif args.live:
        if not real_email:
            print("ERROR: --live set but lead has no contact_email")
            sys.exit(1)
        recipient = real_email
        mode = f"LIVE → real lead email {recipient}"
    else:
        print("ERROR: provide --to <your_email> for test, or --live to send to the real lead")
        sys.exit(1)

    # Preview
    print("=" * 70)
    print(f"Lead:      {lead.get('contact_name')} @ {lead.get('company_name')}")
    print(f"Defense:   {pkm.get('defense_mode')}")
    print(f"Mode:      {mode}")
    print(f"From:      {SMTP_USER}")
    print(f"To:        {recipient}")
    print(f"Subject:   {subject}")
    print("-" * 70)
    print(body)
    print("=" * 70)
    confirm = input("Send this email? [y/N]: ").strip().lower()
    if confirm != "y":
        print("Aborted.")
        return

    # Build + send
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = recipient
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
        print(f"✓ Sent to {recipient}")
    except Exception as e:
        print(f"✗ SMTP error: {type(e).__name__}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
