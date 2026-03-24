"""Signal deduplication across pipeline runs.

Problem: Agent runs daily. Same company appears multiple times.
Without dedup: CleanGrid Energy gets a WhatsApp on Monday AND Tuesday.
With dedup: CleanGrid Energy gets one outreach per 7-day window.

How it works:
  - dedup_hash = SHA256(company_name + city + signal_type + week_bucket)
  - Check SQLite local cache (fast, local)
  - If seen in last 7 days: skip
  - If seen and replied: skip forever (until re-qualification)
  - If new: allow, store hash

SQLite is source of truth. Airtable is the dashboard backup.
"""

from __future__ import annotations

import hashlib
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone

logger = logging.getLogger("myhq.dedup")

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "database", "dedup.db")


def _get_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signal_dedup (
            dedup_hash TEXT PRIMARY KEY,
            company_name TEXT,
            city TEXT,
            signal_type TEXT,
            first_seen TEXT,
            last_seen TEXT,
            send_count INTEGER DEFAULT 0,
            replied INTEGER DEFAULT 0,
            outcome TEXT
        )
    """)
    conn.commit()
    return conn


def make_dedup_hash(company_name: str, city: str, signal_type: str) -> str:
    """Hash stable for 7 days — same company stays deduped per week."""
    week = datetime.now(timezone.utc).strftime("%Y-W%U")
    raw = f"{company_name.lower().strip()}|{city}|{signal_type}|{week}"
    return hashlib.sha256(raw.encode()).hexdigest()[:20]


def is_duplicate(company_name: str, city: str, signal_type: str) -> tuple[bool, str]:
    """Check if this signal was already processed. Returns (is_dup, reason)."""
    if not company_name:
        return False, "no_company"

    h = make_dedup_hash(company_name, city, signal_type)

    try:
        conn = _get_db()
        row = conn.execute(
            "SELECT send_count, replied, first_seen, outcome FROM signal_dedup WHERE dedup_hash = ?",
            (h,),
        ).fetchone()
        conn.close()

        if row is None:
            return False, "new"

        send_count, replied, first_seen, outcome = row

        if replied:
            return True, f"already_replied:{outcome}"

        if send_count >= 1:
            first_dt = datetime.fromisoformat(first_seen.replace("Z", "+00:00"))
            if datetime.now(timezone.utc) - first_dt < timedelta(days=7):
                return True, f"sent_{send_count}_times_this_week"

        return False, "new_week"

    except Exception as e:
        logger.warning("Dedup check error: %s", e)
        return False, "error"


def mark_sent(company_name: str, city: str, signal_type: str):
    """Record that a message was sent."""
    h = make_dedup_hash(company_name, city, signal_type)
    now = datetime.now(timezone.utc).isoformat()

    try:
        conn = _get_db()
        conn.execute("""
            INSERT INTO signal_dedup (dedup_hash, company_name, city, signal_type, first_seen, last_seen, send_count)
            VALUES (?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(dedup_hash) DO UPDATE SET
                last_seen = excluded.last_seen,
                send_count = send_count + 1
        """, (h, company_name, city, signal_type, now, now))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Dedup mark_sent error: %s", e)


def mark_replied(company_name: str, city: str, outcome: str = "HOT"):
    """Record reply. Suppresses future outreach permanently."""
    try:
        conn = _get_db()
        conn.execute(
            "UPDATE signal_dedup SET replied = 1, outcome = ? WHERE company_name = ? AND city = ?",
            (outcome, company_name, city),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Dedup mark_replied error: %s", e)


def filter_duplicates(signals: list[dict]) -> tuple[list[dict], int]:
    """Filter signals to remove duplicates. Returns (filtered, skipped_count)."""
    filtered: list[dict] = []
    skipped = 0

    for sig in signals:
        company = sig.get("company_name", "")
        city = sig.get("city", "")
        sig_type = sig.get("signal_type", "")

        dup, reason = is_duplicate(company, city, sig_type)
        if dup:
            logger.debug("Dedup skip: %s (%s)", company, reason)
            skipped += 1
        else:
            filtered.append(sig)

    if skipped:
        logger.info("Dedup: %d filtered, %d unique passed", skipped, len(filtered))

    return filtered, skipped
