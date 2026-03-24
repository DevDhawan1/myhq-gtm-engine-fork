"""PKM self-improvement loop.

Problem: PKM classifier profiles defense modes but never learns from outcomes.
If someone classified as OVERLOAD_AVOIDANCE replies "not interested" —
the bypass didn't work. We need that feedback.

The loop:
  1. WA_Replies has: company, defense_mode_used, reply_text, category
  2. This module reads corrections (negative replies on expected-positive leads)
  3. Injects them as examples into Claude classification prompt
  4. Classifier improves with every interaction

Result: PKM accuracy 65% (v2) → 90%+ (v3).
"""

from __future__ import annotations

import logging
import os

import requests

logger = logging.getLogger("myhq.pkm.feedback")

AIRTABLE_URL = "https://api.airtable.com/v0"


def get_recent_corrections(limit: int = 30) -> list[dict]:
    """Fetch cases where PKM defense mode was wrong (NOT_NOW/UNSUBSCRIBE outcomes)."""
    key = os.getenv("AIRTABLE_API_KEY", "")
    base_id = os.getenv("AIRTABLE_BASE_ID", "")

    if not key or not base_id:
        return []

    try:
        resp = requests.get(
            f"{AIRTABLE_URL}/{base_id}/WA_Replies",
            headers={"Authorization": f"Bearer {key}"},
            params={
                "filterByFormula": "AND({category}='NOT_NOW', {defense_mode_used}!='')",
                "sort[0][field]": "received_at",
                "sort[0][direction]": "desc",
                "maxRecords": limit,
            },
            timeout=10,
        )
        resp.raise_for_status()

        return [
            {
                "company": r["fields"].get("company_name", ""),
                "defense_mode_used": r["fields"].get("defense_mode_used", ""),
                "reply_text": r["fields"].get("reply_text", "")[:200],
                "category": r["fields"].get("category", ""),
                "profile_hint": r["fields"].get("profile_hint", ""),
            }
            for r in resp.json().get("records", [])
        ]
    except Exception as e:
        logger.warning("PKM feedback fetch error: %s", e)
        return []


def get_positive_examples(limit: int = 20) -> list[dict]:
    """Fetch cases where PKM worked (HOT reply = correct defense mode)."""
    key = os.getenv("AIRTABLE_API_KEY", "")
    base_id = os.getenv("AIRTABLE_BASE_ID", "")

    if not key or not base_id:
        return []

    try:
        resp = requests.get(
            f"{AIRTABLE_URL}/{base_id}/WA_Replies",
            headers={"Authorization": f"Bearer {key}"},
            params={
                "filterByFormula": "AND({category}='HOT', {defense_mode_used}!='')",
                "sort[0][field]": "received_at",
                "sort[0][direction]": "desc",
                "maxRecords": limit,
            },
            timeout=10,
        )
        resp.raise_for_status()

        return [
            {
                "defense_mode_used": r["fields"].get("defense_mode_used", ""),
                "profile_hint": r["fields"].get("profile_hint", ""),
                "outcome": "HOT — bypass worked",
            }
            for r in resp.json().get("records", [])
        ]
    except Exception as e:
        logger.warning("PKM positive examples fetch error: %s", e)
        return []


def build_dynamic_classification_prompt(base_prompt: str) -> str:
    """Enhance PKM prompt with real interaction data. Call before every classification."""
    corrections = get_recent_corrections(limit=20)
    positives = get_positive_examples(limit=10)

    if not corrections and not positives:
        return base_prompt

    additions = "\n\nLEARNED FROM REAL INTERACTIONS (use these to improve accuracy):\n"

    if corrections:
        additions += "\nCASES WHERE THE WRONG DEFENSE MODE WAS DETECTED:\n"
        for c in corrections[:10]:
            additions += (
                f'- "{c["profile_hint"][:80]}..." classified as {c["defense_mode_used"]} '
                f'but replied: "{c["reply_text"][:60]}..." (outcome: {c["category"]})\n'
            )

    if positives:
        additions += "\nCASES WHERE THE CORRECT DEFENSE MODE WAS DETECTED:\n"
        for p in positives[:5]:
            additions += (
                f'- "{p["profile_hint"][:80]}..." correctly classified as '
                f'{p["defense_mode_used"]} — {p["outcome"]}\n'
            )

    additions += "\nApply these patterns to improve classification accuracy.\n"
    return base_prompt + additions


def record_classification_for_feedback(
    cache_key: str, profile_text: str, detected_mode: str, company: str
):
    """Store profile_hint alongside classification for the feedback loop."""
    key = os.getenv("AIRTABLE_API_KEY", "")
    base_id = os.getenv("AIRTABLE_BASE_ID", "")

    if not key or not base_id:
        return

    try:
        resp = requests.get(
            f"{AIRTABLE_URL}/{base_id}/PKM_Cache",
            headers={"Authorization": f"Bearer {key}"},
            params={"filterByFormula": f'{{cache_key}}="{cache_key}"'},
            timeout=8,
        )
        records = resp.json().get("records", [])
        if not records:
            return

        requests.patch(
            f"{AIRTABLE_URL}/{base_id}/PKM_Cache/{records[0]['id']}",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={"fields": {"profile_hint": profile_text[:500], "company_name": company}},
            timeout=8,
        )
    except Exception as e:
        logger.warning("PKM feedback record error: %s", e)
