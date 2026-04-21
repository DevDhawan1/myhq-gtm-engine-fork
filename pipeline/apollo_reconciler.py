"""Apollo reconciler — joins async webhook payloads back to originating leads.

Flow:
  1. `_apollo_enrich()` in enrichment_india_v2.py records every sync match
     into `pending_apollo_enrichments` (apollo_person_id + lead identity).
  2. Apollo's async webhook lands on Render → rows inserted into
     `apollo_phone_reveals`.
  3. This reconciler (run via scheduler) joins the two tables by
     apollo_person_id, picks the best Indian mobile from each reveal, and
     patches matching lead JSON files under `results/`.

TRAI DND check runs before patching if TRAI_DND_KEY is configured.
"""
from __future__ import annotations

import glob
import json
import logging
import os
from pathlib import Path

import psycopg2

logger = logging.getLogger("apollo-reconciler")

DATABASE_URL = os.environ.get("DATABASE_URL", "")
RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"


def _pick_indian_mobile(phones: list[dict]) -> str | None:
    """Prefer +91 mobile; fall back to any valid mobile; else None."""
    if not phones:
        return None
    indian = [
        p for p in phones
        if (p.get("sanitized_number") or "").startswith("+91")
        and p.get("type_cd") == "mobile"
        and p.get("status_cd") == "valid_number"
    ]
    if indian:
        return indian[0].get("sanitized_number")
    mobiles = [
        p for p in phones
        if p.get("type_cd") == "mobile" and p.get("status_cd") == "valid_number"
    ]
    return mobiles[0].get("sanitized_number") if mobiles else None


def _patch_lead_files(linkedin_url: str, phone: str) -> int:
    """Find every results/*.json containing this lead and patch in the phone.

    Returns number of files patched.
    """
    if not linkedin_url or not phone:
        return 0

    patched = 0
    for path in glob.glob(str(RESULTS_DIR / "*.json")):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        if not isinstance(data, list):
            continue

        changed = False
        for lead in data:
            if not isinstance(lead, dict):
                continue
            lead_li = (lead.get("linkedin_url") or lead.get("contact_linkedin") or "").lower()
            if lead_li and lead_li == linkedin_url.lower():
                lead["phone_mobile"] = phone
                lead["contact_phone"] = phone
                lead["phone_source"] = "apollo_waterfall"
                changed = True
        if changed:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            patched += 1
    return patched


def _check_trai_dnd(phone: str) -> bool:
    """True if phone is DND-registered (should NOT be contacted).

    No-op (returns False) unless TRAI_DND_KEY is set — matches existing
    compliance/india.py behavior.
    """
    try:
        from compliance.india import check_dnd
        return bool(check_dnd(phone))
    except Exception:
        return False


def reconcile() -> dict:
    """Process all unconsumed reveals. Returns summary counts."""
    if not DATABASE_URL:
        logger.error("DATABASE_URL not set — skipping reconciliation")
        return {"reconciled": 0, "patched": 0, "dnd_blocked": 0, "no_phone": 0}

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT apr.id, apr.apollo_person_id, apr.phone_numbers,
                       pae.lead_name, pae.lead_company, pae.lead_linkedin
                FROM apollo_phone_reveals apr
                LEFT JOIN pending_apollo_enrichments pae
                    ON pae.apollo_person_id = apr.apollo_person_id
                WHERE apr.consumed = FALSE
                ORDER BY apr.received_at ASC
                """
            )
            rows = cur.fetchall()

        summary = {"reconciled": 0, "patched": 0, "dnd_blocked": 0, "no_phone": 0}
        for apr_id, apollo_id, phones, name, company, linkedin in rows:
            summary["reconciled"] += 1
            phone = _pick_indian_mobile(phones)

            if not phone:
                summary["no_phone"] += 1
                logger.info("No usable phone for %s @ %s (apollo_id=%s)", name, company, apollo_id)
            elif _check_trai_dnd(phone):
                summary["dnd_blocked"] += 1
                logger.info("DND-blocked: %s for %s @ %s", phone, name, company)
                phone = None  # do not patch; compliance gate
            else:
                files_patched = _patch_lead_files(linkedin or "", phone)
                if files_patched:
                    summary["patched"] += 1
                    logger.info("Patched %s (%d files) with %s", name, files_patched, phone)
                else:
                    logger.info("No lead file found for %s (linkedin=%s)", name, linkedin)

            with conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE apollo_phone_reveals SET consumed=TRUE, consumed_at=now() WHERE id=%s",
                    (apr_id,),
                )
                if phone:
                    cur.execute(
                        """
                        UPDATE pending_apollo_enrichments
                        SET patched=TRUE, patched_at=now(), patched_phone=%s
                        WHERE apollo_person_id=%s
                        """,
                        (phone, apollo_id),
                    )
        return summary
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(reconcile())
