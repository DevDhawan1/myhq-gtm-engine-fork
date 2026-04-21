"""Regenerate outreach messages + fix phone formatting on existing lead JSON files.

Useful when outreach templates or phone formatters are updated and you want
to refresh already-enriched leads without re-running the full pipeline
(i.e. without burning signal/enrichment credits again).

Usage:
    python regenerate_outreach.py results/sdr_call_list_2026-04-19_BLR.json
    python regenerate_outreach.py results/sdr_list_20260419_2049.json

What it does:
  1. Loads leads from the JSON file
  2. Strips any "++" from company_phone (bug in old signal ingestion)
  3. Re-runs PKM-calibrated outreach generation on each lead
  4. Writes the file back in place
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(name)s: %(message)s",
)

from pipeline.pkm_myhq import generate_outreach  # noqa: E402


def _fix_phone(phone: str | None) -> str | None:
    if not phone or not isinstance(phone, str):
        return phone
    cleaned = phone.lstrip("+").strip()
    return f"+{cleaned}" if cleaned else phone


def regenerate(path: str) -> None:
    p = Path(path)
    if not p.exists():
        print(f"ERROR: {p} not found")
        sys.exit(1)

    with p.open(encoding="utf-8") as f:
        leads = json.load(f)

    if not isinstance(leads, list):
        print(f"ERROR: expected a JSON list, got {type(leads).__name__}")
        sys.exit(1)

    print(f"Loaded {len(leads)} leads from {p.name}")

    # 1. Fix phone formatting
    fixed_phones = 0
    for lead in leads:
        for key in ("company_phone", "contact_phone", "phone_mobile"):
            v = lead.get(key)
            new_v = _fix_phone(v)
            if v != new_v:
                lead[key] = new_v
                fixed_phones += 1
    if fixed_phones:
        print(f"Fixed {fixed_phones} malformed phone field(s)")

    # 2. Regenerate outreach — only for leads with a PKM profile attached
    with_pkm = [l for l in leads if l.get("pkm", {}).get("defense_mode")]
    without_pkm = len(leads) - len(with_pkm)
    if without_pkm:
        print(f"Skipping {without_pkm} lead(s) without PKM profile (can't regenerate)")

    if with_pkm:
        print(f"Regenerating outreach for {len(with_pkm)} lead(s) via PKM + LLM…")
        # generate_outreach mutates each lead to add lead["messages"]
        generate_outreach(with_pkm, dry_run=False)
        print("Done.")

    # 3. Save back
    with p.open("w", encoding="utf-8") as f:
        json.dump(leads, f, indent=2, ensure_ascii=False)
    print(f"Wrote updated {p.name}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python regenerate_outreach.py <path-to-results-json>")
        sys.exit(1)
    regenerate(sys.argv[1])
