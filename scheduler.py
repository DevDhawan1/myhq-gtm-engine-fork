#!/usr/bin/env python3
"""myHQ GTM Engine v2 — Job scheduler.

Cron schedule (IST):
  06:00 daily    — Signal detection (Tracxn, Naukri, MCA, news)
  06:30 daily    — Enrichment + PKM profiling
  07:00 daily    — WhatsApp sends (7-9am is best open rate in India)
  09:00 daily    — Reply classification + HOT alerts
  Mon  07:00     — Competitor intelligence scan (weekly)
  Wed  09:00     — LLM content generation (weekly)

Usage:
  python scheduler.py --job signals
  python scheduler.py --job enrich
  python scheduler.py --job whatsapp
  python scheduler.py --job replies
  python scheduler.py --job competitors
  python scheduler.py --job content

Crontab example:
  0  0 * * * cd ~/myhq-gtm-engine && python scheduler.py --job signals
  30 0 * * * cd ~/myhq-gtm-engine && python scheduler.py --job enrich
  0  1 * * * cd ~/myhq-gtm-engine && python scheduler.py --job whatsapp
  30 3 * * * cd ~/myhq-gtm-engine && python scheduler.py --job replies
  0  1 * * 1 cd ~/myhq-gtm-engine && python scheduler.py --job competitors
  0  3 * * 3 cd ~/myhq-gtm-engine && python scheduler.py --job content
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("scheduler")

CITIES = ["BLR", "MUM", "DEL", "HYD", "PUN"]


def run_job(job_name: str) -> None:
    logger.info("Starting job: %s", job_name)

    if job_name == "signals":
        from pipeline.signals_india_v2 import collect_all_signals
        results = collect_all_signals(cities=CITIES)
        total = sum(len(v) for v in results.values())
        logger.info("Signals collected: %d total", total)

    elif job_name == "enrich":
        from pipeline.signals_india_v2 import collect_all_signals_flat
        from pipeline.enrichment_india_v2 import enrich_signals
        from pipeline.pkm_myhq import profile_leads
        signals = collect_all_signals_flat(cities=CITIES)
        leads = enrich_signals(signals[:50])
        profiled = profile_leads(leads)
        logger.info("Enriched + profiled: %d leads", len(profiled))

    elif job_name == "whatsapp":
        from pipeline.whatsapp_india import send_whatsapp_batch
        # In production: read qualified leads from Airtable queue
        logger.info("WhatsApp sends — reads queue from Airtable")

    elif job_name == "replies":
        logger.info("Reply classification — WATI webhook processes incoming")

    elif job_name == "competitors":
        from pipeline.competitor_intel import run_weekly_competitor_scan
        results = run_weekly_competitor_scan()
        logger.info("Competitor scan: %s", results)

    elif job_name == "content":
        from pipeline.llm_content_indexer import run_weekly_content_generation
        content = run_weekly_content_generation()
        logger.info("Content generated: %d pieces", len(content))

    elif job_name == "reconcile-apollo":
        from pipeline.apollo_reconciler import reconcile
        summary = reconcile()
        logger.info("Apollo reconcile: %s", summary)

    else:
        logger.error("Unknown job: %s", job_name)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="myHQ GTM Engine v2 — Scheduler")
    parser.add_argument(
        "--job",
        required=True,
        choices=["signals", "enrich", "whatsapp", "replies", "competitors", "content", "reconcile-apollo"],
    )
    args = parser.parse_args()
    run_job(args.job)
