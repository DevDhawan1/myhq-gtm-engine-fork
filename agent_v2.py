#!/usr/bin/env python3
"""myHQ GTM Engine v2 — India-first, PKM-powered, AROS-connected.

Complete pipeline:
  Signal → Enrich → Score → Persona → Compliance → PKM → Outreach → SDR List

Usage:
    python3 agent_v2.py --run full --dry-run           Full pipeline, synthetic data
    python3 agent_v2.py --run full --city BLR           Bengaluru only, live APIs
    python3 agent_v2.py --run full --cities BLR MUM DEL All 3 cities
    python3 agent_v2.py --run signals --dry-run         Signal detection only
    python3 agent_v2.py --run enrich --dry-run          Signals + enrichment
    python3 agent_v2.py --run sdr --persona 1           SDR list for funded founders
    python3 agent_v2.py --run competitors --dry-run     Weekly competitor scan
    python3 agent_v2.py --run content --dry-run         LLM content generation
    python3 agent_v2.py --run whatsapp --dry-run        Send WhatsApp messages
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime

from rich.console import Console
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table
from rich import box

from config.settings_v2 import CITIES, INTENT_TIERS

# v2 modules (India-first data layer)
from pipeline.signals_india_v2 import collect_all_signals, collect_all_signals_flat
from pipeline.enrichment_india_v2 import enrich_signals
from pipeline.pkm_myhq import profile_leads, generate_outreach

# v1 modules (proven scoring, persona, compliance, SDR dashboard)
from pipeline.scorer import score_leads
from pipeline.persona_matcher import match_personas
from compliance.india import check_compliance
from pipeline.sdr_dashboard import generate_sdr_dashboard, SDRDashboard

logger = logging.getLogger("myhq-gtm-v2")


HEADER = r"""
╔══════════════════════════════════════════════════════════════╗
║                   myHQ GTM ENGINE v2.0                       ║
║        India-First Signal Intelligence + PKM Bypass          ║
║                                                              ║
║  Tracxn · MCA · Naukri · NewsAPI · Proxycurl · Lusha         ║
║  PKM profiling → TRAI compliance → WhatsApp-first → AROS    ║
╚══════════════════════════════════════════════════════════════╝
"""


def _normalize_lead_fields(lead: dict) -> dict:
    """Map v2 enrichment fields → v1 schema so scorer/persona/compliance/SDR work.

    v2 uses: name, email, phone_mobile, linkedin_url, title, employee_count
    v1 uses: contact_name, contact_email, contact_phone, contact_whatsapp, contact_linkedin, contact_title, company_size
    """
    lead.setdefault("contact_name", lead.get("name") or lead.get("founder_name", ""))
    lead.setdefault("contact_email", lead.get("email", ""))
    lead.setdefault("contact_phone", lead.get("phone_mobile", ""))
    lead.setdefault("contact_whatsapp", lead.get("phone_mobile", "") if lead.get("whatsapp_verified") else "")
    lead.setdefault("contact_linkedin", lead.get("linkedin_url") or lead.get("founder_linkedin", ""))
    lead.setdefault("contact_title", lead.get("title", ""))
    lead.setdefault("company_size", lead.get("employee_count") or lead.get("employee_count_est"))
    lead.setdefault("company_size_est", lead.get("employee_count"))
    lead.setdefault("employee_count_est", lead.get("employee_count"))
    lead.setdefault("company_website", lead.get("website", ""))
    lead.setdefault("company_last_funding_amount", lead.get("amount_raised", ""))
    lead.setdefault("company_investors", lead.get("investor_names", []))
    lead.setdefault("announcement_date", lead.get("detected_at"))
    lead.setdefault("source", lead.get("raw_source", ""))

    # Signal type mapping for v1 scorer
    sig = lead.get("signal_type", "")
    if sig in ("FUNDING",):
        lead.setdefault("signal_type_v1", "funding")
    elif sig in ("HIRING_SURGE",):
        lead.setdefault("signal_type_v1", "hiring")
    elif sig in ("MCA_NEW_SUBSIDIARY", "GST_NEW_CITY", "CITY_EXPANSION_PR"):
        lead.setdefault("signal_type_v1", "expansion")
    else:
        lead.setdefault("signal_type_v1", "intent")

    # v1 scorer reads signal_type
    if "signal_type" in lead and lead["signal_type"] not in ("funding", "hiring", "expansion", "intent"):
        lead["signal_type_original"] = lead["signal_type"]
        lead["signal_type"] = lead["signal_type_v1"]

    return lead


class GTMEngineV2:
    """Master orchestrator — v2 data layer + v1 scoring/compliance/SDR."""

    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.dry_run = args.dry_run
        self.console = Console()
        self.all_signals: dict[str, list[dict]] = {}
        self.all_leads: list[dict] = []
        self.start_time = time.time()

    def run(self) -> None:
        self._display_header()

        dispatch = {
            "full": self._run_full_pipeline,
            "signals": self._run_signals,
            "enrich": self._run_enrich,
            "outreach": self._run_outreach,
            "sdr": self._run_sdr,
            "competitors": self._run_competitors,
            "content": self._run_content,
            "whatsapp": self._run_whatsapp,
        }

        handler = dispatch.get(self.args.run)
        if handler:
            handler()
        else:
            self.console.print(f"[red]Unknown mode: {self.args.run}[/red]")
            sys.exit(1)

        self._display_footer()

    # ── Full pipeline (8 steps) ──────────────────────────────────────

    def _run_full_pipeline(self) -> None:
        cities = self._get_cities()

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=self.console,
        ) as progress:
            task = progress.add_task("[cyan]Detecting signals…", total=7)

            # Step 1: Signal detection (v2 — India-first APIs)
            self.all_signals = collect_all_signals(
                cities=cities, dry_run=self.dry_run, verbose=self.args.verbose
            )
            flat_signals = [s for sigs in self.all_signals.values() for s in sigs]
            progress.advance(task)

            signal_counts = {k: len(v) for k, v in self.all_signals.items() if v}
            self.console.print(f"  Signals: {signal_counts} = {len(flat_signals)} total")

            # Step 2: Enrichment (v2 — waterfall: Apollo→PDL→Proxycurl→Lusha→Hunter)
            progress.update(task, description="[yellow]Enriching contacts…")
            enriched = enrich_signals(flat_signals[:50], dry_run=self.dry_run)
            progress.advance(task)

            # Normalize fields for v1 modules
            enriched = [_normalize_lead_fields(l) for l in enriched]

            # Filter: only leads with verified contact
            valid = [l for l in enriched if l.get("email_valid") or l.get("whatsapp_verified")]
            self.console.print(f"  Enriched: {len(enriched)} | Valid contacts: {len(valid)}")

            # Step 3: Persona matching (v1 — proven 3-persona system)
            progress.update(task, description="[yellow]Matching personas…")
            matched = match_personas(valid)
            progress.advance(task)

            # Step 4: Intent scoring (v1 — 5-dimension 0-100 scoring)
            progress.update(task, description="[yellow]Scoring intent…")
            scored = score_leads(matched)
            progress.advance(task)

            # Step 5: TRAI compliance (v1 — DND check, suppression, limits)
            progress.update(task, description="[cyan]Compliance filter…")
            compliant = check_compliance(scored, dry_run=self.dry_run)
            progress.advance(task)

            self.console.print(f"  Scored: {len(scored)} | Post-compliance: {len(compliant)}")

            # Step 6: PKM defense profiling (v2 — AROS brain)
            progress.update(task, description="[magenta]PKM defense profiling…")
            profiled = profile_leads(compliant, dry_run=self.dry_run)
            progress.advance(task)

            # Step 7: Outreach generation (v2 — PKM-calibrated messages)
            progress.update(task, description="[green]Generating outreach…")
            self.all_leads = generate_outreach(profiled, dry_run=self.dry_run)
            progress.advance(task)

        # Apply CLI filters
        self.all_leads = self._filter_by_persona(self.all_leads)
        self.all_leads = self._filter_by_tier(self.all_leads)

        self.console.print(f"\n  [green]{len(self.all_leads)} leads ready[/green]\n")

        # Step 8: SDR dashboard (v1 — Rich terminal UI)
        generate_sdr_dashboard(self.all_leads, city=self.args.city, dry_run=self.dry_run)

        # Save full results
        self._save_results(self.all_leads)

    # ── Individual pipeline stages ───────────────────────────────────

    def _run_signals(self) -> None:
        self.console.print("[bold]Running signal detection…[/bold]\n")
        cities = self._get_cities()
        self.all_signals = collect_all_signals(cities=cities, dry_run=self.dry_run)
        for sig_type, signals in self.all_signals.items():
            if signals:
                self.console.print(f"  {sig_type}: {len(signals)} signals")
        flat = [s for sigs in self.all_signals.values() for s in sigs]
        self.console.print(f"\n  [green]Total: {len(flat)} signals[/green]")
        self._save_results(flat, prefix="signals")

    def _run_enrich(self) -> None:
        self.console.print("[bold]Running signals + enrichment…[/bold]\n")
        cities = self._get_cities()
        self.all_signals = collect_all_signals(cities=cities, dry_run=self.dry_run)
        flat = [s for sigs in self.all_signals.values() for s in sigs]
        enriched = enrich_signals(flat[:50], dry_run=self.dry_run)
        self.all_leads = [_normalize_lead_fields(l) for l in enriched]
        self.console.print(f"  [green]Enriched: {len(self.all_leads)} leads[/green]")
        self._save_results(self.all_leads, prefix="enriched")

    def _run_outreach(self) -> None:
        self._run_full_pipeline()

    def _run_sdr(self) -> None:
        self._run_full_pipeline()

    def _run_competitors(self) -> None:
        self.console.print("[bold]Running weekly competitor scan…[/bold]\n")
        from pipeline.competitor_intel import run_weekly_competitor_scan
        results = run_weekly_competitor_scan(dry_run=self.dry_run)
        for comp, data in results.items():
            self.console.print(f"  {comp}: {data}")
        self.console.print(f"\n  [green]Scan complete: {len(results)} competitors[/green]")

    def _run_content(self) -> None:
        self.console.print("[bold]Running LLM content generation…[/bold]\n")
        from pipeline.llm_content_indexer import run_weekly_content_generation
        content = run_weekly_content_generation(dry_run=self.dry_run)
        for piece in content:
            self.console.print(f"  {piece['type']}: {piece['title'][:60]} ({piece['word_count']} words)")
        self.console.print(f"\n  [green]{len(content)} content pieces generated[/green]")

    def _run_whatsapp(self) -> None:
        self.console.print("[bold]Running WhatsApp send…[/bold]\n")
        # Run full pipeline first to get qualified leads
        self._run_full_pipeline()
        from pipeline.whatsapp_india import send_whatsapp_batch
        results = send_whatsapp_batch(self.all_leads, dry_run=self.dry_run)
        sent = sum(1 for r in results if r.get("success"))
        self.console.print(f"\n  [green]WhatsApp: {sent}/{len(results)} sent[/green]")

    # ── Helpers ───────────────────────────────────────────────────────

    def _get_cities(self) -> list[str]:
        if self.args.city:
            return [self.args.city]
        if self.args.cities:
            return self.args.cities
        return list(CITIES.keys())

    def _filter_by_persona(self, leads: list[dict]) -> list[dict]:
        if self.args.persona:
            return [l for l in leads if l.get("persona_id") == self.args.persona
                    or l.get("persona") == self.args.persona]
        return leads

    def _filter_by_tier(self, leads: list[dict]) -> list[dict]:
        if self.args.tier:
            tier = self.args.tier.upper()
            return [l for l in leads if l.get("tier") == tier]
        return leads

    def _display_header(self) -> None:
        self.console.print(f"[bold cyan]{HEADER}[/bold cyan]")
        mode = "[bold red]DRY RUN[/bold red]" if self.dry_run else "[bold green]LIVE[/bold green]"
        cities = ", ".join(self._get_cities())
        persona = f"Persona {self.args.persona}" if self.args.persona else "All"
        tier = self.args.tier.upper() if self.args.tier else "All"
        self.console.print(f"  Mode: {mode}  |  Run: {self.args.run}  |  Cities: {cities}  |  {persona}  |  {tier}")
        self.console.print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M IST')}")
        self.console.print()

    def _display_footer(self) -> None:
        elapsed = time.time() - self.start_time
        flat_signals = sum(len(v) for v in self.all_signals.values()) if self.all_signals else 0
        self.console.print()
        self.console.print(Panel(
            f"[bold green]Pipeline complete in {elapsed:.1f}s[/bold green]\n"
            f"Signals: {flat_signals} | Leads: {len(self.all_leads)}",
            title="Done",
            border_style="green",
        ))

    def _save_results(self, data: list | dict, prefix: str = "sdr_list") -> None:
        os.makedirs("results", exist_ok=True)
        filename = f"results/{prefix}_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
        # Clean non-serializable fields
        clean_data = data
        if isinstance(data, list):
            clean_data = [
                {k: v for k, v in item.items()
                 if k not in ("persona_details", "persona_match_scores", "score_breakdown")}
                for item in data
            ]
        with open(filename, "w") as f:
            json.dump(clean_data, f, indent=2, default=str)
        self.console.print(f"  [dim]Saved: {filename}[/dim]")


# ── CLI ───────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="myHQ GTM Engine v2 — India-first signal intelligence",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 agent_v2.py --run full --dry-run           Full pipeline (synthetic)
  python3 agent_v2.py --run full --city BLR           Bengaluru only (live)
  python3 agent_v2.py --run full --cities BLR MUM     Two cities
  python3 agent_v2.py --run signals --dry-run         Signal detection only
  python3 agent_v2.py --run sdr --persona 1           Funded founders only
  python3 agent_v2.py --run competitors --dry-run     Competitor scan
  python3 agent_v2.py --run content --dry-run         LLM content generation
  python3 agent_v2.py --run whatsapp --dry-run        WhatsApp sends
        """,
    )
    parser.add_argument(
        "--run",
        choices=["full", "signals", "enrich", "outreach", "sdr",
                 "competitors", "content", "whatsapp"],
        default="full",
    )
    parser.add_argument("--cities", nargs="+", choices=list(CITIES.keys()))
    parser.add_argument("--city", choices=list(CITIES.keys()))
    parser.add_argument("--persona", type=int, choices=[1, 2, 3])
    parser.add_argument("--tier", choices=["hot", "warm", "nurture", "monitor"])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--output-dir", default="results")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    engine = GTMEngineV2(args)
    engine.run()


if __name__ == "__main__":
    main()
