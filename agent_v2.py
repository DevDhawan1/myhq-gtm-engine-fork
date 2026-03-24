#!/usr/bin/env python3
"""myHQ GTM Engine v2 — India-first, PKM-powered, AROS-connected.

Usage:
    python3 agent_v2.py --run full --dry-run           Full pipeline, synthetic data
    python3 agent_v2.py --run full --city BLR           Bengaluru only, live APIs
    python3 agent_v2.py --run full --cities BLR MUM DEL All 3 cities
    python3 agent_v2.py --run signals --dry-run         Signal detection only
    python3 agent_v2.py --run enrich --dry-run          Signals + enrichment
    python3 agent_v2.py --run sdr --persona 1           SDR list for funded founders
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
from pipeline.signals_india_v2 import collect_all_signals, collect_all_signals_flat
from pipeline.enrichment_india_v2 import enrich_signals
from pipeline.pkm_myhq import profile_leads, generate_outreach
from pipeline.scorer import score_leads

logger = logging.getLogger("myhq-gtm-v2")


HEADER = r"""
╔══════════════════════════════════════════════════════════════╗
║                   myHQ GTM ENGINE v2.0                       ║
║        India-First Signal Intelligence + PKM Bypass          ║
║                                                              ║
║  Tracxn · MCA · Naukri · NewsAPI · Proxycurl · Lusha         ║
║  PKM defense profiling → WhatsApp-first outreach → AROS     ║
╚══════════════════════════════════════════════════════════════╝
"""


class GTMEngineV2:
    """Master orchestrator for myHQ GTM Engine v2."""

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
        }

        handler = dispatch.get(self.args.run)
        if handler:
            handler()
        else:
            self.console.print(f"[red]Unknown mode: {self.args.run}[/red]")
            sys.exit(1)

        self._display_footer()

    # ── Full pipeline ─────────────────────────────────────────────────

    def _run_full_pipeline(self) -> None:
        cities = self._get_cities()

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=self.console,
        ) as progress:
            task = progress.add_task("[cyan]Detecting signals…", total=5)

            # Step 1: Signal detection
            self.all_signals = collect_all_signals(
                cities=cities, dry_run=self.dry_run, verbose=self.args.verbose
            )
            flat_signals = [s for sigs in self.all_signals.values() for s in sigs]
            progress.advance(task)

            signal_counts = {k: len(v) for k, v in self.all_signals.items()}
            self.console.print(f"  Signals: {signal_counts} = {len(flat_signals)} total")

            # Step 2: Enrichment
            progress.update(task, description="[yellow]Enriching contacts…")
            enriched = enrich_signals(flat_signals[:50], dry_run=self.dry_run)  # cap 50/run
            progress.advance(task)

            # Filter: only leads with verified contact
            valid = [l for l in enriched if l.get("email_valid") or l.get("whatsapp_verified")]
            self.console.print(f"  Enriched: {len(enriched)} | Valid contacts: {len(valid)}")

            # Step 3: Scoring
            progress.update(task, description="[yellow]Scoring leads…")
            scored = score_leads(valid)
            progress.advance(task)

            # Step 4: PKM profiling
            progress.update(task, description="[magenta]PKM defense profiling…")
            profiled = profile_leads(scored, dry_run=self.dry_run)
            progress.advance(task)

            # Step 5: Outreach generation
            progress.update(task, description="[green]Generating outreach…")
            self.all_leads = generate_outreach(profiled, dry_run=self.dry_run)
            progress.advance(task)

        # Apply filters
        self.all_leads = self._filter_by_persona(self.all_leads)
        self.all_leads = self._filter_by_tier(self.all_leads)

        self.console.print(f"\n  [green]Pipeline complete: {len(self.all_leads)} leads ready[/green]\n")

        # SDR call list
        self._print_sdr_list(self.all_leads)

        # Save
        self._save_results(self.all_leads)

    def _run_signals(self) -> None:
        self.console.print("[bold]Running signal detection…[/bold]\n")
        cities = self._get_cities()
        self.all_signals = collect_all_signals(cities=cities, dry_run=self.dry_run)
        for sig_type, signals in self.all_signals.items():
            self.console.print(f"  {sig_type}: {len(signals)} signals")
        flat = [s for sigs in self.all_signals.values() for s in sigs]
        self.console.print(f"\n  [green]Total: {len(flat)} signals[/green]")
        self._save_results(flat, prefix="signals")

    def _run_enrich(self) -> None:
        self.console.print("[bold]Running signals + enrichment…[/bold]\n")
        cities = self._get_cities()
        self.all_signals = collect_all_signals(cities=cities, dry_run=self.dry_run)
        flat = [s for sigs in self.all_signals.values() for s in sigs]
        self.all_leads = enrich_signals(flat[:50], dry_run=self.dry_run)
        self.console.print(f"  [green]Enriched: {len(self.all_leads)} leads[/green]")
        self._save_results(self.all_leads, prefix="enriched")

    def _run_outreach(self) -> None:
        self.console.print("[bold]Running full pipeline through outreach…[/bold]\n")
        self._run_full_pipeline()

    def _run_sdr(self) -> None:
        self.console.print("[bold]Generating SDR call list…[/bold]\n")
        self._run_full_pipeline()

    # ── SDR output ────────────────────────────────────────────────────

    def _print_sdr_list(self, leads: list[dict]) -> None:
        hot = [l for l in leads if l.get("tier") == "HOT"]
        warm = [l for l in leads if l.get("tier") == "WARM"]

        self.console.print()
        self.console.print(Panel(
            f"[bold]{len(hot)} HOT[/bold] (call today) · [bold]{len(warm)} WARM[/bold] (call this week) · {len(leads)} total",
            title="SDR CALL LIST",
            border_style="cyan",
        ))

        table = Table(box=box.SIMPLE_HEAVY, show_lines=True)
        table.add_column("#", style="dim", width=3)
        table.add_column("Urgency", width=12)
        table.add_column("Company", style="bold", width=20)
        table.add_column("Contact", width=20)
        table.add_column("City", width=5)
        table.add_column("Signal", width=30)
        table.add_column("PKM Defense", width=18)
        table.add_column("Score", width=5)

        for i, lead in enumerate(leads[:25], 1):
            urgency_hours = lead.get("urgency_hours", 168)
            if urgency_hours <= 48:
                urgency = "[bold red]CALL NOW[/bold red]"
            elif urgency_hours <= 168:
                urgency = "[yellow]THIS WEEK[/yellow]"
            else:
                urgency = "[dim]2 WEEKS[/dim]"

            pkm = lead.get("pkm", {})
            defense = pkm.get("defense_mode", "—")[:16]

            contact_name = lead.get("name") or lead.get("founder_name", "—")
            contact_title = lead.get("title", "")
            contact_str = f"{contact_name}\n{contact_title}" if contact_title else contact_name

            channels = []
            if lead.get("whatsapp_verified"):
                channels.append("WA")
            if lead.get("email_valid"):
                channels.append("Email")
            if lead.get("linkedin_url"):
                channels.append("LI")
            channel_str = " ".join(channels)

            table.add_row(
                str(i),
                urgency,
                lead.get("company_name", "—"),
                f"{contact_str}\n[dim]{channel_str}[/dim]",
                lead.get("city", "—"),
                lead.get("signal_detail", "—")[:40],
                defense,
                str(lead.get("intent_score", 0)),
            )

        self.console.print(table)

        # Print WhatsApp opening for top 5
        self.console.print("\n[bold]Top WhatsApp Openings:[/bold]")
        for i, lead in enumerate(leads[:5], 1):
            msgs = lead.get("messages", {})
            wa = msgs.get("whatsapp", "—")
            self.console.print(f"\n  [cyan]#{i} {lead.get('company_name', '')}[/cyan]")
            self.console.print(f"  {wa}")

        if len(leads) > 25:
            self.console.print(f"\n  [dim]+ {len(leads) - 25} more in results file[/dim]")

    # ── Helpers ───────────────────────────────────────────────────────

    def _get_cities(self) -> list[str]:
        if self.args.city:
            return [self.args.city]
        if self.args.cities:
            return self.args.cities
        return list(CITIES.keys())

    def _filter_by_persona(self, leads: list[dict]) -> list[dict]:
        if self.args.persona:
            return [l for l in leads if l.get("persona") == self.args.persona]
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
        self.console.print(f"  Mode: {mode}  |  Run: {self.args.run}  |  Cities: {cities}  |  {persona}")
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
        with open(filename, "w") as f:
            json.dump(data, f, indent=2, default=str)
        self.console.print(f"  [dim]Saved: {filename}[/dim]")


# ── CLI ───────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="myHQ GTM Engine v2 — India-first signal intelligence",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--run",
        choices=["full", "signals", "enrich", "outreach", "sdr"],
        default="full",
    )
    parser.add_argument("--cities", nargs="+", choices=list(CITIES.keys()))
    parser.add_argument("--city", choices=list(CITIES.keys()))
    parser.add_argument("--persona", type=int, choices=[1, 2, 3])
    parser.add_argument("--tier", choices=["hot", "warm", "nurture", "monitor"])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")

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
