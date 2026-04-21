"""myHQ GTM Engine — SDR call list dashboard with Rich terminal UI.

Every lead card tells the SDR exactly what to say before dialing.
"""

from __future__ import annotations

import json
import logging
import os
from collections import Counter
from datetime import datetime, date

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import box

from config.settings import CITIES, INTENT_TIERS
from pipeline.utils import IST, hours_since

logger = logging.getLogger(__name__)


class SDRDashboard:
    """Generates and displays prioritised SDR call lists."""

    TIER_STYLE = {"HOT": "bold red", "WARM": "bold yellow", "NURTURE": "blue", "MONITOR": "dim"}
    TIER_EMOJI = {"HOT": "🔥", "WARM": "♨️", "NURTURE": "🌡️", "MONITOR": "👁️"}

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.console = Console()

    # ── Call list generation ────────────────────────────────────────

    def generate_call_list(self, leads: list[dict], city: str | None = None) -> list[dict]:
        """Filter and rank leads for SDR call queue."""
        filtered = leads
        if city:
            filtered = [l for l in filtered if l.get("city") == city]
        # Only HOT and WARM for active call list
        filtered = [l for l in filtered if l.get("tier") in ("HOT", "WARM")]
        filtered.sort(key=lambda x: x.get("intent_score", 0), reverse=True)
        for i, lead in enumerate(filtered):
            lead["priority_rank"] = i + 1
        return filtered

    # ── Terminal display ────────────────────────────────────────────

    def display_call_list(self, call_list: list[dict]) -> None:
        """Display call list as rich lead cards in terminal."""
        if not call_list:
            self.console.print("\n[dim]No leads in call queue.[/dim]\n")
            return

        self.console.print(f"\n[bold]📞 SDR CALL LIST — {len(call_list)} leads[/bold]\n")
        for lead in call_list[:20]:  # Show top 20
            self._render_lead_card(lead)

    def _render_lead_card(self, lead: dict) -> None:
        """Render a single lead as a Rich Panel."""
        tier = lead.get("tier", "MONITOR")
        style = self.TIER_STYLE.get(tier, "white")
        emoji = self.TIER_EMOJI.get(tier, "")
        rank = lead.get("priority_rank", "?")
        score = lead.get("intent_score", 0)
        company = lead.get("company_name", "Unknown")
        city_code = lead.get("city", "?")
        city_name = CITIES.get(city_code, {}).get("name", city_code)
        contact = lead.get("contact_name", "Unknown")
        title = lead.get("contact_title", "")
        phone = lead.get("contact_phone", "N/A")
        whatsapp = "✓" if lead.get("contact_whatsapp") else "✗"
        linkedin = lead.get("contact_linkedin", "N/A")
        email = lead.get("contact_email", "")

        # Trigger info
        signal_type = lead.get("signal_type", "")
        trigger = self._format_trigger(lead)
        time_ago = self._format_time_ago(lead.get("announcement_date") or lead.get("created_at", ""))

        # SDR script
        sdr_notes = lead.get("sdr_notes", "")
        persona_name = lead.get("persona_name", "")

        # Build card sections
        header = f"{emoji} LEAD #{rank} — {'CALL NOW' if tier == 'HOT' else 'CALL TODAY'}"
        identity = f"Company: {company} | City: {city_name} | Score: {score}/100\n"
        identity += f"Contact: {contact}, {title}\n"
        identity += f"Phone: {phone} (WhatsApp {whatsapp})\n"
        if email:
            identity += f"Email: {email}\n"
        if linkedin and linkedin != "N/A":
            identity += f"LinkedIn: {linkedin}"

        trigger_section = f"TRIGGER: {trigger} ({time_ago})"
        if lead.get("company_investors"):
            trigger_section += f"\nINVESTORS: {', '.join(lead['company_investors'][:4])}"
        team_size = lead.get("company_size") or lead.get("employee_count_est") or lead.get("company_size_est")
        if team_size:
            trigger_section += f"\nTEAM SIZE: ~{team_size} people"
        if lead.get("sector"):
            trigger_section += f"\nSECTOR: {lead['sector']}"
        workspace = lead.get("current_workspace", "unknown")
        trigger_section += f"\nCURRENT OFFICE: {workspace}"

        # Outreach info
        outreach = lead.get("_outreach", {})
        script = outreach.get("sdr_call_script", {}) if isinstance(outreach, dict) else {}

        opening = ""
        if isinstance(script, dict) and script.get("opening_line"):
            opening = f'\nOPENING LINE:\n"{script["opening_line"]}"'

        questions = ""
        if isinstance(script, dict) and script.get("qualifying_questions"):
            qs = script["qualifying_questions"]
            questions = "\n3 QUALIFYING QUESTIONS:"
            for i, q in enumerate(qs[:3], 1):
                questions += f"\n  {i}. {q}"

        status_line = f"WHATSAPP SENT: Not yet  |  EMAIL SENT: Not yet\nBEST TIME TO CALL: 10am-12pm IST"

        body = f"{identity}\n{'─' * 56}\n{trigger_section}"
        if opening:
            body += f"\n{'─' * 56}{opening}"
        if questions:
            body += f"\n{'─' * 56}{questions}"
        body += f"\n{'─' * 56}\n{status_line}"

        panel = Panel(
            body,
            title=f"[{style}]{header}[/{style}]",
            subtitle=f"[dim]Persona: {persona_name}[/dim]",
            border_style=style,
            box=box.ROUNDED,
            expand=False,
            width=62,
        )
        self.console.print(panel)
        self.console.print()

    def _format_trigger(self, lead: dict) -> str:
        signal_type = lead.get("signal_type", "")
        if signal_type == "funding":
            amount = lead.get("company_last_funding_amount", "undisclosed")
            round_type = lead.get("round_type", "funding")
            return f"Raised {amount} ({round_type} round)"
        if signal_type == "hiring":
            delta = lead.get("delta", "?")
            return f"{delta} new jobs posted this week"
        if signal_type == "expansion":
            return f"Expanding to {CITIES.get(lead.get('city', ''), {}).get('name', lead.get('city', ''))}"
        if signal_type == "intent":
            return f"Active workspace search on {lead.get('source', 'social media')}"
        return "Signal detected"

    def _format_time_ago(self, date_str: str) -> str:
        if not date_str:
            return "recently"
        h = hours_since(date_str)
        if h < 1:
            return "just now"
        if h < 24:
            return f"{int(h)}h ago"
        days = int(h / 24)
        if days == 1:
            return "1 day ago"
        return f"{days} days ago"

    # ── Summary dashboard ───────────────────────────────────────────

    def display_summary_dashboard(self, all_leads: list[dict]) -> None:
        """Display rich summary dashboard in terminal."""
        stats = self._build_summary_stats(all_leads)

        self.console.print()

        # Signal summary table
        signal_table = Table(title="📡 Signal Summary", box=box.SIMPLE_HEAVY)
        signal_table.add_column("Signal Type", style="bold")
        signal_table.add_column("Count", justify="right")
        for stype, count in stats["by_signal_type"].items():
            signal_table.add_row(stype.title(), str(count))
        signal_table.add_row("[bold]Total[/bold]", f"[bold]{stats['total']}[/bold]")

        # City distribution
        city_table = Table(title="🏙️  Leads by City", box=box.SIMPLE_HEAVY)
        city_table.add_column("City", style="bold")
        city_table.add_column("Count", justify="right")
        city_table.add_column("HOT", justify="right", style="red")
        city_table.add_column("WARM", justify="right", style="yellow")
        for code in ["BLR", "MUM", "DEL", "HYD", "PUN"]:
            name = CITIES.get(code, {}).get("name", code)
            total = stats["by_city"].get(code, 0)
            hot = stats["hot_by_city"].get(code, 0)
            warm = stats["warm_by_city"].get(code, 0)
            if total > 0:
                city_table.add_row(name, str(total), str(hot), str(warm))

        # Persona distribution
        persona_table = Table(title="👤 Persona Breakdown", box=box.SIMPLE_HEAVY)
        persona_table.add_column("Persona", style="bold")
        persona_table.add_column("Count", justify="right")
        persona_names = {1: "Funded Founder", 2: "Ops Expander", 3: "Enterprise Expander"}
        for pid in [1, 2, 3]:
            count = stats["by_persona"].get(pid, 0)
            persona_table.add_row(persona_names.get(pid, f"P{pid}"), str(count))

        # Tier breakdown
        tier_table = Table(title="🎯 Intent Tiers", box=box.SIMPLE_HEAVY)
        tier_table.add_column("Tier", style="bold")
        tier_table.add_column("Count", justify="right")
        tier_table.add_column("Action", style="dim")
        tier_actions = {"HOT": "Call within 2 hours", "WARM": "Call within 24 hours", "NURTURE": "WhatsApp + email", "MONITOR": "Watch for upgrade"}
        for tier in ["HOT", "WARM", "NURTURE", "MONITOR"]:
            count = stats["by_tier"].get(tier, 0)
            style = self.TIER_STYLE.get(tier, "")
            emoji = self.TIER_EMOJI.get(tier, "")
            tier_table.add_row(f"[{style}]{emoji} {tier}[/{style}]", str(count), tier_actions.get(tier, ""))

        self.console.print(signal_table)
        self.console.print(city_table)
        self.console.print(persona_table)
        self.console.print(tier_table)

        if stats.get("total_funding"):
            self.console.print(f"\n[bold green]💰 Total funding in pipeline: {stats['total_funding']}[/bold green]")
        self.console.print(f"[bold]📊 Average intent score: {stats['avg_score']:.0f}/100[/bold]\n")

    def _build_summary_stats(self, leads: list[dict]) -> dict:
        by_signal = Counter(l.get("signal_type", "unknown") for l in leads)
        by_city = Counter(l.get("city", "?") for l in leads)
        by_persona = Counter(l.get("persona_id", 0) for l in leads)
        by_tier = Counter(l.get("tier", "MONITOR") for l in leads)
        hot_by_city = Counter(l.get("city", "?") for l in leads if l.get("tier") == "HOT")
        warm_by_city = Counter(l.get("city", "?") for l in leads if l.get("tier") == "WARM")
        scores = [l.get("intent_score", 0) for l in leads]

        # Try to sum funding amounts (rough)
        funding_leads = [l for l in leads if l.get("signal_type") == "funding" and l.get("company_last_funding_amount")]
        total_funding = f"{len(funding_leads)} funded companies in pipeline"

        return {
            "total": len(leads),
            "by_signal_type": dict(by_signal),
            "by_city": dict(by_city),
            "by_persona": dict(by_persona),
            "by_tier": dict(by_tier),
            "hot_by_city": dict(hot_by_city),
            "warm_by_city": dict(warm_by_city),
            "avg_score": sum(scores) / len(scores) if scores else 0,
            "total_funding": total_funding,
        }

    # ── File output ─────────────────────────────────────────────────

    def save_call_list_json(self, call_list: list[dict], city: str | None = None) -> str:
        date_str = date.today().isoformat()
        city_str = city or "all"
        os.makedirs("results", exist_ok=True)
        filename = f"results/sdr_call_list_{date_str}_{city_str}.json"
        # Clean non-serializable fields
        clean = []
        for l in call_list:
            c = {k: v for k, v in l.items() if not k.startswith("_") and k != "persona_details" and k != "score_breakdown" and k != "persona_match_scores"}
            clean.append(c)
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(clean, f, indent=2, default=str)
        return filename

    def save_briefing_markdown(self, call_list: list[dict], city: str | None = None) -> str:
        date_str = date.today().isoformat()
        city_str = city or "all"
        os.makedirs("results", exist_ok=True)
        filename = f"results/sdr_briefing_{date_str}_{city_str}.md"

        lines = [
            f"# myHQ SDR Briefing — {date_str}",
            f"**City:** {CITIES.get(city, {}).get('name', 'All Cities') if city else 'All Cities'}",
            f"**Total leads:** {len(call_list)}",
            f"**Generated:** {datetime.now(IST).strftime('%Y-%m-%d %H:%M IST')}",
            "",
            "---",
            "",
        ]

        for lead in call_list:
            tier = lead.get("tier", "MONITOR")
            emoji = self.TIER_EMOJI.get(tier, "")
            lines.append(f"## {emoji} Lead #{lead.get('priority_rank', '?')} — {lead.get('company_name', 'Unknown')}")
            lines.append(f"**Score:** {lead.get('intent_score', 0)}/100 | **City:** {CITIES.get(lead.get('city', ''), {}).get('name', lead.get('city', ''))} | **Tier:** {tier}")
            lines.append(f"**Contact:** {lead.get('contact_name', 'N/A')}, {lead.get('contact_title', '')}")
            lines.append(f"**Phone:** {lead.get('contact_phone', 'N/A')} | **WhatsApp:** {'Yes' if lead.get('contact_whatsapp') else 'No'}")
            if lead.get("contact_email"):
                lines.append(f"**Email:** {lead['contact_email']}")
            lines.append("")
            lines.append(f"**Trigger:** {self._format_trigger(lead)}")
            if lead.get("sdr_notes"):
                lines.append(f"**Notes:** {lead['sdr_notes']}")
            lines.append("")
            lines.append("---")
            lines.append("")

        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        return filename


# ── Module entry point ──────────────────────────────────────────────


def generate_sdr_dashboard(leads: list[dict], city: str | None = None, dry_run: bool = False) -> list[dict]:
    """Entry point: generate and display SDR dashboard."""
    dashboard = SDRDashboard(dry_run=dry_run)
    call_list = dashboard.generate_call_list(leads, city)
    dashboard.display_summary_dashboard(leads)
    dashboard.display_call_list(call_list)
    json_file = dashboard.save_call_list_json(call_list, city)
    md_file = dashboard.save_briefing_markdown(call_list, city)
    dashboard.console.print(f"\n[green]Saved:[/green] {json_file}")
    dashboard.console.print(f"[green]Saved:[/green] {md_file}")
    return call_list
