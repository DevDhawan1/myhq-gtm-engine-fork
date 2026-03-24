"""myHQ GTM Engine v2 — PKM defense profiling for outreach.

Connects to AROS brain via Airtable PKM_Cache.

myHQ-specific defense modes by persona:

Persona 1 — Funded Founder (Seed/Series A):
  Primary: MOTIVE_INFERENCE (they detect pitch immediately)
  Secondary: IDENTITY_THREAT (they built this from nothing)
  Bypass: Lead with their funding news + city name + specific desk count

Persona 2 — Ops Expander (50-300 employees):
  Primary: OVERLOAD_AVOIDANCE (drowning in vendor pitches)
  Secondary: COMPLEXITY_FEAR (last coworking was a nightmare)
  Bypass: Ultra short, specific seats + city + one time slot

Persona 3 — Enterprise Expander (300+ employees):
  Primary: SOCIAL_PROOF_SKEPTICISM (needs enterprise names)
  Secondary: AUTHORITY_DEFERENCE (needs ammo for CRE head)
  Bypass: Named enterprise customers + SLA + GST invoice
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone

import requests

from config.settings_v2 import (
    AIRTABLE_API_KEY,
    AIRTABLE_BASE_ID,
    ANTHROPIC_API_KEY,
    CITIES,
    PERSONAS,
)

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
AIRTABLE_URL = "https://api.airtable.com/v0"

# Defense modes the PKM system can detect
DEFENSE_MODES = {
    "MOTIVE_INFERENCE": "Detects your intent immediately, decodes pitch before reading",
    "OVERLOAD_AVOIDANCE": "Too many vendor pitches, archives anything long",
    "IDENTITY_THREAT": "Built everything themselves, automation feels like replacement",
    "SOCIAL_PROOF_SKEPTICISM": "Technical, verifies every claim, needs proof",
    "AUTHORITY_DEFERENCE": "Needs approval from above, wants ammo to forward",
    "COMPLEXITY_FEAR": "Burned by previous tech vendor, fears complexity",
}


class PKMProfiler:
    """Profile prospect defense modes and generate bypass strategies."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.client = None
        if not dry_run and ANTHROPIC_API_KEY:
            try:
                from anthropic import Anthropic
                self.client = Anthropic()
            except Exception as e:
                logger.warning("Could not init Anthropic client: %s", e)

    def profile_prospect(self, lead: dict) -> dict:
        """Profile a prospect and return PKM defense mode + bypass strategy.

        Checks Airtable cache first. Stores result permanently.
        """
        company = lead.get("company_name", "")
        persona = lead.get("persona", 1)
        city = lead.get("city", "BLR")

        cache_key = self._cache_key(company, persona, city)

        # Check cache
        cached = self._check_cache(cache_key)
        if cached:
            logger.debug("PKM cache hit: %s", company)
            return cached

        # Classify
        if self.dry_run or not self.client:
            profile = self._rule_based_profile(lead)
        else:
            profile = self._ai_classify(lead)

        profile["company"] = company
        profile["persona"] = persona
        profile["city"] = city
        profile["cache_key"] = cache_key

        # Store in Airtable (feeds into AROS brain)
        self._store_cache(cache_key, profile, company, city)

        return profile

    def profile_batch(self, leads: list[dict]) -> list[dict]:
        """Profile a batch of leads. Returns leads with 'pkm' key added."""
        profiled: list[dict] = []
        for lead in leads:
            pkm = self.profile_prospect(lead)
            lead["pkm"] = pkm
            profiled.append(lead)
        logger.info("PKM profiled %d leads", len(profiled))
        return profiled

    # ── AI classification ─────────────────────────────────────────────

    def _ai_classify(self, lead: dict) -> dict:
        company = lead.get("company_name", "")
        persona = lead.get("persona", 1)
        city = lead.get("city", "BLR")
        signal_type = lead.get("signal_type", "")
        signal_detail = lead.get("signal_detail", "")
        title = lead.get("title", "")

        persona_ctx = {
            1: "Indian startup founder who just closed a funding round.",
            2: "Operations/Admin manager at a 50-300 person Indian company actively hiring.",
            3: "VP/Director at a 300+ person enterprise expanding to a new Indian city.",
        }

        prompt = f"""You are a persuasion psychology expert for Indian B2B sales.

Classify the defense mode for this prospect:
Company: {company}
City: {city}
Persona: {persona_ctx.get(persona, "Unknown")}
Signal: {signal_type} — {signal_detail}
Title: {title}

Choose the PRIMARY defense mode from this list:
{json.dumps(DEFENSE_MODES, indent=2)}

Return ONLY valid JSON:
{{
  "defense_mode": "MODE_NAME",
  "awareness_score": 0-10,
  "bypass_strategy": "one sentence on how to bypass",
  "forbidden_phrases": ["phrase1", "phrase2", "phrase3"],
  "message_cap_words": 60,
  "reasoning": "one sentence"
}}"""

        try:
            resp = self.client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=400,
                system="Return only valid JSON. No preamble.",
                messages=[{"role": "user", "content": prompt}],
            )
            return json.loads(resp.content[0].text)
        except Exception as e:
            logger.warning("PKM AI classify failed: %s — falling back to rules", e)
            return self._rule_based_profile(lead)

    # ── Rule-based fallback ───────────────────────────────────────────

    def _rule_based_profile(self, lead: dict) -> dict:
        persona = lead.get("persona", 1)
        persona_info = PERSONAS.get(persona, PERSONAS[1])

        profiles = {
            1: {
                "defense_mode": "MOTIVE_INFERENCE",
                "awareness_score": 8,
                "bypass_strategy": "Lead with their funding news + specific desk count + no lock-in",
                "forbidden_phrases": [
                    "hope this finds you well",
                    "I wanted to reach out",
                    "quick call",
                    "circle back",
                    "synergy",
                ],
                "message_cap_words": 60,
                "reasoning": "Funded founders detect sales pitch instantly — lead with their news",
            },
            2: {
                "defense_mode": "OVERLOAD_AVOIDANCE",
                "awareness_score": 5,
                "bypass_strategy": "Under 60 words, specific seat count, one calendar slot",
                "forbidden_phrases": [
                    "hope this finds you well",
                    "just checking in",
                    "would love to",
                    "quick question",
                    "at your convenience",
                ],
                "message_cap_words": 60,
                "reasoning": "Ops managers get 60+ vendor pitches/week — ultra-short wins",
            },
            3: {
                "defense_mode": "SOCIAL_PROOF_SKEPTICISM",
                "awareness_score": 6,
                "bypass_strategy": "Named enterprise customers + SLA guarantees + GST compliance",
                "forbidden_phrases": [
                    "flexible workspace",
                    "community",
                    "vibe",
                    "hustle",
                    "startup culture",
                ],
                "message_cap_words": 100,
                "reasoning": "Enterprise buyers need proof and ammo for internal approval",
            },
        }

        return profiles.get(persona, profiles[1])

    # ── Airtable cache ────────────────────────────────────────────────

    def _cache_key(self, company: str, persona: int, city: str) -> str:
        raw = f"{company}_{persona}_{city}".lower()
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def _check_cache(self, cache_key: str) -> dict:
        if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
            return {}
        try:
            resp = requests.get(
                f"{AIRTABLE_URL}/{AIRTABLE_BASE_ID}/PKM_Cache",
                headers={"Authorization": f"Bearer {AIRTABLE_API_KEY}"},
                params={"filterByFormula": f'{{cache_key}}="{cache_key}"'},
                timeout=8,
            )
            records = resp.json().get("records", [])
            if records:
                f = records[0]["fields"]
                return {
                    "defense_mode": f.get("detected_mode"),
                    "awareness_score": f.get("awareness_score", 5),
                    "bypass_strategy": f.get("bypass_strategy"),
                    "forbidden_phrases": json.loads(f.get("forbidden_phrases", "[]")),
                    "message_cap_words": f.get("message_cap_words", 60),
                    "from_cache": True,
                }
        except Exception as e:
            logger.debug("Airtable cache check failed: %s", e)
        return {}

    def _store_cache(self, cache_key: str, profile: dict, company: str, city: str):
        if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
            return
        try:
            requests.post(
                f"{AIRTABLE_URL}/{AIRTABLE_BASE_ID}/PKM_Cache",
                headers={
                    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "fields": {
                        "cache_key": cache_key,
                        "profile_text": f"{company} {city}",
                        "detected_mode": profile.get("defense_mode"),
                        "confidence": profile.get("awareness_score", 5) * 10,
                        "reasoning": profile.get("reasoning", ""),
                        "awareness_score": profile.get("awareness_score", 5),
                        "bypass_strategy": profile.get("bypass_strategy", ""),
                        "forbidden_phrases": json.dumps(profile.get("forbidden_phrases", [])),
                        "message_cap_words": profile.get("message_cap_words", 60),
                        "source": "myhq_gtm_agent_v2",
                        "analyzed_at": datetime.now(IST).isoformat(),
                    }
                },
                timeout=8,
            )
        except Exception as e:
            logger.debug("Airtable cache store failed: %s", e)


class OutreachGeneratorV2:
    """Generate WhatsApp + Email + LinkedIn messages using PKM bypass strategy."""

    CITY_MHQ_STRENGTH = {
        "BLR": "50+ locations in Bengaluru",
        "MUM": "40+ locations in Mumbai",
        "DEL": "35+ locations in Delhi-NCR",
        "HYD": "25+ locations in Hyderabad",
        "PUN": "20+ locations in Pune",
    }

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.client = None
        if not dry_run and ANTHROPIC_API_KEY:
            try:
                from anthropic import Anthropic
                self.client = Anthropic()
            except Exception:
                pass

    def generate_for_lead(self, lead: dict) -> dict:
        """Generate all outreach messages for a lead."""
        pkm = lead.get("pkm", {})
        company = lead.get("company_name", "")
        contact = lead.get("name") or lead.get("founder_name", "Founder")
        city = lead.get("city", "BLR")
        signal_detail = lead.get("signal_detail", "")
        persona = lead.get("persona", 1)
        emp = lead.get("employee_count") or 10
        est_desks = max(5, min(emp // 3, 150))

        if self.dry_run or not self.client:
            return self._rule_based_messages(
                lead, pkm, company, contact, city, signal_detail, persona, est_desks
            )

        return self._ai_generate(
            lead, pkm, company, contact, city, signal_detail, persona, est_desks
        )

    def generate_batch(self, leads: list[dict]) -> list[dict]:
        """Generate outreach for a batch of leads."""
        for lead in leads:
            lead["messages"] = self.generate_for_lead(lead)
        return leads

    def _ai_generate(self, lead, pkm, company, contact, city, signal_detail, persona, est_desks) -> dict:
        forbidden = pkm.get("forbidden_phrases", [])
        bypass = pkm.get("bypass_strategy", "")
        cap = pkm.get("message_cap_words", 80)
        defense = pkm.get("defense_mode", "OVERLOAD_AVOIDANCE")

        persona_angles = {
            1: f"Funded founders use myHQ to get office-ready in 48 hours. No 11-month lease.",
            2: f"Companies expanding in {city} use myHQ's managed offices — {est_desks} seats ready this week.",
            3: f"Enterprises use myHQ for compliant managed workspaces with GST invoicing and SLA guarantees.",
        }

        system_prompt = f"""You write outreach for myHQ — India's leading flex workspace platform.

Defense mode: {defense}
Bypass strategy: {bypass}
Message cap: {cap} words maximum. HARD LIMIT.

BANNED PHRASES (never use):
{json.dumps(forbidden)}

Context: {persona_angles.get(persona, "")}
myHQ coverage: {self.CITY_MHQ_STRENGTH.get(city, f"locations in {city}")}

Generate 3 messages. Return ONLY valid JSON:
{{
  "whatsapp": "under {min(cap, 80)} words, conversational, no formal salutation",
  "email_subject": "under 8 words",
  "email_body": "under {cap} words, plain text",
  "linkedin": "under 280 chars"
}}"""

        user_prompt = f"""Contact: {contact}
Company: {company}
City: {city}
Signal: {signal_detail}
Estimated seats: {est_desks}

Write 3 messages. WhatsApp first — India is WhatsApp-first."""

        try:
            resp = self.client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=600,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
            )
            return json.loads(resp.content[0].text)
        except Exception as e:
            logger.warning("AI outreach generation failed: %s — using rule-based", e)
            return self._rule_based_messages(
                lead, pkm, company, contact, city, signal_detail, persona, est_desks
            )

    def _rule_based_messages(self, lead, pkm, company, contact, city, signal_detail, persona, est_desks) -> dict:
        first_name = contact.split()[0] if contact else "there"
        city_name = CITIES.get(city, {}).get("name", city)
        myhq_str = self.CITY_MHQ_STRENGTH.get(city, f"locations in {city}")

        if persona == 1:
            wa = (
                f"Hi {first_name} — saw {company} just {signal_detail}. "
                f"myHQ has {est_desks} desks ready in {city_name} this week. "
                f"No lock-in, GST invoicing, 48h setup. Worth 10 min?"
            )
            subj = f"{company} x myHQ — {city_name} desks ready"
            body = (
                f"Hi {first_name},\n\n"
                f"Saw {signal_detail}. myHQ has managed offices in {city_name} — "
                f"{est_desks} seats, ready in 48 hours, no lock-in.\n\n"
                f"Worth exploring?\n\nBest,\nmyHQ Team"
            )
            li = f"Hi {first_name}, saw {signal_detail}. myHQ has {est_desks} desks in {city_name} — ready in 48h."
        elif persona == 2:
            wa = (
                f"Hi {first_name} — {company} is hiring fast in {city_name}. "
                f"myHQ has {est_desks} seats ready this week. {myhq_str}. "
                f"One call, we handle the rest."
            )
            subj = f"{est_desks} seats in {city_name} — ready this week"
            body = (
                f"Hi {first_name},\n\n"
                f"Noticed {company} is scaling in {city_name}. myHQ handles workspace "
                f"end-to-end — shortlisting, site visits, GST docs.\n\n"
                f"{est_desks} seats available now. Worth a 15-min call?\n\nBest,\nmyHQ Team"
            )
            li = f"Hi {first_name}, {company} is growing in {city_name}. myHQ has {est_desks} seats ready — GST invoicing, zero brokerage."
        else:
            wa = (
                f"Hi {first_name} — {company} expanding to {city_name}? "
                f"myHQ works with enterprises on managed offices. SLA guarantees, "
                f"GST compliance, {myhq_str}. Happy to share references."
            )
            subj = f"Enterprise workspace in {city_name} — myHQ"
            body = (
                f"Hi {first_name},\n\n"
                f"Saw {company} is expanding to {city_name}. myHQ provides enterprise "
                f"managed workspaces with SLA guarantees, GST invoicing, and compliance "
                f"documentation.\n\nHappy to share references from similar companies.\n\nBest,\nmyHQ Team"
            )
            li = f"Hi {first_name}, saw {company} expanding to {city_name}. myHQ does enterprise managed offices — SLA, GST, compliance."

        return {
            "whatsapp": wa,
            "email_subject": subj,
            "email_body": body,
            "linkedin": li[:280],
        }


# ── Module entry points ──────────────────────────────────────────────


def profile_leads(leads: list[dict], dry_run: bool = False) -> list[dict]:
    """Profile all leads with PKM defense modes."""
    profiler = PKMProfiler(dry_run=dry_run)
    return profiler.profile_batch(leads)


def generate_outreach(leads: list[dict], dry_run: bool = False) -> list[dict]:
    """Generate outreach for all leads using PKM bypass strategies."""
    generator = OutreachGeneratorV2(dry_run=dry_run)
    return generator.generate_batch(leads)
