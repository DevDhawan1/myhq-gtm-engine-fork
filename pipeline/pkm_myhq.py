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
    OPENROUTER_API_KEY,
    OPENROUTER_MODEL,
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
        # OpenRouter (gpt-oss-120b) — swap back to Anthropic by uncommenting below
        if not dry_run and OPENROUTER_API_KEY:
            try:
                from openai import OpenAI
                self.client = OpenAI(
                    base_url="https://openrouter.ai/api/v1",
                    api_key=OPENROUTER_API_KEY,
                )
            except Exception as e:
                logger.warning("Could not init OpenRouter client: %s", e)
        # REVERT TO ANTHROPIC: uncomment below, comment out OpenRouter block above
        # if not dry_run and ANTHROPIC_API_KEY:
        #     try:
        #         from anthropic import Anthropic
        #         self.client = Anthropic()
        #     except Exception as e:
        #         logger.warning("Could not init Anthropic client: %s", e)

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
            # OpenRouter (OpenAI-compatible) — was Anthropic claude-haiku-4-5
            resp = self.client.chat.completions.create(
                model=OPENROUTER_MODEL,
                max_tokens=400,
                messages=[
                    {"role": "system", "content": "Return only valid JSON. No preamble."},
                    {"role": "user", "content": prompt},
                ],
            )
            return json.loads(resp.choices[0].message.content)
            # REVERT TO ANTHROPIC:
            # resp = self.client.messages.create(
            #     model="claude-haiku-4-5-20251001",
            #     max_tokens=400,
            #     system="Return only valid JSON. No preamble.",
            #     messages=[{"role": "user", "content": prompt}],
            # )
            # return json.loads(resp.content[0].text)
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
        # OpenRouter (gpt-oss-120b) — swap back to Anthropic by uncommenting below
        if dry_run:
            logger.info("OutreachAgent: dry_run=True, skipping LLM client init")
        elif not OPENROUTER_API_KEY:
            logger.warning("OutreachAgent: OPENROUTER_API_KEY empty — rule-based fallback only")
        else:
            try:
                from openai import OpenAI
                self.client = OpenAI(
                    base_url="https://openrouter.ai/api/v1",
                    api_key=OPENROUTER_API_KEY,
                )
                logger.info("OutreachAgent: OpenRouter client initialized (model=%s)", OPENROUTER_MODEL)
            except Exception as e:
                logger.warning("OutreachAgent: OpenRouter init failed (%s: %s) — rule-based fallback", type(e).__name__, e)
        # REVERT TO ANTHROPIC: uncomment below, comment out OpenRouter block above
        # if not dry_run and ANTHROPIC_API_KEY:
        #     try:
        #         from anthropic import Anthropic
        #         self.client = Anthropic()
        #     except Exception:
        #         pass

    def generate_for_lead(self, lead: dict) -> dict:
        """Generate all outreach messages for a lead. PKM is MANDATORY."""
        pkm = lead.get("pkm")
        if not pkm or not pkm.get("defense_mode"):
            logger.warning("PKM BLOCKED outreach: %s — no defense profile", lead.get("company_name"))
            return {}

        company = lead.get("company_name", "")
        contact = lead.get("name") or lead.get("founder_name", "Founder")
        city = lead.get("city", "BLR")
        signal_detail = lead.get("signal_detail", "")
        persona = lead.get("persona") or lead.get("persona_id", 1)
        emp = lead.get("employee_count") or lead.get("company_size") or 10
        est_desks = max(5, min(emp // 3, 150))

        if self.dry_run or not self.client:
            if not self.client and not self.dry_run:
                logger.warning("Rule-based fallback for %s @ %s — LLM client not available", contact, company)
            return self._rule_based_messages(
                lead, pkm, company, contact, city, signal_detail, persona, est_desks
            )

        return self._ai_generate(
            lead, pkm, company, contact, city, signal_detail, persona, est_desks
        )

    def generate_batch(self, leads: list[dict]) -> list[dict]:
        """Generate outreach for a batch of leads. PKM is MANDATORY — no profile, no message."""
        pkm_blocked = 0
        for lead in leads:
            if not lead.get("pkm") or not lead.get("pkm", {}).get("defense_mode"):
                pkm_blocked += 1
                lead["messages"] = {}
                continue
            lead["messages"] = self.generate_for_lead(lead)
        if pkm_blocked:
            logger.warning("PKM BLOCKED: %d leads had no defense profile — messages not generated", pkm_blocked)
        return leads

    # Concrete, model-facing guidance per defense mode. The abstract
    # `bypass_strategy` string from PKM is not enough — LLMs need examples
    # and hard don'ts to actually internalize the frame.
    _DEFENSE_GUIDANCE = {
        "MOTIVE_INFERENCE": (
            "This founder spots sales pitches instantly. DO NOT open with "
            '"Congrats" or any enthusiasm about their raise. State the '
            "signal as fact, say what you have, leave. No CTA that assumes "
            "interest. Good opener: 'Saw the Nava news. If office is on "
            "the 90-day list, myHQ has 5 desks in BLR.'"
        ),
        "OVERLOAD_AVOIDANCE": (
            "This founder is drowning in messages. Your whole message must "
            "fit in 2 short sentences. No softeners, no warm-up. The "
            "reader must be able to decide in 3 seconds. Good opener: "
            "'3 lines: 5 desks in BLR, 48h setup, no lock-in. Reply y/n.'"
        ),
        "IDENTITY_THREAT": (
            "This founder protects status. Frame yourself as a peer or "
            "neutral observer, never as a vendor eager to help. No "
            "exclamation marks. No 'Your team is growing fast'. Good "
            "opener: 'Colleague recommended I mention this — myHQ runs "
            "workspace for a lot of Series-A founders in BLR. Worth a "
            "look when you're ready.'"
        ),
        "SOCIAL_PROOF_SKEPTICISM": (
            "This founder distrusts testimonials and generic claims. Use "
            "ONLY verifiable specifics you've been given: 48h setup, GST "
            "invoicing, no lock-in, desk count, city coverage. Never say "
            "'leading', 'trusted', '10000+ customers'. No adjectives."
        ),
        "AUTHORITY_DEFERENCE": (
            "This founder defers to known names and institutions. Lead "
            "with the institutional framing ('We work with portfolio "
            "companies of top Indian VCs across BLR'). Never invent "
            "specific VC or customer names you don't know."
        ),
        "COMPLEXITY_FEAR": (
            "This founder is overwhelmed by choice. Offer ONE clear path. "
            "No list of options. No 'whether you need X or Y'. Good "
            "opener: 'One path: send team size, pick from 3 desks we "
            "pre-shortlist, move in 48h. That's it.'"
        ),
    }

    def _ai_generate(self, lead, pkm, company, contact, city, signal_detail, persona, est_desks) -> dict:
        forbidden = pkm.get("forbidden_phrases", [])
        bypass = pkm.get("bypass_strategy", "")
        cap = pkm.get("message_cap_words", 80)
        defense = pkm.get("defense_mode", "OVERLOAD_AVOIDANCE")
        defense_guidance = self._DEFENSE_GUIDANCE.get(defense, "")

        persona_angles = {
            1: f"Funded founders use myHQ to get office-ready in 48 hours. No 11-month lease.",
            2: f"Companies expanding in {city} use myHQ's managed offices — {est_desks} seats ready this week.",
            3: f"Enterprises use myHQ for compliant managed workspaces with GST invoicing and SLA guarantees.",
        }

        system_prompt = f"""You write outreach for myHQ — India's leading flex workspace platform.

Defense mode: {defense}
Bypass strategy (abstract): {bypass}

Concrete guidance for this defense mode — follow this precisely:
{defense_guidance}

IMPORTANT about the example opener above: use its STYLE and REGISTER as a
reference, but DO NOT copy its exact words. Write a fresh opener for THIS
lead. Verbatim reuse across messages is a failure.

Message cap: {cap} words maximum. HARD LIMIT.

BANNED PHRASES (never use):
{json.dumps(forbidden)}

Context: {persona_angles.get(persona, "")}
myHQ coverage: {self.CITY_MHQ_STRENGTH.get(city, f"locations in {city}")}

FACTUAL GUARDRAILS — violating any of these is a failure:
- NEVER name any myHQ employee by first name ("Mukul at myHQ", "Priya from our team", etc.). You do not know their names.
- NEVER claim a colleague or shared connection passed along the contact unless the context explicitly says so.
- NEVER invent or name specific myHQ customers (no "InstaBites", "Hiver", "Razorpay", etc.). You do not know myHQ's customer list.
- NEVER name specific VCs or investors the lead is "talking to" (no "Accel talks", "Sequoia conversations"). You do not know this.
- NEVER cite specific % savings, ROI numbers, lease costs, or dollar/rupee amounts. You do not know them.
- NEVER use square-bracket placeholders like [link to X] or [Food delivery startup]. Every word in the output must be publishable as-is.
- NEVER claim a specific team/seat-count was "set up in X hours" at any company. You have no such data.
- If you would benefit from a case study or stat, REPHRASE without it. Omission is always safer than fabrication.

STYLE RULES:
- NO emojis. Zero. Not even 👋 or 🚀. Indian B2B founders read emoji as amateur-hour.
- NO exclamation marks. Keep the register neutral-professional.
- NO phrases like "going pro", "level up", "smart workspace momentum", "scale with your startup", or other marketing filler.
- NO rhetorical questions that presume excitement ("just raised $2M? Time to...").
- First name only on first reference, then no name repetition.

Allowed specifics (from the context above): myHQ's 48-hour setup, no lock-in, GST invoicing, the listed city coverage, the desk count {est_desks}, and the lead's own company name and trigger event.

Generate 3 messages. Return ONLY valid JSON, no prose, no markdown fences:
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

        # Try the LLM up to 3 times before falling back — transient 504s,
        # empty responses, and JSON parse errors are often one-time glitches.
        last_error = None
        last_raw = ""
        for attempt in range(1, 4):
            raw = ""
            try:
                resp = self.client.chat.completions.create(
                    model=OPENROUTER_MODEL,
                    max_tokens=1500,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                )
                raw = (resp.choices[0].message.content or "").strip()
                last_raw = raw
                if not raw:
                    raise ValueError("empty response body")
                # Strip ```json … ``` fences if present.
                if raw.startswith("```"):
                    raw = raw.strip("`")
                    if raw.lower().startswith("json"):
                        raw = raw[4:].lstrip()
                # Some models preface with prose — find the first { and parse from there.
                if raw and not raw.startswith("{"):
                    brace = raw.find("{")
                    if brace > 0:
                        raw = raw[brace:]
                return json.loads(raw)
            except Exception as e:
                last_error = e
                if attempt < 3:
                    logger.info(
                        "LLM attempt %d/3 failed for %s @ %s (%s) — retrying",
                        attempt, contact, company, type(e).__name__,
                    )
                continue

        snippet = last_raw[:200].replace("\n", " ") if last_raw else "<no response>"
        logger.warning(
            "AI outreach generation failed for %s @ %s after 3 attempts (%s: %s) | raw=%s — using rule-based fallback",
            contact, company, type(last_error).__name__, last_error, snippet,
        )
        return self._rule_based_messages(
            lead, pkm, company, contact, city, signal_detail, persona, est_desks
        )

    @staticmethod
    def _format_trigger(signal_detail: str, company: str, persona: int) -> str:
        """Return a grammatical clause that can follow "Hi {name} — …".

        Rule-based fallback must survive sparse/noisy signal data. Never
        emit broken sentences like "saw X just Grant (prize money) —
        Undisclosed".
        """
        raw = (signal_detail or "").strip()
        lowered = raw.lower()

        # Detect undisclosed / missing amount markers
        junk_markers = ("undisclosed", "none", "raised none", "n/a", "null")
        has_bad_amount = any(m in lowered for m in junk_markers)

        # Funding-like language present?
        is_funding = any(k in lowered for k in (
            "raised", "seed", "series", "pre-", "grant", "round", "bridge",
            "funding", "angel",
        ))

        # Hiring-like language present?
        is_hiring = any(k in lowered for k in ("hiring", "opening", "role", "jobs"))

        if not raw:
            return f"saw {company} is growing fast"

        if is_funding:
            if has_bad_amount:
                return f"saw {company} just closed a funding round"
            # Drop stray parenthetical noise and trim length
            clean = raw.replace("Raised ", "raised ").replace("  ", " ")
            clean = clean.split(" — ")[0].split(" - ")[0].strip()
            if len(clean) > 80:
                clean = clean[:80].rsplit(" ", 1)[0]
            # Ensure it reads naturally
            if not clean.startswith(("raised", "closed", "announced")):
                clean = "just closed a funding round"
            return f"saw {company} {clean}"

        if is_hiring:
            return f"saw {company} is hiring fast"

        # Unknown signal type — use a safe generic phrase rather than risk
        # pasting the raw string into a sentence.
        return f"saw {company} is growing fast"

    # Defense-mode-specific WhatsApp framings for persona 1 (funded founders).
    # Each mode needs a distinct angle; PKM is only useful if output differs.
    _P1_DEFENSE_WA = {
        "MOTIVE_INFERENCE":  # they detect pitch — lead with their news, no sell
            "{trigger}. Genuine congrats. Quick note: myHQ has {desks} desks in "
            "{city_name} ready in 48h if office is on the 90-day list. No pitch.",
        "OVERLOAD_AVOIDANCE":  # too busy — brevity first
            "{trigger}. One line: {desks} desks in {city_name}, 48h setup, no lock-in. "
            "Reply 'yes' if relevant, 'no' if not.",
        "IDENTITY_THREAT":  # protects status — peer framing
            "{trigger}. Wanted to flag: myHQ quietly handles workspace for most "
            "funded founders in {city_name}. {desks} desks open this week. "
            "Thought worth mentioning.",
        "SOCIAL_PROOF_SKEPTICISM":  # distrusts testimonials — specifics
            "{trigger}. myHQ: {desks} vetted desks in {city_name}, ₹X/seat transparent, "
            "48h move-in, GST invoicing. Data room if helpful.",
        "AUTHORITY_DEFERENCE":  # name-drops work
            "{trigger}. Portfolio companies of Elevation, Blume, Peak XV use myHQ "
            "for {city_name} desks. {desks} ready this week. Intro?",
        "COMPLEXITY_FEAR":  # simplify — offer ONE path
            "{trigger}. Simple: one link, 3 options, pick one, move in 48h. "
            "myHQ {city_name}. Want the link?",
    }

    def _rule_based_messages(self, lead, pkm, company, contact, city, signal_detail, persona, est_desks) -> dict:
        first_name = contact.split()[0] if contact else "there"
        city_name = CITIES.get(city, {}).get("name", city)
        myhq_str = self.CITY_MHQ_STRENGTH.get(city, f"locations in {city}")

        # Normalise signal_detail into a grammatical clause. Raw values like
        # "Grant (prize money) — Undisclosed" or "Raised None (Seed round)"
        # produce broken sentences when dropped into "saw X just {detail}".
        trigger_clause = self._format_trigger(signal_detail, company, persona)
        defense = (pkm or {}).get("defense_mode", "MOTIVE_INFERENCE")

        if persona == 1:
            # Select defense-specific WA template; fall through to the
            # generic MOTIVE_INFERENCE style if defense is unknown.
            template = self._P1_DEFENSE_WA.get(defense, self._P1_DEFENSE_WA["MOTIVE_INFERENCE"])
            body_line = template.format(
                trigger=(trigger_clause[:1].upper() + trigger_clause[1:]) if trigger_clause else "",
                desks=est_desks,
                city_name=city_name,
            )
            wa = f"Hi {first_name} — {body_line}"
            subj = f"{company} x myHQ — {city_name} desks ready"
            body = (
                f"Hi {first_name},\n\n"
                f"{(trigger_clause[:1].upper() + trigger_clause[1:]) if trigger_clause else ''}. "
                f"myHQ has managed offices in "
                f"{city_name} — {est_desks} seats, ready in 48 hours, no lock-in.\n\n"
                f"Worth exploring?\n\nBest,\nmyHQ Team"
            )
            li = (
                f"Hi {first_name}, {trigger_clause}. "
                f"myHQ has {est_desks} desks in {city_name} — ready in 48h."
            )
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
