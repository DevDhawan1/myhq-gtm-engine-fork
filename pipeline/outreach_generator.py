"""myHQ GTM Engine — AI-powered hyper-personalized outreach generation.

Uses Claude API to generate WhatsApp, email, LinkedIn, and SDR call scripts.
India-first: WhatsApp primary, email secondary. Every message references the trigger.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime

from config.settings import ANTHROPIC_API_KEY, OPENROUTER_API_KEY, OPENROUTER_MODEL, DRY_RUN
from pipeline.utils import IST, upsert_to_supabase

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a senior sales consultant at myHQ, India's leading assisted workspace marketplace. myHQ helps companies find and set up dedicated office spaces across Bengaluru, Mumbai, Delhi-NCR, Hyderabad, and Pune — with zero broker games, GST-compliant billing, and setup in days not months.

Write outreach that is:
- Warm and direct (Indian B2B communication style)
- References the specific trigger event
- Professional but not stiff
- Has ONE CTA: book a 15-min call or site visit

Real myHQ statistics:
- 10,000+ workspace options across 5 cities
- Setup in 48-72 hours
- GST invoicing and compliance documentation
- No brokerage, no hidden charges
- Trusted by 5,000+ companies"""


class OutreachGenerator:
    """Generates hyper-personalized multi-channel outreach for leads."""

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
            except Exception as exc:
                logger.warning("Could not init OpenRouter client: %s", exc)
        # REVERT TO ANTHROPIC: uncomment below, comment out OpenRouter block above
        # if not dry_run and ANTHROPIC_API_KEY:
        #     try:
        #         import anthropic
        #         self.client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        #     except Exception as exc:
        #         logger.warning("Could not init Anthropic client: %s", exc)

    def generate_batch(self, leads: list[dict]) -> list[dict]:
        """Generate outreach for leads. PKM profile is MANDATORY — no profile, no outreach."""
        records: list[dict] = []
        pkm_blocked = 0
        for lead in leads:
            # PKM MANDATE: every message must be defense-calibrated
            if not lead.get("pkm") or not lead.get("pkm", {}).get("defense_mode"):
                pkm_blocked += 1
                logger.warning("PKM BLOCKED outreach: %s — no defense profile", lead.get("company_name"))
                continue
            try:
                rec = self.generate_for_lead(lead)
                if rec:
                    records.append(rec)
            except Exception as exc:
                logger.error("Outreach generation failed for %s: %s", lead.get("company_name"), exc)
        if pkm_blocked:
            logger.warning("PKM BLOCKED: %d leads had no defense profile — outreach skipped", pkm_blocked)
        self._store(records)
        return records

    def generate_for_lead(self, lead: dict) -> dict:
        """Generate outreach for a single lead. Requires lead['pkm'] with defense_mode."""
        wa1 = self._generate_whatsapp_touch1(lead)
        wa2 = self._generate_whatsapp_touch2(lead)
        email1 = self._generate_email(lead)
        email2 = self._generate_email_followup(lead)
        li = self._generate_linkedin_message(lead)
        script = self._generate_sdr_script(lead)
        return self._build_outreach_record(lead, wa1, wa2, email1, email2, li, script)

    # ── WhatsApp ────────────────────────────────────────────────────

    def _generate_whatsapp_touch1(self, lead: dict) -> str:
        if self.dry_run or not self.client:
            return self._synthetic_whatsapp1(lead)
        ctx = self._build_lead_context(lead)
        rules = self._get_personalization_rules(lead)
        prompt = f"""Write a WhatsApp message (under 100 words) for this lead. Conversational, warm, Indian B2B style. Use first name. ONE CTA: 15-min call.

{rules}

Lead context:
{ctx}

End with: Reply STOP to unsubscribe"""
        return self._call_claude(prompt, max_tokens=300)

    def _generate_whatsapp_touch2(self, lead: dict) -> str:
        if self.dry_run or not self.client:
            return self._synthetic_whatsapp2(lead)
        ctx = self._build_lead_context(lead)
        prompt = f"""Write a day-3 WhatsApp follow-up (under 100 words). Different angle from first message. Maybe share a stat or case study. Warm, not pushy.

Lead context:
{ctx}

End with: Reply STOP to unsubscribe"""
        return self._call_claude(prompt, max_tokens=300)

    # ── Email ───────────────────────────────────────────────────────

    def _generate_email(self, lead: dict) -> dict:
        if self.dry_run or not self.client:
            return self._synthetic_email1(lead)
        ctx = self._build_lead_context(lead)
        rules = self._get_personalization_rules(lead)
        prompt = f"""Write a cold email (subject + body under 150 words). Professional but warm. Reference the trigger event. ONE CTA.

{rules}

Lead context:
{ctx}

Output format:
SUBJECT: ...
BODY: ..."""
        text = self._call_claude(prompt, max_tokens=500)
        parts = text.split("BODY:", 1)
        subject = parts[0].replace("SUBJECT:", "").strip() if parts else "Quick question about office space"
        body = parts[1].strip() if len(parts) > 1 else text
        return {"subject": subject, "body": body}

    def _generate_email_followup(self, lead: dict) -> dict:
        if self.dry_run or not self.client:
            return self._synthetic_email2(lead)
        ctx = self._build_lead_context(lead)
        prompt = f"""Write a day-5 email follow-up (under 120 words). Reference the first email. New value prop or case study. Soft CTA.

Lead context:
{ctx}

Output format:
SUBJECT: ...
BODY: ..."""
        text = self._call_claude(prompt, max_tokens=400)
        parts = text.split("BODY:", 1)
        subject = parts[0].replace("SUBJECT:", "").strip() if parts else "Following up"
        body = parts[1].strip() if len(parts) > 1 else text
        return {"subject": subject, "body": body}

    # ── LinkedIn ────────────────────────────────────────────────────

    def _generate_linkedin_message(self, lead: dict) -> str:
        if self.dry_run or not self.client:
            return self._synthetic_linkedin(lead)
        ctx = self._build_lead_context(lead)
        prompt = f"""Write a LinkedIn connection request message (under 300 characters). Reference their trigger. Professional.

Lead context:
{ctx}"""
        return self._call_claude(prompt, max_tokens=150)[:300]

    # ── SDR call script ─────────────────────────────────────────────

    def _generate_sdr_script(self, lead: dict) -> dict:
        if self.dry_run or not self.client:
            return self._synthetic_sdr_script(lead)
        ctx = self._build_lead_context(lead)
        rules = self._get_personalization_rules(lead)
        prompt = f"""Generate an SDR call script as JSON with these keys:
- opening_line: Reference the trigger event
- qualifying_questions: Array of 3 questions
- value_proposition: For their persona
- objection_handlers: Object with keys "price", "timing", "competitor"
- cta: Closing CTA

{rules}

Lead context:
{ctx}"""
        text = self._call_claude(prompt, max_tokens=800)
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return {"opening_line": text[:200], "qualifying_questions": [], "value_proposition": "", "objection_handlers": {}, "cta": ""}

    # ── Helpers ─────────────────────────────────────────────────────

    def _build_lead_context(self, lead: dict) -> str:
        parts = [
            f"Company: {lead.get('company_name', 'N/A')}",
            f"Contact: {lead.get('contact_name', 'N/A')}, {lead.get('contact_title', 'N/A')}",
            f"City: {lead.get('city', 'N/A')}",
            f"Signal: {lead.get('signal_type', 'N/A')}",
            f"Company size: {lead.get('company_size', 'N/A')}",
            f"Persona: {lead.get('persona_name', 'N/A')}",
        ]
        if lead.get("company_last_funding_amount"):
            parts.append(f"Funding: {lead['company_last_funding_amount']}")
        if lead.get("company_investors"):
            parts.append(f"Investors: {', '.join(lead['company_investors'][:3])}")
        if lead.get("current_workspace") and lead["current_workspace"] != "unknown":
            parts.append(f"Current workspace: {lead['current_workspace']}")
        if lead.get("delta"):
            parts.append(f"New jobs this week: {lead['delta']}")
        if lead.get("sector"):
            parts.append(f"Sector: {lead['sector']}")
        return "\n".join(parts)

    def _get_personalization_rules(self, lead: dict) -> str:
        persona = lead.get("persona_id", 1)
        rules: list[str] = []

        # PKM defense mode rules (MANDATORY — injected into every prompt)
        pkm = lead.get("pkm", {})
        defense = pkm.get("defense_mode", "OVERLOAD_AVOIDANCE")
        bypass = pkm.get("bypass_strategy", "")
        forbidden = pkm.get("forbidden_phrases", [])
        cap = pkm.get("message_cap_words", 100)

        rules.append(f"DEFENSE MODE DETECTED: {defense}")
        if bypass:
            rules.append(f"BYPASS STRATEGY: {bypass}")
        if forbidden:
            rules.append(f"BANNED PHRASES (never use): {', '.join(forbidden)}")
        rules.append(f"WORD CAP: {cap} words maximum. HARD LIMIT.")

        # Persona-specific rules
        if persona == 1:
            rules.append("Mention their funding round and amount.")
        elif persona == 2:
            rules.append("Mention the specific city and hiring surge.")
        elif persona == 3:
            rules.append("Mention expansion city and compliance needs.")
        if lead.get("contact_name"):
            rules.append(f"Use first name: {lead['contact_name'].split()[0]}")
        if lead.get("current_workspace") and lead["current_workspace"] != "unknown":
            rules.append(f'Reference competitor: "Better than {lead["current_workspace"]} because..."')
        return "\n".join(rules)

    def _call_claude(self, user_prompt: str, max_tokens: int = 500) -> str:
        if not self.client:
            return ""
        try:
            # OpenRouter (OpenAI-compatible) — was Anthropic claude-sonnet-4
            resp = self.client.chat.completions.create(
                model=OPENROUTER_MODEL,
                max_tokens=max_tokens,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
            )
            return resp.choices[0].message.content
            # REVERT TO ANTHROPIC:
            # msg = self.client.messages.create(
            #     model="claude-sonnet-4-20250514",
            #     max_tokens=max_tokens,
            #     system=SYSTEM_PROMPT,
            #     messages=[{"role": "user", "content": user_prompt}],
            # )
            # return msg.content[0].text
        except Exception as exc:
            logger.error("LLM API call failed: %s", exc)
            return ""

    def _build_outreach_record(self, lead, wa1, wa2, email1, email2, li, script):
        return {
            "lead_id": lead.get("id"),
            "persona_id": lead.get("persona_id"),
            "whatsapp_touch_1": wa1,
            "whatsapp_touch_2": wa2,
            "email_subject": email1.get("subject", "") if isinstance(email1, dict) else "",
            "email_touch_1": email1.get("body", "") if isinstance(email1, dict) else str(email1),
            "email_touch_2": email2.get("body", "") if isinstance(email2, dict) else str(email2),
            "linkedin_connect": li,
            "sdr_call_script": script if isinstance(script, dict) else {"raw": str(script)},
            "generated_at": datetime.now(IST).isoformat(),
            "conversion_status": "pending",
            # Attach lead context for SDR dashboard
            "_lead": lead,
        }

    def _store(self, records: list[dict]) -> int:
        if self.dry_run:
            return len(records)
        clean = [{k: v for k, v in r.items() if not k.startswith("_")} for r in records]
        count = 0
        for rec in clean:
            if upsert_to_supabase("outreach", rec, dedup_field="lead_id"):
                count += 1
        return count

    # ── Synthetic outreach (dry run) ────────────────────────────────

    def _synthetic_whatsapp1(self, lead: dict) -> str:
        name = (lead.get("contact_name") or "there").split()[0]
        company = lead.get("company_name", "your company")
        persona = lead.get("persona_id", 1)
        if persona == 1:
            amount = lead.get("company_last_funding_amount", "your round")
            return f"Hi {name} 👋 Congrats on the {amount} raise for {company}! I'm from myHQ — we help funded startups get office-ready in 48 hours across Bangalore, Mumbai, Delhi, Hyderabad & Pune. Most founders need space within 60 days of raising. Would a quick 15-min call work to explore options? \n\nReply STOP to unsubscribe"
        elif persona == 2:
            city = lead.get("city", "your city")
            return f"Hi {name} 👋 Noticed {company} is scaling up hiring in {city} — that's exciting! At myHQ we help ops teams find the right workspace fast. GST invoicing, site visits, everything handled. Worth a 15-min chat to see options? \n\nReply STOP to unsubscribe"
        else:
            city = lead.get("city", "the city")
            return f"Hi {name} 👋 Saw that {company} is expanding to {city}. myHQ works with enterprises needing managed offices — full compliance docs, dedicated account manager, setup in days. Can we schedule a 15-min call? \n\nReply STOP to unsubscribe"

    def _synthetic_whatsapp2(self, lead: dict) -> str:
        name = (lead.get("contact_name") or "there").split()[0]
        return f"Hi {name}, just following up — we recently helped a similar company get set up with a 30-seat office in 72 hours, full GST billing from day one. Happy to share details if useful. Let me know if a quick call works this week! \n\nReply STOP to unsubscribe"

    def _synthetic_email1(self, lead: dict) -> dict:
        name = (lead.get("contact_name") or "there").split()[0]
        company = lead.get("company_name", "your company")
        persona = lead.get("persona_id", 1)
        if persona == 1:
            amount = lead.get("company_last_funding_amount", "your recent round")
            return {
                "subject": f"Office space for {company} post-funding?",
                "body": f"Hi {name},\n\nCongrats on raising {amount} for {company}! Most founders we work with need a proper office within 60 days of their raise.\n\nmyHQ helps funded startups find and set up dedicated workspace across 5 cities — in 48-72 hours, with GST invoicing, zero brokerage.\n\nWe've helped 5,000+ companies including many at your stage. Would love to share options that fit your team size and budget.\n\nFree for a 15-min call this week?\n\nBest,\nmyHQ Team",
            }
        return {
            "subject": f"Workspace options for {company} in {lead.get('city', 'your city')}",
            "body": f"Hi {name},\n\nI noticed {company} is growing its presence in {lead.get('city', 'the city')}. At myHQ, we help companies find the perfect workspace — from 10-seat startups to 500-seat enterprises.\n\n10,000+ options, 5 cities, setup in days not months. GST-compliant billing, no brokerage.\n\nWould love to understand your needs. Free for a quick call?\n\nBest,\nmyHQ Team",
        }

    def _synthetic_email2(self, lead: dict) -> dict:
        name = (lead.get("contact_name") or "there").split()[0]
        return {
            "subject": "Quick follow-up — workspace options",
            "body": f"Hi {name},\n\nJust circling back on my earlier note. Wanted to share that we recently helped a company similar to yours get set up in just 3 days — fully furnished, GST-ready, no hidden charges.\n\nIf timing isn't right, no worries at all. But if you'd like to see what's available, I'm happy to send over a few curated options.\n\nBest,\nmyHQ Team",
        }

    def _synthetic_linkedin(self, lead: dict) -> str:
        name = (lead.get("contact_name") or "").split()[0] or "there"
        company = lead.get("company_name", "your company")
        return f"Hi {name}, saw the exciting developments at {company}. I help growing teams find workspace across India's top cities. Would love to connect!"[:300]

    def _synthetic_sdr_script(self, lead: dict) -> dict:
        name = (lead.get("contact_name") or "there").split()[0]
        company = lead.get("company_name", "your company")
        persona = lead.get("persona_id", 1)

        if persona == 1:
            amount = lead.get("company_last_funding_amount", "your round")
            opening = f"Hi {name}, congrats on the {amount} raise for {company}! I'm from myHQ — we help funded startups get office-ready in 48 hours. Most founders we work with need space within 60 days of their raise. Do you have a minute?"
        elif persona == 2:
            city = lead.get("city", "your city")
            opening = f"Hi {name}, I noticed {company} is really ramping up hiring in {city}. I'm from myHQ — we handle workspace from shortlisting to setup so ops teams don't have to. Do you have a moment?"
        else:
            city = lead.get("city", "the city")
            opening = f"Hi {name}, I saw {company} is expanding to {city}. I'm from myHQ — we work with enterprise clients on managed office setups with full compliance documentation. Worth a quick chat?"

        return {
            "opening_line": opening,
            "qualifying_questions": [
                "How many desks/seats do you need initially?",
                f"Any specific areas in {lead.get('city', 'the city')} you're considering?",
                "What's your target date to be set up by?",
            ],
            "value_proposition": f"myHQ has 10,000+ workspace options across 5 cities. We handle everything — shortlisting, site visits, GST documentation. Setup in 48-72 hours, zero brokerage. Trusted by 5,000+ companies.",
            "objection_handlers": {
                "price": "We have options across every budget range. Plus zero brokerage means the price you see is what you pay — no hidden charges.",
                "timing": "Totally understand. We can set up a shortlist now so when the time comes, you're ready. Most of our clients wish they'd started the conversation earlier.",
                "competitor": f"We've actually had several teams switch from {lead.get('current_workspace', 'other providers')} to myHQ for better pricing and service. Happy to share references.",
            },
            "cta": "Can I send you 3-4 curated workspace options that match your needs? Takes 2 minutes to review.",
        }


# ── Module entry point ──────────────────────────────────────────────


def generate_outreach(leads: list[dict], dry_run: bool = False) -> list[dict]:
    """Entry point for outreach generation."""
    gen = OutreachGenerator(dry_run=dry_run)
    return gen.generate_batch(leads)
