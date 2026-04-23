"""myHQ GTM Engine v2 — WhatsApp automation for India B2B.

BSP: WATI (Meta certified, simple REST API, good dashboard)
Auth: Bearer token from WATI dashboard → API → Access Token
Base URL: WATI_BASE_URL env var (e.g. https://live-mt-server.wati.io/<account_id>)

Cost: ~$49/mo (1K conversations) or $99/mo (3K) + Meta per-conversation fees.

Sequence per lead (3-touch rule, TRAI compliant):
  Day 0:  WhatsApp template (PKM-calibrated)
  Day 3:  WhatsApp follow-up (different angle, same defense bypass)
  Day 5:  Email (if no WA reply)
  Day 7:  LinkedIn message (if no email reply)
  STOP.   7-day cooling period minimum.

All messages reference the specific signal that triggered outreach.
All templates pre-approved by Meta via WATI dashboard.
"""

from __future__ import annotations

import json
import logging
import os
import smtplib
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText

import requests

from config.settings_v2 import (
    AIRTABLE_API_KEY,
    AIRTABLE_BASE_ID,
    ANTHROPIC_API_KEY,
    OPENROUTER_API_KEY,
    OPENROUTER_MODEL,
    SMTP_PASS,
    SMTP_USER,
)

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))

WATI_API_TOKEN = os.getenv("WATI_API_TOKEN", "")
WATI_BASE_URL = os.getenv("WATI_BASE_URL", "").rstrip("/")

ALERT_EMAIL = os.getenv("ALERT_EMAIL", SMTP_USER)

# ── Template registry — each calibrated to a PKM defense mode ────────
# Templates must be pre-approved by Meta via WATI dashboard.
# Approval takes 24-72 hours per template. Submit all 5 on day one.

# Each template's `variables` list defines the ordered mapping of lead data
# into the Meta-approved body's {{1}}, {{2}}, ... placeholders. The order here
# MUST match the placeholder order in the template submitted to WATI/Meta.
WHATSAPP_TEMPLATES: dict[str, dict] = {
    "MOTIVE_INFERENCE": {
        "template_name": "myhq_data_first_v1",
        "variables": ["contact_name", "funding_round", "city", "seats", "calendar_link"],
    },
    "OVERLOAD_AVOIDANCE": {
        "template_name": "myhq_ultra_short_v1",
        "variables": ["contact_name", "company", "city", "seats", "calendar_link"],
    },
    "IDENTITY_THREAT": {
        "template_name": "myhq_amplify_v1",
        "variables": ["contact_name", "company", "headcount", "city", "seats", "calendar_link"],
    },
    "SOCIAL_PROOF_SKEPTICISM": {
        "template_name": "myhq_proof_v1",
        "variables": ["contact_name", "city", "headcount", "seats"],
    },
    "AUTHORITY_DEFERENCE": {
        "template_name": "myhq_authority_v1",
        "variables": ["contact_name", "city", "seats"],
    },
    "COMPLEXITY_FEAR": {
        "template_name": "myhq_simple_v1",
        "variables": ["contact_name", "seats", "city"],
    },
}


class WhatsAppSender:
    """Send PKM-calibrated WhatsApp messages via WATI BSP."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run

    def send_for_lead(self, lead: dict) -> dict:
        """Send a WhatsApp message to a single lead. PKM profile is MANDATORY."""
        phone = lead.get("phone_mobile", "")
        contact = lead.get("name") or lead.get("founder_name", "")
        company = lead.get("company_name", "")

        # PKM MANDATE: no profile → no send
        pkm = lead.get("pkm")
        if not pkm or not pkm.get("defense_mode"):
            logger.warning("PKM BLOCKED: %s — no defense profile, refusing to send", company)
            return {"success": False, "error": "pkm_missing", "company": company}

        defense = pkm["defense_mode"]

        # Guard: MOTIVE_INFERENCE template opens with "Congrats on the {funding_round}"
        # which only reads correctly when the triggering signal is actually a funding event.
        # For any other signal type, fall back to the neutral short template.
        if defense == "MOTIVE_INFERENCE" and lead.get("signal_type") != "FUNDING":
            defense = "OVERLOAD_AVOIDANCE"

        clean_phone = _clean_indian_number(phone)
        if not clean_phone:
            return {"success": False, "error": "invalid_phone", "company": company}

        template_cfg = WHATSAPP_TEMPLATES.get(defense, WHATSAPP_TEMPLATES["OVERLOAD_AVOIDANCE"])
        template_name = template_cfg["template_name"]
        vars_dict = self._build_vars(lead)
        parameters = [
            {"name": str(i + 1), "value": str(vars_dict.get(var, ""))}
            for i, var in enumerate(template_cfg["variables"])
        ]

        if self.dry_run or not WATI_API_TOKEN:
            return self._mock_send(clean_phone, company, defense, template_name, parameters)

        # TRAI DND check
        if _is_on_dnd(clean_phone):
            self._queue_to_airtable(lead, "dnd_blocked")
            return {"success": False, "error": "dnd_registered", "company": company}

        try:
            number = clean_phone.replace("+", "")
            resp = requests.post(
                f"{WATI_BASE_URL}/api/v1/sendTemplateMessage?whatsappNumber={number}",
                headers={
                    "Authorization": f"Bearer {WATI_API_TOKEN}",
                    "Content-Type": "application/json",
                },
                json={
                    "template_name": template_name,
                    "broadcast_name": "myhq_gtm",
                    "parameters": parameters,
                },
                timeout=10,
            )

            result = resp.json()
            success = result.get("result") is True

            send_result = {
                "success": success,
                "message_id": result.get("messageId"),
                "template_used": template_name,
                "defense_mode": defense,
                "phone": clean_phone,
                "company": company,
                "timestamp": datetime.now(IST).isoformat(),
            }

            # Queue to Airtable
            self._queue_to_airtable(lead, "sent" if success else "failed", send_result)

            return send_result

        except Exception as e:
            logger.error("WA send failed for %s: %s", company, e)
            return {"success": False, "error": str(e), "company": company}

    def send_batch(self, leads: list[dict]) -> list[dict]:
        """Send WhatsApp to all qualified leads. PKM is mandatory — no profile, no send."""
        results: list[dict] = []
        pkm_blocked = 0
        for lead in leads:
            if not lead.get("whatsapp_verified"):
                continue
            if lead.get("dnd_status"):
                continue
            if not lead.get("pkm") or not lead.get("pkm", {}).get("defense_mode"):
                pkm_blocked += 1
                continue
            result = self.send_for_lead(lead)
            results.append(result)
        if pkm_blocked:
            logger.warning("PKM BLOCKED: %d leads skipped — no defense profile", pkm_blocked)
        logger.info("WA batch: %d sent, %d failed",
                     sum(1 for r in results if r.get("success")),
                     sum(1 for r in results if not r.get("success")))
        return results

    def _build_vars(self, lead: dict) -> dict:
        """Superset of every variable any template might need. Each template's
        `variables` list projects this into the ordered WATI parameters array."""
        emp = lead.get("employee_count") or 10
        return {
            "company": lead.get("company_name", "your company"),
            "city": lead.get("city", "Bengaluru"),
            "seats": max(5, emp // 3),
            "funding_round": lead.get("signal_detail", "your round"),
            "headcount": emp,
            "contact_name": (lead.get("name") or "").split()[0] or "there",
            "calendar_link": "myhq.in/book",
            "job_count": lead.get("job_count", ""),
        }

    def _mock_send(self, phone: str, company: str, defense: str,
                   template_name: str, parameters: list) -> dict:
        logger.info("[DRY RUN WA] %s → %s (%s, %s): %s",
                    company, phone, defense, template_name, parameters)
        return {
            "success": True,
            "dry_run": True,
            "company": company,
            "defense_mode": defense,
            "template_used": template_name,
            "parameters": parameters,
        }

    def _queue_to_airtable(self, lead: dict, status: str, send_result: dict | None = None):
        if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
            return
        try:
            requests.post(
                f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/WhatsApp_Queue",
                headers={
                    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "fields": {
                        "company_name": lead.get("company_name", ""),
                        "contact_name": lead.get("name") or lead.get("founder_name", ""),
                        "phone": lead.get("phone_mobile", ""),
                        "city": lead.get("city", ""),
                        "defense_mode": lead.get("pkm", {}).get("defense_mode", ""),
                        "template_used": (send_result or {}).get("template_used", ""),
                        "send_status": status,
                        "message_id": (send_result or {}).get("message_id", ""),
                        "sent_at": datetime.now(IST).isoformat(),
                        "signal_type": lead.get("signal_type", ""),
                        "signal_detail": lead.get("signal_detail", ""),
                    }
                },
                timeout=8,
            )
        except Exception as e:
            logger.debug("Airtable WA queue write failed: %s", e)


class ReplyClassifier:
    """Classify incoming WhatsApp replies using Claude Haiku."""

    CATEGORIES = ["HOT", "OBJECTION", "REFERRAL", "NOT_NOW", "UNSUBSCRIBE", "UNKNOWN"]

    def __init__(self):
        self.client = None
        # OpenRouter (gpt-oss-120b) — swap back to Anthropic by uncommenting below
        if OPENROUTER_API_KEY:
            try:
                from openai import OpenAI
                self.client = OpenAI(
                    base_url="https://openrouter.ai/api/v1",
                    api_key=OPENROUTER_API_KEY,
                )
            except Exception:
                pass
        # REVERT TO ANTHROPIC: uncomment below, comment out OpenRouter block above
        # if ANTHROPIC_API_KEY:
        #     try:
        #         from anthropic import Anthropic
        #         self.client = Anthropic()
        #     except Exception:
        #         pass

    def classify(self, reply_text: str, company: str, original_defense: str) -> dict:
        if not self.client:
            return {"category": "UNKNOWN", "next_action": "Manual review", "urgency": "today"}

        try:
            # OpenRouter (OpenAI-compatible) — was Anthropic claude-haiku-4-5
            resp = self.client.chat.completions.create(
                model=OPENROUTER_MODEL,
                max_tokens=200,
                messages=[
                    {"role": "system", "content": (
                        "Classify this WhatsApp reply from an Indian B2B prospect for myHQ. "
                        "Return JSON only:\n"
                        '{"category": "HOT|OBJECTION|REFERRAL|NOT_NOW|UNSUBSCRIBE|UNKNOWN", '
                        '"next_action": "one sentence", "urgency": "immediate|today|this_week|archive", '
                        '"key_info": "names, emails, dates mentioned"}'
                    )},
                    {"role": "user", "content": (
                        f"Company: {company}\n"
                        f"Original defense: {original_defense}\n"
                        f"Reply: {reply_text}"
                    )},
                ],
            )
            return json.loads(resp.choices[0].message.content)
            # REVERT TO ANTHROPIC:
            # resp = self.client.messages.create(
            #     model="claude-haiku-4-5-20251001",
            #     max_tokens=200,
            #     system=(...),
            #     messages=[{"role": "user", "content": ...}],
            # )
            # return json.loads(resp.content[0].text)
        except Exception:
            return {"category": "UNKNOWN", "next_action": "Manual review", "urgency": "today"}

    def process_and_alert(self, reply_text: str, company: str, contact_name: str,
                          phone: str, original_defense: str):
        """Classify reply and fire alert if HOT."""
        classification = self.classify(reply_text, company, original_defense)

        # Store in Airtable
        self._store_reply(company, phone, reply_text, classification)

        # Fire HOT alert
        if classification.get("category") == "HOT":
            _send_hot_alert(classification, company, contact_name, phone)

        return classification

    def _store_reply(self, company: str, phone: str, reply_text: str, classification: dict):
        if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
            return
        try:
            requests.post(
                f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/WA_Replies",
                headers={
                    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "fields": {
                        "company_name": company,
                        "phone": phone,
                        "reply_text": reply_text,
                        "category": classification.get("category", "UNKNOWN"),
                        "next_action": classification.get("next_action", ""),
                        "urgency": classification.get("urgency", "today"),
                        "key_info": classification.get("key_info", ""),
                        "received_at": datetime.now(IST).isoformat(),
                    }
                },
                timeout=8,
            )
        except Exception as e:
            logger.debug("Airtable reply store failed: %s", e)


# ── Shared helpers ────────────────────────────────────────────────────


def _clean_indian_number(phone: str) -> str:
    clean = phone.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if clean.startswith("+91") and len(clean) == 13:
        return clean
    if clean.startswith("91") and len(clean) == 12:
        return "+" + clean
    if len(clean) == 10 and clean[0] in "6789":
        return "+91" + clean
    return ""


def _is_on_dnd(phone: str) -> bool:
    key = os.getenv("TRAI_DND_KEY", "")
    if not key:
        return False
    try:
        resp = requests.get(
            "https://api.trai.gov.in/ndnc/check",
            params={"number": phone.replace("+", ""), "key": key},
            timeout=8,
        )
        return resp.json().get("dnd_status") == "registered"
    except Exception:
        return False


def _send_hot_alert(classification: dict, company: str, contact_name: str, phone: str):
    """Fire immediate email alert for HOT replies."""
    if not SMTP_USER:
        logger.info("HOT LEAD: %s — %s — %s", company, contact_name, phone)
        return

    alert_text = (
        f"HOT LEAD ALERT — myHQ GTM Agent\n\n"
        f"Company: {company}\n"
        f"Contact: {contact_name}\n"
        f"Phone: {phone}\n"
        f"Next action: {classification.get('next_action')}\n"
        f"Key info: {classification.get('key_info', 'None')}\n\n"
        f"Reply within 5 minutes for maximum close rate."
    )

    msg = MIMEText(alert_text)
    msg["Subject"] = f"HOT: {company} replied on WhatsApp"
    msg["From"] = SMTP_USER
    msg["To"] = ALERT_EMAIL

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
    except Exception as e:
        logger.error("HOT alert email failed: %s", e)


# ── Module entry points ──────────────────────────────────────────────


def send_whatsapp_batch(leads: list[dict], dry_run: bool = False) -> list[dict]:
    sender = WhatsAppSender(dry_run=dry_run)
    return sender.send_batch(leads)
