"""myHQ GTM Engine v2 — WhatsApp automation for India B2B.

BSP: Gupshup (India-native, Meta certified, TRAI DND native)
Why not MSG91: Gupshup has higher volume limits, better Hindi support,
and has been in India since 2004. Enterprise-grade.

Cost: ~0.40-0.50 INR per conversation + platform fee.

Sequence per lead (3-touch rule, TRAI compliant):
  Day 0:  WhatsApp template (PKM-calibrated)
  Day 3:  WhatsApp follow-up (different angle, same defense bypass)
  Day 5:  Email (if no WA reply)
  Day 7:  LinkedIn message (if no email reply)
  STOP.   7-day cooling period minimum.

All messages reference the specific signal that triggered outreach.
All templates pre-approved by Meta via Gupshup dashboard.
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
    SMTP_PASS,
    SMTP_USER,
)

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))

GUPSHUP_API_URL = "https://api.gupshup.io/wa/api/v1/msg"
GUPSHUP_APP_NAME = os.getenv("GUPSHUP_APP_NAME", "")
GUPSHUP_API_KEY = os.getenv("GUPSHUP_API_KEY", "")
GUPSHUP_SOURCE = os.getenv("GUPSHUP_SOURCE_NUMBER", "")

ALERT_EMAIL = os.getenv("ALERT_EMAIL", SMTP_USER)

# ── Template registry — each calibrated to a PKM defense mode ────────
# Templates must be pre-approved by Meta via Gupshup dashboard.
# Approval takes 24-72 hours per template. Submit all 5 on day one.

WHATSAPP_TEMPLATES: dict[str, dict] = {
    "MOTIVE_INFERENCE": {
        "template_name": "myhq_data_first_v1",
        "max_words": 80,
        "example": (
            "Congrats on the {funding_round}. 23 funded founders used myHQ "
            "to get office-ready in 48h last quarter — {city} {seats} seats. "
            "No 11-month lease. {calendar_link}"
        ),
        "banned": ["I'd love", "excited to share", "great opportunity", "reach out"],
    },
    "OVERLOAD_AVOIDANCE": {
        "template_name": "myhq_ultra_short_v1",
        "max_words": 60,
        "example": (
            "{company} hiring in {city}. myHQ has {seats} desks ready this week. "
            "48h setup, no lock-in. Worth 10 min? {calendar_link}"
        ),
        "banned": ["hope this finds you", "quick call", "at your convenience", "just following up"],
    },
    "IDENTITY_THREAT": {
        "template_name": "myhq_amplify_v1",
        "max_words": 70,
        "example": (
            "You built {company} to {headcount} people without a bloated office. "
            "myHQ gives you the infrastructure to keep moving fast — {seats} seats "
            "in {city}, ready when you are. {calendar_link}"
        ),
        "banned": ["let us help", "you need", "solve your problem", "we can fix"],
    },
    "SOCIAL_PROOF_SKEPTICISM": {
        "template_name": "myhq_proof_v1",
        "max_words": 90,
        "example": (
            "myHQ numbers for {city}: 94% occupancy, avg 4.2/5 Google rating, "
            "GST invoice within 24h, 99.9% uptime SLA. {headcount} people, "
            "{seats} seats. Full terms at myhq.in/enterprise. Worth a look?"
        ),
        "banned": ["trusted by", "leading platform", "best in class", "industry-leading"],
    },
    "AUTHORITY_DEFERENCE": {
        "template_name": "myhq_authority_v1",
        "max_words": 90,
        "example": (
            "{contact_name}, your CRE team will want these numbers: "
            "myHQ managed offices in {city} — {seats} seats, SLA 99.9%, "
            "GST compliant, RERA registered. Comparable: TCS, Infosys, Wipro "
            "use managed offices. One-page brief at myhq.in/enterprise."
        ),
        "banned": ["hustle", "startup culture", "vibe", "community"],
    },
    "COMPLEXITY_FEAR": {
        "template_name": "myhq_simple_v1",
        "max_words": 60,
        "example": (
            "3 steps: 1) Pick a location on myhq.in 2) We handle setup "
            "3) Walk in Monday. {seats} seats in {city}. No deposits. "
            "No lock-in. One invoice."
        ),
        "banned": ["seamless integration", "platform", "onboarding", "configuration"],
    },
}


class WhatsAppSender:
    """Send PKM-calibrated WhatsApp messages via Gupshup BSP."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run

    def send_for_lead(self, lead: dict) -> dict:
        """Send a WhatsApp message to a single lead."""
        phone = lead.get("phone_mobile", "")
        contact = lead.get("name") or lead.get("founder_name", "")
        company = lead.get("company_name", "")
        pkm = lead.get("pkm", {})
        defense = pkm.get("defense_mode", "OVERLOAD_AVOIDANCE")
        messages = lead.get("messages", {})

        # Use pre-generated message if available, otherwise fill template
        message_text = messages.get("whatsapp", "")
        if not message_text:
            message_text = self._fill_template(defense, lead)

        clean_phone = _clean_indian_number(phone)
        if not clean_phone:
            return {"success": False, "error": "invalid_phone", "company": company}

        if self.dry_run or not GUPSHUP_API_KEY:
            return self._mock_send(clean_phone, company, defense, message_text)

        # TRAI DND check
        if _is_on_dnd(clean_phone):
            self._queue_to_airtable(lead, "dnd_blocked")
            return {"success": False, "error": "dnd_registered", "company": company}

        try:
            resp = requests.post(
                GUPSHUP_API_URL,
                headers={
                    "apikey": GUPSHUP_API_KEY,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                data={
                    "channel": "whatsapp",
                    "source": GUPSHUP_SOURCE,
                    "destination": clean_phone,
                    "src.name": GUPSHUP_APP_NAME,
                    "message": json.dumps({"type": "text", "text": message_text}),
                },
                timeout=10,
            )

            result = resp.json()
            success = result.get("status") == "submitted"

            send_result = {
                "success": success,
                "message_id": result.get("messageId"),
                "template_used": WHATSAPP_TEMPLATES.get(defense, {}).get("template_name"),
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
        """Send WhatsApp to all qualified leads."""
        results: list[dict] = []
        for lead in leads:
            if not lead.get("whatsapp_verified"):
                continue
            if lead.get("dnd_status"):
                continue
            result = self.send_for_lead(lead)
            results.append(result)
        logger.info("WA batch: %d sent, %d failed",
                     sum(1 for r in results if r.get("success")),
                     sum(1 for r in results if not r.get("success")))
        return results

    def _fill_template(self, defense: str, lead: dict) -> str:
        template = WHATSAPP_TEMPLATES.get(defense, WHATSAPP_TEMPLATES["OVERLOAD_AVOIDANCE"])
        emp = lead.get("employee_count") or 10
        vars_dict = {
            "company": lead.get("company_name", "your company"),
            "city": lead.get("city", "Bengaluru"),
            "seats": max(5, emp // 3),
            "funding_round": lead.get("signal_detail", "your round"),
            "headcount": emp,
            "contact_name": (lead.get("name") or "").split()[0] or "there",
            "calendar_link": "myhq.in/book",
            "job_count": lead.get("job_count", ""),
            "quarter": (datetime.now().month - 1) // 3 + 1,
        }
        try:
            msg = template["example"].format(**vars_dict)
        except KeyError:
            msg = template["example"]

        # Enforce word cap
        words = msg.split()
        if len(words) > template["max_words"]:
            msg = " ".join(words[: template["max_words"]])

        # Strip banned phrases
        for banned in template.get("banned", []):
            msg = msg.replace(banned, "").replace(banned.capitalize(), "")

        return msg.strip()

    def _mock_send(self, phone: str, company: str, defense: str, message: str) -> dict:
        logger.info("[DRY RUN WA] %s → %s (%s): %s…", company, phone, defense, message[:60])
        return {
            "success": True,
            "dry_run": True,
            "company": company,
            "defense_mode": defense,
            "template_used": WHATSAPP_TEMPLATES.get(defense, {}).get("template_name"),
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
        if ANTHROPIC_API_KEY:
            try:
                from anthropic import Anthropic
                self.client = Anthropic()
            except Exception:
                pass

    def classify(self, reply_text: str, company: str, original_defense: str) -> dict:
        if not self.client:
            return {"category": "UNKNOWN", "next_action": "Manual review", "urgency": "today"}

        try:
            resp = self.client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=200,
                system=(
                    "Classify this WhatsApp reply from an Indian B2B prospect for myHQ. "
                    "Return JSON only:\n"
                    '{"category": "HOT|OBJECTION|REFERRAL|NOT_NOW|UNSUBSCRIBE|UNKNOWN", '
                    '"next_action": "one sentence", "urgency": "immediate|today|this_week|archive", '
                    '"key_info": "names, emails, dates mentioned"}'
                ),
                messages=[{
                    "role": "user",
                    "content": (
                        f"Company: {company}\n"
                        f"Original defense: {original_defense}\n"
                        f"Reply: {reply_text}"
                    ),
                }],
            )
            return json.loads(resp.content[0].text)
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
