#!/usr/bin/env python3
"""Creates required Airtable tables for myHQ GTM Engine v2.

Run once: python setup_airtable.py

Tables created:
  PKM_Cache        — defense profiles (shared brain)
  WhatsApp_Queue   — pending and sent WA messages
  WA_Replies       — incoming replies + classifications
  Competitor_Intel — weekly competitor scan results
  LLM_Content      — generated content pieces
"""

from __future__ import annotations

import os

import requests
from dotenv import load_dotenv

load_dotenv()

_DT_OPTIONS = {
    "dateFormat": {"name": "iso"},
    "timeFormat": {"name": "24hour"},
    "timeZone": "Asia/Kolkata",
}


def create_tables() -> None:
    key = os.getenv("AIRTABLE_API_KEY", "")
    base_id = os.getenv("AIRTABLE_BASE_ID", "")

    if not key or not base_id:
        print("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID in .env")
        return

    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }

    tables = [
        {
            "name": "PKM_Cache",
            "fields": [
                {"name": "cache_key", "type": "singleLineText"},
                {"name": "profile_text", "type": "singleLineText"},
                {"name": "detected_mode", "type": "singleLineText"},
                {"name": "confidence", "type": "number", "options": {"precision": 0}},
                {"name": "reasoning", "type": "multilineText"},
                {"name": "awareness_score", "type": "number", "options": {"precision": 0}},
                {"name": "bypass_strategy", "type": "multilineText"},
                {"name": "forbidden_phrases", "type": "multilineText"},
                {"name": "message_cap_words", "type": "number", "options": {"precision": 0}},
                {"name": "source", "type": "singleLineText"},
                {"name": "company_name", "type": "singleLineText"},
                {"name": "analyzed_at", "type": "dateTime", "options": _DT_OPTIONS},
            ],
        },
        {
            "name": "WhatsApp_Queue",
            "fields": [
                {"name": "company_name", "type": "singleLineText"},
                {"name": "contact_name", "type": "singleLineText"},
                {"name": "phone", "type": "phoneNumber"},
                {"name": "city", "type": "singleLineText"},
                {"name": "defense_mode", "type": "singleLineText"},
                {"name": "template_used", "type": "singleLineText"},
                {"name": "send_status", "type": "singleSelect", "options": {
                    "choices": [
                        {"name": "pending"}, {"name": "sent"},
                        {"name": "failed"}, {"name": "dnd_blocked"},
                    ]
                }},
                {"name": "message_id", "type": "singleLineText"},
                {"name": "sent_at", "type": "dateTime", "options": _DT_OPTIONS},
                {"name": "signal_type", "type": "singleLineText"},
                {"name": "signal_detail", "type": "singleLineText"},
            ],
        },
        {
            "name": "WA_Replies",
            "fields": [
                {"name": "company_name", "type": "singleLineText"},
                {"name": "phone", "type": "phoneNumber"},
                {"name": "reply_text", "type": "multilineText"},
                {"name": "category", "type": "singleSelect", "options": {
                    "choices": [
                        {"name": "HOT"}, {"name": "OBJECTION"},
                        {"name": "REFERRAL"}, {"name": "NOT_NOW"},
                        {"name": "UNSUBSCRIBE"}, {"name": "UNKNOWN"},
                    ]
                }},
                {"name": "next_action", "type": "multilineText"},
                {"name": "urgency", "type": "singleLineText"},
                {"name": "key_info", "type": "multilineText"},
                {"name": "alert_sent", "type": "checkbox"},
                {"name": "received_at", "type": "dateTime", "options": _DT_OPTIONS},
            ],
        },
        {
            "name": "Competitor_Intel",
            "fields": [
                {"name": "competitor", "type": "singleLineText"},
                {"name": "intel_type", "type": "singleLineText"},
                {"name": "data_json", "type": "multilineText"},
                {"name": "scraped_at", "type": "dateTime", "options": _DT_OPTIONS},
            ],
        },
        {
            "name": "LLM_Content",
            "fields": [
                {"name": "title", "type": "singleLineText"},
                {"name": "type", "type": "singleLineText"},
                {"name": "content", "type": "multilineText"},
                {"name": "target_queries", "type": "multilineText"},
                {"name": "word_count", "type": "number", "options": {"precision": 0}},
                {"name": "perplexity_submitted", "type": "checkbox"},
                {"name": "generated_at", "type": "dateTime", "options": _DT_OPTIONS},
                {"name": "status", "type": "singleLineText"},
            ],
        },
    ]

    for table in tables:
        try:
            resp = requests.post(
                f"https://api.airtable.com/v0/meta/bases/{base_id}/tables",
                headers=headers,
                json=table,
                timeout=10,
            )
            if resp.status_code == 200:
                print(f"✓ Created: {table['name']}")
            elif resp.status_code == 422:
                print(f"  {table['name']}: already exists, skipping")
            else:
                print(f"  {table['name']}: {resp.status_code} — {resp.text[:120]}")
        except Exception as e:
            print(f"  Error creating {table['name']}: {e}")


if __name__ == "__main__":
    create_tables()
