#!/usr/bin/env python3
"""Creates required Airtable tables for myHQ GTM Engine v2.

Run once: python setup_airtable.py

Tables created:
  WhatsApp_Queue   — pending and sent WA messages
  WA_Replies       — incoming replies + classifications
  Competitor_Intel — weekly competitor scan results
  LLM_Content      — generated content pieces
  (PKM_Cache already exists from AROS/ARIA)
"""

from __future__ import annotations

import os

import requests
from dotenv import load_dotenv

load_dotenv()


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
                {"name": "sent_at", "type": "dateTime"},
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
                {"name": "received_at", "type": "dateTime"},
            ],
        },
        {
            "name": "Competitor_Intel",
            "fields": [
                {"name": "competitor", "type": "singleLineText"},
                {"name": "intel_type", "type": "singleLineText"},
                {"name": "data_json", "type": "multilineText"},
                {"name": "scraped_at", "type": "dateTime"},
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
                {"name": "generated_at", "type": "dateTime"},
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
                print(f"Created: {table['name']}")
            else:
                print(f"{table['name']}: {resp.status_code} (may already exist)")
        except Exception as e:
            print(f"Error creating {table['name']}: {e}")


if __name__ == "__main__":
    create_tables()
