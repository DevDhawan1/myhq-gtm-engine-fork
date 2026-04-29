"""Webhook receiver — deployed to Render.

Handles two inbound webhooks:

  POST /apollo/webhook?t=<secret>
    Apollo enriched-phone payloads. Buffered into Postgres; consumed later
    by pipeline/apollo_reconciler.py.

  POST /sendgrid/events?t=<secret>
    SendGrid Event Webhook. Updates email_events rows in Supabase with
    delivered / open / click / bounce status.
    unsubscribe and spamreport events are also written so compliance can
    suppress those addresses.

Required env vars:
  DATABASE_URL             Supabase postgres URI
  APOLLO_WEBHOOK_SECRET    Shared secret for Apollo endpoint
  SENDGRID_WEBHOOK_SECRET  Shared secret for SendGrid endpoint
"""
from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager

import psycopg2
from fastapi import FastAPI, HTTPException, Request
from psycopg2.extras import Json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("apollo-webhook")

DATABASE_URL = os.environ.get("DATABASE_URL", "")
APOLLO_WEBHOOK_SECRET = os.environ.get("APOLLO_WEBHOOK_SECRET", "")
SENDGRID_WEBHOOK_SECRET = os.environ.get("SENDGRID_WEBHOOK_SECRET", "")

if not DATABASE_URL:
    logger.warning("DATABASE_URL missing — service will 500 on any DB call")
if not APOLLO_WEBHOOK_SECRET:
    logger.warning("APOLLO_WEBHOOK_SECRET missing")
if not SENDGRID_WEBHOOK_SECRET:
    logger.warning("SENDGRID_WEBHOOK_SECRET missing")

app = FastAPI(title="myHQ Webhook Receiver")

# SendGrid event priority — higher index wins if multiple events arrive
# for the same message (e.g. delivered then open then click).
_SG_EVENT_PRIORITY = {
    "processed": 0,
    "delivered": 1,
    "open": 2,
    "click": 3,
    "bounce": 4,
    "spamreport": 4,
    "unsubscribe": 4,
}


@contextmanager
def db_cursor():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn, conn.cursor() as cur:
            yield cur
    finally:
        conn.close()


@app.api_route("/health", methods=["GET", "HEAD"])
def health():
    return {"ok": True}


@app.post("/apollo/webhook")
async def apollo_webhook(request: Request, t: str = ""):
    if t != APOLLO_WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="invalid secret")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid json")

    people = payload.get("people") or []
    stored = 0
    try:
        with db_cursor() as cur:
            for person in people:
                cur.execute(
                    """
                    INSERT INTO apollo_phone_reveals
                        (apollo_person_id, status, phone_numbers, raw_payload)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        person.get("id") or "",
                        person.get("status") or "",
                        Json(person.get("phone_numbers") or []),
                        Json(payload),
                    ),
                )
                stored += 1
    except Exception as e:
        logger.exception("DB insert failed")
        raise HTTPException(status_code=500, detail="db error")

    logger.info("stored %d people from webhook", stored)
    return {"ok": True, "stored": stored}


@app.post("/sendgrid/events")
async def sendgrid_events(request: Request, t: str = ""):
    if t != SENDGRID_WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="invalid secret")

    try:
        events = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid json")

    if not isinstance(events, list):
        raise HTTPException(status_code=400, detail="expected json array")

    processed = 0
    try:
        with db_cursor() as cur:
            for ev in events:
                event_type = ev.get("event", "")
                # sg_message_id in events is "<message_id>.<filter_info>" —
                # strip everything after the first dot to get the base ID.
                raw_msg_id = ev.get("sg_message_id", "")
                message_id = raw_msg_id.split(".")[0] if raw_msg_id else ""
                email = ev.get("email", "")
                timestamp = ev.get("timestamp")
                url = ev.get("url", "")  # populated for click events only

                if not message_id or not event_type:
                    continue

                # Update email_events only if this event is higher priority
                # than what's already stored (avoid overwriting click with open).
                cur.execute(
                    """
                    UPDATE email_events
                    SET
                        status = %s,
                        event_type = %s,
                        event_time = to_timestamp(%s),
                        clicked_url = CASE WHEN %s != '' THEN %s ELSE clicked_url END
                    WHERE message_id = %s
                      AND COALESCE(
                            (SELECT priority FROM (VALUES
                              ('processed',0),('delivered',1),('open',2),
                              ('click',3),('bounce',4),('spamreport',4),('unsubscribe',4)
                            ) AS p(evt, priority) WHERE p.evt = status LIMIT 1), 0
                          ) < %s
                    """,
                    (
                        event_type,
                        event_type,
                        timestamp,
                        url, url,
                        message_id,
                        _SG_EVENT_PRIORITY.get(event_type, 0),
                    ),
                )

                # For unsubscribe / spamreport — write to suppression table so
                # compliance picks it up and adds to suppression list.
                if event_type in ("unsubscribe", "spamreport") and email:
                    cur.execute(
                        """
                        INSERT INTO suppression_list (email, reason, suppressed_at)
                        VALUES (%s, %s, to_timestamp(%s))
                        ON CONFLICT (email) DO NOTHING
                        """,
                        (email, event_type, timestamp),
                    )

                processed += 1

    except Exception:
        logger.exception("DB update failed processing SendGrid events")
        raise HTTPException(status_code=500, detail="db error")

    logger.info("processed %d SendGrid events", processed)
    return {"ok": True, "processed": processed}
