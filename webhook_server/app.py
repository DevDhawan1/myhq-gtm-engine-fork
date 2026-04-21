"""Apollo webhook receiver — deployed to Render.

Apollo POSTs enriched phone payloads here after a successful waterfall
match. This service only buffers payloads into Postgres; the reconciler
(pipeline/apollo_reconciler.py) consumes them and patches leads later.

Required env vars:
  DATABASE_URL           Supabase postgres URI
  APOLLO_WEBHOOK_SECRET  Shared secret validated via ?t=<secret>
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

if not DATABASE_URL or not APOLLO_WEBHOOK_SECRET:
    logger.warning("DATABASE_URL or APOLLO_WEBHOOK_SECRET missing — service will 500")

app = FastAPI(title="Apollo Webhook Receiver")


@contextmanager
def db_cursor():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn, conn.cursor() as cur:
            yield cur
    finally:
        conn.close()


@app.get("/health")
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
