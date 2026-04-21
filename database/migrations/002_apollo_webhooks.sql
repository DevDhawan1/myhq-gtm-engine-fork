-- ═══════════════════════════════════════════════════════════════════
-- Apollo Waterfall Webhook Infrastructure
-- Run against Supabase DATABASE_URL after Phase 1 validation succeeds.
-- ═══════════════════════════════════════════════════════════════════

-- Buffer for webhook payloads arriving from Apollo async delivery.
-- The webhook server (Render) writes here; the reconciler reads here.
CREATE TABLE IF NOT EXISTS apollo_phone_reveals (
    id               BIGSERIAL PRIMARY KEY,
    received_at      TIMESTAMPTZ DEFAULT now(),
    apollo_person_id TEXT NOT NULL,
    status           TEXT,
    phone_numbers    JSONB NOT NULL DEFAULT '[]'::jsonb,
    raw_payload      JSONB NOT NULL,
    consumed         BOOLEAN DEFAULT FALSE,
    consumed_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_apr_person
    ON apollo_phone_reveals (apollo_person_id);

CREATE INDEX IF NOT EXISTS idx_apr_unconsumed
    ON apollo_phone_reveals (received_at)
    WHERE consumed = FALSE;

-- Records every Apollo sync call made by the pipeline so the reconciler
-- can join an incoming webhook payload back to the original lead.
-- Written by enrichment_india_v2.py right after a successful /people/match.
CREATE TABLE IF NOT EXISTS pending_apollo_enrichments (
    id               BIGSERIAL PRIMARY KEY,
    created_at       TIMESTAMPTZ DEFAULT now(),
    apollo_person_id TEXT NOT NULL UNIQUE,
    lead_name        TEXT,
    lead_company     TEXT,
    lead_linkedin    TEXT,
    lead_email       TEXT,
    patched          BOOLEAN DEFAULT FALSE,
    patched_at       TIMESTAMPTZ,
    patched_phone    TEXT
);

CREATE INDEX IF NOT EXISTS idx_pae_person
    ON pending_apollo_enrichments (apollo_person_id);

CREATE INDEX IF NOT EXISTS idx_pae_linkedin
    ON pending_apollo_enrichments (lead_linkedin);

CREATE INDEX IF NOT EXISTS idx_pae_unpatched
    ON pending_apollo_enrichments (created_at)
    WHERE patched = FALSE;
