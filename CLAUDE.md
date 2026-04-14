# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

myHQ GTM Engine v2 is a B2B revenue intelligence and outreach automation system for myHQ (flexible workspace provider). It detects demand signals from Indian companies, profiles their psychological defenses (PKM), scores/segments leads, enforces TRAI compliance, and generates calibrated outreach via WhatsApp/email/LinkedIn.

## Commands

### Setup
```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # then populate all API keys
python setup_airtable.py    # one-time Airtable schema creation
# Apply Supabase schema via database/schema.sql
```

### Running the Pipeline
```bash
# Via shell wrapper (recommended)
./run_myhq.sh dry              # full pipeline with synthetic data, zero API calls
./run_myhq.sh full             # full pipeline with live APIs
./run_myhq.sh signals          # signal detection only
./run_myhq.sh enrich           # signals + enrichment
./run_myhq.sh sdr              # full pipeline → SDR call list
./run_myhq.sh competitors      # weekly competitor scan
./run_myhq.sh content          # LLM content generation
./run_myhq.sh blr|mum|del      # single city pipeline

# Direct Python
python agent_v2.py --run full --dry-run
python agent_v2.py --run full --city BLR
python agent_v2.py --run full --cities BLR MUM DEL HYD PUN
python agent_v2.py --run sdr --persona 1 --dry-run

# Scheduler jobs
python scheduler.py --job signals|enrich|whatsapp|replies|competitors|content
```

### Testing
```bash
pytest tests/
pytest tests/test_scorer.py -v
pytest tests/test_persona_matcher.py -v
pytest tests/test_compliance.py -v
pytest tests/test_deduplication.py -v
```

## Architecture

### 8-Step Pipeline Flow
```
Signal Detection (Tier 1-5) → Dedup + Fusion
  → Enrichment Waterfall (Apollo → PDL → Netrows → Lusha → Hunter)
  → Verification (Millionverifier + MSG91 + TRAI DND)
  → Persona Matching (3 personas by size/titles/signal)
  → Intent Scoring (5 dimensions, 0-100)
  → TRAI Compliance (DND, suppression, 3-touch, 7-day cooling)
  → PKM Defense Profiling [MANDATORY GATE — no PKM = no send]
  → Outreach Generation (Claude Sonnet) → WhatsApp/Email/LinkedIn
```

Parallel systems run independently: Competitor Intelligence (weekly), LLM Content Indexer, Reply Classifier.

### PKM: The Critical Mandatory Gate

**PKM (Psychological Knowledge Model) must be called before any message is sent — this is non-negotiable.** `pkm_myhq.py` profiles one of 6 defense modes per prospect and all outreach is calibrated to bypass that specific defense. Enforcement is checked at 8 points across `outreach_generator.py`, `pkm_myhq.py`, `whatsapp_india.py`, and `whatsapp_formatter.py`. Adding new outreach paths requires adding PKM enforcement.

6 defense modes: `MOTIVE_INFERENCE`, `OVERLOAD_AVOIDANCE`, `IDENTITY_THREAT`, `SOCIAL_PROOF_SKEPTICISM`, `AUTHORITY_DEFERENCE`, `COMPLEXITY_FEAR`.

### Signal Tiers (Priority Order)
1. **Tier 1** — MCA new incorporation (free government data, highest confidence)
2. **Tier 2** — Tracxn/Crunchbase funding (48h urgency, 95% confidence)
3. **Tier 3** — Naukri hiring surge (7-day urgency, 80% confidence)
4. **Tier 4** — NewsAPI signals (14-day urgency, 60% confidence)
5. **Tier 5** — Intent signals: property listings, LinkedIn posts (30-day, 55-70%)

### 3 Buyer Personas
1. **Funded Founder** (5-50 employees, Seed→Series A) — primary defense: `MOTIVE_INFERENCE`
2. **Ops Expander** (50-300 employees) — primary defense: `OVERLOAD_AVOIDANCE`
3. **Enterprise Expander** (300+ employees) — primary defense: `SOCIAL_PROOF_SKEPTICISM`

### Scoring (5 dimensions × 0-20 = 0-100)
- Trigger Recency, Trigger Strength, Company Fit, Reachability, City+Product Fit
- **HOT** ≥80 | **WARM** 60-79 | **NURTURE** 40-59 | **MONITOR** <40

### Shared Airtable Brain
PKM profiles, WhatsApp queues, replies, competitor intel, and LLM content are stored in Airtable and shared with sibling agents (AROS home-care agent, ARIA capital-raising agent). Airtable is the cross-agent memory layer; Supabase/PostgreSQL is the operational database.

### Key Files
| File | Purpose |
|------|---------|
| `agent_v2.py` | Master 8-step pipeline orchestrator |
| `run_myhq.sh` | Shell wrapper for quick execution |
| `scheduler.py` | Cron job runner (6 job types, IST timestamps) |
| `pipeline/signals_india_v2.py` | Tier 1-5 signal detection |
| `pipeline/enrichment_india_v2.py` | 5-source waterfall enrichment |
| `pipeline/pkm_myhq.py` | Defense profiling via Airtable cache or Claude Haiku |
| `pipeline/outreach_generator.py` | Claude Sonnet multi-channel outreach (PKM-gated) |
| `pipeline/whatsapp_india.py` | WATI BSP send/receive + reply classification |
| `pipeline/scorer.py` | 5-dimension intent scoring |
| `pipeline/persona_matcher.py` | 3-persona matching logic |
| `compliance/india.py` | TRAI DND, suppression list, touch limits, PDPB consent |
| `pipeline/competitor_intel.py` | Weekly scan of 5 competitors |
| `pipeline/pkm_feedback_loop.py` | Learns from WA replies to improve PKM accuracy |
| `config/settings_v2.py` | v2 config (API keys, cities, signal tiers) |
| `config/settings.py` | v1 config (legacy, still used by persona_matcher, scorer) |
| `database/schema.sql` | PostgreSQL/Supabase schema (7 signal tables) |

### Claude API Usage
The system uses three Claude models for different tasks:
- **Claude Haiku** — PKM defense profiling (cost-sensitive, high-volume), reply classification
- **Claude Sonnet** — Outreach generation, competitor analysis, LLM content creation

### v1 vs v2 Compatibility
`agent_v2.py` normalizes v2 signal field names to v1 field names before passing to `persona_matcher.py` and `scorer.py` (which still use v1 config). `agent.py` is the legacy v1 orchestrator — still functional but use `agent_v2.py` for new work.

## Key Constraints

- **TRAI compliance is mandatory**: DND registry check, suppression list, 3-touch max per lead, 7-day cooling period between contacts. These are legal requirements in India, not optional.
- **WhatsApp primary, email fallback**: WATI BSP is the primary delivery channel. Only pre-approved Meta templates can be used in WhatsApp (see `pipeline/wa_templates.py`).
- **`DRY_RUN=true`** in `.env` runs the full pipeline with synthetic data and zero external API calls — use this for local development and testing.
- **PrivateCircle** integration provides MCA/RoC/BSE structured data (replaces manual scraping) — configured via `PRIVATECIRCLE_API_KEY`.
- **Netrows** replaced Proxycurl (LinkedIn shut down Proxycurl) — update any documentation or code that references Proxycurl.
