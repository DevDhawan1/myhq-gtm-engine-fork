# myHQ GTM Engine — Architecture

> From signal detection to AGI revenue core. Five versions. One brain.

## v2 System Architecture (Current)

```
                         ┌─────────────────────────────────────┐
                         │        INDIA-FIRST DATA LAYER       │
                         │                                     │
                         │  Tracxn API ──── Funding signals    │
                         │  MCA Public ──── Incorporation      │
                         │  Naukri/Apify ── Hiring signals     │
                         │  NewsAPI ─────── NLP signals        │
                         │  99acres ─────── Property signals   │
                         └──────────────┬──────────────────────┘
                                        │
                              ┌─────────▼─────────┐
                              │   DEDUP + FUSION   │
                              │  Tracxn primary    │
                              │  Crunchbase dedup  │
                              └─────────┬─────────┘
                                        │
                    ┌───────────────────▼───────────────────┐
                    │        ENRICHMENT WATERFALL            │
                    │                                       │
                    │  Apollo ──→ PDL ──→ Proxycurl ──→     │
                    │  Lusha ──→ Hunter (stop when found)   │
                    │                                       │
                    │  Verify: Millionverifier (email)      │
                    │          MSG91 (WhatsApp check)       │
                    │          TRAI DND (regulatory)        │
                    └───────────────────┬───────────────────┘
                                        │
                         ┌──────────────▼──────────────┐
                         │     PERSONA MATCHING (v1)    │
                         │  1. Funded Founder (5-50)    │
                         │  2. Ops Expander (50-300)    │
                         │  3. Enterprise (300+)        │
                         └──────────────┬──────────────┘
                                        │
                              ┌─────────▼─────────┐
                              │  INTENT SCORING    │
                              │  5 dims × 0-20     │
                              │  = 0-100 score      │
                              │  HOT/WARM/NURTURE  │
                              └─────────┬─────────┘
                                        │
                            ┌───────────▼───────────┐
                            │   TRAI COMPLIANCE     │
                            │  DND check            │
                            │  Suppression list     │
                            │  3-touch limit        │
                            │  7-day cooling        │
                            └───────────┬───────────┘
                                        │
                    ╔═══════════════════▼═══════════════════╗
                    ║     PKM DEFENSE PROFILING             ║
                    ║     *** MANDATORY GATE ***            ║
                    ║                                       ║
                    ║  No profile = No message. Period.     ║
                    ║                                       ║
                    ║  6 defense modes:                     ║
                    ║  MOTIVE_INFERENCE                     ║
                    ║  OVERLOAD_AVOIDANCE                   ║
                    ║  IDENTITY_THREAT                      ║
                    ║  SOCIAL_PROOF_SKEPTICISM              ║
                    ║  AUTHORITY_DEFERENCE                  ║
                    ║  COMPLEXITY_FEAR                      ║
                    ║                                       ║
                    ║  Claude Haiku classifies               ║
                    ║  Airtable PKM_Cache stores            ║
                    ║  Shared with AROS + ARIA brain        ║
                    ╚═══════════════════╤═══════════════════╝
                                        │
                    ┌───────────────────▼───────────────────┐
                    │     OUTREACH GENERATION (PKM-gated)   │
                    │                                       │
                    │  WhatsApp (Gupshup BSP)  ← primary    │
                    │  Email (Instantly)        ← secondary  │
                    │  LinkedIn                 ← tertiary   │
                    │  SDR call script          ← if needed  │
                    │                                       │
                    │  Defense mode → bypass strategy        │
                    │  Forbidden phrases stripped            │
                    │  Word cap enforced                     │
                    └───────────────────┬───────────────────┘
                                        │
                    ┌───────────────────▼───────────────────┐
                    │          SDR DASHBOARD                 │
                    │  Rich terminal cards                   │
                    │  JSON + Markdown export                │
                    │  Priority-ranked by score              │
                    └───────────────────────────────────────┘

          ┌──────────────────────────────────────────────────────┐
          │                 PARALLEL SYSTEMS                      │
          │                                                      │
          │  Competitor Intel ──── Awfis/WeWork/IndiQube/        │
          │  (weekly scan)        Smartworks/91SB                │
          │                       Pricing + Reviews + Blog gaps  │
          │                                                      │
          │  LLM Content ──────── Claude Sonnet generation       │
          │  (weekly)             Perplexity real-time indexing   │
          │                       Reddit distribution            │
          │                                                      │
          │  Reply Classifier ─── Claude Haiku on WA replies     │
          │  (real-time)          HOT → 60-second email alert    │
          └──────────────────────────────────────────────────────┘

                    ┌───────────────────────────────────────┐
                    │           AIRTABLE BRAIN              │
                    │  (shared with AROS + ARIA)            │
                    │                                       │
                    │  PKM_Cache ─── defense profiles       │
                    │  WhatsApp_Queue ─── pending sends     │
                    │  WA_Replies ─── classified replies     │
                    │  Competitor_Intel ─── weekly scans     │
                    │  LLM_Content ─── generated pieces     │
                    └───────────────────────────────────────┘
```

## Why These APIs (Not Those)

### Signal Detection: Tracxn over SerpAPI

| Old (v1) | New (v2) | Why |
|----------|----------|-----|
| SerpAPI scraping Google News | Tracxn API | Structured Indian funding data. Seed to Series C with investor names, amounts, sectors. Not scraping search results and hoping. |
| ScraperAPI on Entrackr/Inc42 | NewsAPI with India domain filter | Structured API, not fragile HTML scraping. Entrackr changes their CSS, ScraperAPI breaks. NewsAPI doesn't. |
| SerpAPI on Crunchbase | Crunchbase API (secondary) | Direct API, not scraping. India data lags by 2-3 weeks, so Tracxn is primary. |
| Nothing | MCA public API | Free government data. A new CIN is the hardest expansion proof available. Nobody else uses this. |
| LinkedIn scraping via ScraperAPI | Naukri via Apify | Naukri has 3x more Indian jobs than LinkedIn. Apify actors are community-maintained. |

### Enrichment: Waterfall over Apollo-Only

v1 used Apollo alone. Apollo's India coverage is ~60%. The waterfall hits 85%+:

```
Apollo (general) → miss? →
PDL (tech roles, better email) → miss? →
Proxycurl (LinkedIn-fresh) → miss? →
Lusha (mobile/WhatsApp numbers) → miss? →
Hunter (email-only fallback)
```

Then verify: Millionverifier (email) → MSG91 (WhatsApp check) → TRAI DND (regulatory).

### WhatsApp: Gupshup over Meta Direct

- India-native BSP (Bengaluru HQ, since 2004)
- TRAI DND handling built in
- Hindi template support for Tier 2 expansion
- 10,000+ messages/day on basic plan
- ₹0.40-0.50 per conversation
- Webhook for reply detection → Claude Haiku classification → HOT alerts

## PKM Enforcement Points

PKM is not optional. It is enforced at every point where a message could be generated or sent:

| File | Function | Enforcement |
|------|----------|-------------|
| `pipeline/outreach_generator.py` | `generate_batch()` | Blocks leads without `pkm.defense_mode` |
| `pipeline/outreach_generator.py` | `_get_personalization_rules()` | Injects defense mode + bypass + forbidden phrases + word cap into Claude prompts |
| `pipeline/pkm_myhq.py` | `generate_for_lead()` | Returns `{}` if no PKM profile |
| `pipeline/pkm_myhq.py` | `generate_batch()` | Skips unprofiled leads, logs warning |
| `pipeline/whatsapp_india.py` | `send_for_lead()` | Returns `pkm_missing` error, refuses to send |
| `pipeline/whatsapp_india.py` | `send_batch()` | Skips unprofiled leads before send loop |
| `pipeline/whatsapp_formatter.py` | `format_message()` | Blocks format, strips forbidden phrases, enforces word cap |
| `pipeline/whatsapp_formatter.py` | `format_whatsapp_messages()` | Batch gate — skips leads without PKM |
| `agent.py` (v1) | `_run_full_pipeline()` | PKM profiling step injected before outreach |
| `agent_v2.py` | `_run_full_pipeline()` | PKM profiling is step 6 of 8, before outreach |

## Scoring Algorithm

Five dimensions, each 0-20 points:

| Dimension | What it measures | 20 points | 0 points |
|-----------|-----------------|-----------|----------|
| Trigger Recency | How fresh | <24 hours | >14 days |
| Trigger Strength | How strong | Funding signal | Weak intent |
| Company Fit | Size match to persona | Perfect range | Outside range |
| Reachability | Contact quality | Phone + WhatsApp verified | No contact info |
| City + Product Fit | City alignment | BLR (priority 1) + sector match | Unknown city |

Tiers: **HOT ≥80** (call in 2h) · **WARM ≥60** (call in 24h) · **NURTURE ≥40** (sequence) · **MONITOR <40** (watch)

---

## Version Roadmap Architecture

### v3 — Autonomous Deal Orchestrator

```
                    ┌─────────────────────────────────┐
                    │         SELF-LEARNING PKM        │
                    │                                  │
                    │  Every reply trains the model    │
                    │  65% accuracy → 90%+ accuracy    │
                    │  New defense modes discovered    │
                    │  from interaction clustering     │
                    └────────────────┬────────────────┘
                                     │
                    ┌────────────────▼────────────────┐
                    │    AUTONOMOUS SEQUENCER          │
                    │                                  │
                    │  WA no reply 48h → Email         │
                    │  Email opened → LinkedIn         │
                    │  LI accepted → WA follow-up      │
                    │  A/B test bypass strategies      │
                    │  Agent decides channel + timing   │
                    └────────────────┬────────────────┘
                                     │
                    ┌────────────────▼────────────────┐
                    │    MEETING BOOKING               │
                    │                                  │
                    │  Cal.com integration             │
                    │  3 time slots in WhatsApp        │
                    │  Prospect picks → SDR gets       │
                    │  calendar invite + context card   │
                    └────────────────┬────────────────┘
                                     │
                    ┌────────────────▼────────────────┐
                    │    SDR COACHING (Gong/Chorus)    │
                    │                                  │
                    │  Analyze call recordings         │
                    │  Which openers convert           │
                    │  Feed back into PKM prompts      │
                    └─────────────────────────────────┘
```

**Key metric:** SDR team goes from "find + call + close" to "show up to booked meetings + close."

### v4 — Predictive Market Intelligence

```
     ┌──────────────────────────────────────────────────────────┐
     │                90-DAY DEMAND PREDICTION                   │
     │                                                          │
     │  ML model: signal history + deal outcomes + macro data   │
     │  Input: company age, sector, funding, headcount slope    │
     │  Output: P(workspace need) in 30/60/90 days              │
     │          + estimated seats + estimated budget             │
     └────────────────────────┬─────────────────────────────────┘
                              │
     ┌────────────────────────▼─────────────────────────────────┐
     │              TERRITORY OPTIMIZER                          │
     │                                                          │
     │  SDR Priya: 3x close rate on FinTech BLR → assign all   │
     │  SDR Vikram: 2.5x on Enterprise DEL → assign all        │
     │  Rebalance monthly from performance data                 │
     └────────────────────────┬─────────────────────────────────┘
                              │
     ┌────────────────────────▼─────────────────────────────────┐
     │           COMPETITIVE WAR ROOM                            │
     │                                                          │
     │  Awfis raises BLR price → campaign triggers in 2 hours  │
     │  WeWork opens HYD → target their pipeline before ramp   │
     │  Real-time pricing: competitor × city × product          │
     └────────────────────────┬─────────────────────────────────┘
                              │
     ┌────────────────────────▼─────────────────────────────────┐
     │           REVENUE FORECASTING                             │
     │                                                          │
     │  Every lead: P(close) × deal_size × timeline             │
     │  Dashboard: "Next month: ₹X lakhs MRR projected"        │
     │  Board deck input. Fundraising model input.              │
     └──────────────────────────────────────────────────────────┘
```

**Key metric:** VP Sales sees 90-day forward demand. Every decision is data-driven.

### v5 — AGI Revenue Core

```
     ╔══════════════════════════════════════════════════════════╗
     ║                                                          ║
     ║              THE AGENT IS THE SALES TEAM                 ║
     ║                                                          ║
     ║  Humans close deals. The agent does everything else.     ║
     ║                                                          ║
     ╚══════════════════════════╤═══════════════════════════════╝
                                │
     ┌──────────────────────────▼──────────────────────────────┐
     │              AUTONOMOUS NEGOTIATION                      │
     │                                                          │
     │  Handles pricing objections via WhatsApp in real-time   │
     │  Knows: company budget (funding data + benchmarks)      │
     │  Knows: competitor price (weekly scraping)              │
     │  Knows: defense mode (PKM profile)                      │
     │  Generates response that reframes value                 │
     │                                                          │
     │  Escalates to human only when:                          │
     │    - Deal > ₹50L/year                                   │
     │    - Enterprise compliance requirements                 │
     │    - Prospect explicitly asks for human                 │
     │                                                          │
     │  Closes 60% of deals under ₹10L/year autonomously      │
     └──────────────────────────┬──────────────────────────────┘
                                │
     ┌──────────────────────────▼──────────────────────────────┐
     │           SELF-EVOLVING PLAYBOOKS                        │
     │                                                          │
     │  Analyzes 10,000+ interactions                          │
     │  Discovers: "Sequoia portfolio → data-first > congrats" │
     │  Splits: OVERLOAD_AVOIDANCE into _MORNING / _EVENING   │
     │  Creates new modes from reply clustering                │
     │  Playbooks evolve weekly — adapts faster than humans    │
     └──────────────────────────┬──────────────────────────────┘
                                │
     ┌──────────────────────────▼──────────────────────────────┐
     │         CROSS-AGENT INTELLIGENCE                         │
     │                                                          │
     │  ┌──────────┐  ┌──────────┐  ┌──────────────┐          │
     │  │  AROS    │  │  ARIA    │  │  myHQ GTM    │          │
     │  │  (home   │  │  (capital│  │  (workspace)  │          │
     │  │   care)  │  │  raising)│  │               │          │
     │  └────┬─────┘  └────┬─────┘  └──────┬───────┘          │
     │       │              │               │                   │
     │       └──────────────┼───────────────┘                   │
     │                      │                                   │
     │              ┌───────▼───────┐                           │
     │              │  SHARED BRAIN │                           │
     │              │  PKM_Cache    │                           │
     │              │  Airtable     │                           │
     │              └───────────────┘                           │
     │                                                          │
     │  Founder profiled by myHQ agent → AROS knows defense    │
     │  mode 6 months later when they become a care lead.      │
     │  Every interaction makes every agent smarter.            │
     └──────────────────────────┬──────────────────────────────┘
                                │
     ┌──────────────────────────▼──────────────────────────────┐
     │              MARKET CREATION                             │
     │                                                          │
     │  Identifies companies that SHOULD use flex workspace    │
     │  but don't know it yet.                                 │
     │                                                          │
     │  Pattern: 15-30 employees + 2yr old + no MCA sub +      │
     │  remote-first jobs → never had office → doesn't know    │
     │  what they're missing.                                  │
     │                                                          │
     │  Creates demand through educational content:             │
     │  "Why your 20-person remote team is 40% less            │
     │   productive without a physical anchor"                  │
     │                                                          │
     │  The agent creates pipeline that didn't exist before.   │
     └──────────────────────────┬──────────────────────────────┘
                                │
     ┌──────────────────────────▼──────────────────────────────┐
     │           RELATIONSHIP GRAPH                             │
     │                                                          │
     │  Maps every decision-maker connection in Indian B2B     │
     │                                                          │
     │  This CEO was CTO at WeWork user → knows WeWork flaws   │
     │  → lead with myHQ advantages over WeWork specifically   │
     │                                                          │
     │  Warm intro routing: "Your investor has 3 portfolio     │
     │  companies using myHQ. Want a reference call?"          │
     │                                                          │
     │  Cold outreach → warm outreach at scale.                │
     │  This is the moat.                                      │
     └──────────────────────────┬──────────────────────────────┘
                                │
     ┌──────────────────────────▼──────────────────────────────┐
     │         THE COMPOUND EFFECT                              │
     │                                                          │
     │  v5 month 1:  Good.                                     │
     │  v5 month 6:  3x better (5,000 interactions learned)   │
     │  v5 month 12: 10x better (20,000 interactions)         │
     │  v5 month 24: 50,000 interactions, 10,000 profiles,    │
     │               patterns no human team could hold.        │
     │                                                          │
     │  If an SDR leaves → zero knowledge lost.                │
     │  If VP Sales leaves → playbooks keep running.           │
     │  The agent IS myHQ's institutional memory.              │
     │                                                          │
     │  This is why myHQ can't live without it.                │
     └─────────────────────────────────────────────────────────┘
```

## Version Comparison

| | v2 (Now) | v3 | v4 | v5 |
|---|---------|-----|-----|-----|
| **Signal sources** | 5 APIs | 5 APIs + signal fusion | ML prediction model | Market creation |
| **PKM accuracy** | ~65% | ~90% (self-learning) | ~95% | Self-evolving modes |
| **Outreach** | Generate messages | Autonomous sequencing | A/B optimized | Autonomous negotiation |
| **Meeting** | SDR books manually | Agent books via Cal.com | Agent qualifies + books | Agent closes <₹10L |
| **Competitors** | Weekly scraping | Real-time monitoring | War room + triggers | Counter-moves in hours |
| **Content** | Weekly generation | Daily + performance loop | Auto-fills LLM gaps | Creates demand |
| **Revenue model** | Lead list | Pipeline tracking | 90-day forecast | LTV per lead |
| **Human role** | SDR does everything | SDR closes meetings | SDR is optimally assigned | Human closes >₹50L only |
| **Brain** | Airtable | Airtable + PostgreSQL | Vector DB + graph | AGI compound brain |
| **Leads/month** | 200-500 | 1,000-2,000 | 3,000-5,000 | 10,000+ |
| **myHQ dependency** | Nice to have | Hard to replace | Critical infrastructure | Cannot operate without |

---

**Built for:** myHQ (myhq.in)
**Part of:** Aonxi AGI infrastructure
**Brain:** Shared PKM_Cache — compounds across AROS + ARIA + myHQ GTM
