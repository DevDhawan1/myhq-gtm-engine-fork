# myHQ GTM Engine

> India-first B2B revenue intelligence system that detects workspace demand signals, profiles decision-maker psychology, and delivers PKM-calibrated outreach across WhatsApp, email, and LinkedIn — before competitors know the lead exists.

**Current: v2** | Roadmap: v3 → v4 → v5 (AGI Revenue Core)

---

## v2 — What's Live Now

### 10 Trigger Signals

A company doing any of these needs workspace:

| # | Signal | Source | Urgency | Confidence |
|---|--------|--------|---------|------------|
| 1 | Seed/Series A funding closed | Tracxn API (primary), Crunchbase | 48h | 95% |
| 2 | LinkedIn headcount +20% in 90 days | Naukri via Apify, Netrows | 7 days | 80% |
| 3 | New GST registration in a city | GST portal | 14 days | 90% |
| 4 | MCA new subsidiary incorporated | MCA public API | 14 days | 90% |
| 5 | Office lease expiry signal | 99acres/MagicBricks | 30 days | 70% |
| 6 | WFH policy reversal announcement | NewsAPI + NLP | 14 days | 60% |
| 7 | New city expansion PR | Inc42, Entrackr, YourStory | 7 days | 60% |
| 8 | Enterprise contract win | News + LinkedIn | 7 days | 60% |
| 9 | Founder LinkedIn post "hiring fast" | Netrows | 7 days | 55% |
| 10 | Competitor workspace closure | Competitor scraping | 48h | 85% |

Signals 1-4 are **automated with structured Indian data APIs**. Signals 5-10 use NLP on unstructured sources.

### 8-Step Pipeline

```
Signal Detection (Tracxn + MCA + Naukri + NewsAPI)
        ↓
Enrichment Waterfall (Apollo → PDL → Netrows → Lusha → Hunter)
        ↓
Persona Matching (3 personas, keyword + size scoring)
        ↓
Intent Scoring (5 dimensions × 0-20 = 0-100)
        ↓
TRAI Compliance (DND check, suppression, 3-touch limit)
        ↓
PKM Defense Profiling ← MANDATORY GATE — no profile, no message
        ↓
Outreach Generation (WhatsApp + Email + LinkedIn, defense-calibrated)
        ↓
SDR Dashboard (Rich terminal cards + JSON + Markdown export)
```

### PKM Defense Profiling — Mandatory on Every Message Path

Every message sent by this system is calibrated to bypass the recipient's psychological defense mode. **No PKM profile = no message sent. Period.**

| Defense Mode | Who has it | Bypass strategy |
|-------------|-----------|-----------------|
| MOTIVE_INFERENCE | Funded founders | Lead with their funding news + specific desk count |
| OVERLOAD_AVOIDANCE | Ops managers (60+ vendor pitches/week) | Under 60 words, one calendar slot |
| IDENTITY_THREAT | Self-made founders | Amplify what they built, don't imply they need help |
| SOCIAL_PROOF_SKEPTICISM | Enterprise buyers | Named customers + SLA numbers + GST compliance |
| AUTHORITY_DEFERENCE | Middle managers | Give them ammo to forward up the chain |
| COMPLEXITY_FEAR | Previously burned buyers | 3 steps, no jargon, no "onboarding" |

PKM enforced in: `outreach_generator.py`, `whatsapp_india.py`, `whatsapp_formatter.py`, `pkm_myhq.py`

### Three Buyer Personas

**Persona 1 — The Funded Founder** (5-50 employees, Seed→Series A)
- Signal: Just closed a round
- PKM: MOTIVE_INFERENCE → lead with their news, 48h setup, no lock-in
- Product: Fixed Desks, Private Cabins, Managed Office (10-30 seats)

**Persona 2 — The Ops Expander** (50-300 employees)
- Signal: 8+ jobs posted in one city
- PKM: OVERLOAD_AVOIDANCE → under 60 words, specific seats, one time slot
- Product: Managed Office (30-100 seats)

**Persona 3 — The Enterprise Expander** (300+ employees)
- Signal: New subsidiary, GST, contract win
- PKM: SOCIAL_PROOF_SKEPTICISM → enterprise names, SLA, GST invoice
- Product: Managed Office (100+ seats), Commercial Leasing

### API Stack — Chosen for 5-Year Durability

**Signal detection (India-primary):**
Tracxn (~$600/mo) · Crunchbase ($199/mo) · MCA public API (free) · NewsAPI ($449/mo) · Apify/Naukri (~$100/mo)

**Enrichment waterfall:**
Apollo ($99/mo) → People Data Labs ($0.01/call) → Netrows ($0.03/profile) → Lusha ($29/mo) → Hunter (free tier)

**Verification (India-critical):**
Millionverifier · MSG91 WhatsApp check · TRAI DND registry

**WhatsApp automation:**
WATI BSP (Meta certified, clean REST API, ~$49-99/mo + Meta fees)

**Competitor intelligence:**
Apify website crawler · SerpAPI Google Reviews · Claude Haiku classification

**LLM content indexing:**
Claude Sonnet generation · Perplexity Pages API · Reddit distribution

**Total: ~$1,876/month at production scale across 5 cities.**

### Quick Start

```bash
# Dry run (zero API calls, synthetic data)
./run_myhq.sh dry

# Full pipeline — Bengaluru only
./run_myhq.sh blr

# All 5 cities
python3 agent_v2.py --run full --cities BLR MUM DEL HYD PUN

# Competitor scan
./run_myhq.sh competitors

# LLM content generation
./run_myhq.sh content

# SDR list for funded founders only
python3 agent_v2.py --run sdr --persona 1 --dry-run
```

### Project Structure

```
myhq-gtm-engine/
├── agent_v2.py                     # v2 master orchestrator (8-step pipeline)
├── agent.py                        # v1 orchestrator (PKM-gated)
├── run_myhq.sh                     # Quick-run wrapper
├── scheduler.py                    # Cron job runner
├── setup_airtable.py               # Airtable schema creation
├── config/
│   ├── settings_v2.py              # v2 config (15+ API keys, 10 signals, 3 personas)
│   └── settings.py                 # v1 config
├── pipeline/
│   ├── signals_india_v2.py         # 5-tier India-first signal detection
│   ├── enrichment_india_v2.py      # Waterfall enrichment (5 sources + 3 verifiers)
│   ├── pkm_myhq.py                 # PKM defense profiling + outreach generation
│   ├── whatsapp_india.py           # WATI BSP + reply classifier + HOT alerts
│   ├── competitor_intel.py         # Weekly competitor scraping (5 competitors)
│   ├── llm_content_indexer.py      # Content generation + Perplexity indexing
│   ├── scorer.py                   # 5-dimension intent scoring (0-100)
│   ├── persona_matcher.py          # 3-persona matching
│   ├── outreach_generator.py       # Claude-powered outreach (PKM-gated)
│   ├── sdr_dashboard.py            # Rich terminal SDR cards
│   ├── whatsapp_formatter.py       # WhatsApp Business API formatting (PKM-gated)
│   └── utils.py                    # Shared utilities
├── compliance/
│   └── india.py                    # TRAI DND + PDPB + suppression + limits
├── tests/                          # Scorer, persona, dedup, compliance tests
└── results/                        # Generated SDR lists + reports
```

---

## Version Roadmap: v2 → v5

### v2 (CURRENT) — India-First Signal Intelligence + PKM

What we have now. Replaces scrapers-on-scrapers with structured Indian data APIs. PKM mandatory gate on every message. WATI WhatsApp. Competitor intel. LLM content indexing. Manual SDR still closes the deal.

**The human SDR reads the card, picks up the phone, closes the call.**

---

### v3 — AUTONOMOUS DEAL ORCHESTRATOR

*Target: Month 3-6. The agent runs the entire top-of-funnel autonomously.*

**Self-Learning PKM**
- Every WhatsApp reply trains the defense classifier. Reply "not interested" after MOTIVE_INFERENCE message → the model learns this founder's defense was misclassified.
- Defense mode accuracy: v2 starts at ~65%. v3 reaches 90%+ by learning from 5,000+ interactions.
- New defense modes discovered from data — the system finds patterns humans didn't name yet.

**Autonomous Multi-Channel Sequencing**
- The agent decides which channel to use next based on reply patterns. If WhatsApp gets no reply in 48h → email. If email opens but no reply → LinkedIn. If LinkedIn connection accepted → WhatsApp follow-up.
- No human decides the sequence. The agent optimizes for reply rate per persona × city × sector.
- A/B testing built in — two PKM bypass strategies compete, winner gets 80% of sends.

**Meeting Booking**
- Cal.com / Calendly integration. The agent books the meeting directly from WhatsApp.
- "Worth a 10-min call?" becomes a calendar link with 3 time slots. Prospect picks one. SDR gets a calendar invite with full context card.
- The SDR shows up to a meeting that was sourced, qualified, and booked by the agent.

**Signal Fusion**
- Single weak signals are noise. Combined weak signals are conviction.
- Company X posted 12 jobs on Naukri + founder posted "scaling fast" on LinkedIn + Tracxn shows they raised but it's not public yet = 95% confidence, 24h urgency.
- Signal fusion score replaces individual signal confidence.

**SDR Coaching**
- Integrates with Gong/Chorus call recording APIs.
- Analyzes which opening lines converted → feeds back into PKM outreach generation.
- "Your funded founder opener converts at 34%. Try this variation that converts at 52% for similar companies."

**What changes for myHQ:**
The SDR team shrinks from "find leads + call them" to "show up to booked meetings + close." The agent handles everything before the meeting.

---

### v4 — PREDICTIVE MARKET INTELLIGENCE

*Target: Month 6-12. The agent predicts demand before companies know they need workspace.*

**90-Day Demand Prediction**
- ML model trained on 2 years of signal → deal → close data.
- Input: company age, sector, city, funding history, headcount trajectory, LinkedIn growth rate, GST filing frequency.
- Output: probability of workspace need in 30/60/90 days + estimated seats + estimated budget.
- myHQ sees a pipeline 90 days into the future. No competitor has this.

**Territory Optimization**
- Assigns leads to SDRs based on historical close rates per city × sector × persona.
- SDR Priya closes 3x better with FinTech founders in BLR → she gets all FinTech BLR leads.
- Territory rebalances monthly based on performance data.

**Dynamic Pricing War Room**
- Detects when Awfis raises prices in BLR by ₹2,000/seat → triggers an outreach campaign within 2 hours.
- Detects when WeWork opens a new HYD location → targets their pipeline leads before WeWork's sales team ramps.
- Real-time competitive pricing dashboard: every competitor, every city, every product, updated weekly.

**Autonomous Content Engine**
- Generates 20+ content pieces/month calibrated to LLM training data.
- Tracks which queries return myHQ in Claude/GPT/Perplexity answers. Identifies gaps. Writes content to fill them.
- Content performance feedback loop: articles that appear in LLM answers get expanded, articles that don't get rewritten.

**Revenue Forecasting**
- Every lead in pipeline has: probability of close × estimated deal size × estimated timeline.
- Dashboard: "Next month projected: ₹X lakhs MRR from pipeline of Y leads."
- myHQ leadership makes hiring, expansion, and marketing decisions based on agent-predicted revenue.

**Multi-Product Routing**
- Not just managed offices. Meeting rooms, virtual offices, event spaces, parking, cafeteria access.
- Signal: company posts 2 jobs in a city they don't have an office → route to virtual office product.
- Signal: company has an office but posts "team offsite" → route to event space product.
- The agent knows which product to sell before the SDR does.

**What changes for myHQ:**
The VP Sales opens a dashboard and sees: "Next 90 days: 847 companies will need workspace. 312 are in your pipeline. 535 are not — here's the campaign to reach them." Every decision is data-driven. Every SDR is optimized. Every competitor move triggers a counter-move.

---

### v5 — AGI REVENUE CORE

*Target: Month 12-24. The system IS the revenue team. myHQ cannot function without it.*

**The agent doesn't assist the sales team. The agent IS the sales team.**

Human SDRs become closers-only. Everything before the meeting — signal detection, enrichment, profiling, outreach, follow-up, objection handling, qualification, meeting booking — is autonomous. The human shows up when the deal needs a handshake.

**Autonomous Negotiation**
- Handles pricing objections via WhatsApp in real-time.
- "₹15,000/seat is too much" → agent knows this company's budget range (from funding data + employee count + sector benchmarks), knows the competitor's price (from weekly scraping), knows the PKM defense mode → generates a response that reframes value.
- Escalates to human only when: deal size >₹50L/year, enterprise compliance requirements, or prospect explicitly asks for a human.
- The agent closes 60% of deals under ₹10L/year autonomously. SDR team handles the 40% that need a human touch.

**Self-Evolving Playbooks**
- Analyzes 10,000+ interactions across all personas, cities, and sectors.
- Discovers new playbooks: "Companies that raised from Sequoia respond 3.2x better to data-first messages than congratulatory openers."
- Creates new defense modes from clustering reply patterns. A mode that started as "OVERLOAD_AVOIDANCE" splits into "OVERLOAD_AVOIDANCE_MORNING" (responds to 7am sends) and "OVERLOAD_AVOIDANCE_EVENING" (responds to 6pm sends).
- Playbooks evolve weekly. What worked in January may not work in March. The agent adapts faster than any sales manager could retrain a team.

**Cross-Agent Intelligence (AROS + ARIA + myHQ)**
- Every lead profiled by the myHQ agent shares its PKM defense profile with AROS (home care revenue agent) and ARIA (capital raising agent).
- A founder profiled as MOTIVE_INFERENCE by the myHQ agent → AROS knows this person's defense mode when they become a home care lead 6 months later.
- The brain compounds across all three agents. Every interaction makes every agent smarter.
- Single Airtable brain → shared PKM_Cache → cross-vertical defense intelligence.

**Market Creation**
- Doesn't just find companies that need workspace. Identifies companies that SHOULD use flex workspace but don't know it yet.
- Pattern: Companies with 15-30 employees, 2+ years old, no MCA subsidiary, remote-first job postings → they've never had an office → they don't know what they're missing.
- Creates demand by targeting these companies with educational content: "Why your 20-person remote team is 40% less productive without a physical anchor — and what myHQ's data says about hybrid."
- The agent creates pipeline that didn't exist before. This is the difference between intelligence and AGI.

**Relationship Graph**
- Maps every decision-maker connection in Indian B2B coworking.
- Knows: this CEO was CTO at a company that used WeWork → they know WeWork's flaws → lead with myHQ's advantages over WeWork specifically.
- Warm intro routing: "Your investor Sequoia has 3 portfolio companies using myHQ. Want me to connect you with their ops lead for a reference?"
- The relationship graph turns cold outreach into warm outreach at scale. This is the moat.

**Autonomous City + Vertical Expansion**
- Identifies new cities where demand is growing: Ahmedabad GST registrations +45% QoQ, Naukri postings +60%, but myHQ has zero presence → recommends expansion with projected demand model.
- Identifies new verticals: GCC (Global Capability Centers) are 250+ in India and growing → creates a GCC-specific playbook, content, and outreach sequence autonomously.
- Presents to myHQ leadership: "If you open 10 locations in Ahmedabad, I project ₹2.3Cr ARR within 18 months based on these signals. Here's the demand model."

**Financial Model Integration**
- Every lead has: projected LTV, cost-to-acquire, time-to-close, probability-of-churn.
- Pipeline dashboard shows not just lead count but projected revenue impact.
- "This week's HOT leads represent ₹47L projected annual revenue. Closing 40% = ₹18.8L MRR added."
- myHQ's finance team uses agent projections for board decks and fundraising models.

**The Compound Effect**
- v5 at month 1 is good. v5 at month 12 is 10x better.
- Every interaction trains the PKM. Every deal trains the pricing model. Every lost deal trains the objection handler. Every competitor move trains the war room.
- By month 24, the system has processed 50,000+ interactions, profiled 10,000+ decision-makers, and learned patterns no human team could hold in their heads.
- **This is why myHQ can't live without it.** The institutional knowledge is in the system, not in any person's head. If an SDR leaves, zero knowledge is lost. If the VP Sales leaves, the playbooks keep running. The agent IS the institutional memory of myHQ's revenue operation.

**What v5 means for myHQ:**
The CEO opens one dashboard and sees: every lead in India that needs workspace, ranked by revenue potential, with autonomous outreach running, meetings booking themselves, objections being handled, competitors being countered, content ranking in LLMs, and revenue projecting 90 days forward. The sales team's job is to show up to meetings and close. Everything else is the agent.

---

## Version Summary

| Version | Core Capability | Human Role | Leads/Month | Close Rate |
|---------|----------------|-----------|-------------|------------|
| **v2** | Signal detection + PKM outreach | SDR reads card, calls, closes | 200-500 | 5-8% |
| **v3** | Autonomous sequencing + meeting booking | SDR shows up to booked meetings | 1,000-2,000 | 10-15% |
| **v4** | Predictive demand + territory optimization | SDR is optimally assigned + coached | 3,000-5,000 | 15-25% |
| **v5** | AGI revenue core — autonomous negotiation | Human closes only >₹50L deals | 10,000+ | 25-40% |

---

**Target cities:** Bengaluru · Mumbai · Delhi-NCR · Hyderabad · Pune
**Built for:** myHQ (myhq.in)
**Part of:** Aonxi AGI infrastructure — AROS (revenue) · ARIA (capital) · myHQ GTM (workspace)
**Brain:** Shared Airtable PKM_Cache across all agents
