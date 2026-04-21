"""myHQ GTM Engine v2 — Central configuration.

India-first API stack — no scrapers-on-scrapers.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Signal Detection APIs (India-primary) ────────────────────────────
DATAGOV_API_KEY: str = os.getenv("DATAGOV_API_KEY", "")
DATAGOV_RESOURCE_ID: str = os.getenv("DATAGOV_RESOURCE_ID", "")
TRACXN_API_KEY: str = os.getenv("TRACXN_API_KEY", "")
CRUNCHBASE_API_KEY: str = os.getenv("CRUNCHBASE_API_KEY", "")
NEWS_API_KEY: str = os.getenv("NEWS_API_KEY", "")
APIFY_TOKEN: str = os.getenv("APIFY_TOKEN", "")

# ── Contact Enrichment (waterfall) ───────────────────────────────────
APOLLO_API_KEY: str = os.getenv("APOLLO_API_KEY", "")
APOLLO_WEBHOOK_URL: str = os.getenv("APOLLO_WEBHOOK_URL", "")
DATABASE_URL: str = os.getenv("DATABASE_URL", "")
PDL_API_KEY: str = os.getenv("PDL_API_KEY", "")
NETROWS_API_KEY: str = os.getenv("NETROWS_API_KEY", "")
LUSHA_API_KEY: str = os.getenv("LUSHA_API_KEY", "")
HUNTER_API_KEY: str = os.getenv("HUNTER_API_KEY", "")

# ── Verification (India-critical) ────────────────────────────────────
MILLIONVERIFIER_KEY: str = os.getenv("MILLIONVERIFIER_KEY", "")
MSG91_API_KEY: str = os.getenv("MSG91_API_KEY", "")
TRAI_DND_KEY: str = os.getenv("TRAI_DND_KEY", "")
WHATSAPP_TOKEN: str = os.getenv("WHATSAPP_TOKEN", "")
WHATSAPP_PHONE_ID: str = os.getenv("WHATSAPP_PHONE_ID", "")

# ── AI + Memory ──────────────────────────────────────────────────────
ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
OPENROUTER_API_KEY: str = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL: str = os.getenv("OPENROUTER_MODEL", "openai/gpt-oss-120b")
AIRTABLE_API_KEY: str = os.getenv("AIRTABLE_API_KEY", "")
AIRTABLE_BASE_ID: str = os.getenv("AIRTABLE_BASE_ID", "")

# ── Delivery ─────────────────────────────────────────────────────────
INSTANTLY_API_KEY: str = os.getenv("INSTANTLY_API_KEY", "")
SMTP_USER: str = os.getenv("SMTP_USER", "")
SMTP_PASS: str = os.getenv("SMTP_PASS", "")

# ── Pipeline flags ───────────────────────────────────────────────────
DRY_RUN: bool = os.getenv("DRY_RUN", "true").lower() in ("true", "1", "yes")
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

# ── Target Cities ────────────────────────────────────────────────────
CITIES: dict = {
    "BLR": {
        "name": "Bengaluru",
        "aliases": ["Bangalore", "Bengaluru", "Bangaluru"],
        "priority": 1,
        "naukri_name": "Bengaluru/Bangalore",
        "gst_state_code": "29",
        "mca_state": "Karnataka",
        "myhq_locations": "50+",
        "sector_strengths": ["tech", "startup", "saas", "fintech"],
        "news_keywords": ["Bengaluru", "Bangalore"],
    },
    "MUM": {
        "name": "Mumbai",
        "aliases": ["Mumbai", "Bombay"],
        "priority": 2,
        "naukri_name": "Mumbai",
        "gst_state_code": "27",
        "mca_state": "Maharashtra",
        "myhq_locations": "40+",
        "sector_strengths": ["finance", "media", "fintech", "d2c"],
        "news_keywords": ["Mumbai", "Bombay"],
    },
    "DEL": {
        "name": "Delhi-NCR",
        "aliases": ["Delhi", "Gurgaon", "Gurugram", "Noida", "New Delhi", "NCR"],
        "priority": 3,
        "naukri_name": "Delhi/NCR",
        "gst_state_code": "07",
        "mca_state": "Delhi",
        "myhq_locations": "35+",
        "sector_strengths": ["enterprise", "govt", "consulting", "edtech"],
        "news_keywords": ["Delhi", "Gurgaon", "Gurugram", "Noida", "NCR"],
    },
    "HYD": {
        "name": "Hyderabad",
        "aliases": ["Hyderabad", "Secunderabad", "HITEC City"],
        "priority": 4,
        "naukri_name": "Hyderabad",
        "gst_state_code": "36",
        "mca_state": "Telangana",
        "myhq_locations": "25+",
        "sector_strengths": ["tech", "pharma", "enterprise_it"],
        "news_keywords": ["Hyderabad", "HITEC City"],
    },
    "PUN": {
        "name": "Pune",
        "aliases": ["Pune"],
        "priority": 5,
        "naukri_name": "Pune",
        "gst_state_code": "27",
        "mca_state": "Maharashtra",
        "myhq_locations": "20+",
        "sector_strengths": ["engineering", "it_services", "auto", "saas"],
        "news_keywords": ["Pune"],
    },
}

# ── 3 Buyer Personas ─────────────────────────────────────────────────
PERSONAS: dict = {
    1: {
        "name": "The Funded Founder",
        "size_range": (1, 50),
        "urgency_hours": 48,
        "contact_window_days": 2,
        "decision_maker_titles": [
            "Founder", "Co-founder", "CEO", "CTO",
        ],
        "product_fit": [
            "Fixed Desks", "Private Cabins", "Managed Office (10-30 seats)",
        ],
        "defense_mode_primary": "MOTIVE_INFERENCE",
        "defense_mode_secondary": "IDENTITY_THREAT",
        "bypass": "Lead with their funding news + city name + specific desk count",
    },
    2: {
        "name": "The Ops Expander",
        "size_range": (50, 300),
        "urgency_hours": 168,
        "contact_window_days": 7,
        "decision_maker_titles": [
            "Operations Manager", "Admin Manager", "Facilities Manager",
            "Office Manager", "HR Manager", "People Ops",
        ],
        "product_fit": [
            "Managed Office (30-100 seats)", "Fixed Desks",
        ],
        "defense_mode_primary": "OVERLOAD_AVOIDANCE",
        "defense_mode_secondary": "COMPLEXITY_FEAR",
        "bypass": "Ultra short, specific seats + city + one time slot",
    },
    3: {
        "name": "The Enterprise Expander",
        "size_range": (300, 100_000),
        "urgency_hours": 336,
        "contact_window_days": 14,
        "decision_maker_titles": [
            "VP Sales", "Director BD", "Sales Head",
            "VP Business Development", "Country Manager", "Regional Head",
            "VP Operations", "VP Admin", "Head of Facilities",
        ],
        "product_fit": [
            "Managed Office (100+ seats)", "Commercial Leasing",
        ],
        "defense_mode_primary": "SOCIAL_PROOF_SKEPTICISM",
        "defense_mode_secondary": "AUTHORITY_DEFERENCE",
        "bypass": "Named enterprise customers + SLA + GST invoice + ammo for their CRE head",
    },
}

# ── 10 Trigger Signals ───────────────────────────────────────────────
TRIGGER_SIGNALS: dict = {
    "FUNDING": {"urgency_hours": 48, "persona": 1, "tier": 1, "automatable": True},
    "HIRING_SURGE": {"urgency_hours": 168, "persona": 2, "tier": 1, "automatable": True},
    "GST_NEW_CITY": {"urgency_hours": 336, "persona": 3, "tier": 1, "automatable": True},
    "MCA_NEW_SUBSIDIARY": {"urgency_hours": 336, "persona": 3, "tier": 1, "automatable": True},
    "LEASE_EXPIRY": {"urgency_hours": 720, "persona": 2, "tier": 2, "automatable": False},
    "WFH_REVERSAL": {"urgency_hours": 336, "persona": 2, "tier": 2, "automatable": False},
    "CITY_EXPANSION_PR": {"urgency_hours": 168, "persona": 3, "tier": 2, "automatable": False},
    "CONTRACT_WIN": {"urgency_hours": 168, "persona": 3, "tier": 2, "automatable": False},
    "FOUNDER_HIRING_POST": {"urgency_hours": 168, "persona": 1, "tier": 2, "automatable": False},
    "COMPETITOR_CLOSURE": {"urgency_hours": 48, "persona": 2, "tier": 2, "automatable": False},
}

# ── Competitors ──────────────────────────────────────────────────────
COMPETITORS: list[dict] = [
    {"name": "WeWork India", "website": "wework.co.in"},
    {"name": "Awfis", "website": "awfis.com"},
    {"name": "IndiQube", "website": "indiqube.com"},
    {"name": "Smartworks", "website": "smartworks.co"},
    {"name": "Regus India", "website": "regus.co.in"},
    {"name": "91springboard", "website": "91springboard.com"},
    {"name": "CoWrks", "website": "cowrks.com"},
    {"name": "Innov8", "website": "innov8.work"},
    {"name": "The Executive Centre", "website": "executivecentre.com"},
    {"name": "Simpliwork", "website": "simpliwork.com"},
]

# ── Indian news sources for NLP signals ──────────────────────────────
INDIA_NEWS_DOMAINS: list[str] = [
    "entrackr.com",
    "inc42.com",
    "yourstory.com",
    "economictimes.com",
    "livemint.com",
    "moneycontrol.com",
]

# ── Scoring tiers ────────────────────────────────────────────────────
INTENT_TIERS: dict[str, tuple[int, int]] = {
    "HOT": (80, 100),
    "WARM": (60, 79),
    "NURTURE": (40, 59),
    "MONITOR": (0, 39),
}

# ── Compliance ───────────────────────────────────────────────────────
MAX_OUTREACH_TOUCHES: int = 3
COOLING_PERIOD_DAYS: int = 7
