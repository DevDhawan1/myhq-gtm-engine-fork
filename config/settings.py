"""myHQ GTM Engine — Central configuration."""

import os
from dotenv import load_dotenv

load_dotenv()

# ── API Keys ────────────────────────────────────────────────────────
SERP_API_KEY: str = os.getenv("SERP_API_KEY", "")
SCRAPER_API_KEY: str = os.getenv("SCRAPER_API_KEY", "")
APOLLO_API_KEY: str = os.getenv("APOLLO_API_KEY", "")
ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
OPENROUTER_API_KEY: str = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL: str = os.getenv("OPENROUTER_MODEL", "openai/gpt-oss-120b")
SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "")
WHATSAPP_TOKEN: str = os.getenv("WHATSAPP_TOKEN", "")
WHATSAPP_PHONE_ID: str = os.getenv("WHATSAPP_PHONE_ID", "")

# ── Pipeline flags ──────────────────────────────────────────────────
DRY_RUN: bool = os.getenv("DRY_RUN", "true").lower() in ("true", "1", "yes")
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

# ── Target Cities ───────────────────────────────────────────────────
CITIES: dict = {
    "BLR": {
        "name": "Bengaluru",
        "aliases": ["Bangalore", "Bengaluru", "Bangaluru"],
        "priority": 1,
        "timezone": "Asia/Kolkata",
        "sector_strengths": ["tech", "startup", "saas", "fintech"],
    },
    "MUM": {
        "name": "Mumbai",
        "aliases": ["Mumbai", "Bombay"],
        "priority": 2,
        "timezone": "Asia/Kolkata",
        "sector_strengths": ["finance", "media", "fintech", "d2c"],
    },
    "DEL": {
        "name": "Delhi-NCR",
        "aliases": ["Delhi", "Gurgaon", "Gurugram", "Noida", "New Delhi", "NCR"],
        "priority": 3,
        "timezone": "Asia/Kolkata",
        "sector_strengths": ["enterprise", "govt", "consulting", "edtech"],
    },
    "HYD": {
        "name": "Hyderabad",
        "aliases": ["Hyderabad", "Secunderabad"],
        "priority": 4,
        "timezone": "Asia/Kolkata",
        "sector_strengths": ["tech", "pharma", "enterprise_it"],
    },
    "PUN": {
        "name": "Pune",
        "aliases": ["Pune"],
        "priority": 5,
        "timezone": "Asia/Kolkata",
        "sector_strengths": ["engineering", "it_services", "auto", "saas"],
    },
}

# ── 3 Buyer Personas ───────────────────────────────────────────────
PERSONAS: dict = {
    1: {
        "name": "The Funded Founder",
        "size_range": (1, 50),
        "urgency": "HIGH",
        "contact_window_days": 2,
        "decision_maker_titles": [
            "Founder", "Co-founder", "CEO", "CTO",
        ],
        "product_fit": [
            "Fixed Desks", "Private Cabins", "Managed Office (10-30 seats)",
        ],
        "sdr_angle": (
            "Congratulations on the raise — most founders we work with "
            "need a real office within 60 days. We can have you set up in a week."
        ),
    },
    2: {
        "name": "The Ops Expander",
        "size_range": (50, 300),
        "urgency": "MEDIUM",
        "contact_window_days": 7,
        "decision_maker_titles": [
            "Operations Manager", "Admin Manager", "Facilities Manager",
            "Office Manager", "HR Manager", "People Ops",
        ],
        "product_fit": [
            "Managed Office (30-100 seats)", "Fixed Desks",
        ],
        "sdr_angle": (
            "We handle everything — shortlisting, site visits, documentation, "
            "GST invoicing. You present one vetted recommendation to your leadership."
        ),
    },
    3: {
        "name": "The Enterprise Expander",
        "size_range": (300, 100_000),
        "urgency": "LOW-MEDIUM",
        "contact_window_days": 14,
        "decision_maker_titles": [
            "VP Sales", "Director BD", "Sales Head",
            "VP Business Development", "Country Manager", "Regional Head",
        ],
        "product_fit": [
            "Managed Office (100+ seats)", "Commercial Leasing",
        ],
        "sdr_angle": (
            "We work with JLL and CBRE-level clients. Full compliance "
            "documentation, dedicated account manager, references from "
            "similar companies."
        ),
    },
}

# ── Competitors ─────────────────────────────────────────────────────
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

# ── Scoring tiers ───────────────────────────────────────────────────
INTENT_TIERS: dict[str, tuple[int, int]] = {
    "HOT": (80, 100),
    "WARM": (60, 79),
    "NURTURE": (40, 59),
    "MONITOR": (0, 39),
}

# ── Compliance ──────────────────────────────────────────────────────
MAX_OUTREACH_TOUCHES: int = 3
COOLING_PERIOD_DAYS: int = 7

# ── Supabase table names ───────────────────────────────────────────
TABLE_SIGNALS_FUNDING = "signals_funding"
TABLE_SIGNALS_HIRING = "signals_hiring"
TABLE_SIGNALS_EXPANSION = "signals_expansion"
TABLE_SIGNALS_INTENT = "signals_intent"
TABLE_LEADS = "leads"
TABLE_OUTREACH = "outreach"
TABLE_SDR_CALL_LIST = "sdr_call_list"
TABLE_AD_INTELLIGENCE = "ad_intelligence"
TABLE_WHATSAPP_TEMPLATES = "whatsapp_templates"
TABLE_SUPPRESSION = "suppression_list"
