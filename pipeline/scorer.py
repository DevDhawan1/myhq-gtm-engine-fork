"""myHQ GTM Engine — Intent scoring system (0-100) across 5 dimensions.

Each lead is scored across:
  1. Trigger Recency (0-20)
  2. Trigger Strength (0-20)
  3. Company Fit (0-20)
  4. Decision Maker Reachability (0-20)
  5. City + Product Fit (0-20)

Tiers: HOT 80-100 | WARM 60-79 | NURTURE 40-59 | MONITOR 0-39
"""

from __future__ import annotations

import logging
from datetime import datetime

from config.settings import CITIES, INTENT_TIERS
from pipeline.utils import IST, hours_since, days_since

logger = logging.getLogger(__name__)


class IntentScorer:
    """Scores leads 0-100 for workspace purchase intent."""

    TIER_LABELS = {
        "HOT": "🔥 HOT — SDR calls within 2 hours",
        "WARM": "♨️  WARM — SDR calls within 24 hours",
        "NURTURE": "🌡️  NURTURE — WhatsApp + email sequence",
        "MONITOR": "👁️  MONITOR — watch for upgrade",
    }

    # ── Public API ──────────────────────────────────────────────────

    def score_lead(self, lead: dict) -> dict:
        """Score a single lead. Mutates and returns the lead dict."""
        scores = {
            "trigger_recency": self._score_trigger_recency(lead),
            "trigger_strength": self._score_trigger_strength(lead),
            "company_fit": self._score_company_fit(lead),
            "reachability": self._score_reachability(lead),
            "city_product_fit": self._score_city_product_fit(lead),
        }
        total = sum(scores.values())
        lead["intent_score"] = total
        lead["tier"] = self._determine_tier(total)
        lead["score_breakdown"] = scores
        return lead

    def score_batch(self, leads: list[dict]) -> list[dict]:
        """Score and sort a batch of leads by score descending."""
        scored = [self.score_lead(l) for l in leads]
        return sorted(scored, key=lambda x: x.get("intent_score", 0), reverse=True)

    # ── Dimension 1: Trigger Recency (0-20) ─────────────────────────

    def _score_trigger_recency(self, lead: dict) -> int:
        date_str = lead.get("announcement_date") or lead.get("created_at") or ""
        h = hours_since(date_str)
        if h < 24:
            return 20
        d = h / 24
        if d <= 3:
            return 17
        if d <= 7:
            return 13
        if d <= 14:
            return 8
        return 3

    # ── Dimension 2: Trigger Strength (0-20) ────────────────────────

    def _score_trigger_strength(self, lead: dict) -> int:
        signal_type = lead.get("signal_type", "")
        delta = lead.get("delta") or 0
        urgency = lead.get("urgency_level", "")
        source = lead.get("source", "")

        if signal_type == "funding":
            return 20
        if signal_type == "hiring":
            if delta >= 10:
                return 20
            if delta >= 5:
                return 15
            return 10
        if signal_type == "expansion":
            if source in ("press_release", "business_news"):
                return 18
            if source in ("mca_filings", "gst_portal"):
                return 17
            return 15
        if signal_type == "intent":
            if urgency == "high":
                return 18
            if urgency == "medium":
                return 14
            if source == "google_trends":
                return 10
            return 8
        return 5

    # ── Dimension 3: Company Fit (0-20) ─────────────────────────────

    def _score_company_fit(self, lead: dict) -> int:
        persona = lead.get("persona_id")
        size = lead.get("company_size") or lead.get("company_size_est") or lead.get("employee_count_est") or 0

        if persona == 1 and 5 <= size <= 50:
            return 20
        if persona == 2 and 50 <= size <= 300:
            return 18
        if persona == 3 and size >= 300:
            return 18

        # Fallback size-based scoring
        if 5 <= size <= 50:
            return 16
        if 50 < size <= 300:
            return 15
        if size > 300:
            return 14
        if 1 <= size < 5:
            return 8
        return 3

    # ── Dimension 4: Decision Maker Reachability (0-20) ─────────────

    def _score_reachability(self, lead: dict) -> int:
        phone = lead.get("contact_phone") or lead.get("contact_whatsapp") or ""
        email = lead.get("contact_email", "")
        linkedin = lead.get("contact_linkedin", "")
        whatsapp = lead.get("contact_whatsapp", "")

        if phone and whatsapp:
            return 20
        if phone:
            return 18
        if email and linkedin:
            return 17
        if linkedin:
            return 12
        if email:
            return 7
        return 0

    # ── Dimension 5: City + Product Fit (0-20) ─────────────────────

    def _score_city_product_fit(self, lead: dict) -> int:
        city = lead.get("city", "")
        sector = (lead.get("sector") or "").lower()
        signal_type = lead.get("signal_type", "")

        city_scores = {
            "BLR": 20,
            "HYD": 18,
            "MUM": 18,
            "DEL": 17,
            "PUN": 16,
        }
        base = city_scores.get(city, 0)
        if base == 0:
            return 0

        # Sector-city alignment bonus (up to +2, capped at 20)
        bonus = 0
        city_strengths = CITIES.get(city, {}).get("sector_strengths", [])
        for strength in city_strengths:
            if strength in sector:
                bonus = 2
                break

        return min(20, base + bonus)

    # ── Tier determination ──────────────────────────────────────────

    def _determine_tier(self, score: int) -> str:
        for tier, (lo, hi) in INTENT_TIERS.items():
            if lo <= score <= hi:
                return tier
        return "MONITOR"

    def get_tier_label(self, tier: str) -> str:
        return self.TIER_LABELS.get(tier, tier)


# ── Sector LTV multiplier (myHQ historical data) ────────────────────
# Based on avg months retained x avg seats x avg INR/seat

SECTOR_LTV_MULTIPLIER: dict[str, float] = {
    "saas": 1.8, "software": 1.8, "it": 1.6,
    "fintech": 1.7, "financial services": 1.7,
    "healthtech": 1.9, "health": 1.9, "pharma": 1.6,
    "edtech": 1.8, "education": 1.7,
    "cleantech": 1.6, "renewable": 1.6, "sustainability": 1.5,
    "logistics": 1.4, "supply chain": 1.4,
    "ecommerce": 1.3, "retail": 1.2,
    "consulting": 1.5, "professional services": 1.4,
    "media": 1.2, "entertainment": 1.1, "gaming": 1.1,
    "real estate": 1.2, "proptech": 1.2,
    "hrtech": 1.3, "recruitment": 1.2,
    "crypto": 0.7, "web3": 0.7, "blockchain": 0.7,
    "social media": 0.8,
}


def get_sector_score(sector: str) -> int:
    """Return 0-20 sector fit score based on historical LTV."""
    if not sector:
        return 10
    sector_lower = sector.lower()
    for key, multiplier in SECTOR_LTV_MULTIPLIER.items():
        if key in sector_lower:
            return max(0, min(20, int((multiplier - 0.7) / (1.9 - 0.7) * 20)))
    return 10


def score_lead_with_sector(lead: dict) -> dict:
    """Add sector LTV score as 6th dimension. Modifies lead in place."""
    sector = lead.get("sector") or lead.get("company_sector", "")
    sector_score = get_sector_score(sector)
    current = lead.get("intent_score", 0)

    lead["sector_score"] = sector_score
    lead["intent_score_raw"] = current
    # Sector adds up to 15% weighting on top of base score
    lead["intent_score"] = min(100, int(current * 0.85 + sector_score * 0.15 * 5))

    return lead


# ── Module entry point ──────────────────────────────────────────────


def score_leads(leads: list[dict]) -> list[dict]:
    """Entry point for lead scoring."""
    scorer = IntentScorer()
    return scorer.score_batch(leads)
