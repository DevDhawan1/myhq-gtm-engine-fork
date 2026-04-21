"""PrivateCircle API — structured Indian company intelligence.

Why PrivateCircle over Tracxn for India:
- MCA + RoC + BSE data in one structured API
- Director networks (who sits on which board)
- Shareholding patterns (funding rounds inferred from share allotments)
- Real-time company filings
- Covers unlisted companies Tracxn misses
- Catches funding rounds BEFORE news outlets via SH-7 filings

Endpoints used:
  /company/filings     — recent MCA filings (new subsidiaries, capital raise)
  /company/directors   — director network (who to contact)
  /company/financials  — revenue trajectory (buying intent signal)

Signal types produced:
  PRIVATE_CIRCLE_FILING   — new MCA filing detected (expansion)
  PRIVATE_CIRCLE_CAPITAL  — share allotment SH-7 (funding proxy)
  PRIVATE_CIRCLE_DIRECTOR — new director added (team expansion)
"""

from __future__ import annotations

import logging
import os
import random
from datetime import datetime, timedelta, timezone

import requests

logger = logging.getLogger("myhq.signals.privatecircle")

PRIVATECIRCLE_API_KEY = os.getenv("PRIVATECIRCLE_API_KEY", "")
BASE_URL = "https://api.privatecircle.co/v1"

CITY_MAP: dict[str, list[str]] = {
    "BLR": ["Bengaluru", "Bangalore", "Karnataka"],
    "MUM": ["Mumbai", "Maharashtra"],
    "DEL": ["Delhi", "Gurugram", "Gurgaon", "Noida"],
    "HYD": ["Hyderabad", "Telangana"],
    "PUN": ["Pune", "Maharashtra"],
}

HIGH_LTV_SECTORS = [
    "information technology", "software", "saas", "fintech",
    "edtech", "healthtech", "cleantech", "logistics",
    "ecommerce", "media", "consulting",
]


def fetch_new_filings(city_code: str, days_back: int = 7) -> list[dict]:
    """Fetch companies with new MCA filings in a city.

    New subsidiary or capital allotment = expansion signal.
    """
    if not PRIVATECIRCLE_API_KEY:
        logger.debug("PRIVATECIRCLE_API_KEY not set — skipping filings")
        return []

    cities = CITY_MAP.get(city_code, [city_code])
    signals: list[dict] = []

    for city_name in cities[:1]:
        try:
            resp = requests.get(
                f"{BASE_URL}/company/filings",
                headers={"x-api-key": PRIVATECIRCLE_API_KEY},
                params={
                    "city": city_name,
                    "form_type": "MGT-14,SH-7,INC-22",
                    "filed_after": (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d"),
                    "limit": 100,
                },
                timeout=15,
            )
            resp.raise_for_status()

            for filing in resp.json().get("filings", []):
                co = filing.get("company", {})
                sector = co.get("industry", "").lower()
                sector_fit = any(s in sector for s in HIGH_LTV_SECTORS)

                signals.append({
                    "company_name": co.get("name"),
                    "city": city_code,
                    "signal_type": "PRIVATE_CIRCLE_FILING",
                    "signal_detail": f"MCA filing: {filing.get('form_type')} — {filing.get('description', 'new filing')}",
                    "urgency_hours": 168,
                    "persona": _infer_persona(co),
                    "confidence_score": 75 if sector_fit else 50,
                    "employee_count": co.get("employee_count"),
                    "sector": co.get("industry"),
                    "cin": co.get("cin"),
                    "website": co.get("website"),
                    "raw_source": "privatecircle",
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                })
        except Exception as e:
            logger.warning("PrivateCircle filings %s: %s", city_code, e)

    return signals


def fetch_share_allotments(city_code: str, days_back: int = 14) -> list[dict]:
    """Fetch recent SH-7 share allotments.

    Share allotment = company raised money = workspace need imminent.
    Catches funding rounds before Tracxn or Inc42 cover them.
    """
    if not PRIVATECIRCLE_API_KEY:
        return []

    cities = CITY_MAP.get(city_code, [city_code])
    signals: list[dict] = []

    for city_name in cities[:1]:
        try:
            resp = requests.get(
                f"{BASE_URL}/company/filings",
                headers={"x-api-key": PRIVATECIRCLE_API_KEY},
                params={
                    "city": city_name,
                    "form_type": "SH-7",
                    "filed_after": (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d"),
                    "limit": 50,
                },
                timeout=15,
            )
            resp.raise_for_status()

            for filing in resp.json().get("filings", []):
                co = filing.get("company", {})
                amount = filing.get("allotment_amount_cr", 0)
                if amount < 1:
                    continue

                signals.append({
                    "company_name": co.get("name"),
                    "city": city_code,
                    "signal_type": "PRIVATE_CIRCLE_CAPITAL",
                    "signal_detail": f"Share allotment ₹{amount}Cr — funding proxy signal",
                    "urgency_hours": 48,
                    "persona": 1,
                    "confidence_score": 85,
                    "employee_count": co.get("employee_count"),
                    "sector": co.get("industry"),
                    "amount_raised_cr": amount,
                    "cin": co.get("cin"),
                    "website": co.get("website"),
                    "raw_source": "privatecircle_sh7",
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                })
        except Exception as e:
            logger.warning("PrivateCircle SH-7 %s: %s", city_code, e)

    return signals


def fetch_director_changes(city_code: str, days_back: int = 30) -> list[dict]:
    """Fetch companies with new directors. New director = expansion or funding."""
    if not PRIVATECIRCLE_API_KEY:
        return []

    cities = CITY_MAP.get(city_code, [city_code])
    signals: list[dict] = []

    for city_name in cities[:1]:
        try:
            resp = requests.get(
                f"{BASE_URL}/company/filings",
                headers={"x-api-key": PRIVATECIRCLE_API_KEY},
                params={
                    "city": city_name,
                    "form_type": "DIR-12",
                    "filed_after": (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d"),
                    "change_type": "appointment",
                    "limit": 50,
                },
                timeout=15,
            )
            resp.raise_for_status()

            for filing in resp.json().get("filings", []):
                co = filing.get("company", {})
                signals.append({
                    "company_name": co.get("name"),
                    "city": city_code,
                    "signal_type": "PRIVATE_CIRCLE_DIRECTOR",
                    "signal_detail": f"New director: {filing.get('director_name', 'unknown')}",
                    "urgency_hours": 336,
                    "persona": _infer_persona(co),
                    "confidence_score": 45,
                    "employee_count": co.get("employee_count"),
                    "sector": co.get("industry"),
                    "cin": co.get("cin"),
                    "raw_source": "privatecircle_dir",
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                })
        except Exception as e:
            logger.warning("PrivateCircle DIR-12 %s: %s", city_code, e)

    return signals


def collect_all_privatecircle(cities: list[str], dry_run: bool = False) -> list[dict]:
    """Entry point — collect all PrivateCircle signals for given cities."""
    all_signals: list[dict] = []
    for city in cities:
        all_signals.extend(fetch_new_filings(city))
        all_signals.extend(fetch_share_allotments(city))
        all_signals.extend(fetch_director_changes(city))
    logger.info("PrivateCircle: %d signals across %d cities", len(all_signals), len(cities))
    return all_signals


def _infer_persona(company: dict) -> int:
    emp = company.get("employee_count") or 0
    if emp < 50:
        return 1
    if emp < 300:
        return 2
    return 3


def _synthetic_filings(city_code: str) -> list[dict]:
    sectors = ["SaaS", "FinTech", "EdTech", "CleanTech", "HealthTech"]
    return [
        {
            "company_name": f"PC_Synthetic_{city_code}_{i}",
            "city": city_code,
            "signal_type": "PRIVATE_CIRCLE_CAPITAL",
            "signal_detail": f"SH-7 share allotment ₹{random.randint(2, 30)}Cr",
            "urgency_hours": 48,
            "persona": 1,
            "confidence_score": 85,
            "employee_count": random.randint(10, 60),
            "sector": random.choice(sectors),
            "cin": f"U72200KA2023PTC{random.randint(100000, 999999)}",
            "raw_source": "privatecircle_synthetic",
            "detected_at": datetime.now(timezone.utc).isoformat(),
        }
        for i in range(random.randint(2, 4))
    ]
