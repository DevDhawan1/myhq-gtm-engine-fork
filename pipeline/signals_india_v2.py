"""myHQ GTM Engine v2 — India-first signal detection.

Signal priority (highest to lowest confidence):
  TIER 1 — Structured government data (MCA, GST) — hardest proof
  TIER 2 — Verified funding data (Tracxn, Crunchbase) — 48h urgency
  TIER 3 — Hiring signals (Naukri + LinkedIn via Proxycurl)
  TIER 4 — News/announcement NLP (Inc42, Entrackr, YourStory)
  TIER 5 — Intent signals (property listings, LinkedIn posts)

Each signal produces a structured dict:
  {
    company_name, city, signal_type, signal_detail,
    urgency_hours, persona, confidence_score,
    raw_source, detected_at
  }
"""

from __future__ import annotations

import logging
import os
import random
from datetime import datetime, timedelta, timezone

import requests

from config.settings_v2 import (
    APIFY_TOKEN,
    CITIES,
    CRUNCHBASE_API_KEY,
    NEWS_API_KEY,
    PROXYCURL_API_KEY,
    TRACXN_API_KEY,
    TRIGGER_SIGNALS,
)

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))


# ═══════════════════════════════════════════════════════════════════════
# TIER 1 — MCA new incorporation signal
# ═══════════════════════════════════════════════════════════════════════

class MCASignalCollector:
    """Fetch companies newly incorporated via MCA (Ministry of Corporate Affairs).

    MCA data is free and authoritative — a new CIN is the hardest
    expansion signal available. Uses araystech/mca-data-api wrapper.
    """

    # MCA public API via araystech wrapper
    BASE_URL = "https://mca-data-api.onrender.com"

    def collect(self, cities: list[str], days_back: int = 14) -> list[dict]:
        signals: list[dict] = []
        for city_code in cities:
            city_info = CITIES.get(city_code, {})
            state = city_info.get("mca_state", "")
            if not state:
                continue
            try:
                resp = requests.get(
                    f"{self.BASE_URL}/api/companies",
                    params={
                        "state": state,
                        "days": days_back,
                        "status": "Active",
                        "limit": 50,
                    },
                    timeout=20,
                )
                resp.raise_for_status()
                companies = resp.json().get("data", [])

                for co in companies:
                    # Filter: only Private Limited / LLP (startups)
                    company_class = co.get("company_class", "").lower()
                    if "private" not in company_class and "llp" not in company_class:
                        continue

                    signals.append(self._to_signal(co, city_code))

                logger.info("MCA %s: %d new incorporations", city_code, len(companies))
            except Exception as e:
                logger.warning("MCA %s failed: %s", city_code, e)

        return signals

    def _to_signal(self, co: dict, city_code: str) -> dict:
        return {
            "company_name": co.get("company_name", "Unknown"),
            "cin": co.get("cin", ""),
            "city": city_code,
            "signal_type": "MCA_NEW_SUBSIDIARY",
            "signal_detail": f"New incorporation: {co.get('company_name')} (CIN: {co.get('cin', 'N/A')})",
            "urgency_hours": TRIGGER_SIGNALS["MCA_NEW_SUBSIDIARY"]["urgency_hours"],
            "persona": TRIGGER_SIGNALS["MCA_NEW_SUBSIDIARY"]["persona"],
            "confidence_score": 90,
            "employee_count": None,
            "sector": co.get("principal_business_activity", ""),
            "founder_name": co.get("director_name", ""),
            "founder_linkedin": None,
            "website": None,
            "raw_source": "mca",
            "detected_at": datetime.now(IST).isoformat(),
        }


# ═══════════════════════════════════════════════════════════════════════
# TIER 2 — Tracxn funding signal
# ═══════════════════════════════════════════════════════════════════════

class TracxnFundingCollector:
    """Fetch recent Indian startup funding rounds from Tracxn API.

    Tracxn has structured India funding data with:
    - Company name, CIN, website
    - Funding amount, round type (Seed, Series A, etc.)
    - Lead investor names
    - Sector, employee count
    - Founder details

    Signal: Any Seed, Pre-Series A, Series A = 48h urgency for myHQ
    """

    def collect(self, cities: list[str], days_back: int = 7) -> list[dict]:
        if not TRACXN_API_KEY:
            logger.info("TRACXN_API_KEY not set — using synthetic data")
            return self._synthetic(cities)

        signals: list[dict] = []
        city_names = {code: CITIES[code]["name"] for code in cities if code in CITIES}

        for city_code, city_name in city_names.items():
            try:
                resp = requests.get(
                    "https://tracxn.com/api/2.2/companies/search",
                    headers={"accessToken": TRACXN_API_KEY},
                    params={
                        "hqLocation": city_name,
                        "hqCountry": "India",
                        "fundedSinceDays": days_back,
                        "roundType": "Seed,Pre-Series A,Series A",
                        "limit": 50,
                    },
                    timeout=20,
                )
                resp.raise_for_status()
                companies = resp.json().get("results", [])

                for co in companies:
                    signals.append(self._to_signal(co, city_code))

                logger.info("Tracxn %s: %d funding signals", city_code, len(companies))
            except Exception as e:
                logger.warning("Tracxn %s: %s", city_code, e)

        return signals

    def _to_signal(self, co: dict, city_code: str) -> dict:
        latest_round = co.get("latestRound", {})
        founders = co.get("founders", [])
        return {
            "company_name": co.get("name", "Unknown"),
            "city": city_code,
            "signal_type": "FUNDING",
            "signal_detail": f"{latest_round.get('roundType', 'Seed')} — {latest_round.get('amount', 'Undisclosed')}",
            "urgency_hours": TRIGGER_SIGNALS["FUNDING"]["urgency_hours"],
            "persona": TRIGGER_SIGNALS["FUNDING"]["persona"],
            "confidence_score": 95,
            "employee_count": co.get("employeeCount"),
            "sector": co.get("sector", ""),
            "founder_name": founders[0].get("name") if founders else None,
            "founder_linkedin": founders[0].get("linkedinUrl") if founders else None,
            "website": co.get("domain", ""),
            "investor_names": [inv.get("name") for inv in latest_round.get("investors", [])],
            "amount_raised": latest_round.get("amount", ""),
            "round_type": latest_round.get("roundType", "seed"),
            "raw_source": "tracxn",
            "detected_at": datetime.now(IST).isoformat(),
        }

    def _synthetic(self, cities: list[str]) -> list[dict]:
        """Synthetic data for dry runs — matches real Tracxn data shape."""
        sectors = ["SaaS", "FinTech", "HealthTech", "EdTech", "LogisTech", "CleanTech", "D2C", "DevTools"]
        rounds = [
            ("Pre-Seed", "2-5", [2, 5]),
            ("Seed", "5-15", [5, 15]),
            ("Pre-Series A", "15-30", [15, 30]),
            ("Series A", "25-60", [25, 60]),
        ]
        investor_pool = [
            "Peak XV Partners", "Blume Ventures", "Accel", "Lightspeed India",
            "Matrix Partners", "Elevation Capital", "Tiger Global", "Sequoia India",
            "3one4 Capital", "Stellaris", "India Quotient", "Nexus Venture Partners",
        ]
        founder_first = ["Arjun", "Priya", "Karthik", "Sneha", "Rohan", "Aisha", "Vikram", "Neeraj", "Deepak", "Ananya"]
        founder_last = ["Mehta", "Sharma", "Rajan", "Patil", "Deshmukh", "Khan", "Singh", "Gupta", "Iyer", "Reddy"]

        signals: list[dict] = []
        now = datetime.now(IST)

        for city in cities:
            for i in range(random.randint(3, 7)):
                round_type, amount_range, amt_bounds = random.choice(rounds)
                amt = random.randint(amt_bounds[0], amt_bounds[1])
                investors = random.sample(investor_pool, k=random.randint(1, 3))
                fname = random.choice(founder_first)
                lname = random.choice(founder_last)
                sector = random.choice(sectors)
                emp = random.randint(5, 50)
                hours_ago = random.randint(4, 168)

                signals.append({
                    "company_name": f"{sector}Co-{city}-{i + 1}",
                    "city": city,
                    "signal_type": "FUNDING",
                    "signal_detail": f"{round_type} — ₹{amt}Cr (synthetic)",
                    "urgency_hours": 48,
                    "persona": 1,
                    "confidence_score": 95,
                    "employee_count": emp,
                    "sector": sector,
                    "founder_name": f"{fname} {lname}",
                    "founder_linkedin": f"linkedin.com/in/{fname.lower()}{lname.lower()}",
                    "website": f"https://{sector.lower()}co{i + 1}.in",
                    "investor_names": investors,
                    "amount_raised": f"₹{amt}Cr",
                    "round_type": round_type.lower().replace(" ", "_").replace("-", "_"),
                    "raw_source": "tracxn_synthetic",
                    "detected_at": (now - timedelta(hours=hours_ago)).isoformat(),
                })

        return signals


# ═══════════════════════════════════════════════════════════════════════
# TIER 2b — Crunchbase secondary funding (cross-reference)
# ═══════════════════════════════════════════════════════════════════════

class CrunchbaseFundingCollector:
    """Secondary funding source — better for international rounds and cross-referencing."""

    def collect(self, cities: list[str], days_back: int = 7) -> list[dict]:
        if not CRUNCHBASE_API_KEY:
            logger.debug("CRUNCHBASE_API_KEY not set — skipping")
            return []

        signals: list[dict] = []
        for city_code in cities:
            city_name = CITIES.get(city_code, {}).get("name", city_code)
            try:
                resp = requests.post(
                    "https://api.crunchbase.com/api/v4/searches/funding_rounds",
                    headers={"X-cb-user-key": CRUNCHBASE_API_KEY},
                    json={
                        "field_ids": [
                            "identifier", "funded_organization_identifier",
                            "money_raised", "investment_type", "announced_on",
                        ],
                        "query": [
                            {"type": "predicate", "field_id": "location_identifiers",
                             "operator_id": "includes", "values": [city_name]},
                            {"type": "predicate", "field_id": "announced_on",
                             "operator_id": "gte",
                             "values": [(datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")]},
                        ],
                        "limit": 30,
                    },
                    timeout=20,
                )
                resp.raise_for_status()
                entities = resp.json().get("entities", [])
                for entity in entities:
                    props = entity.get("properties", {})
                    org = props.get("funded_organization_identifier", {})
                    signals.append({
                        "company_name": org.get("value", "Unknown"),
                        "city": city_code,
                        "signal_type": "FUNDING",
                        "signal_detail": f"{props.get('investment_type', 'Funding')} — {props.get('money_raised', {}).get('value_usd', 'Undisclosed')}",
                        "urgency_hours": 48,
                        "persona": 1,
                        "confidence_score": 85,
                        "raw_source": "crunchbase",
                        "detected_at": datetime.now(IST).isoformat(),
                    })
                logger.info("Crunchbase %s: %d signals", city_code, len(entities))
            except Exception as e:
                logger.warning("Crunchbase %s: %s", city_code, e)

        return signals


# ═══════════════════════════════════════════════════════════════════════
# TIER 3 — Hiring signal (Naukri via Apify + LinkedIn via Proxycurl)
# ═══════════════════════════════════════════════════════════════════════

class HiringSignalCollector:
    """Detect companies posting 8+ jobs in a city within 30 days.

    Sources (in order of priority):
    1. Naukri.com via Apify actor — primary Indian hiring data
    2. LinkedIn Jobs India via Proxycurl API

    Signal: 8+ postings in one city = active expansion = workspace need
    """

    MIN_POSTINGS = 8

    def collect(self, cities: list[str]) -> list[dict]:
        signals: list[dict] = []

        for city_code in cities:
            naukri_signals = self._collect_naukri(city_code)
            signals.extend(naukri_signals)

            if not naukri_signals:
                linkedin_signals = self._collect_linkedin_jobs(city_code)
                signals.extend(linkedin_signals)

        return signals

    def _collect_naukri(self, city_code: str) -> list[dict]:
        if not APIFY_TOKEN:
            logger.debug("APIFY_TOKEN not set — skipping Naukri")
            return []

        city_info = CITIES.get(city_code, {})
        naukri_name = city_info.get("naukri_name", city_info.get("name", city_code))

        try:
            # Start Apify Naukri scraper actor run
            run_resp = requests.post(
                "https://api.apify.com/v2/acts/scrapingworld~naukri-jobs-scraper/runs",
                headers={"Authorization": f"Bearer {APIFY_TOKEN}"},
                json={
                    "location": naukri_name,
                    "maxItems": 500,
                    "datePosted": "last30days",
                },
                timeout=30,
            )
            run_resp.raise_for_status()
            run_id = run_resp.json().get("data", {}).get("id")

            if not run_id:
                return []

            # Wait for run to complete (poll with timeout)
            import time
            for _ in range(60):  # max 5 minutes
                status_resp = requests.get(
                    f"https://api.apify.com/v2/actor-runs/{run_id}",
                    headers={"Authorization": f"Bearer {APIFY_TOKEN}"},
                    timeout=10,
                )
                status = status_resp.json().get("data", {}).get("status")
                if status == "SUCCEEDED":
                    break
                if status in ("FAILED", "ABORTED", "TIMED-OUT"):
                    logger.warning("Naukri scraper %s: %s", city_code, status)
                    return []
                time.sleep(5)

            # Get results
            dataset_id = status_resp.json().get("data", {}).get("defaultDatasetId")
            items_resp = requests.get(
                f"https://api.apify.com/v2/datasets/{dataset_id}/items",
                headers={"Authorization": f"Bearer {APIFY_TOKEN}"},
                params={"format": "json"},
                timeout=30,
            )
            items = items_resp.json()

            # Aggregate by company
            company_jobs: dict[str, list] = {}
            for item in items:
                company = item.get("company", "Unknown")
                company_jobs.setdefault(company, []).append(item)

            # Filter: 8+ postings
            signals: list[dict] = []
            for company, jobs in company_jobs.items():
                if len(jobs) >= self.MIN_POSTINGS:
                    signals.append({
                        "company_name": company,
                        "city": city_code,
                        "signal_type": "HIRING_SURGE",
                        "signal_detail": f"{len(jobs)} jobs posted in {naukri_name} (30 days)",
                        "urgency_hours": TRIGGER_SIGNALS["HIRING_SURGE"]["urgency_hours"],
                        "persona": 2,
                        "confidence_score": 80,
                        "employee_count": None,
                        "job_count": len(jobs),
                        "sample_titles": [j.get("title", "") for j in jobs[:5]],
                        "raw_source": "naukri_apify",
                        "detected_at": datetime.now(IST).isoformat(),
                    })

            logger.info("Naukri %s: %d companies with %d+ jobs", city_code, len(signals), self.MIN_POSTINGS)
            return signals

        except Exception as e:
            logger.warning("Naukri %s: %s", city_code, e)
            return []

    def _collect_linkedin_jobs(self, city_code: str) -> list[dict]:
        """Fallback: LinkedIn Jobs via Proxycurl."""
        if not PROXYCURL_API_KEY:
            return []

        city_name = CITIES.get(city_code, {}).get("name", city_code)
        try:
            resp = requests.get(
                "https://nubela.co/proxycurl/api/v2/linkedin/company/job",
                params={
                    "geo_id": city_name,
                    "job_type": "full-time",
                    "when": "past-month",
                },
                headers={"Authorization": f"Bearer {PROXYCURL_API_KEY}"},
                timeout=15,
            )
            resp.raise_for_status()
            jobs = resp.json().get("job", [])

            # Aggregate by company
            company_jobs: dict[str, int] = {}
            for job in jobs:
                company = job.get("company", "Unknown")
                company_jobs[company] = company_jobs.get(company, 0) + 1

            return [
                {
                    "company_name": company,
                    "city": city_code,
                    "signal_type": "HIRING_SURGE",
                    "signal_detail": f"{count} LinkedIn jobs in {city_name}",
                    "urgency_hours": 168,
                    "persona": 2,
                    "confidence_score": 70,
                    "job_count": count,
                    "raw_source": "linkedin_proxycurl",
                    "detected_at": datetime.now(IST).isoformat(),
                }
                for company, count in company_jobs.items()
                if count >= self.MIN_POSTINGS
            ]
        except Exception as e:
            logger.warning("LinkedIn Jobs %s: %s", city_code, e)
            return []


# ═══════════════════════════════════════════════════════════════════════
# TIER 4 — News NLP signal (Indian startup news)
# ═══════════════════════════════════════════════════════════════════════

class IndiaNewsSignalCollector:
    """Scan Indian startup news for workspace-intent signals.

    Primary sources (all via NewsAPI):
    - entrackr.com — funding + expansion news
    - inc42.com — startup news
    - yourstory.com — startup news
    - economictimes.com/tech — enterprise expansion

    NLP triggers (Claude Haiku classifies):
    - "expanding to [city]" → expansion signal
    - "raised [amount]" → funding signal (dedup with Tracxn)
    - "hiring [X] people" → hiring signal
    - "new office in" → workspace-ready signal
    - "return to office" → WFH reversal signal
    """

    # Keywords that indicate workspace need
    WORKSPACE_KEYWORDS = [
        "expanding to", "new office", "opening office", "hired",
        "hiring", "raised", "funding", "return to office",
        "back to office", "hybrid work", "coworking", "office space",
        "new city", "expanding operations",
    ]

    def collect(self, cities: list[str], days_back: int = 7) -> list[dict]:
        if not NEWS_API_KEY:
            logger.debug("NEWS_API_KEY not set — skipping news signals")
            return []

        signals: list[dict] = []
        from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

        for city_code in cities:
            city_info = CITIES.get(city_code, {})
            keywords = city_info.get("news_keywords", [city_info.get("name", city_code)])

            for kw in keywords[:1]:  # one query per city to save credits
                try:
                    resp = requests.get(
                        "https://newsapi.org/v2/everything",
                        params={
                            "q": f"startup office {kw}",
                            "domains": ",".join([
                                "entrackr.com", "inc42.com", "yourstory.com",
                                "economictimes.com", "livemint.com",
                            ]),
                            "language": "en",
                            "sortBy": "publishedAt",
                            "from": from_date,
                            "pageSize": 20,
                            "apiKey": NEWS_API_KEY,
                        },
                        timeout=15,
                    )
                    resp.raise_for_status()
                    articles = resp.json().get("articles", [])

                    for article in articles:
                        signal = self._classify_article(article, city_code)
                        if signal:
                            signals.append(signal)

                    logger.info("NewsAPI %s: %d articles, %d relevant", city_code, len(articles),
                                sum(1 for a in articles if self._is_relevant(a)))
                except Exception as e:
                    logger.warning("NewsAPI %s: %s", city_code, e)

        return signals

    def _classify_article(self, article: dict, city_code: str) -> dict | None:
        title = article.get("title", "")
        description = article.get("description", "")
        text = f"{title} {description}".lower()

        if not self._is_relevant(article):
            return None

        # Classify signal type from text
        signal_type = "CITY_EXPANSION_PR"
        urgency = 168
        persona = 3

        if any(kw in text for kw in ["raised", "funding", "seed", "series"]):
            signal_type = "FUNDING"
            urgency = 48
            persona = 1
        elif any(kw in text for kw in ["hiring", "hired", "new jobs", "recruiting"]):
            signal_type = "HIRING_SURGE"
            urgency = 168
            persona = 2
        elif any(kw in text for kw in ["return to office", "back to office", "wfh reversal", "hybrid"]):
            signal_type = "WFH_REVERSAL"
            urgency = 336
            persona = 2
        elif any(kw in text for kw in ["expanding", "new office", "opening office", "new city"]):
            signal_type = "CITY_EXPANSION_PR"
            urgency = 168
            persona = 3

        # Extract company name (best effort from title)
        company_name = title.split(" raise")[0].split(" Raise")[0].split(" hire")[0].split(" open")[0].strip()
        if len(company_name) > 80:
            company_name = company_name[:80]

        return {
            "company_name": company_name,
            "city": city_code,
            "signal_type": signal_type,
            "signal_detail": title[:200],
            "urgency_hours": urgency,
            "persona": persona,
            "confidence_score": 60,
            "article_url": article.get("url", ""),
            "article_source": article.get("source", {}).get("name", ""),
            "raw_source": "newsapi",
            "detected_at": datetime.now(IST).isoformat(),
        }

    def _is_relevant(self, article: dict) -> bool:
        text = f"{article.get('title', '')} {article.get('description', '')}".lower()
        return any(kw in text for kw in self.WORKSPACE_KEYWORDS)


# ═══════════════════════════════════════════════════════════════════════
# TIER 5 — Commercial property signal (unique edge)
# ═══════════════════════════════════════════════════════════════════════

class PropertySignalCollector:
    """Detect companies subletting office space or commercial property moves.

    Sources:
    - 99acres.com commercial listings via Apify
    - JLL India / CBRE India press releases

    Signal: Company listing office for sublease = consolidating into flex workspace.
    """

    def collect(self, cities: list[str]) -> list[dict]:
        if not APIFY_TOKEN:
            logger.debug("APIFY_TOKEN not set — skipping property signals")
            return []

        # TODO: Implement 99acres scraping via Apify actor
        # This is a Tier 5 signal — build after Tiers 1-4 are validated
        return []


# ═══════════════════════════════════════════════════════════════════════
# Master signal collector
# ═══════════════════════════════════════════════════════════════════════

class SignalCollectorV2:
    """Orchestrates all signal collectors."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.mca = MCASignalCollector()
        self.tracxn = TracxnFundingCollector()
        self.crunchbase = CrunchbaseFundingCollector()
        self.hiring = HiringSignalCollector()
        self.news = IndiaNewsSignalCollector()
        self.property = PropertySignalCollector()

    def collect_all(self, cities: list[str], verbose: bool = False) -> dict[str, list[dict]]:
        """Run all collectors and return signals grouped by type."""
        results = {
            "mca": [],
            "funding": [],
            "hiring": [],
            "news": [],
            "property": [],
        }

        if self.dry_run:
            results["funding"] = self.tracxn._synthetic(cities)
            results["hiring"] = self._synthetic_hiring(cities)
            logger.info("[DRY RUN] Generated %d synthetic signals",
                        sum(len(v) for v in results.values()))
            return results

        # Tier 1: MCA (structured government data)
        results["mca"] = self.mca.collect(cities)

        # Tier 2: Tracxn + Crunchbase funding
        tracxn_signals = self.tracxn.collect(cities)
        cb_signals = self.crunchbase.collect(cities)
        # Dedup: prefer Tracxn, add unique Crunchbase signals
        tracxn_companies = {s["company_name"].lower() for s in tracxn_signals}
        unique_cb = [s for s in cb_signals if s["company_name"].lower() not in tracxn_companies]
        results["funding"] = tracxn_signals + unique_cb

        # Tier 3: Hiring
        results["hiring"] = self.hiring.collect(cities)

        # Tier 4: News NLP
        results["news"] = self.news.collect(cities)

        # Tier 5: Property
        results["property"] = self.property.collect(cities)

        return results

    def _synthetic_hiring(self, cities: list[str]) -> list[dict]:
        """Synthetic hiring signals for dry run."""
        signals: list[dict] = []
        companies = [
            ("TechServe Solutions", "IT Services", 120),
            ("GrowthPay", "FinTech", 65),
            ("CloudNine Health", "HealthTech", 80),
            ("EduBridge", "EdTech", 55),
        ]
        now = datetime.now(IST)
        for city in cities:
            for name, sector, emp in random.sample(companies, k=min(2, len(companies))):
                job_count = random.randint(10, 30)
                signals.append({
                    "company_name": f"{name} ({city})",
                    "city": city,
                    "signal_type": "HIRING_SURGE",
                    "signal_detail": f"{job_count} jobs posted in {CITIES[city]['name']} (synthetic)",
                    "urgency_hours": 168,
                    "persona": 2,
                    "confidence_score": 80,
                    "employee_count": emp,
                    "job_count": job_count,
                    "sector": sector,
                    "raw_source": "naukri_synthetic",
                    "detected_at": (now - timedelta(hours=random.randint(12, 72))).isoformat(),
                })
        return signals


# ── Module entry points ──────────────────────────────────────────────


def collect_all_signals(
    cities: list[str] | None = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> dict[str, list[dict]]:
    """Entry point — returns signals grouped by type."""
    if cities is None:
        cities = list(CITIES.keys())
    collector = SignalCollectorV2(dry_run=dry_run)
    return collector.collect_all(cities, verbose=verbose)


def collect_all_signals_flat(
    cities: list[str] | None = None,
    dry_run: bool = False,
) -> list[dict]:
    """Entry point — returns flat list of all signals."""
    grouped = collect_all_signals(cities=cities, dry_run=dry_run)
    return [sig for signals in grouped.values() for sig in signals]
