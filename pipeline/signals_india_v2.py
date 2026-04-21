"""myHQ GTM Engine v2 — India-first signal detection.

Signal priority (highest to lowest confidence):
  TIER 1 — Structured government data (MCA, GST) — hardest proof
  TIER 2 — Verified funding data (Tracxn, Crunchbase) — 48h urgency
  TIER 3 — Hiring signals (Naukri + LinkedIn via Netrows)
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
import re
from datetime import datetime, timedelta, timezone

import requests

from config.settings_v2 import (
    APIFY_TOKEN,
    CITIES,
    CRUNCHBASE_API_KEY,
    DATAGOV_API_KEY,
    DATAGOV_RESOURCE_ID,
    NEWS_API_KEY,
    NETROWS_API_KEY,
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
    expansion signal available.

    Uses data.gov.in OGD API (free, requires registration at https://data.gov.in).
    Set DATAGOV_API_KEY and DATAGOV_RESOURCE_ID in .env.
    """

    BASE_URL = "https://api.data.gov.in/resource"

    def collect(self, cities: list[str], days_back: int = 90) -> list[dict]:
        """Fetch recently incorporated companies.

        days_back defaults to 90 because data.gov.in updates monthly —
        the dataset may lag by several weeks.
        """
        if not DATAGOV_API_KEY:
            logger.warning("DATAGOV_API_KEY not set — skipping MCA signals. "
                           "Register free at https://data.gov.in to get a key.")
            return []
        if not DATAGOV_RESOURCE_ID:
            logger.warning("DATAGOV_RESOURCE_ID not set — skipping MCA signals. "
                           "Find the Company Master Data resource ID at data.gov.in.")
            return []

        signals: list[dict] = []
        cutoff = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

        # Collect unique states (MUM and PUN both map to Maharashtra)
        seen_states: set[str] = set()
        state_to_cities: dict[str, list[str]] = {}
        for city_code in cities:
            state = CITIES.get(city_code, {}).get("mca_state", "")
            if not state or state in seen_states:
                if state and state in state_to_cities:
                    state_to_cities[state].append(city_code)
                continue
            seen_states.add(state)
            state_to_cities[state] = [city_code]

        for state, city_codes in state_to_cities.items():
            try:
                # data.gov.in uses lowercase state names and PascalCase fields
                resp = requests.get(
                    f"{self.BASE_URL}/{DATAGOV_RESOURCE_ID}",
                    params={
                        "api-key": DATAGOV_API_KEY,
                        "format": "json",
                        "limit": 500,
                        "filters[CompanyStateCode]": state.lower(),
                        "filters[CompanyStatus]": "Active",
                        "filters[CompanyClass]": "Private",
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
                records = data.get("records", [])

                count = 0
                for co in records:
                    # Filter: only recent registrations (date filtering
                    # must be client-side — API only does exact match)
                    reg_date = co.get("CompanyRegistrationdate_date") or ""
                    if reg_date and reg_date < cutoff:
                        continue

                    # Assign to the first matching city for this state
                    for cc in city_codes:
                        signals.append(self._to_signal(co, cc))
                        count += 1
                        break

                logger.info("MCA %s: %d new incorporations (data.gov.in)", state, count)
            except Exception as e:
                logger.warning("MCA %s failed: %s", state, e)

        return signals

    def _to_signal(self, co: dict, city_code: str) -> dict:
        company_name = co.get("CompanyName") or "Unknown"
        cin = co.get("CIN") or ""
        return {
            "company_name": company_name,
            "cin": cin,
            "city": city_code,
            "signal_type": "MCA_NEW_SUBSIDIARY",
            "signal_detail": f"New incorporation: {company_name} (CIN: {cin or 'N/A'})",
            "urgency_hours": TRIGGER_SIGNALS["MCA_NEW_SUBSIDIARY"]["urgency_hours"],
            "persona": TRIGGER_SIGNALS["MCA_NEW_SUBSIDIARY"]["persona"],
            "confidence_score": 90,
            "employee_count": None,
            "sector": co.get("CompanyIndustrialClassification") or "",
            "founder_name": "",
            "founder_linkedin": None,
            "website": None,
            "raw_source": "mca_datagov",
            "detected_at": datetime.now(IST).isoformat(),
        }


# ═══════════════════════════════════════════════════════════════════════
# TIER 2 — Tracxn funding signal
# ═══════════════════════════════════════════════════════════════════════

class TracxnFundingCollector:
    """Fetch recent Indian startup funding rounds from Tracxn API.

    Endpoint: POST https://platform.tracxn.com/api/2.2/companies
    Auth: header `accessToken: <TRACXN_API_KEY>`

    The API rejects requests with more than 2 filters or with `companyStage`/`sort`
    blocks (returns 400), so we filter only by `latestFundingRoundDate` (DD/MM/YYYY)
    and `location.country = ["India"]`. Stage and city filtering happens client-side.
    """

    BASE_URL = "https://platform.tracxn.com/api/2.2/companies"

    ACCEPTED_STAGES = {
        "Seed", "Early-Stage Funded", "Series A", "Series B",
        "Angel", "Funding Raised",
    }

    # Tracxn reports raw city names; normalize to our internal city codes.
    # Delhi-NCR collapses several satellite cities into one code.
    CITY_NAME_TO_CODE = {
        "bengaluru": "BLR", "bangalore": "BLR",
        "mumbai": "MUM",
        "delhi": "DEL", "new delhi": "DEL",
        "gurugram": "DEL", "gurgaon": "DEL",
        "noida": "DEL", "faridabad": "DEL",
        "hyderabad": "HYD",
        "pune": "PUN",
        "chennai": "CHN",
    }

    # Cap on companies fetched per run (single page, no pagination).
    # Bump this when scaling up — for now we keep API spend bounded.
    MAX_COMPANIES = 30

    def collect(self, cities: list[str], days_back: int = 14) -> list[dict]:
        if not TRACXN_API_KEY:
            logger.info("TRACXN_API_KEY not set — skipping funding signals")
            return []

        target_codes = {c.upper() for c in cities}
        today = datetime.now(IST).date()
        date_min = (today - timedelta(days=days_back)).strftime("%d/%m/%Y")
        date_max = today.strftime("%d/%m/%Y")

        body = {
            "filter": {
                "latestFundingRoundDate": {"min": date_min, "max": date_max},
                "location": {"country": ["India"]},
            },
            "from": 0,
            "size": self.MAX_COMPANIES,
        }

        try:
            resp = requests.post(
                self.BASE_URL,
                headers={
                    "accessToken": TRACXN_API_KEY,
                    "Content-Type": "application/json",
                },
                json=body,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning("Tracxn fetch failed: %s", e)
            return []

        results = (data.get("result") or data.get("results") or [])[: self.MAX_COMPANIES]

        signals: list[dict] = []
        for co in results:
            # Stage filter (client-side)
            stage = (co.get("stage") or "").strip()
            if stage and stage not in self.ACCEPTED_STAGES:
                continue

            # Noise filter: drop grants with no disclosed amount.
            # Accelerator grants (e.g. "Saksham") flood the feed with
            # tiny non-actionable signals — skip if amount missing and
            # the round name contains "grant".
            latest = (co.get("fundingInfo") or {}).get("latestRoundInfo") or {}
            round_name = (latest.get("name") or "")
            amount_val = ((latest.get("amount") or {}).get("amount"))
            if amount_val is None and "grant" in round_name.lower():
                continue

            # City filter (client-side)
            city_code = self._resolve_city_code(co)
            if not city_code or city_code not in target_codes:
                continue

            signals.append(self._to_signal(co, city_code))

        logger.info(
            "Tracxn: %d signals from %d companies (window=%dd, cap=%d, cities=%d)",
            len(signals), len(results), days_back, self.MAX_COMPANIES, len(target_codes),
        )
        return signals

    def _resolve_city_code(self, co: dict) -> str | None:
        # Try result.location.city first, then result.locations[0].city.name
        loc = co.get("location") or {}
        city = (loc.get("city") or "").strip()
        if not city:
            locs = co.get("locations") or []
            if locs:
                city = ((locs[0].get("city") or {}).get("name") or "").strip()
        return self.CITY_NAME_TO_CODE.get(city.lower()) if city else None

    def _to_signal(self, co: dict, city_code: str) -> dict:
        funding_info = co.get("fundingInfo") or {}
        latest = funding_info.get("latestRoundInfo") or {}

        # Funding date can be returned as either an ISO string or {day,month,year}
        funding_date = latest.get("date")
        detected_iso: str
        if isinstance(funding_date, dict):
            try:
                detected_iso = datetime(
                    int(funding_date.get("year")),
                    int(funding_date.get("month", 1)),
                    int(funding_date.get("day", 1)),
                    tzinfo=IST,
                ).isoformat()
            except (TypeError, ValueError):
                detected_iso = datetime.now(IST).isoformat()
        elif isinstance(funding_date, str) and funding_date:
            detected_iso = funding_date
        else:
            detected_iso = datetime.now(IST).isoformat()

        amount_usd = ((latest.get("amount") or {}).get("amount"))
        total_raised = (((co.get("totalMoneyRaised") or {}).get("totalAmount") or {}).get("amount"))

        # Location may be in either result.location or result.locations[0].city
        loc = co.get("location") or {}
        loc_state = loc.get("state") or ""
        loc_country = loc.get("country") or "India"
        if not loc_state:
            locs = co.get("locations") or []
            if locs:
                loc_state = ((locs[0].get("city") or {}).get("state") or "")

        # Founder: first key person from employeeInfo.employeeList
        employees = ((co.get("employeeInfo") or {}).get("employeeList") or [])
        key_person = next(
            (e for e in employees if e.get("isKeyPeople")),
            employees[0] if employees else {},
        )
        founder_name = key_person.get("name") or ""
        founder_title = key_person.get("designation") or ""
        founder_linkedin = ((key_person.get("profileLinks") or {}).get("linkedinHandle"))
        founder_email = ((key_person.get("emailInfo") or {}).get("primaryEmail"))

        # Company contact — Tracxn returns countryCode as "+91"/"91"/""
        # and number as either "+918056..." or "8056...". Strip any leading
        # "+" from both parts before concatenating so we never end up with
        # double "+" when countryCode is empty but number already has one.
        contact_list = co.get("contactNumberList") or []
        if contact_list:
            cc = str(contact_list[0].get("countryCode") or "").lstrip("+").strip()
            num = str(contact_list[0].get("number") or "").lstrip("+").strip()
            if cc and num:
                company_phone = f"+{cc}{num}"
            elif num:
                company_phone = f"+{num}"
            else:
                company_phone = ""
        else:
            company_phone = ""
        email_list = co.get("emailList") or []
        company_email = email_list[0].get("email") if email_list else ""

        news_list = ((co.get("newsInfo") or {}).get("newsList") or [])
        news_headline = news_list[0].get("headLine") if news_list else ""
        news_url = news_list[0].get("sourceUrl") if news_list else ""

        round_name = latest.get("name") or "Funding"
        # Format for India context: ₹ Cr/Lakh (assume USD→INR at 83)
        if isinstance(amount_usd, (int, float)) and amount_usd > 0:
            inr = amount_usd * 83
            if inr >= 1e7:
                amount_str = f"₹{inr / 1e7:.1f} Cr"
            else:
                amount_str = f"₹{inr / 1e5:.0f} L"
            signal_detail = f"raised {amount_str} ({round_name} round)"
        else:
            signal_detail = f"closed a {round_name} round"

        return {
            "company_name": co.get("name", "Unknown"),
            "domain": co.get("domain", ""),
            "website": co.get("domain", ""),  # v1 compat
            "city": city_code,
            "state": loc_state,
            "country": loc_country,
            "stage": co.get("stage") or "",
            "signal_type": "FUNDING",
            "signal_detail": signal_detail,
            "urgency_hours": TRIGGER_SIGNALS["FUNDING"]["urgency_hours"],
            "persona": TRIGGER_SIGNALS["FUNDING"]["persona"],
            "confidence_score": 0.9,
            "round_type": round_name,
            "amount_usd": float(amount_usd) if isinstance(amount_usd, (int, float)) else None,
            "amount_raised": float(amount_usd) if isinstance(amount_usd, (int, float)) else None,
            "funding_date": funding_date,
            "detected_at": detected_iso,
            "investor_names": [
                inv.get("name") for inv in (latest.get("investorList") or []) if inv.get("name")
            ],
            "total_raised_usd": float(total_raised) if isinstance(total_raised, (int, float)) else None,
            "description": ((co.get("description") or {}).get("short") or ""),
            "tracxn_url": co.get("tracxnUrl", ""),
            "tracxn_id": co.get("tracxnId", ""),
            "company_linkedin": ((co.get("profileLinks") or {}).get("linkedIn")),
            "news_headline": news_headline,
            "news_url": news_url,
            "founded_year": co.get("foundedYear"),
            "founder_name": founder_name,
            "founder_title": founder_title,
            "founder_linkedin": founder_linkedin,
            "founder_email": founder_email,
            "company_phone": company_phone,
            "company_email": company_email,
            "employee_count": None,
            "sector": "",
            "raw_source": "tracxn",
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
# TIER 3 — Hiring signal (Naukri via Apify + LinkedIn via Netrows)
# ═══════════════════════════════════════════════════════════════════════

class HiringSignalCollector:
    """Detect companies posting 8+ jobs in a city within 30 days.

    Sources (in order of priority):
    1. Naukri.com via Apify actor — primary Indian hiring data
    2. LinkedIn Jobs India via Netrows API (replaces Proxycurl, shut down by LinkedIn lawsuit)

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
        """Fallback: LinkedIn Jobs via Netrows API.

        Replaces Proxycurl (shut down by LinkedIn lawsuit Jan 2025).
        Netrows: 48+ LinkedIn endpoints, €0.005/req, real-time.
        """
        if not NETROWS_API_KEY:
            return []

        city_name = CITIES.get(city_code, {}).get("name", city_code)
        try:
            resp = requests.get(
                "https://api.netrows.com/api/linkedin/jobs/search",
                params={
                    "location": city_name,
                    "country": "India",
                    "job_type": "full-time",
                    "posted_within": "past-month",
                    "limit": 500,
                },
                headers={
                    "x-api-key": NETROWS_API_KEY,
                    "Accept": "application/json",
                },
                timeout=20,
            )
            resp.raise_for_status()
            jobs = resp.json().get("data", [])

            # Aggregate by company
            company_jobs: dict[str, int] = {}
            for job in jobs:
                company = job.get("company_name") or job.get("company", "Unknown")
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
                    "raw_source": "linkedin_netrows",
                    "detected_at": datetime.now(IST).isoformat(),
                }
                for company, count in company_jobs.items()
                if count >= self.MIN_POSTINGS
            ]
        except Exception as e:
            logger.warning("LinkedIn Jobs (Netrows) %s: %s", city_code, e)
            return []


# ═══════════════════════════════════════════════════════════════════════
# TIER 4 — News NLP signal (Indian startup news)
# ═══════════════════════════════════════════════════════════════════════

class IndiaNewsSignalCollector:
    """Scan Indian startup news for workspace-intent signals.

    Extracts real company names from news articles using regex patterns
    common in Indian startup journalism (inc42, entrackr, yourstory, ET).

    Only keeps articles that name a specific company doing something
    actionable (raising funding, hiring, expanding, opening offices).
    """

    # Regex patterns to extract company names from Indian startup headlines.
    # Order matters — most specific patterns first.
    # "in talks to raise" is extremely common in Indian business journalism.
    COMPANY_PATTERNS = [
        # "CompanyName in talks to raise $X" / "CompanyName is set to raise ₹X"
        re.compile(r"^(?P<company>.+?)\s+(?:in\s+talks?\s+to|is\s+set\s+to|is\s+looking\s+to|is\s+in\s+talks?\s+to)\s+(?:raise|secure|bag|close|get)", re.IGNORECASE),
        # "[Funding alert] CompanyName raises..." (inc42 style)
        re.compile(r"^\[.*?\]\s*(?P<company>.+?)\s+(?:raises?|secures?|gets?)", re.IGNORECASE),
        # "CompanyName raises ₹X Cr" / "CompanyName secures $X M funding"
        re.compile(r"^(?P<company>.+?)\s+(?:raises?|secures?|gets?|bags?|closes?|lands?)\s+[\$₹€\d]", re.IGNORECASE),
        # "CompanyName raises Series A" / "CompanyName raises seed round"
        re.compile(r"^(?P<company>.+?)\s+(?:raises?|secures?|closes?)\s+(?:seed|series|pre-seed|pre-series|bridge|funding)", re.IGNORECASE),
        # "CompanyName to hire X people" / "CompanyName hiring X engineers"
        re.compile(r"^(?P<company>.+?)\s+(?:to\s+)?(?:hires?|hiring|plans?\s+to\s+hire|looking\s+to\s+hire)\s+\d", re.IGNORECASE),
        # "CompanyName opens new office in City"
        re.compile(r"^(?P<company>.+?)\s+(?:opens?|launches?|inaugurates?)\s+(?:new\s+)?(?:office|hub|centre|center|campus)", re.IGNORECASE),
        # "CompanyName expands to City" / "CompanyName to expand operations"
        re.compile(r"^(?P<company>.+?)\s+(?:expands?|expanding)\s+(?:to|in|into|operations)", re.IGNORECASE),
        # "CompanyName plans City expansion"
        re.compile(r"^(?P<company>.+?)\s+(?:plans?|announces?|unveils?)\s+.{0,20}(?:expansion|office|hiring|recruitment)", re.IGNORECASE),
        # "CompanyName to set up / invest in / open office"
        re.compile(r"^(?P<company>.+?)\s+to\s+(?:set up|open|launch|establish|invest)\s+", re.IGNORECASE),
    ]

    # Reject articles matching these — not about a specific company signal
    NOISE_PATTERNS = re.compile(
        r"box\s+office|election|cricket|bollywood|movie|film\s+review|"
        r"sensex|nifty|stock\s+market|mutual\s+fund|gold\s+price|"
        r"horoscope|weather|ipl\s+|world\s+cup|"
        r"top\s+\d+\s+|best\s+\d+\s+|list\s+of\s+|"
        r"government\s+scheme|budget\s+202|union\s+budget|"
        r"india\s+vs\s+|modi\s+|parliament|"
        r"deals?\s+digest|newsletter|roundup|wrap\b|weekly\s+wrap",
        re.IGNORECASE,
    )

    def collect(self, cities: list[str], days_back: int = 7) -> list[dict]:
        if not NEWS_API_KEY:
            logger.debug("NEWS_API_KEY not set — skipping news signals")
            return []

        signals: list[dict] = []
        seen_companies: set[str] = set()  # dedup across cities
        from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

        for city_code in cities:
            city_info = CITIES.get(city_code, {})
            keywords = city_info.get("news_keywords", [city_info.get("name", city_code)])

            for kw in keywords[:1]:  # one query per city to save credits
                try:
                    # Targeted query for startup/business news only
                    query = f'"{kw}" AND (raises OR funding OR hiring OR "new office" OR expansion OR coworking)'
                    resp = requests.get(
                        "https://newsapi.org/v2/everything",
                        params={
                            "q": query,
                            "language": "en",
                            "sortBy": "publishedAt",
                            "from": from_date,
                            "pageSize": 50,
                            "apiKey": NEWS_API_KEY,
                        },
                        timeout=15,
                    )
                    resp.raise_for_status()
                    articles = resp.json().get("articles", [])

                    relevant = 0
                    for article in articles:
                        signal = self._classify_article(article, city_code, seen_companies)
                        if signal:
                            signals.append(signal)
                            seen_companies.add(signal["company_name"].lower())
                            relevant += 1

                    logger.info("NewsAPI %s: %d articles, %d with extractable company",
                                city_code, len(articles), relevant)
                except Exception as e:
                    logger.warning("NewsAPI %s: %s", city_code, e)

        return signals

    def _classify_article(self, article: dict, city_code: str,
                          seen: set[str]) -> dict | None:
        title = (article.get("title") or "").strip()
        description = (article.get("description") or "").strip()
        text = f"{title} {description}".lower()

        # Reject noise (Bollywood, elections, cricket, listicles, etc.)
        if self.NOISE_PATTERNS.search(text):
            return None

        # Try to extract a real company name via regex patterns
        company_name = self._extract_company(title)
        if not company_name:
            return None  # Can't identify a specific company — skip

        # Dedup: same company already seen from another city's query
        if company_name.lower() in seen:
            return None

        # Classify signal type
        signal_type = "CITY_EXPANSION_PR"
        urgency = 168
        persona = 3

        if any(kw in text for kw in ["raised", "raises", "funding", "seed", "series", "secures"]):
            signal_type = "FUNDING"
            urgency = 48
            persona = 1
        elif any(kw in text for kw in ["hiring", "hired", "hires", "new jobs", "recruiting", "to hire"]):
            signal_type = "HIRING_SURGE"
            urgency = 168
            persona = 2
        elif any(kw in text for kw in ["return to office", "back to office", "wfh reversal", "hybrid work"]):
            signal_type = "WFH_REVERSAL"
            urgency = 336
            persona = 2

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

    # Attribution prefixes to strip: "Ola alumni's X" → "X"
    _ATTRIBUTION_RE = re.compile(
        r"^(?:.*?(?:alumni|alumnus|ex-?\w+|former\s+\w+)['\u2019]?s?\s+)",
        re.IGNORECASE,
    )
    # Common non-company prefixes
    _PREFIX_RE = re.compile(
        r"^(?:exclusive|breaking|watch|update|report|alert|"
        r"startup|indian\s+startup|india['\u2019]?s)\s*[:\-–—]\s*",
        re.IGNORECASE,
    )

    def _extract_company(self, title: str) -> str | None:
        """Extract company name from a news headline using regex patterns."""
        for pattern in self.COMPANY_PATTERNS:
            m = pattern.match(title)
            if m:
                name = m.group("company").strip()

                # Strip "Exclusive: ", "Breaking — " etc.
                name = self._PREFIX_RE.sub("", name).strip()

                # Strip attribution: "Ola alumni's Manav Robotics" → "Manav Robotics"
                cleaned = self._ATTRIBUTION_RE.sub("", name).strip()
                if cleaned and len(cleaned) >= 2:
                    name = cleaned

                # Strip trailing possessive/punctuation
                name = re.sub(r"['\u2019]s\s*$", "", name).strip()
                name = name.rstrip(":-–—,;.")

                # Reject if still too long (likely a sentence, not a name)
                if len(name) > 50:
                    return None
                # Reject if too short or generic
                if len(name) < 2:
                    return None
                if name.lower() in ("the", "a", "an", "this", "india", "startup", "company"):
                    return None
                return name
        return None


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
        # Sort Tracxn by confidence desc so highest-quality leads survive any cap
        tracxn_signals.sort(key=lambda s: s.get("confidence_score") or 0, reverse=True)
        cb_signals = self.crunchbase.collect(cities)
        # Dedup: prefer Tracxn, add unique Crunchbase signals
        tracxn_companies = {s["company_name"].lower() for s in tracxn_signals}
        unique_cb = [s for s in cb_signals if s["company_name"].lower() not in tracxn_companies]
        results["funding"] = tracxn_signals + unique_cb

        # Tier 3: Hiring
        results["hiring"] = self.hiring.collect(cities)

        # Tier 4: News NLP — disabled for now. Headline-regex company extraction
        # produces too many junk entries ("Curefoods bets on premium..."), and
        # Hunter/Apollo domain matching on loose names mis-routes enrichment
        # (e.g. "Plazza" → Swiss plazza.ch). Re-enable after tightening extraction.
        results["news"] = []

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
