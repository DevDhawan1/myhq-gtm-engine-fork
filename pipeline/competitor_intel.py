"""myHQ GTM Engine v2 — Competitor intelligence scraping.

Competitors tracked:
  Awfis          — awfis.com (listed company, public data)
  WeWork India   — wework.com/en-IN
  IndiQube       — indiqube.com
  Smartworks     — smartworks.in
  91Springboard  — 91springboard.com

Weekly scrape:
  1. Pricing pages (detect price changes, new packages)
  2. Blog content (topics they're targeting, gaps myHQ can own)
  3. Google Reviews (customer pain points → myHQ content opportunities)
  4. Job postings (where they're expanding)

Why:
  - Awfis raises prices in BLR → myHQ advantage window
  - WeWork opens in HYD → 30-day head start to capture demand
  - 91SB reviews "slow WiFi" → myHQ content: "our 99.9% SLA"
  - IndiQube blog targets "GCC workspace" → myHQ needs content there too

All insights → Airtable Competitor_Intel table → LLM content pipeline.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

import requests

from config.settings_v2 import AIRTABLE_API_KEY, AIRTABLE_BASE_ID, ANTHROPIC_API_KEY, OPENROUTER_API_KEY, OPENROUTER_MODEL

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))

COMPETITORS: dict[str, dict] = {
    "awfis": {
        "name": "Awfis",
        "domain": "awfis.com",
        "pricing_url": "https://www.awfis.com/space-solutions/",
        "blog_url": "https://www.awfis.com/blog/",
        "linkedin_slug": "awfis-space-solutions",
    },
    "wework_india": {
        "name": "WeWork India",
        "domain": "wework.com/en-IN",
        "pricing_url": "https://www.wework.com/en-IN/workspace/all-access",
        "blog_url": "https://www.wework.com/en-IN/ideas/",
        "linkedin_slug": "wework",
    },
    "indiqube": {
        "name": "IndiQube",
        "domain": "indiqube.com",
        "pricing_url": "https://www.indiqube.com/flexible-workspace/",
        "blog_url": "https://www.indiqube.com/blog/",
        "linkedin_slug": "indiqube",
    },
    "smartworks": {
        "name": "Smartworks",
        "domain": "smartworks.in",
        "pricing_url": "https://smartworks.in/workspace/",
        "blog_url": "https://smartworks.in/blog/",
        "linkedin_slug": "smartworks-coworking",
    },
    "91springboard": {
        "name": "91springboard",
        "domain": "91springboard.com",
        "pricing_url": "https://www.91springboard.com/coworking-space/",
        "blog_url": "https://www.91springboard.com/blog/",
        "linkedin_slug": "91springboard",
    },
}


class CompetitorScanner:
    """Weekly competitor intelligence scanner."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.apify_token = os.getenv("APIFY_TOKEN", "")
        self.client = None
        # OpenRouter (gpt-oss-120b) — swap back to Anthropic by uncommenting below
        if OPENROUTER_API_KEY:
            try:
                from openai import OpenAI
                self.client = OpenAI(
                    base_url="https://openrouter.ai/api/v1",
                    api_key=OPENROUTER_API_KEY,
                )
            except Exception:
                pass
        # REVERT TO ANTHROPIC: uncomment below, comment out OpenRouter block above
        # if ANTHROPIC_API_KEY:
        #     try:
        #         from anthropic import Anthropic
        #         self.client = Anthropic()
        #     except Exception:
        #         pass

    def run_full_scan(self) -> dict:
        """Run weekly competitor scan across all competitors."""
        logger.info("Starting weekly competitor scan")
        results: dict = {}

        for comp_key, comp_info in COMPETITORS.items():
            logger.info("Scanning %s", comp_info["name"])

            pricing = self.scrape_pricing(comp_key)
            blog_gaps = self.scrape_blog_gaps(comp_key)
            pain_points = self.scrape_reviews(comp_key, "Bengaluru")  # Top city first

            results[comp_key] = {
                "pricing": pricing,
                "content_gaps": len(blog_gaps),
                "pain_points": len(pain_points),
            }

        logger.info("Competitor scan complete: %d competitors", len(results))
        return results

    def scrape_pricing(self, comp_key: str) -> dict:
        """Scrape pricing page → extract with Claude Haiku → detect changes."""
        comp = COMPETITORS.get(comp_key)
        if not comp or not self.apify_token or not self.client:
            return {"competitor": comp_key, "status": "skipped"}

        if self.dry_run:
            return self._synthetic_pricing(comp_key)

        try:
            # Apify Website Content Crawler
            run_resp = requests.post(
                "https://api.apify.com/v2/acts/apify~website-content-crawler/run-sync-get-dataset-items",
                headers={"Authorization": f"Bearer {self.apify_token}"},
                json={
                    "startUrls": [{"url": comp["pricing_url"]}],
                    "maxCrawlPages": 1,
                    "maxCrawlDepth": 0,
                },
                params={"token": self.apify_token},
                timeout=90,
            )

            content = run_resp.json()
            if not content:
                return {"competitor": comp_key, "status": "no_content"}

            text = content[0].get("text", "")[:3000]

            # OpenRouter extraction — was Claude haiku
            resp = self.client.chat.completions.create(
                model=OPENROUTER_MODEL,
                max_tokens=400,
                messages=[
                    {"role": "system", "content": "Extract pricing intel from coworking website. Return JSON only."},
                    {"role": "user", "content": (
                        f"Extract from {comp['name']} pricing page:\n{text}\n\n"
                        "Return JSON: {\"hot_desk_min\": number|null, "
                        "\"dedicated_desk_min\": number|null, "
                        "\"private_cabin_min\": number|null, "
                        "\"promotions\": [\"list\"], \"currency\": \"INR\"}"
                    )},
                ],
            )

            pricing = json.loads(resp.choices[0].message.content)
            pricing["competitor"] = comp_key
            pricing["scraped_at"] = datetime.now(IST).isoformat()

            _store_intel(comp_key, "pricing", pricing)
            return pricing

        except Exception as e:
            logger.warning("Pricing scrape %s: %s", comp_key, e)
            return {"competitor": comp_key, "status": "error"}

    def scrape_blog_gaps(self, comp_key: str) -> list[dict]:
        """Scrape competitor blog → find content gaps myHQ can own."""
        comp = COMPETITORS.get(comp_key)
        if not comp or not self.apify_token or not self.client:
            return []

        if self.dry_run:
            return self._synthetic_blog_gaps(comp_key)

        try:
            run_resp = requests.post(
                "https://api.apify.com/v2/acts/apify~website-content-crawler/run-sync-get-dataset-items",
                headers={"Authorization": f"Bearer {self.apify_token}"},
                json={
                    "startUrls": [{"url": comp["blog_url"]}],
                    "maxCrawlPages": 10,
                    "maxCrawlDepth": 1,
                },
                params={"token": self.apify_token},
                timeout=120,
            )

            pages = run_resp.json()
            titles = [p.get("title", "") for p in (pages or [])[:20] if p.get("title")]

            if not titles:
                return []

            resp = self.client.chat.completions.create(
                model=OPENROUTER_MODEL,
                max_tokens=500,
                messages=[
                    {"role": "system", "content": "Analyze competitor blog. Find content gaps. Return JSON only."},
                    {"role": "user", "content": (
                        f"{comp['name']} blog topics:\n" + "\n".join(titles) +
                        "\n\nReturn JSON: {\"gaps\": [\"topic\"], "
                        "\"myhq_content_priorities\": ["
                        "{\"title\": \"str\", \"why\": \"str\", \"keywords\": [\"str\"]}]}"
                    )},
                ],
            )

            result = json.loads(resp.choices[0].message.content)
            priorities = result.get("myhq_content_priorities", [])

            _store_intel(comp_key, "content_gaps", result)
            return priorities

        except Exception as e:
            logger.warning("Blog scrape %s: %s", comp_key, e)
            return []

    def scrape_reviews(self, comp_key: str, city: str) -> list[dict]:
        """Scrape Google Reviews → extract pain points → myHQ opportunities."""
        comp = COMPETITORS.get(comp_key)
        serpapi_key = os.getenv("SERPAPI_KEY", "")
        if not comp or not serpapi_key or not self.client:
            return []

        if self.dry_run:
            return self._synthetic_reviews(comp_key, city)

        try:
            resp = requests.get(
                "https://serpapi.com/search",
                params={
                    "engine": "google",
                    "q": f'{comp["name"]} coworking {city} reviews',
                    "gl": "in",
                    "hl": "en",
                    "api_key": serpapi_key,
                },
                timeout=15,
            )

            snippets = [
                r.get("snippet", "")
                for r in resp.json().get("organic_results", [])[:5]
                if any(w in r.get("snippet", "").lower()
                       for w in ["review", "rating", "experience", "good", "bad", "issue"])
            ]

            if not snippets:
                return []

            ai_resp = self.client.chat.completions.create(
                model=OPENROUTER_MODEL,
                max_tokens=400,
                messages=[
                    {"role": "system", "content": "Extract customer pain points from reviews. Return JSON only."},
                    {"role": "user", "content": (
                        f"Reviews about {comp['name']} in {city}:\n"
                        + "\n".join(snippets)
                        + "\n\nReturn JSON: {\"pain_points\": ["
                        "{\"complaint\": \"str\", \"myhq_advantage\": \"str\", "
                        "\"content_idea\": \"str\"}]}"
                    )},
                ],
            )

            result = json.loads(ai_resp.choices[0].message.content)
            pain_points = result.get("pain_points", [])

            for pp in pain_points:
                pp["competitor"] = comp_key
                pp["city"] = city

            _store_intel(comp_key, "reviews", {"city": city, "pain_points": pain_points})
            return pain_points

        except Exception as e:
            logger.warning("Reviews scrape %s %s: %s", comp_key, city, e)
            return []

    # ── Synthetic data for dry runs ───────────────────────────────────

    def _synthetic_pricing(self, comp_key: str) -> dict:
        import random
        return {
            "competitor": comp_key,
            "hot_desk_min": random.randint(5000, 12000),
            "dedicated_desk_min": random.randint(8000, 18000),
            "private_cabin_min": random.randint(15000, 35000),
            "promotions": ["First month free", "10% annual discount"],
            "currency": "INR",
            "scraped_at": datetime.now(IST).isoformat(),
            "synthetic": True,
        }

    def _synthetic_blog_gaps(self, comp_key: str) -> list[dict]:
        return [
            {"title": f"GCC workspace guide (missing from {comp_key})", "why": "No content on GCC", "keywords": ["gcc office india"]},
            {"title": f"Series A office checklist (gap from {comp_key})", "why": "No funded startup content", "keywords": ["startup office india"]},
        ]

    def _synthetic_reviews(self, comp_key: str, city: str) -> list[dict]:
        return [
            {"complaint": "WiFi drops during video calls", "myhq_advantage": "99.9% uptime SLA", "content_idea": "Coworking WiFi SLA guide"},
            {"complaint": "Hidden charges on invoice", "myhq_advantage": "Zero hidden charges, GST from day 1", "content_idea": "Transparent pricing comparison"},
        ]


def _store_intel(competitor: str, intel_type: str, data: dict):
    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        return
    try:
        requests.post(
            f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/Competitor_Intel",
            headers={
                "Authorization": f"Bearer {AIRTABLE_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "fields": {
                    "competitor": competitor,
                    "intel_type": intel_type,
                    "data_json": json.dumps(data, default=str)[:5000],
                    "scraped_at": datetime.now(IST).isoformat(),
                }
            },
            timeout=8,
        )
    except Exception as e:
        logger.debug("Airtable competitor intel store failed: %s", e)


# ── Module entry point ────────────────────────────────────────────────


def run_weekly_competitor_scan(dry_run: bool = False) -> dict:
    scanner = CompetitorScanner(dry_run=dry_run)
    return scanner.run_full_scan()
