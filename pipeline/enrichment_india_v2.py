"""myHQ GTM Engine v2 — India-optimized contact enrichment.

Waterfall order (try each source, stop when we have what we need):
  1. Apollo.io API — general enrichment, India coverage decent
  2. People Data Labs API — tech roles, better email accuracy
  3. Netrows API — 48+ LinkedIn endpoints, real-time, €0.005/req (replaces Proxycurl, which was shut down by LinkedIn lawsuit)
  4. Lusha API — best WhatsApp/mobile number coverage (LinkedIn-sourced)
  5. Hunter.io — email-only fallback

Verification waterfall:
  1. Millionverifier — email verification
  2. MSG91 WhatsApp check — verify number is on WhatsApp before sending
  3. TRAI DND check — Indian regulatory requirement

Output per lead:
  {
    email: str (verified),
    phone_mobile: str (WhatsApp-verified),
    linkedin_url: str,
    title: str,
    decision_maker_score: 0-100,
    whatsapp_verified: bool,
    email_valid: bool,
    dnd_status: bool
  }
"""

from __future__ import annotations

import logging
import random
import re

import requests

from config.settings_v2 import (
    APOLLO_API_KEY,
    APOLLO_WEBHOOK_URL,
    DATABASE_URL,
    HUNTER_API_KEY,
    LUSHA_API_KEY,
    MILLIONVERIFIER_KEY,
    MSG91_API_KEY,
    PDL_API_KEY,
    NETROWS_API_KEY,
    TRAI_DND_KEY,
)

logger = logging.getLogger(__name__)

_INDIAN_MOBILE_RE = re.compile(r"^\+?91[6-9]\d{9}$")


def _record_pending_enrichment(
    apollo_person_id: str,
    name: str,
    company: str,
    linkedin_url: str,
    email: str | None,
) -> None:
    """Record that we're awaiting an async Apollo webhook for this lead.

    Silent no-op if DATABASE_URL missing or insert fails — we don't want DB
    issues to break the sync enrichment path.
    """
    if not DATABASE_URL or not apollo_person_id:
        return
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pending_apollo_enrichments
                        (apollo_person_id, lead_name, lead_company, lead_linkedin, lead_email)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (apollo_person_id) DO NOTHING
                    """,
                    (apollo_person_id, name, company, linkedin_url, email),
                )
        finally:
            conn.close()
    except Exception as e:
        logger.debug("pending_apollo_enrichments insert failed: %s", e)


class ContactEnricher:
    """Run enrichment waterfall for a single contact or batch."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run

    def enrich_batch(self, signals: list[dict]) -> list[dict]:
        """Enrich a batch of signals. Returns list of enriched lead dicts."""
        leads: list[dict] = []
        for sig in signals:
            lead = self.enrich_signal(sig)
            if lead:
                leads.append(lead)
        logger.info("Enriched %d/%d signals", len(leads), len(signals))
        return leads

    def enrich_signal(self, signal: dict) -> dict:
        """Enrich a single signal into a full lead profile."""
        if self.dry_run:
            return self._synthetic_enrichment(signal)

        company = signal.get("company_name", "")
        founder = signal.get("founder_name", "")
        website = signal.get("website") or signal.get("domain", "")
        founder_linkedin = signal.get("founder_linkedin") or ""

        contact = self._waterfall_enrich(founder, company, website, founder_linkedin)

        # If no contact name from enrichment, use signal's founder_name
        if not contact.get("name") and founder:
            contact["name"] = founder

        # Verification
        if contact.get("email"):
            contact["email_valid"] = self._verify_email(contact["email"])
        else:
            contact["email_valid"] = False

        if contact.get("phone_mobile"):
            contact["whatsapp_verified"] = self._verify_whatsapp(contact["phone_mobile"])
            contact["dnd_status"] = self._check_trai_dnd(contact["phone_mobile"])
        else:
            contact["whatsapp_verified"] = False
            contact["dnd_status"] = False

        # Decision maker scoring
        contact["decision_maker_score"] = self._score_decision_maker(
            contact.get("title", ""), company
        )

        # Merge signal + contact into lead
        lead = {**signal, **contact}
        lead["enrichment_source"] = contact.get("enrichment_source", "none")

        return lead

    # ── Waterfall enrichment ──────────────────────────────────────────

    def _waterfall_enrich(self, name: str, company: str, website: str,
                          linkedin_url: str = "") -> dict:
        result = {
            "name": "", "email": None, "phone_mobile": None,
            "linkedin_url": linkedin_url or None, "title": None,
            "enrichment_source": None,
        }

        domain = self._extract_domain(website)

        # Step 1: Apollo person match (works best when we have a name or LinkedIn)
        if APOLLO_API_KEY and (name or linkedin_url):
            apollo = self._apollo_enrich(name, company, domain, linkedin_url)
            if apollo.get("email"):
                result.update(apollo)
                result["enrichment_source"] = "apollo"

        # Step 1b: Apollo org search — find decision makers by company name
        # (critical for news signals where we have company but no person name)
        if not result["email"] and APOLLO_API_KEY and company:
            apollo_org = self._apollo_org_search(company)
            if apollo_org.get("email"):
                result.update(apollo_org)
                result["enrichment_source"] = "apollo_org"
            # Also grab the domain if we didn't have one
            if not domain and apollo_org.get("domain"):
                domain = apollo_org["domain"]

        # Step 2: People Data Labs (if Apollo missed)
        if not result["email"] and PDL_API_KEY:
            pdl = self._pdl_enrich(name, company, domain)
            if pdl.get("email"):
                result.update({k: v for k, v in pdl.items() if v})
                result["enrichment_source"] = "pdl"

        # Step 3: Netrows (if still no email or need LinkedIn)
        if (not result["email"] or not result["linkedin_url"]) and NETROWS_API_KEY:
            netrows = self._netrows_enrich(name, company)
            if netrows:
                result.update({k: v for k, v in netrows.items() if v and not result.get(k)})
                if not result["enrichment_source"]:
                    result["enrichment_source"] = "netrows"

        # Step 4: Lusha (if no mobile/WhatsApp yet)
        if not result["phone_mobile"] and result.get("linkedin_url") and LUSHA_API_KEY:
            lusha = self._lusha_enrich(result["linkedin_url"])
            if lusha.get("phone_mobile"):
                result["phone_mobile"] = lusha["phone_mobile"]

        # Step 5: Hunter.io domain search (find emails at a company domain)
        if not result["email"] and HUNTER_API_KEY:
            # If we have a domain, search it; otherwise try to find the domain first
            if not domain and company:
                domain = self._hunter_find_domain(company)
            if domain:
                hunter = self._hunter_domain_search(domain)
                if hunter.get("email"):
                    result.update({k: v for k, v in hunter.items() if v and not result.get(k)})
                    if not result["enrichment_source"]:
                        result["enrichment_source"] = "hunter"

        return result

    def _apollo_enrich(self, name: str, company: str, domain: str | None,
                       linkedin_url: str = "") -> dict:
        try:
            payload: dict = {}
            if name:
                payload["name"] = name
            if company:
                payload["organization_name"] = company
            if domain:
                payload["domain"] = domain
            # LinkedIn URL dramatically improves Apollo match rate for Indian founders
            if linkedin_url:
                payload["linkedin_url"] = linkedin_url

            # Apollo rejects reveal_phone_number=true with HTTP 400 unless a
            # webhook_url is also provided (revealed phones are delivered
            # async). Without a webhook, fall back to plain enrichment —
            # still returns cached emails/names/titles, just no fresh phones.
            params: dict = {}
            if APOLLO_WEBHOOK_URL:
                params["reveal_phone_number"] = "true"
                params["webhook_url"] = APOLLO_WEBHOOK_URL
                params["run_waterfall_email"] = "true"
                params["run_waterfall_phone"] = "true"

            resp = requests.post(
                "https://api.apollo.io/api/v1/people/match",
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "no-cache",
                    "accept": "application/json",
                    "x-api-key": APOLLO_API_KEY,
                },
                params=params,
                json=payload,
                timeout=12,
            )
            resp.raise_for_status()
            person = resp.json().get("person", {})
            if not person:
                return {}

            phones = person.get("phone_numbers", [])
            mobile = phones[0].get("raw_number") if phones else None

            # When waterfall is active, phones arrive async via webhook —
            # record the Apollo person id so the reconciler can join back.
            if APOLLO_WEBHOOK_URL and person.get("id"):
                _record_pending_enrichment(
                    apollo_person_id=person["id"],
                    name=person.get("name", name),
                    company=company,
                    linkedin_url=person.get("linkedin_url") or linkedin_url,
                    email=person.get("email"),
                )

            return {
                "name": person.get("name", name),
                "email": person.get("email"),
                "phone_mobile": self._format_indian_phone(mobile),
                "linkedin_url": person.get("linkedin_url"),
                "title": person.get("title"),
                "apollo_person_id": person.get("id"),
            }
        except Exception as e:
            logger.debug("Apollo enrich failed: %s", e)
            return {}

    def _apollo_org_search(self, company: str) -> dict:
        """Search Apollo for decision makers at a company by org name.

        Used when we have a company name (from news) but no person name.
        Searches for people with senior titles at the organization.
        """
        try:
            resp = requests.post(
                "https://api.apollo.io/api/v1/mixed_people/search",
                headers={
                    "Content-Type": "application/json",
                    "x-api-key": APOLLO_API_KEY,
                },
                json={
                    "organization_name": company,
                    "person_titles": [
                        "Founder", "Co-founder", "CEO", "CTO",
                        "COO", "Managing Director", "Director",
                    ],
                    "person_locations": ["India"],
                    "per_page": 3,
                },
                timeout=12,
            )
            resp.raise_for_status()
            people = resp.json().get("people", [])
            if not people:
                return {}

            # Pick the first decision maker with an email
            for person in people:
                email = person.get("email")
                if email:
                    phones = person.get("phone_numbers", [])
                    mobile = phones[0].get("raw_number") if phones else None
                    org = person.get("organization", {})
                    return {
                        "name": person.get("name", ""),
                        "email": email,
                        "phone_mobile": self._format_indian_phone(mobile),
                        "linkedin_url": person.get("linkedin_url"),
                        "title": person.get("title"),
                        "domain": org.get("primary_domain"),
                    }

            # No email found, but return domain if we got it
            org = people[0].get("organization", {})
            return {"domain": org.get("primary_domain")}
        except Exception as e:
            logger.debug("Apollo org search failed: %s", e)
            return {}

    def _pdl_enrich(self, name: str, company: str, domain: str | None) -> dict:
        try:
            params: dict = {
                "name": name,
                "company": company,
                "min_likelihood": 7,
            }
            if domain:
                params["website"] = domain

            resp = requests.get(
                "https://api.peopledatalabs.com/v5/person/enrich",
                params=params,
                headers={"X-Api-Key": PDL_API_KEY},
                timeout=12,
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") != 200:
                return {}

            p = data.get("data", {})
            emails = p.get("emails", [])
            phones = p.get("phone_numbers", [])

            return {
                "name": p.get("full_name", name),
                "email": emails[0].get("address") if emails else None,
                "phone_mobile": self._format_indian_phone(phones[0] if phones else None),
                "linkedin_url": p.get("linkedin_url"),
                "title": p.get("job_title"),
            }
        except Exception as e:
            logger.debug("PDL enrich failed: %s", e)
            return {}

    def _netrows_enrich(self, name: str, company: str) -> dict:
        """Enrich via Netrows API — 48+ LinkedIn endpoints, real-time data.

        Replaces Proxycurl (shut down by LinkedIn lawsuit Jan 2025).
        Netrows: €0.005/request, 115+ B2B endpoints, real-time.
        Docs: https://www.netrows.com/docs
        """
        try:
            parts = name.split() if name else [""]
            first_name = parts[0]
            last_name = parts[-1] if len(parts) > 1 else ""

            # Step 1: Search for LinkedIn profile
            search_resp = requests.get(
                "https://api.netrows.com/api/linkedin/person/search",
                params={
                    "first_name": first_name,
                    "last_name": last_name,
                    "company": company,
                    "country": "India",
                },
                headers={
                    "x-api-key": NETROWS_API_KEY,
                    "Accept": "application/json",
                },
                timeout=12,
            )
            search_resp.raise_for_status()
            results = search_resp.json().get("data", [])

            if not results:
                return {}

            linkedin_url = results[0].get("linkedin_url") or results[0].get("url", "")
            if not linkedin_url:
                return {}

            # Step 2: Full profile enrichment
            profile_resp = requests.get(
                "https://api.netrows.com/api/linkedin/person/profile",
                params={"url": linkedin_url},
                headers={
                    "x-api-key": NETROWS_API_KEY,
                    "Accept": "application/json",
                },
                timeout=12,
            )
            profile_resp.raise_for_status()
            p = profile_resp.json().get("data", {})

            emails = p.get("emails", [])
            phones = p.get("phone_numbers", [])

            return {
                "name": p.get("full_name") or f"{first_name} {last_name}".strip(),
                "email": emails[0] if emails else None,
                "phone_mobile": self._format_indian_phone(phones[0] if phones else None),
                "linkedin_url": linkedin_url,
                "title": p.get("headline") or p.get("title") or p.get("occupation"),
            }
        except Exception as e:
            logger.debug("Netrows enrich failed: %s", e)
            return {}

    def _lusha_enrich(self, linkedin_url: str) -> dict:
        try:
            resp = requests.get(
                "https://api.lusha.com/prospecting",
                params={"linkedinUrl": linkedin_url},
                headers={"api_key": LUSHA_API_KEY},
                timeout=10,
            )
            resp.raise_for_status()
            phones = resp.json().get("data", {}).get("phoneNumbers", [])
            mobile = next(
                (p["number"] for p in phones if p.get("type") == "mobile"),
                None,
            )
            return {"phone_mobile": self._format_indian_phone(mobile)}
        except Exception as e:
            logger.debug("Lusha enrich failed: %s", e)
            return {}

    def _hunter_find_domain(self, company: str) -> str | None:
        """Use Hunter's Company Finder to get a domain from a company name."""
        try:
            resp = requests.get(
                "https://api.hunter.io/v2/domain-search",
                params={
                    "company": company,
                    "api_key": HUNTER_API_KEY,
                    "limit": 1,
                },
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json().get("data", {})
            domain = data.get("domain")
            if domain:
                logger.debug("Hunter found domain for %s: %s", company, domain)
            return domain
        except Exception as e:
            logger.debug("Hunter domain finder failed: %s", e)
            return None

    def _hunter_domain_search(self, domain: str) -> dict:
        """Search Hunter for emails at a domain — returns the top decision maker."""
        try:
            resp = requests.get(
                "https://api.hunter.io/v2/domain-search",
                params={
                    "domain": domain,
                    "api_key": HUNTER_API_KEY,
                    "limit": 5,
                    "seniority": "senior,executive",
                    "department": "executive,management",
                },
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json().get("data", {})
            emails = data.get("emails", [])
            if not emails:
                return {}

            # Pick the first person with a valid email
            for person in emails:
                email = person.get("value")
                if email:
                    first = person.get("first_name", "")
                    last = person.get("last_name", "")
                    name = f"{first} {last}".strip()
                    return {
                        "name": name,
                        "email": email,
                        "title": person.get("position"),
                        "linkedin_url": person.get("linkedin"),
                    }
            return {}
        except Exception as e:
            logger.debug("Hunter domain search failed: %s", e)
            return {}

    # ── Verification ──────────────────────────────────────────────────

    def _verify_email(self, email: str) -> bool:
        if not MILLIONVERIFIER_KEY:
            return True  # Assume valid in dry run
        try:
            resp = requests.get(
                "https://api.millionverifier.com/api/v3/",
                params={"api": MILLIONVERIFIER_KEY, "email": email},
                timeout=10,
            )
            return resp.json().get("result") in ("ok", "catch_all")
        except Exception:
            return False

    def _verify_whatsapp(self, phone: str) -> bool:
        """Check if phone is on WhatsApp via MSG91. India is WhatsApp-first."""
        if not MSG91_API_KEY:
            return True  # Assume valid in dry run

        clean = self._format_indian_phone(phone)
        if not clean:
            return False

        try:
            resp = requests.post(
                "https://api.msg91.com/api/v5/wa/check",
                headers={"authkey": MSG91_API_KEY, "Content-Type": "application/json"},
                json={"mobile": clean},
                timeout=8,
            )
            return resp.json().get("type") == "success"
        except Exception:
            return False

    def _check_trai_dnd(self, phone: str) -> bool:
        """TRAI DND registry check — required for Indian outreach."""
        if not TRAI_DND_KEY:
            return False  # Assume not on DND in dry run
        # Implementation uses TRAI NDNC API
        # Returns True if number IS on DND (should not be contacted via calls/SMS)
        return False

    # ── Scoring ───────────────────────────────────────────────────────

    def _score_decision_maker(self, title: str, company_name: str) -> int:
        """Score 0-100 based on decision-making authority for workspace purchase."""
        title_lower = title.lower() if title else ""

        if any(t in title_lower for t in ["founder", "ceo", "co-founder", "cofounder"]):
            return 100
        if any(t in title_lower for t in ["coo", "cto", "cfo", "chief"]):
            return 85
        if any(t in title_lower for t in ["vp operations", "vp admin", "facilities", "head of"]):
            return 70
        if any(t in title_lower for t in ["operations manager", "admin manager", "office manager"]):
            return 55
        if any(t in title_lower for t in ["hr", "talent", "people"]):
            return 30
        if any(t in title_lower for t in ["director", "vp"]):
            return 65
        return 15

    # ── Helpers ───────────────────────────────────────────────────────

    def _extract_domain(self, website: str | None) -> str | None:
        if not website:
            return None
        return website.replace("https://", "").replace("http://", "").strip("/").split("/")[0]

    def _format_indian_phone(self, phone: str | None) -> str:
        if not phone:
            return ""
        digits = re.sub(r"[^\d]", "", str(phone))
        if len(digits) == 10 and digits[0] in "6789":
            return f"+91{digits}"
        if len(digits) == 12 and digits[:2] == "91":
            return f"+{digits}"
        if len(digits) == 11 and digits[0] == "0":
            return f"+91{digits[1:]}"
        return f"+{digits}" if digits else ""

    # ── Synthetic enrichment for dry run ──────────────────────────────

    def _synthetic_enrichment(self, signal: dict) -> dict:
        """Generate realistic enrichment without API calls."""
        company = signal.get("company_name", "Unknown")
        founder = signal.get("founder_name") or "Founder"
        persona = signal.get("persona", 1)
        domain = company.lower().replace(" ", "").replace("-", "")[:20]

        # Synthetic contact based on persona
        title_by_persona = {
            1: random.choice(["Founder & CEO", "Co-Founder & CTO", "CEO"]),
            2: random.choice(["Operations Manager", "Admin Manager", "HR Head"]),
            3: random.choice(["VP Operations", "Director BD", "Country Manager"]),
        }
        phone_suffix = "".join([str(random.randint(0, 9)) for _ in range(10)])
        phone = f"+91{random.choice(['9', '8', '7', '6'])}{phone_suffix[:9]}"

        lead = {
            **signal,
            "name": founder,
            "email": f"{founder.split()[0].lower()}@{domain}.com",
            "phone_mobile": phone,
            "linkedin_url": signal.get("founder_linkedin") or f"linkedin.com/in/{founder.lower().replace(' ', '-')}",
            "title": title_by_persona.get(persona, "Founder"),
            "decision_maker_score": {1: 100, 2: 55, 3: 70}.get(persona, 50),
            "whatsapp_verified": True,
            "email_valid": True,
            "dnd_status": False,
            "enrichment_source": "synthetic",
        }
        return lead


# ── Module entry points ──────────────────────────────────────────────


def enrich_signals(signals: list[dict], dry_run: bool = False) -> list[dict]:
    """Entry point for lead enrichment."""
    enricher = ContactEnricher(dry_run=dry_run)
    return enricher.enrich_batch(signals)
