"""WhatsApp Business API Template Registry for myHQ.

CRITICAL: All templates must be pre-approved by Meta before sending.
Submit via Gupshup dashboard on Day 1. Approval: 24-72 hours.

Template categories:
  UTILITY     — transactional (higher approval rate)
  MARKETING   — promotional (needs opt-in list)

All 5 PKM-calibrated templates submitted as UTILITY.
Run: python3 pipeline/wa_templates.py → prints submission guide.
"""

from __future__ import annotations

TEMPLATES: dict[str, dict] = {
    "MOTIVE_INFERENCE": {
        "name": "myhq_data_first_v1",
        "category": "UTILITY",
        "language": "en_IN",
        "body": (
            "{{1}} — {{2}} founders used myHQ to get desk-ready in 48 hours "
            "after their round. {{3}} seats in {{4}}, no 11-month lease. "
            "Worth 10 minutes? {{5}}"
        ),
        "variables": ["Priya", "23", "15", "Koramangala", "https://cal.myhq.in/book"],
        "defense_mode": "MOTIVE_INFERENCE",
        "bypass": "PURE_DATA — open with number, no 'I', no pitch language",
        "max_words": 80,
        "banned": ["I'm excited", "love to connect", "great opportunity", "just wanted to"],
        "gupshup_template_id": None,
        "meta_approval_status": "pending",
    },
    "OVERLOAD_AVOIDANCE": {
        "name": "myhq_ultra_short_v1",
        "category": "UTILITY",
        "language": "en_IN",
        "body": (
            "{{1}} — {{2}} hiring in {{3}}. myHQ has {{4}} desks ready this week. "
            "48h setup, no lock-in. Worth 10 min? {{5}}"
        ),
        "variables": ["TechCo", "12", "Bengaluru", "20", "https://cal.myhq.in/book"],
        "defense_mode": "OVERLOAD_AVOIDANCE",
        "bypass": "ULTRA_SHORT — under 60 words, one ask, one slot",
        "max_words": 60,
        "banned": ["hope this finds you", "quick call", "at your convenience", "just following up"],
        "gupshup_template_id": None,
        "meta_approval_status": "pending",
    },
    "IDENTITY_THREAT": {
        "name": "myhq_amplify_v1",
        "category": "UTILITY",
        "language": "en_IN",
        "body": (
            "{{1}} — you built {{2}} to {{3}} people without locking into "
            "11-month leases. myHQ keeps that flexibility — {{4}} seats in "
            "{{5}} when you need them. {{6}}"
        ),
        "variables": ["Rahul", "StartupX", "40", "15", "Koramangala", "https://cal.myhq.in/book"],
        "defense_mode": "IDENTITY_THREAT",
        "bypass": "AMPLIFICATION — they built this, we support it",
        "max_words": 70,
        "banned": ["let us help you", "you need", "solve your problem", "struggling with"],
        "gupshup_template_id": None,
        "meta_approval_status": "pending",
    },
    "SOCIAL_PROOF_SKEPTICISM": {
        "name": "myhq_proof_v1",
        "category": "UTILITY",
        "language": "en_IN",
        "body": (
            "{{1}} — myHQ {{2}} numbers: {{3}} occupancy, {{4}} avg Google "
            "rating, GST invoice in 24h, 99.9% uptime SLA. {{5}} seats "
            "available. Full terms: myhq.in/enterprise"
        ),
        "variables": ["Ananya", "Bengaluru", "94%", "4.3/5", "20"],
        "defense_mode": "SOCIAL_PROOF_SKEPTICISM",
        "bypass": "CREDIBILITY_FIRST — exact numbers, verifiable, no vague claims",
        "max_words": 90,
        "banned": ["trusted by", "leading platform", "best in class", "industry-leading"],
        "gupshup_template_id": None,
        "meta_approval_status": "pending",
    },
    "TIMING_SKEPTICISM": {
        "name": "myhq_trigger_v1",
        "category": "UTILITY",
        "language": "en_IN",
        "body": (
            "{{1}} — {{2}} posted {{3}} jobs in {{4}} this month. That team "
            "needs desks before Q{{5}} headcount locks. myHQ has {{6}} seats, "
            "48h ready. {{7}}"
        ),
        "variables": ["Vikram", "TechCo", "14", "Mumbai", "2", "25", "https://cal.myhq.in/book"],
        "defense_mode": "TIMING_SKEPTICISM",
        "bypass": "TRIGGER_EVENT — why now is different, specific deadline",
        "max_words": 75,
        "banned": ["whenever you're ready", "no rush", "at your pace", "just checking in"],
        "gupshup_template_id": None,
        "meta_approval_status": "pending",
    },
}


def get_template_for_defense(defense_mode: str) -> dict:
    """Get the pre-approved template for a defense mode."""
    return TEMPLATES.get(defense_mode, TEMPLATES["OVERLOAD_AVOIDANCE"])


def generate_gupshup_submission_guide() -> str:
    """Print the Day 1 template submission guide."""
    lines = [
        "=" * 60,
        "WHATSAPP TEMPLATE SUBMISSION GUIDE — DO THIS ON DAY 1",
        "=" * 60,
        "",
        "Step 1: Log in to Gupshup dashboard (app.gupshup.io)",
        "Step 2: Go to Templates > Create New Template",
        "Step 3: Submit each template below. 24-72h for approval.",
        "",
        "IMPORTANT: Do not change template text after submission.",
        "Meta rejects edited templates. Submit exactly as shown.",
        "",
    ]

    for mode, t in TEMPLATES.items():
        lines.extend([
            f"--- {mode} ---",
            f"Name: {t['name']}",
            f"Category: {t['category']}",
            f"Language: {t['language']}",
            f"Body:",
            f"  {t['body']}",
            f"Example variables: {t['variables']}",
            f"Max words: {t['max_words']}",
            "",
        ])

    lines.extend([
        "After approval:",
        "  1. Copy each Template ID from Gupshup",
        "  2. Paste into wa_templates.py > gupshup_template_id",
        "  3. Change meta_approval_status to 'approved'",
        "  4. The agent is now ready to send.",
        "",
        "=" * 60,
    ])

    return "\n".join(lines)


if __name__ == "__main__":
    print(generate_gupshup_submission_guide())
