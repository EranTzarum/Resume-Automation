"""
Orchestrator: hunting loop (Google → Comeet → CV match → LinkedIn targeting → connection)
and follow-up loop (accepted → Hebrew message + CV PDF → Message Sent).

Login once:  python main.py --save-login
Run hunting: python main.py --phase hunt
Run follow-up: python main.py --phase followup
Run both:     python main.py --phase both
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

import config
from hunter import run_hunting_pipeline
from networker import LinkedInNetworker, run_followup_loop
from notion_db import NotionDB, canonical_job_link

# ---------------------------------------------------------------------------
# First-run seed data (connections already sent manually before automation)
# ---------------------------------------------------------------------------
_SEED_DATA = [
    {
        "company": "Surecomp",
        "job_title": "Junior Full Stack Engineer",
        "job_url": "https://www.comeet.com/jobs/Surecomp/24.00E/junior-full-stuck-engineer/37.363/",
        "employees": ["Yair Keter", "Tali H.", "Moshi Cohen"],
    },
    {
        "company": "Bond Sports",
        "job_title": "Junior Full Stack Engineer",
        "job_url": "https://www.comeet.com/jobs/bondsports/F7.009/junior-full-stack-engineer/F9.B58/",
        "employees": ["Ori Naveh", "Ido Naveh", "Noam Ben Zeev"],
    },
    {
        "company": "inManage",
        "job_title": "Junior Backend Developer (PHP)",
        "job_url": "https://www.comeet.com/jobs/inmanage/B7.006/junior-backend-developer-php/A8.A20/",
        "employees": [
            "Guy Honen", "Elhai Mansbach", "Raphael Aboohi",
            "Amit Furman", "Naor Cohav", "Roy Margalit", "Dima Varo",
        ],
    },
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")


async def save_login() -> None:
    # force_visible=True so the browser window is always visible for manual login
    async with LinkedInNetworker(force_visible=True) as net:
        await net.wait_for_manual_login()
        print("A browser window opened. Sign in to LinkedIn, then come back here and press Enter.")
        await asyncio.get_event_loop().run_in_executor(None, lambda: input("Press Enter after you are logged in: "))
        await net.save_state()
        print(f"Session saved to {config.LINKEDIN_STATE_PATH} — you're good to go.")


async def run_hunt() -> None:
    notion = NotionDB()
    logger.info(
        "Hunting: %s search→CV pair(s); pool≤%s URLs / %s page(s); ≤%s new URLs/query after Notion filter",
        len(config.SEARCH_QUERY_CV_MAP),
        config.GOOGLE_POOL_MAX,
        config.GOOGLE_SEARCH_PAGES_MAX,
        config.NEW_URLS_PER_QUERY_CAP,
    )
    await run_hunting_pipeline(notion)


async def run_follow() -> None:
    notion = NotionDB()
    await run_followup_loop(notion)


async def run_seed() -> None:
    """Seed Notion with first-run employees (status = Connection Sent).
    LinkedIn URLs are left empty — fill them manually in Notion so Phase 0 can follow up.
    """
    notion = NotionDB()
    existing_links = await notion.fetch_all_job_links()
    total = 0
    for job in _SEED_DATA:
        for employee in job["employees"]:
            pid = await notion.create_row(
                company=job["company"],
                job_title=job["job_title"],
                job_link=job["job_url"],
                status=config.STATUS_CONNECTION_SENT,
                employee_name=employee,
            )
            logger.info("Seeded: %s @ %s (page_id=%s)", employee, job["company"], pid)
            total += 1
    print(f"\nSeeded {total} employee rows with status 'Connection Sent'.")
    print("ACTION REQUIRED: Open Notion and add the LinkedIn URL for each person.")
    print("Without the LinkedIn URL, Phase 0 cannot check if the connection was accepted.")


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Eran Referral Engine")
    p.add_argument("--save-login", action="store_true", help="Capture LinkedIn session to state.json")
    p.add_argument(
        "--phase",
        choices=("hunt", "followup", "both", "seed"),
        default="both",
        help="Which automation phase to run (seed = one-time first-run data import)",
    )
    args = p.parse_args(argv or sys.argv[1:])

    if args.save_login:
        asyncio.run(save_login())
        return

    if args.phase == "seed":
        asyncio.run(run_seed())
    elif args.phase == "both":
        asyncio.run(run_follow())
        asyncio.run(run_hunt())
    elif args.phase == "hunt":
        asyncio.run(run_hunt())
    else:
        asyncio.run(run_follow())


if __name__ == "__main__":
    main()
