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
from notion_db import NotionDB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")


async def save_login() -> None:
    async with LinkedInNetworker() as net:
        await net.wait_for_manual_login()
        print("Sign in in the browser, then press Enter here.")
        await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: input(""),
        )
        await net.save_state()
        print(f"Saved session to {config.LINKEDIN_STATE_PATH}")


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


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Eran Referral Engine")
    p.add_argument("--save-login", action="store_true", help="Capture LinkedIn session to state.json")
    p.add_argument(
        "--phase",
        choices=("hunt", "followup", "both"),
        default="both",
        help="Which automation phase to run",
    )
    args = p.parse_args(argv or sys.argv[1:])

    if args.save_login:
        asyncio.run(save_login())
        return

    if args.phase == "both":
        asyncio.run(run_hunt())
        asyncio.run(run_follow())
    elif args.phase == "hunt":
        asyncio.run(run_hunt())
    else:
        asyncio.run(run_follow())


if __name__ == "__main__":
    main()
