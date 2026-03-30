"""
LinkedIn automation with persisted browser state (state.json).
"""

from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urljoin, urlparse

from playwright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright

import config
from hunter import generate_referral_message_he

if TYPE_CHECKING:
    from notion_db import NotionDB

logger = logging.getLogger(__name__)


def _exclude_role(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in config.EXCLUDE_TITLE_KEYWORDS)


class LinkedInNetworker:
    def __init__(self) -> None:
        self._pw: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None

    async def __aenter__(self) -> LinkedInNetworker:
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def start(self) -> None:
        self._pw = await async_playwright().start()
        state_path = Path(config.LINKEDIN_STATE_PATH)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        launch = {"headless": config.PLAYWRIGHT_HEADLESS}
        self._browser = await self._pw.chromium.launch(**launch)
        if state_path.is_file():
            self._context = await self._browser.new_context(storage_state=str(state_path))
            logger.info("Loaded LinkedIn state from %s", state_path)
        else:
            self._context = await self._browser.new_context()
            logger.warning(
                "No %s — run `python main.py --save-login` once to capture LinkedIn session.",
                state_path,
            )
        self._page = await self._context.new_page()
        self._page.set_default_timeout(60_000)

    async def close(self) -> None:
        try:
            if self._context:
                await self._context.close()
            if self._browser:
                await self._browser.close()
            if self._pw:
                await self._pw.stop()
        finally:
            self._page = None
            self._context = None
            self._browser = None
            self._pw = None

    def _require_page(self) -> Page:
        if not self._page:
            raise RuntimeError("Networker not started")
        return self._page

    async def save_state(self) -> None:
        if self._context:
            path = Path(config.LINKEDIN_STATE_PATH)
            path.parent.mkdir(parents=True, exist_ok=True)
            await self._context.storage_state(path=str(path))
            logger.info("Saved LinkedIn state to %s", path)

    async def wait_for_manual_login(self) -> None:
        page = self._require_page()
        await page.goto("https://www.linkedin.com/login", wait_until="domcontentloaded")

    async def find_target_employee(self, company_linkedin_url: str) -> tuple[str, str] | None:
        """Company LinkedIn /people/ — first profile not matching exclude keywords."""
        page = self._require_page()
        base = company_linkedin_url.rstrip("/")
        people = f"{base}/people/"
        await page.goto(people, wait_until="domcontentloaded")
        await asyncio.sleep(config.ACTION_DELAY_SEC)
        try:
            await page.wait_for_selector("a[href*='/in/']", timeout=25_000)
        except Exception:
            logger.warning("No profile links on people page (login or layout).")
            return None

        links = await page.locator("a[href*='/in/']").all()
        seen: set[str] = set()
        for loc in links[:100]:
            try:
                href = await loc.get_attribute("href")
                if not href or "/in/" not in href:
                    continue
                full = urljoin(page.url, href)
                p = urlparse(full)
                parts = p.path.strip("/").split("/")
                if len(parts) < 2 or parts[0] != "in":
                    continue
                clean = f"{p.scheme}://{p.netloc}/in/{parts[1]}/"
                if clean in seen:
                    continue
                seen.add(clean)
                ctx = ""
                try:
                    ctx = await loc.evaluate(
                        "el => el.closest('li, article, section')?.innerText || el.innerText || ''"
                    )
                except Exception:
                    ctx = ""
                if _exclude_role(ctx):
                    continue
                name_guess = (
                    ctx.split("\n")[0].strip()[:120]
                    if ctx
                    else parts[1].replace("-", " ").title()
                )
                return name_guess, clean.rstrip("/")
            except Exception:
                continue
        return None

    async def send_connection(self, profile_url: str) -> bool:
        """Blank connection request (no note)."""
        page = self._require_page()
        url = profile_url if profile_url.endswith("/") else profile_url + "/"
        try:
            await page.goto(url, wait_until="domcontentloaded")
            await asyncio.sleep(config.ACTION_DELAY_SEC)
        except Exception:
            logger.exception("Open profile failed")
            return False

        try:
            main = page.locator("main")
            connect = main.get_by_role("button", name=re.compile(r"^\s*connect\s*$", re.I))
            if await connect.count() == 0:
                connect = page.get_by_role("button", name=re.compile(r"connect", re.I))
            await connect.first.click(timeout=15_000)
            await asyncio.sleep(config.ACTION_DELAY_SEC)
        except Exception:
            logger.exception("Connect button not found")
            return False

        try:
            send = page.get_by_role("button", name=re.compile(r"send without a note", re.I))
            if await send.count():
                await send.first.click(timeout=10_000)
            else:
                add_note = page.get_by_role("button", name=re.compile(r"add a note", re.I))
                if await add_note.count():
                    await add_note.click(timeout=8000)
                    no_thanks = page.get_by_role("button", name=re.compile(r"send", re.I))
                    await no_thanks.first.click(timeout=8000)
                else:
                    done = page.get_by_role("button", name=re.compile(r"send", re.I))
                    await done.first.click(timeout=10_000)
            await asyncio.sleep(config.ACTION_DELAY_SEC)
        except Exception:
            try:
                await page.get_by_role("button", name=re.compile(r"send", re.I)).first.click(
                    timeout=8000
                )
            except Exception:
                logger.exception("Could not complete invitation dialog")
                return False

        return True

    async def is_connection_accepted(self, profile_url: str) -> bool:
        page = self._require_page()
        try:
            await page.goto(profile_url, wait_until="domcontentloaded")
            await asyncio.sleep(config.ACTION_DELAY_SEC)
        except Exception:
            return False
        try:
            if await page.get_by_text(re.compile(r"pending", re.I)).count() > 0:
                return False
        except Exception:
            pass
        try:
            if await page.get_by_role("button", name=re.compile(r"message", re.I)).count() > 0:
                return True
        except Exception:
            pass
        return False

    async def send_followup(
        self,
        *,
        profile_url: str,
        employee_name: str,
        job_title: str,
        company_name: str,
        job_link: str,
        pdf_path: Path,
    ) -> bool:
        """Open message, attach CV PDF, paste Hebrew body from Claude."""
        page = self._require_page()
        body = await generate_referral_message_he(
            employee_name,
            job_title,
            company_name,
            job_link,
        )
        try:
            await page.goto(profile_url, wait_until="domcontentloaded")
            await asyncio.sleep(config.ACTION_DELAY_SEC)
        except Exception:
            return False

        try:
            await page.get_by_role("button", name=re.compile(r"message", re.I)).first.click(
                timeout=20_000
            )
            await asyncio.sleep(config.ACTION_DELAY_SEC)
        except Exception:
            logger.exception("Message button missing")
            return False

        try:
            inp = page.locator('input[type="file"]')
            await inp.first.set_input_files(str(pdf_path.resolve()))
            await asyncio.sleep(1.0)
        except Exception:
            logger.warning("PDF attach may have failed; sending text only.")

        try:
            editor = page.locator(
                "div.msg-form__contenteditable[contenteditable='true'], "
                "div[role='textbox'][contenteditable='true']"
            )
            await editor.first.click()
            await editor.first.fill(body)
            await asyncio.sleep(config.ACTION_DELAY_SEC)
        except Exception:
            try:
                await page.keyboard.type(body, delay=5)
            except Exception:
                logger.exception("Could not fill message body")
                return False

        try:
            await page.get_by_role("button", name=re.compile(r"^send$", re.I)).first.click(
                timeout=15_000
            )
            await asyncio.sleep(config.ACTION_DELAY_SEC)
        except Exception:
            logger.exception("Send message failed")
            return False

        return True


async def run_followup_loop(notion: NotionDB) -> None:
    rows = await notion.list_by_status(config.STATUS_CONNECTION_SENT)
    pdf = config.CV_PDF_PATH
    if not pdf.is_file():
        logger.error("CV PDF missing: %s", pdf)
        return

    async with LinkedInNetworker() as net:
        for row in rows:
            data = notion.parse_page(row)
            pid = data["page_id"] or ""
            emp_url = data["employee_linkedin"]
            if not emp_url:
                continue
            try:
                if not await net.is_connection_accepted(emp_url):
                    continue
            except Exception:
                logger.exception("Accept check failed")
                continue

            ok = await net.send_followup(
                profile_url=emp_url,
                employee_name=(data["employee_name"] or "שם")[:200],
                job_title=(data["job_title"] or "")[:500],
                company_name=(data["company"] or "")[:500],
                job_link=(data["job_link"] or "")[:2000],
                pdf_path=pdf,
            )
            if ok and pid:
                await notion.update_row(pid, status=config.STATUS_MESSAGE_SENT)
