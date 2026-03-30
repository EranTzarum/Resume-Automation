"""
Hunting: multi-page Google → Comeet URL pool, Notion dedupe, per-query CV routing,
job scrape, Claude match with the CV tied to the originating search query.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus, urljoin, urlparse

from anthropic import AsyncAnthropic
from playwright.async_api import Page, async_playwright
from pypdf import PdfReader

import config
from notion_db import NotionDB, canonical_job_link

logger = logging.getLogger(__name__)

COMEET_HOST = "comeet.com"
ANTHROPIC_MODEL = "claude-3-5-haiku-20241022"

_cv_text_cache: dict[Path, str] = {}
_anthropic_client = AsyncAnthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))


@dataclass
class ComeetJob:
    url: str
    company: str
    job_title: str
    description: str
    company_linkedin_url: str | None


def read_cv_pdf(path: Path) -> str:
    if not path.is_file():
        raise FileNotFoundError(f"CV PDF not found: {path}")
    reader = PdfReader(str(path))
    parts: list[str] = []
    for page in reader.pages:
        try:
            parts.append(page.extract_text() or "")
        except Exception:
            continue
    return "\n".join(parts).strip()


def get_cv_text_for_path(cv_path: Path) -> str:
    """Cached PDF text for the CV mapped to the search query that produced the job."""
    resolved = cv_path.expanduser().resolve()
    if resolved not in _cv_text_cache:
        _cv_text_cache[resolved] = read_cv_pdf(resolved)
    return _cv_text_cache[resolved]


async def cv_matches_job(cv_text: str, job_description: str) -> bool:
    system = (
        "You compare a candidate CV to a job posting. "
        "Reply with ONLY valid JSON: {\"match\": true} or {\"match\": false}. "
        "match is true only if the candidate is a plausible fit for the role (skills, level, domain)."
    )
    user = f"=== CV ===\n{cv_text[:50_000]}\n\n=== JOB ===\n{job_description[:50_000]}"
    if not os.environ.get("ANTHROPIC_API_KEY"):
        logger.error("ANTHROPIC_API_KEY is missing.")
        return False
    try:
        resp = await _anthropic_client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=300,
            messages=[{"role": "user", "content": user}],
            system=system,
        )
        raw = "".join(
            block.text for block in resp.content if getattr(block, "type", "") == "text"
        ).strip()
    except Exception:
        logger.exception("Anthropic evaluation request failed")
        return False
    m = re.search(r"\{[\s\S]*\}", raw)
    if not m:
        return False
    try:
        return bool(json.loads(m.group(0)).get("match"))
    except json.JSONDecodeError:
        return False


async def generate_referral_message_he(
    employee_name: str,
    job_title: str,
    company_or_dept: str,
    job_link: str,
) -> str:
    """
    Fill Hebrew template: infer gender from name; use correct imperative/forms (תראה/תראי, תוכל/י).
    Output must follow the structure of config.HE_REFERRAL_TEMPLATE.
    """
    system = (
        "You write Hebrew referral DMs for job networking. "
        "Infer likely gender from the employee's given name (Israeli/international). "
        "Use grammatically correct Hebrew: for feminine use תראי, תוכלי, שמחה, etc.; "
        "for masculine use תראה, תוכל, שמח, etc. Do not leave slashes or backslash placeholders. "
        "Return ONLY the final message text, no quotes or markdown."
    )
    user = (
        f"Template (follow content and tone; replace variables):\n{config.HE_REFERRAL_TEMPLATE}\n\n"
        f"Variables:\n"
        f"- Employee Name: {employee_name}\n"
        f"- Job Title: {job_title}\n"
        f"- Company / department: {company_or_dept}\n"
        f"- Job Link (paste full URL as in template): {job_link}\n"
    )
    if not os.environ.get("ANTHROPIC_API_KEY"):
        logger.error("ANTHROPIC_API_KEY is missing.")
        return ""
    try:
        resp = await _anthropic_client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=300,
            messages=[{"role": "user", "content": user}],
            system=system,
        )
        return "".join(
            block.text for block in resp.content if getattr(block, "type", "") == "text"
        ).strip()
    except Exception:
        logger.exception("Anthropic message generation failed")
        return ""


def _normalize_comeet_href(href: str, base: str) -> str | None:
    if not href or COMEET_HOST not in href.lower():
        return None
    if "/jobs/" not in href:
        return None
    full = urljoin(base, href)
    p = urlparse(full)
    if "comeet" not in (p.netloc or "").lower():
        return None
    path = (p.path or "").rstrip("/")
    return f"{p.scheme}://{p.netloc}{path}/"


async def _maybe_dismiss_google_consent(page: Page) -> None:
    for sel in ('button:has-text("Accept all")', 'button:has-text("I agree")', "#L2AGLb"):
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=2000):
                await btn.click()
                await asyncio.sleep(1)
                break
        except Exception:
            continue


async def google_search_comeet_url_pool(
    page: Page,
    query: str,
    *,
    pool_max: int,
    pages_max: int,
) -> list[str]:
    """
    Walk Google result pages (start=0,10,20,…) and collect Comeet job URLs in order
    until ``pool_max`` or pages exhausted / no new links on a page.
    """
    q = quote_plus(query)
    seen_canonical: set[str] = set()
    ordered: list[str] = []
    consent_done = False

    for page_idx in range(pages_max):
        start = page_idx * 10
        search_url = f"https://www.google.com/search?q={q}&num=10&start={start}"
        await page.goto(search_url, wait_until="domcontentloaded")
        await asyncio.sleep(config.ACTION_DELAY_SEC)

        if not consent_done:
            await _maybe_dismiss_google_consent(page)
            consent_done = True

        # Ensure the results DOM is present and Comeet links are discoverable.
        # If Google returns a CAPTCHA page, this will time out and we still extract what we can.
        try:
            await page.wait_for_selector(
                'a[href*="comeet.com/jobs"]',
                timeout=12_000,
            )
        except Exception:
            await asyncio.sleep(1.5)

        # Ultra-robust extraction: do not depend on Google-specific CSS classes.
        # Only collect anchors that already contain the Comeet jobs path.
        hrefs = await page.locator('a[href*="comeet.com/jobs"]').evaluate_all(
            "els => els.map(e => e.href)"
        )
        added_this_page = 0
        for h in hrefs:
            nu = _normalize_comeet_href(h, page.url)
            if not nu:
                continue
            key = canonical_job_link(nu)
            if not key or key in seen_canonical:
                continue
            seen_canonical.add(key)
            ordered.append(nu.rstrip("/") + "/")
            added_this_page += 1
            if len(ordered) >= pool_max:
                return ordered[:pool_max]

        if added_this_page == 0:
            logger.info("Google page %s: no new Comeet links; stopping pagination.", page_idx + 1)
            break

    return ordered[:pool_max]


def select_new_urls_for_query(
    pool: list[str],
    existing: set[str],
    cap: int,
) -> list[str]:
    """Keep order; skip URLs already in Notion (canonical); at most ``cap`` items."""
    out: list[str] = []
    for raw in pool:
        key = canonical_job_link(raw)
        if not key or key in existing:
            continue
        out.append(raw)
        if len(out) >= cap:
            break
    return out


async def scrape_comeet_job(page: Page, job_url: str) -> ComeetJob:
    await page.goto(job_url, wait_until="domcontentloaded", timeout=90_000)
    await asyncio.sleep(config.ACTION_DELAY_SEC)
    body = ""
    try:
        body = await page.locator("body").inner_text()
    except Exception:
        body = ""
    title = ""
    try:
        title = (await page.locator("h1").first.inner_text()).strip()
    except Exception:
        pass
    company = ""
    try:
        cand = page.locator('[class*="company"], [class*="employer"], header').first
        company = (await cand.inner_text()).split("\n")[0].strip()[:200]
    except Exception:
        pass
    if not company and body:
        m = re.search(r"^\s*([^\n]+)", body)
        company = m.group(1).strip()[:200] if m else "Unknown"

    linkedin_company: str | None = None
    try:
        for a in await page.locator('a[href*="linkedin.com/company"]').all():
            h = await a.get_attribute("href")
            if h and "/company/" in h:
                linkedin_company = h.split("?")[0].rstrip("/")
                break
    except Exception:
        pass

    desc = body[:25_000] if body else ""
    return ComeetJob(
        url=job_url,
        company=company or "Unknown",
        job_title=title or "Role",
        description=desc,
        company_linkedin_url=linkedin_company,
    )


async def resolve_company_linkedin(page: Page, company_name: str) -> str | None:
    """Fallback: Google company name + linkedin company."""
    q = quote_plus(f"{company_name} site:linkedin.com/company")
    await page.goto(f"https://www.google.com/search?q={q}&num=5", wait_until="domcontentloaded")
    await asyncio.sleep(config.ACTION_DELAY_SEC)
    hrefs = await page.evaluate(
        """() => {
          const out = [];
          document.querySelectorAll('a[href*="linkedin.com/company"]').forEach(a => out.push(a.href));
          return out;
        }"""
    )
    for h in hrefs:
        if "/company/" in h:
            return h.split("?")[0].rstrip("/")
    return None


async def run_hunting_pipeline(notion: NotionDB) -> None:
    """Per mapped query: large Google pool → Notion cross-reference → up to N new URLs → correct CV → networking."""
    from networker import LinkedInNetworker

    if not config.SEARCH_QUERY_CV_MAP:
        logger.error("SEARCH_QUERY_CV_MAP is empty — add queries in config.py")
        return

    existing_links = await notion.fetch_all_job_links()
    logger.info("Notion has %s existing job link(s) for cross-reference.", len(existing_links))

    async with async_playwright() as pw:
        # Temporarily show the Google search phase for visual debugging.
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        try:
            async with LinkedInNetworker() as net:
                for query, cv_path in config.SEARCH_QUERY_CV_MAP:
                    try:
                        cv_text = get_cv_text_for_path(cv_path)
                    except FileNotFoundError as e:
                        logger.error("%s — skipping query %r", e, query)
                        continue

                    pool = await google_search_comeet_url_pool(
                        page,
                        query,
                        pool_max=config.GOOGLE_POOL_MAX,
                        pages_max=config.GOOGLE_SEARCH_PAGES_MAX,
                    )
                    logger.info(
                        "Query %r → pooled %s Comeet URL(s) (max pool %s)",
                        query,
                        len(pool),
                        config.GOOGLE_POOL_MAX,
                    )

                    fresh = select_new_urls_for_query(
                        pool,
                        existing_links,
                        config.NEW_URLS_PER_QUERY_CAP,
                    )
                    logger.info(
                        "After Notion filter: %s new URL(s) for this query (cap %s)",
                        len(fresh),
                        config.NEW_URLS_PER_QUERY_CAP,
                    )

                    for job_url in fresh:
                        job = await scrape_comeet_job(page, job_url)
                        key = canonical_job_link(job.url)
                        match = await cv_matches_job(cv_text, job.description)
                        if not match:
                            await notion.create_row(
                                company=job.company,
                                job_title=job.job_title,
                                job_link=job.url,
                                status=config.STATUS_CV_REJECTED,
                            )
                            if key:
                                existing_links.add(key)
                            continue

                        company_li = job.company_linkedin_url
                        if not company_li:
                            company_li = await resolve_company_linkedin(page, job.company)
                        if not company_li:
                            logger.warning("No LinkedIn company URL for %s", job.company)
                            await notion.create_row(
                                company=job.company,
                                job_title=job.job_title,
                                job_link=job.url,
                                status=config.STATUS_JOB_FOUND,
                            )
                            if key:
                                existing_links.add(key)
                            continue

                        picked = await net.find_target_employee(company_li)
                        if not picked:
                            logger.warning("No employee candidate at %s", company_li)
                            await notion.create_row(
                                company=job.company,
                                job_title=job.job_title,
                                job_link=job.url,
                                status=config.STATUS_JOB_FOUND,
                            )
                            if key:
                                existing_links.add(key)
                            continue

                        emp_name, emp_url = picked
                        page_row = await notion.create_row(
                            company=job.company,
                            job_title=job.job_title,
                            job_link=job.url,
                            status=config.STATUS_JOB_FOUND,
                            employee_name=emp_name,
                            employee_linkedin=emp_url,
                        )
                        if key:
                            existing_links.add(key)

                        ok = await net.send_connection(emp_url)
                        if ok:
                            await notion.update_row(
                                page_row,
                                status=config.STATUS_CONNECTION_SENT,
                                employee_name=emp_name,
                                employee_linkedin=emp_url,
                            )
                        else:
                            logger.error("Connection failed for %s", emp_url)
        finally:
            await browser.close()
