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

import httpx
from anthropic import AsyncAnthropic
from playwright.async_api import Page, async_playwright
from pypdf import PdfReader

import config
from notion_db import NotionDB, canonical_job_link

logger = logging.getLogger(__name__)

COMEET_HOST = "comeet.com"

_cv_text_cache: dict[Path, str] = {}
_anthropic_client: AsyncAnthropic | None = None


def _get_anthropic() -> AsyncAnthropic:
    global _anthropic_client
    if _anthropic_client is None:
        _anthropic_client = AsyncAnthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
    return _anthropic_client


@dataclass
class ComeetJob:
    url: str
    company: str
    job_title: str
    description: str
    company_linkedin_url: str | None
    partial_load: bool = False  # True when SPA didn't render the specific job (Incapsula block)


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
        resp = await _get_anthropic().messages.create(
            model=config.ANTHROPIC_MODEL,
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
        resp = await _get_anthropic().messages.create(
            model=config.ANTHROPIC_MODEL,
            max_tokens=1200,
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
    # Require at least 4 path segments after /jobs/ to ensure it's a specific job posting,
    # not a company landing page (/jobs/company/id/ has only 3 segments).
    segments = [s for s in path.split("/") if s]
    if len(segments) < 4:
        return None
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


_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


async def brave_search_comeet(query: str, *, pool_max: int) -> list[str]:
    """
    Brave Search — supports site: operator, no API key, no CAPTCHA.
    Returns up to pool_max unique Comeet job URLs for the given query.
    """
    seen_canonical: set[str] = set()
    ordered: list[str] = []

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        try:
            r = await client.get(
                "https://search.brave.com/search",
                params={"q": query},
                headers=_BROWSER_HEADERS,
            )
            r.raise_for_status()
            html = r.text
        except Exception:
            logger.exception("Brave search request failed")
            return []

    for href in re.findall(r'https?://www\.comeet\.com/jobs/[a-zA-Z0-9._/\-]+', html):
        nu = _normalize_comeet_href(href, "https://www.comeet.com")
        if not nu:
            continue
        key = canonical_job_link(nu)
        if not key or key in seen_canonical:
            continue
        seen_canonical.add(key)
        ordered.append(nu)
        if len(ordered) >= pool_max:
            break

    logger.info("Brave search %r → %s Comeet URL(s)", query[:60], len(ordered))
    return ordered[:pool_max]


async def comeet_direct_search(
    page: Page,
    keywords: str,
    *,
    pool_max: int,
) -> list[str]:
    """
    Search Comeet's job board directly using Playwright.
    No external search engine needed — no CAPTCHA, always up to date.
    keywords: plain text like 'fullstack junior' or 'backend junior'
    """
    seen_canonical: set[str] = set()
    ordered: list[str] = []

    # Navigate to Comeet jobs and trigger their internal search
    await page.goto("https://www.comeet.com/jobs", wait_until="load")
    await asyncio.sleep(3)

    # Try to find and use the search input
    try:
        search_sel = 'input[type="search"], input[type="text"][placeholder*="earch"], input[placeholder*="earch"], input[placeholder*="ob"]'
        search_input = page.locator(search_sel).first
        if await search_input.count() and await search_input.is_visible(timeout=5000):
            await search_input.fill(keywords)
            await page.keyboard.press("Enter")
            await asyncio.sleep(3)
    except Exception:
        pass  # If no search box, we'll still scrape whatever jobs are visible

    # Collect all comeet job links from the rendered page
    try:
        await page.wait_for_selector('a[href*="/jobs/"]', timeout=15_000)
    except Exception:
        logger.warning("comeet_direct_search: no job links found on page")
        return []

    hrefs = await page.locator('a[href*="/jobs/"]').evaluate_all("els => els.map(e => e.href)")
    for href in hrefs:
        nu = _normalize_comeet_href(href, "https://www.comeet.com")
        if not nu:
            continue
        key = canonical_job_link(nu)
        if not key or key in seen_canonical:
            continue
        seen_canonical.add(key)
        ordered.append(nu)
        if len(ordered) >= pool_max:
            break

    logger.info("Comeet direct search %r → %s URL(s)", keywords, len(ordered))
    return ordered[:pool_max]


async def google_search_cse(query: str, *, pool_max: int, pages_max: int) -> list[str]:
    """
    Optional: Google Custom Search JSON API.
    Requires GOOGLE_API_KEY + GOOGLE_CSE_ID in .env.
    Free tier: 100 queries/day (we use ~6/week).
    """
    seen_canonical: set[str] = set()
    ordered: list[str] = []

    async with httpx.AsyncClient(timeout=30) as client:
        for page_idx in range(pages_max):
            start = page_idx * 10 + 1
            params = {
                "key": config.GOOGLE_API_KEY,
                "cx": config.GOOGLE_CSE_ID,
                "q": query,
                "num": 10,
                "start": start,
            }
            try:
                r = await client.get("https://www.googleapis.com/customsearch/v1", params=params)
                r.raise_for_status()
                data = r.json()
            except Exception:
                logger.exception("CSE request failed on page %s", page_idx + 1)
                break

            items = data.get("items") or []
            if not items:
                break

            added_this_page = 0
            for item in items:
                link = item.get("link", "")
                nu = _normalize_comeet_href(link, "https://www.comeet.com")
                if not nu:
                    continue
                key = canonical_job_link(nu)
                if not key or key in seen_canonical:
                    continue
                seen_canonical.add(key)
                ordered.append(nu)
                added_this_page += 1
                if len(ordered) >= pool_max:
                    return ordered[:pool_max]

            if added_this_page == 0:
                break
            await asyncio.sleep(0.5)

    return ordered[:pool_max]


async def google_search_browser_fallback(
    page: Page,
    query: str,
    *,
    pool_max: int,
    pages_max: int,
) -> list[str]:
    """
    Browser-based Google search — fallback when CSE credentials are not set.
    Runs headless=False to reduce CAPTCHA risk.
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

        try:
            await page.wait_for_selector('a[href*="comeet.com/jobs"]', timeout=12_000)
        except Exception:
            # Possible CAPTCHA — try extracting redirect-wrapped links too
            pass

        # Extract both direct comeet links and Google-redirect-wrapped ones
        hrefs: list[str] = await page.evaluate(
            """() => {
                const out = [];
                document.querySelectorAll('a').forEach(a => {
                    const raw = a.getAttribute('href') || '';
                    const full = a.href || '';
                    if (full.includes('comeet.com/jobs')) { out.push(full); return; }
                    if (raw.startsWith('/url') && raw.includes('comeet.com')) {
                        try {
                            const url = new URL('https://google.com' + raw);
                            const q = url.searchParams.get('q');
                            if (q && q.includes('comeet.com/jobs')) out.push(q);
                        } catch(e) {}
                    }
                });
                return out;
            }"""
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
            ordered.append(nu)
            added_this_page += 1
            if len(ordered) >= pool_max:
                return ordered[:pool_max]

        if added_this_page == 0:
            logger.info("Browser Google page %s: no new Comeet links; stopping.", page_idx + 1)
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


def _job_title_is_senior(title: str) -> bool:
    t = title.lower()
    return any(kw in t for kw in config.JOB_TITLE_EXCLUDE_KEYWORDS)


async def scrape_comeet_job(page: Page, job_url: str) -> ComeetJob:
    # Use "load" (not "domcontentloaded") so the React SPA has time to render.
    await page.goto(job_url, wait_until="load", timeout=90_000)
    await asyncio.sleep(config.ACTION_DELAY_SEC + 2)

    # Always extract title/company from URL slug first (reliable even when SPA fails)
    _url_parts = urlparse(job_url).path.strip("/").split("/")
    _url_title = _url_parts[3].replace("-", " ").title() if len(_url_parts) >= 4 else ""
    _url_company = _url_parts[1].replace("-", " ").title()[:200] if len(_url_parts) >= 2 else ""

    # --- Job Title + Company from page <title> (server-rendered, always reliable) ---
    # Specific job page:  "Job Title at Company | Comeet"
    # Generic company:    "Jobs at Company | Comeet"
    title = ""
    company = ""
    _used_url_title = False
    try:
        title_tag = await page.title()
        if " at " in title_tag and not title_tag.lower().startswith("jobs at "):
            # Specific job page loaded → extract both title and company
            parts_at = title_tag.split(" at ", 1)
            title = parts_at[0].strip()[:200]
            company = parts_at[1].split("|")[0].split(" - ")[0].strip()[:200]
        else:
            # Generic company page (SPA didn't render the specific job)
            # Extract company name and fall back to URL slug for job title
            if "jobs at " in title_tag.lower():
                company = title_tag.lower().replace("jobs at ", "").split("|")[0].strip().title()[:200]
            title = _url_title
            _used_url_title = True
    except Exception:
        title = _url_title
        _used_url_title = True

    if not company:
        company = _url_company

    # --- Body text ---
    body = ""
    try:
        body = await page.locator("body").inner_text()
    except Exception:
        body = ""

    # --- LinkedIn company URL ---
    linkedin_company: str | None = None
    try:
        for a in await page.locator('a[href*="linkedin.com/company"]').all():
            h = await a.get_attribute("href")
            if h and "/company/" in h:
                linkedin_company = h.split("?")[0].rstrip("/")
                break
    except Exception:
        pass

    return ComeetJob(
        url=job_url,
        company=company or "Unknown",
        job_title=title or "Unknown Role",
        description=body[:25_000] if body else "",
        company_linkedin_url=linkedin_company,
        partial_load=_used_url_title,
    )


async def resolve_company_linkedin(company_name: str, li_page: "Page | None" = None) -> str | None:
    """
    Find company LinkedIn URL.
    Priority:
      1. LinkedIn company search (via logged-in browser) — most accurate
      2. DuckDuckGo text search — fallback if no browser
    """
    from playwright.async_api import Page as _Page

    # 1. LinkedIn search (requires logged-in page)
    if li_page is not None:
        q = quote_plus(company_name)
        search_url = f"https://www.linkedin.com/search/results/companies/?keywords={q}"
        try:
            await li_page.goto(search_url, wait_until="domcontentloaded")
            await asyncio.sleep(config.ACTION_DELAY_SEC)
            # First company result link
            for loc in (await li_page.locator('a[href*="/company/"]').all())[:10]:
                href = await loc.get_attribute("href")
                if href and "/company/" in href and "/search/" not in href:
                    clean = href.split("?")[0].rstrip("/")
                    # Validate: navigate and check it exists
                    return clean
        except Exception:
            logger.debug("LinkedIn company search failed for %s", company_name)

    # 2. Brave Search fallback
    q = f"{company_name} site:linkedin.com/company"
    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        try:
            r = await client.get(
                "https://search.brave.com/search",
                params={"q": q},
                headers=_BROWSER_HEADERS,
            )
            r.raise_for_status()
            for href in re.findall(r'https?://[a-z]{2,3}\.linkedin\.com/company/[^\s"\'<>&]+', r.text):
                if "/company/" in href:
                    return href.split("?")[0].rstrip("/")
        except Exception:
            logger.debug("Brave company lookup failed for %s", company_name)

    return None


async def run_hunting_pipeline(notion: NotionDB) -> None:
    """Per mapped query: large Google pool → Notion cross-reference → up to N new URLs → correct CV → networking."""
    from networker import LinkedInNetworker

    if not config.SEARCH_QUERY_CV_MAP:
        logger.error("SEARCH_QUERY_CV_MAP is empty — add queries in config.py")
        return

    existing_links = await notion.fetch_all_job_links()
    logger.info("Notion has %s existing job link(s) for cross-reference.", len(existing_links))

    use_cse = bool(config.GOOGLE_API_KEY and config.GOOGLE_CSE_ID)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=config.PLAYWRIGHT_HEADLESS)
        context = await browser.new_context()
        page = await context.new_page()
        try:
            async with LinkedInNetworker() as net:
                connections_sent = 0  # total across all queries this run

                for query, cv_path in config.SEARCH_QUERY_CV_MAP:
                    if connections_sent >= config.MAX_CONNECTIONS_PER_RUN:
                        logger.info("Reached max connections per run (%s); stopping.", config.MAX_CONNECTIONS_PER_RUN)
                        break

                    try:
                        cv_text = get_cv_text_for_path(cv_path)
                    except FileNotFoundError as e:
                        logger.error("%s — skipping query %r", e, query)
                        continue

                    # 1. Google CSE if configured (best coverage)
                    # 2. DuckDuckGo via httpx (no API key, no CAPTCHA, good for fullstack)
                    # 3. Comeet direct search via browser (fallback when DDG has poor coverage)
                    if use_cse:
                        pool = await google_search_cse(
                            query,
                            pool_max=config.GOOGLE_POOL_MAX,
                            pages_max=config.GOOGLE_SEARCH_PAGES_MAX,
                        )
                    else:
                        pool = await brave_search_comeet(query, pool_max=config.GOOGLE_POOL_MAX)
                        if len(pool) < config.NEW_URLS_PER_QUERY_CAP:
                            # Brave returned fewer than needed — supplement with Comeet direct search
                            # Extract plain keywords for Comeet's internal search (strip "site:..." operator)
                            kw_parts = [w for w in query.split() if not w.startswith("site:") and w.lower() != "israel"]
                            kw = " ".join(kw_parts)
                            direct = await comeet_direct_search(page, kw, pool_max=config.GOOGLE_POOL_MAX)
                            seen = {canonical_job_link(u) for u in pool}
                            for u in direct:
                                key = canonical_job_link(u)
                                if key and key not in seen:
                                    seen.add(key)
                                    pool.append(u)
                            logger.info("After Comeet direct supplement: %s URL(s) total", len(pool))

                    logger.info("Query %r → pooled %s Comeet URL(s)", query, len(pool))

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
                        if connections_sent >= config.MAX_CONNECTIONS_PER_RUN:
                            logger.info("Reached max connections per run (%s)", config.MAX_CONNECTIONS_PER_RUN)
                            break
                        try:
                            job = await scrape_comeet_job(page, job_url)
                            key = canonical_job_link(job.url)

                            # Skip non-junior roles (Senior/Lead/Staff/Principal/etc.)
                            if _job_title_is_senior(job.job_title):
                                logger.info("Skipping non-junior role: %s at %s", job.job_title, job.company)
                                await notion.create_row(
                                    company=job.company,
                                    job_title=job.job_title,
                                    job_link=job.url,
                                    status=config.STATUS_CV_REJECTED,
                                )
                                if key:
                                    existing_links.add(key)
                                continue

                            # Skip CV match when SPA didn't render the job page fully.
                            if job.partial_load:
                                logger.warning(
                                    "SPA partial load for %s (%s) — skipping CV match, proceeding",
                                    job.job_title, job_url,
                                )
                                match = True
                            else:
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
                                company_li = await resolve_company_linkedin(job.company, net._require_page())
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

                            remaining = config.MAX_CONNECTIONS_PER_RUN - connections_sent
                            max_for_company = min(config.MAX_EMPLOYEES_PER_COMPANY, remaining)
                            employees = await net.find_target_employees(company_li, max_count=max_for_company)
                            if not employees:
                                logger.warning("No employee candidates at %s", company_li)
                                await notion.create_row(
                                    company=job.company,
                                    job_title=job.job_title,
                                    job_link=job.url,
                                    status=config.STATUS_JOB_FOUND,
                                )
                                if key:
                                    existing_links.add(key)
                                continue

                            if key:
                                existing_links.add(key)

                            for emp_name, emp_url in employees:
                                if connections_sent >= config.MAX_CONNECTIONS_PER_RUN:
                                    break
                                page_row = await notion.create_row(
                                    company=job.company,
                                    job_title=job.job_title,
                                    job_link=job.url,
                                    status=config.STATUS_JOB_FOUND,
                                    employee_name=emp_name,
                                    employee_linkedin=emp_url,
                                )
                                await asyncio.sleep(config.ACTION_DELAY_SEC)
                                ok = await net.send_connection(emp_url)
                                if ok:
                                    await notion.update_row(page_row, status=config.STATUS_CONNECTION_SENT)
                                    connections_sent += 1
                                    logger.info(
                                        "Connection sent to %s (%s/%s)",
                                        emp_name, connections_sent, config.MAX_CONNECTIONS_PER_RUN,
                                    )
                                else:
                                    logger.error("Connection failed for %s", emp_url)

                        except Exception:
                            logger.exception("Error processing job %s — skipping", job_url)
                            continue
        finally:
            await browser.close()
