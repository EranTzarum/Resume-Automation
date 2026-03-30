"""
Notion database operations for «Eran Referral Engine».
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any
from urllib.parse import urlparse

import httpx

import config

logger = logging.getLogger(__name__)


def canonical_job_link(url: str | None) -> str:
    """Normalize job URLs for set comparison with Notion (scheme/host/path, trailing slash, lower host)."""
    if not url or not str(url).strip():
        return ""
    p = urlparse(str(url).strip())
    if not p.netloc:
        return ""
    path = (p.path or "").rstrip("/")
    scheme = (p.scheme or "https").lower()
    host = p.netloc.lower()
    return f"{scheme}://{host}{path}/"


def _db_id() -> str:
    raw = config.NOTION_DATABASE_ID.replace("-", "")
    if len(raw) == 32:
        return f"{raw[0:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:32]}"
    return config.NOTION_DATABASE_ID


class NotionDB:
    def __init__(self) -> None:
        if not config.NOTION_API_KEY:
            raise ValueError("NOTION_API_KEY is required")
        self._base = "https://api.notion.com/v1"
        self._headers = {
            "Authorization": f"Bearer {config.NOTION_API_KEY}",
            "Notion-Version": config.NOTION_API_VERSION,
            "Content-Type": "application/json",
        }
        self._database_id = _db_id()

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{self._base}{path}"
        async with httpx.AsyncClient(timeout=config.REQUEST_TIMEOUT_SEC) as client:
            r = await client.request(method, url, headers=self._headers, json=json_body)
            r.raise_for_status()
            if r.content:
                return r.json()
            return {}

    @staticmethod
    def _title(content: str) -> dict[str, Any]:
        return {"title": [{"type": "text", "text": {"content": content[:2000]}}]}

    @staticmethod
    def _rich_text(content: str) -> dict[str, Any]:
        return {"rich_text": [{"type": "text", "text": {"content": content[:2000]}}]}

    @staticmethod
    def _url(url: str | None) -> dict[str, Any]:
        return {"url": url}

    @staticmethod
    def _select(name: str) -> dict[str, Any]:
        return {"select": {"name": name}}

    @staticmethod
    def _date_today() -> dict[str, Any]:
        return {"date": {"start": date.today().isoformat()}}

    def _props(
        self,
        *,
        company: str,
        job_title: str,
        job_link: str,
        employee_name: str = "",
        employee_linkedin: str | None = None,
        status: str = config.STATUS_JOB_FOUND,
    ) -> dict[str, Any]:
        p: dict[str, Any] = {
            config.PROP_COMPANY: self._title(company),
            config.PROP_JOB_TITLE: self._rich_text(job_title),
            config.PROP_JOB_LINK: self._url(job_link),
            config.PROP_STATUS: self._select(status),
            config.PROP_DATE_ADDED: self._date_today(),
        }
        if employee_name:
            p[config.PROP_EMPLOYEE_NAME] = self._rich_text(employee_name)
        if employee_linkedin:
            p[config.PROP_EMPLOYEE_LINKEDIN] = self._url(employee_linkedin)
        return p

    async def job_link_exists(self, job_link: str) -> bool:
        body = {
            "filter": {
                "property": config.PROP_JOB_LINK,
                "url": {"equals": job_link},
            },
            "page_size": 1,
        }
        data = await self._request(
            "POST",
            f"/databases/{self._database_id}/query",
            json_body=body,
        )
        return bool(data.get("results"))

    async def fetch_all_job_links(self) -> set[str]:
        """All Job Link values in the database (paginated), canonicalized."""
        out: set[str] = set()
        cursor: str | None = None
        while True:
            body: dict[str, Any] = {"page_size": 100}
            if cursor:
                body["start_cursor"] = cursor
            data = await self._request(
                "POST",
                f"/databases/{self._database_id}/query",
                json_body=body,
            )
            for row in data.get("results") or []:
                raw = self.parse_page(row).get("job_link")
                c = canonical_job_link(raw)
                if c:
                    out.add(c)
            if not data.get("has_more"):
                break
            cursor = data.get("next_cursor")
            if not cursor:
                break
        return out

    async def find_page_id_by_job_link(self, job_link: str) -> str | None:
        body = {
            "filter": {
                "property": config.PROP_JOB_LINK,
                "url": {"equals": job_link},
            },
            "page_size": 1,
        }
        data = await self._request(
            "POST",
            f"/databases/{self._database_id}/query",
            json_body=body,
        )
        results = data.get("results") or []
        if not results:
            return None
        return results[0].get("id")

    async def create_row(
        self,
        *,
        company: str,
        job_title: str,
        job_link: str,
        status: str = config.STATUS_JOB_FOUND,
        employee_name: str = "",
        employee_linkedin: str | None = None,
    ) -> str:
        payload = {
            "parent": {"database_id": self._database_id},
            "properties": self._props(
                company=company,
                job_title=job_title,
                job_link=job_link,
                status=status,
                employee_name=employee_name,
                employee_linkedin=employee_linkedin,
            ),
        }
        created = await self._request("POST", "/pages", json_body=payload)
        pid = created.get("id", "")
        logger.info("Notion create %s (%s)", pid, status)
        return pid

    async def update_row(
        self,
        page_id: str,
        *,
        company: str | None = None,
        job_title: str | None = None,
        job_link: str | None = None,
        employee_name: str | None = None,
        employee_linkedin: str | None = None,
        status: str | None = None,
    ) -> None:
        props: dict[str, Any] = {}
        if company is not None:
            props[config.PROP_COMPANY] = self._title(company)
        if job_title is not None:
            props[config.PROP_JOB_TITLE] = self._rich_text(job_title)
        if job_link is not None:
            props[config.PROP_JOB_LINK] = self._url(job_link)
        if employee_name is not None:
            props[config.PROP_EMPLOYEE_NAME] = self._rich_text(employee_name)
        if employee_linkedin is not None:
            props[config.PROP_EMPLOYEE_LINKEDIN] = self._url(employee_linkedin)
        if status is not None:
            props[config.PROP_STATUS] = self._select(status)
        props[config.PROP_DATE_ADDED] = self._date_today()
        await self._request("PATCH", f"/pages/{page_id}", json_body={"properties": props})

    async def list_by_status(self, status: str, page_size: int = 50) -> list[dict[str, Any]]:
        body = {
            "filter": {
                "property": config.PROP_STATUS,
                "select": {"equals": status},
            },
            "page_size": page_size,
        }
        data = await self._request(
            "POST",
            f"/databases/{self._database_id}/query",
            json_body=body,
        )
        return list(data.get("results") or [])

    @staticmethod
    def parse_page(row: dict[str, Any]) -> dict[str, str | None]:
        props = row.get("properties") or {}

        def plain_title() -> str:
            t = props.get(config.PROP_COMPANY, {}).get("title") or []
            if not t:
                return ""
            return (t[0].get("plain_text") or "")[:4000]

        def plain_rich(key: str) -> str:
            r = props.get(key, {}).get("rich_text") or []
            if not r:
                return ""
            return "".join(x.get("plain_text", "") for x in r)[:4000]

        def url_prop(key: str) -> str | None:
            return props.get(key, {}).get("url")

        return {
            "page_id": row.get("id") or "",
            "company": plain_title(),
            "job_title": plain_rich(config.PROP_JOB_TITLE),
            "job_link": url_prop(config.PROP_JOB_LINK),
            "employee_name": plain_rich(config.PROP_EMPLOYEE_NAME),
            "employee_linkedin": url_prop(config.PROP_EMPLOYEE_LINKEDIN),
        }
