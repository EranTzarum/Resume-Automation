"""

Environment variables, Notion property names, Hebrew referral template, and search→CV routing.

"""



from __future__ import annotations



import os

from pathlib import Path



from dotenv import load_dotenv



load_dotenv(Path(__file__).resolve().parent / ".env")



_PROJECT_ROOT = Path(__file__).resolve().parent



# --- API keys ---

NOTION_API_KEY: str = os.getenv("NOTION_API_KEY", "")

NOTION_API_VERSION: str = os.getenv("NOTION_API_VERSION", "2022-06-28")

# Created via MCP: "Eran Referral Engine" (Job Search Tracker → Eran)

NOTION_DATABASE_ID: str = (os.getenv("NOTION_DATABASE_ID", "") or "").strip() or "789da2178f0d4d93a66689b5e2c7e72b"



ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")

ANTHROPIC_MODEL: str = os.getenv("ANTHROPIC_MODEL", "claude-3-5-haiku-20241022")

ANTHROPIC_API_URL: str = "https://api.anthropic.com/v1/messages"



# --- Notion schema (exact names from database) ---

PROP_COMPANY: str = "Company"

PROP_JOB_TITLE: str = "Job Title"

PROP_JOB_LINK: str = "Job Link"

PROP_EMPLOYEE_NAME: str = "Employee Name"

PROP_EMPLOYEE_LINKEDIN: str = "Employee LinkedIn"

PROP_STATUS: str = "Status"

PROP_DATE_ADDED: str = "Date Added"



STATUS_JOB_FOUND: str = "Job Found"

STATUS_CV_REJECTED: str = "CV Rejected"

STATUS_CONNECTION_SENT: str = "Connection Sent"

STATUS_MESSAGE_SENT: str = "Message Sent"



# --- Dynamic CV routing: Google search query → local PDF (used for Claude match + routing) ---

SEARCH_QUERY_CV_MAP: list[tuple[str, Path]] = [

    (

        "site:www.comeet.com/jobs fullstack junior israel",

        _PROJECT_ROOT / "cv_fullstack.pdf",

    ),

    (

        "site:www.comeet.com/jobs backend junior israel",

        _PROJECT_ROOT / "cv_backend.pdf",

    ),

]



# Google: collect a pool across pages before Notion dedupe; then cap new URLs per query.

GOOGLE_POOL_MAX: int = int(os.getenv("GOOGLE_POOL_MAX", "30"))

GOOGLE_SEARCH_PAGES_MAX: int = int(os.getenv("GOOGLE_SEARCH_PAGES_MAX", "3"))

NEW_URLS_PER_QUERY_CAP: int = int(os.getenv("NEW_URLS_PER_QUERY_CAP", "4"))



# Follow-up DM attachment (single file; override if you prefer another default)

CV_PDF_PATH: Path = Path(

    os.getenv("CV_PDF_PATH", str(_PROJECT_ROOT / "cv_fullstack.pdf"))

).expanduser()



LINKEDIN_STATE_PATH: Path = Path(os.getenv("LINKEDIN_STATE_PATH", "state.json")).expanduser()



PLAYWRIGHT_HEADLESS: bool = os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() in (

    "1",

    "true",

    "yes",

)

ACTION_DELAY_SEC: float = float(os.getenv("ACTION_DELAY_SEC", "2.0"))

REQUEST_TIMEOUT_SEC: float = float(os.getenv("REQUEST_TIMEOUT_SEC", "120"))



# Targeting: exclude titles containing any of these (case-insensitive)

EXCLUDE_TITLE_KEYWORDS: tuple[str, ...] = tuple(

    x.strip().lower()

    for x in os.getenv(

        "EXCLUDE_TITLE_KEYWORDS",

        "hr,human resources,senior,director,manager,lead,founder",

    ).split(",")

    if x.strip()

)



# Hebrew referral template — AI adjusts gender (תראה/י, תוכל/י) and fills variables.

HE_REFERRAL_TEMPLATE: str = os.getenv(

    "HE_REFERRAL_TEMPLATE",

    """היי [Employee Name] מה קורה?

תראה\\י, אנחנו לא מכירים אבל אשמח ממש להיעזר בך (:

ראיתי שנפתחה אצלכם משרת [Job Title] לצוות של [Company Name/Department].

אני אעשה את זה קצר ודוגרי

סיימתי תואר במדעי המחשב עם ממוצע 95 בשנה האחרונה, ואני מגיע עם ניסיון מעשי כ-Lead Developer בפרויקט מורכב (בוגר אקסלרטור 8200 IMPACT).

אני רגיל ללכלך את הידיים, להרים ארכיטקטורה מאפס ולחבר Backend ל-Frontend.

אני מגיע מרקע חזק של פיתוח תשתיות.

יש לי עקומת למידה מהירה מאוד ויודע להיכנס לעניינים ולהרים פיצ'רים שעובדים בלי לבזבז זמן.

צירפתי לכאן את קורות החיים שלי. אשמח ממש אם תוכל\\י להגיש אותם דרך מערכת ההמלצות הפנימית שלכם [Job Link].

ככה גם אעקוף את הערימה, וזה גם יסדר לך אחלה בונוס חבר מביא חבר אם זה יתקדם לשנינו. (:

תודה מראש אשמח לשמוע ממך""",

)


