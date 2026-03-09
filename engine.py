from __future__ import annotations
import re
from datetime import datetime, timezone

import aiosqlite

SQL = """
CREATE TABLE IF NOT EXISTS prospect_lists (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS prospects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL,
    company TEXT NOT NULL,
    job_title TEXT,
    website TEXT,
    linkedin_url TEXT,
    notes TEXT,
    list_id INTEGER,
    status TEXT NOT NULL DEFAULT 'new',
    created_at TEXT NOT NULL,
    FOREIGN KEY (list_id) REFERENCES prospect_lists(id)
);

CREATE TABLE IF NOT EXISTS templates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    subject_template TEXT NOT NULL,
    body_template TEXT NOT NULL,
    tone TEXT NOT NULL DEFAULT 'professional',
    category TEXT NOT NULL DEFAULT 'cold',
    times_used INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS draft_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    prospect_id INTEGER NOT NULL,
    template_id INTEGER,
    tone TEXT NOT NULL,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS usage_stats (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    total_drafts INTEGER NOT NULL DEFAULT 0
);

INSERT OR IGNORE INTO usage_stats (id) VALUES (1);
"""

TONE_OPENERS = {
    "professional": [
        "I noticed {company} is {context_hint} — I wanted to reach out directly.",
        "Given {company}'s focus on {context_hint}, I thought this might be relevant.",
    ],
    "friendly": [
        "Hey {first_name}, quick note that might actually be useful for you at {company}.",
        "Hope this finds you well! I came across {company} and had to reach out.",
    ],
    "direct": [
        "{first_name}, I'll be brief.",
        "Straight to the point, {first_name}:",
    ],
    "witty": [
        "Bold move, emailing a stranger — but here I am, {first_name}.",
        "{first_name}, I promise this is the most interesting cold email you'll open today.",
    ],
}

TONE_CLOSES = {
    "professional": "Would you be open to a brief conversation?",
    "friendly": "Would love to chat — even 10 minutes would be great!",
    "direct": "Worth a quick chat?",
    "witty": "If this resonated even 10%, let's talk.",
}


async def init_db(path: str) -> aiosqlite.Connection:
    db = await aiosqlite.connect(path)
    db.row_factory = aiosqlite.Row
    await db.executescript(SQL)
    await db.commit()
    return db


def _prospect_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"], "first_name": r["first_name"], "last_name": r["last_name"],
        "email": r["email"], "company": r["company"], "job_title": r["job_title"],
        "website": r["website"], "linkedin_url": r["linkedin_url"],
        "notes": r["notes"], "list_id": r["list_id"],
        "status": r["status"], "created_at": r["created_at"],
    }


def _template_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"], "name": r["name"],
        "subject_template": r["subject_template"], "body_template": r["body_template"],
        "tone": r["tone"], "category": r["category"],
        "times_used": r["times_used"], "created_at": r["created_at"],
    }


def _render_template(template: str, prospect: dict, value_prop: str, cta: str) -> str:
    replacements = {
        "{{first_name}}": prospect.get("first_name", ""),
        "{{last_name}}": prospect.get("last_name", ""),
        "{{company}}": prospect.get("company", ""),
        "{{job_title}}": prospect.get("job_title", "their role"),
        "{{value_prop}}": value_prop,
        "{{cta}}": cta,
    }
    result = template
    for key, val in replacements.items():
        result = result.replace(key, val or "")
    return result.strip()


def generate_draft(prospect: dict, tone: str, context: str | None,
                   value_prop: str, cta: str) -> dict:
    """Generate a personalised cold email draft from prospect data."""
    first = prospect.get("first_name", "there")
    company = prospect.get("company", "your company")
    job_title = prospect.get("job_title", "")
    context_hint = context or (job_title or "your space")

    openers = TONE_OPENERS.get(tone, TONE_OPENERS["professional"])
    opener = openers[hash(first) % len(openers)].format(
        first_name=first, company=company, context_hint=context_hint
    )
    close_line = TONE_CLOSES.get(tone, TONE_CLOSES["professional"])

    subject = f"Quick idea for {company}"
    if tone == "direct":
        subject = f"{value_prop.split('.')[0][:50]} — {first}"
    elif tone == "witty":
        subject = f"Not another cold email, {first} (okay, maybe it is)"

    body_lines = [
        f"Hi {first},",
        "",
        opener,
        "",
        f"{value_prop}",
        "",
        f"I think this could be particularly useful for {company} because {context_hint}.",
        "",
        f"Would you be open to {cta}? {close_line}",
        "",
        "Best,",
    ]
    body = "
".join(body_lines)

    signals = []
    if context:
        signals.append(f"custom context: {context[:40]}")
    if job_title:
        signals.append(f"role-aware: {job_title}")
    if prospect.get("notes"):
        signals.append("notes referenced")
    signals.append(f"tone: {tone}")

    return {
        "subject": subject,
        "body": body,
        "personalization_signals": signals,
        "word_count": len(body.split()),
    }


# ── CRUD ────────────────────────────────────────────────────────────────────

async def create_prospect(db: aiosqlite.Connection, data: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    cur = await db.execute(
        """INSERT INTO prospects (first_name, last_name, email, company, job_title, website,
           linkedin_url, notes, list_id, status, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'new', ?)""",
        (data["first_name"], data["last_name"], data["email"], data["company"],
         data.get("job_title"), data.get("website"), data.get("linkedin_url"),
         data.get("notes"), data.get("list_id"), now)
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM prospects WHERE id = ?", (cur.lastrowid,))
    return _prospect_row(rows[0])


async def list_prospects(db: aiosqlite.Connection, list_id: int | None = None,
                         status: str | None = None) -> list[dict]:
    q, params = "SELECT * FROM prospects", []
    conds = []
    if list_id is not None:
        conds.append("list_id = ?"); params.append(list_id)
    if status:
        conds.append("status = ?"); params.append(status)
    if conds:
        q += " WHERE " + " AND ".join(conds)
    q += " ORDER BY created_at DESC"
    rows = await db.execute_fetchall(q, params)
    return [_prospect_row(r) for r in rows]


async def get_prospect(db: aiosqlite.Connection, prospect_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM prospects WHERE id = ?", (prospect_id,))
    return _prospect_row(rows[0]) if rows else None


async def update_prospect_status(db: aiosqlite.Connection, prospect_id: int, status: str) -> dict | None:
    await db.execute("UPDATE prospects SET status = ? WHERE id = ?", (status, prospect_id))
    await db.commit()
    return await get_prospect(db, prospect_id)


async def bulk_import_prospects(db: aiosqlite.Connection, prospects: list[dict],
                                 list_id: int | None) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    created, skipped = 0, 0
    for p in prospects:
        if list_id:
            p["list_id"] = list_id
        try:
            await create_prospect(db, p)
            created += 1
        except Exception:
            skipped += 1
    return {"created": created, "skipped": skipped}


async def create_draft(db: aiosqlite.Connection, prospect_id: int, template_id: int | None,
                       tone: str, context: str | None, value_prop: str, cta: str) -> dict:
    prospect = await get_prospect(db, prospect_id)
    if not prospect:
        return {}
    draft = generate_draft(prospect, tone, context, value_prop, cta)

    # Use template if provided
    if template_id:
        rows = await db.execute_fetchall("SELECT * FROM templates WHERE id = ?", (template_id,))
        if rows:
            t = rows[0]
            draft["subject"] = _render_template(t["subject_template"], prospect, value_prop, cta)
            draft["body"] = _render_template(t["body_template"], prospect, value_prop, cta)
            draft["word_count"] = len(draft["body"].split())
            await db.execute("UPDATE templates SET times_used = times_used + 1 WHERE id = ?", (template_id,))

    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        "INSERT INTO draft_log (prospect_id, template_id, tone, subject, body, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (prospect_id, template_id, tone, draft["subject"], draft["body"], now)
    )
    await db.execute("UPDATE usage_stats SET total_drafts = total_drafts + 1 WHERE id = 1")
    await db.commit()
    return {"prospect_id": prospect_id, **draft, "tone": tone}


async def create_template(db: aiosqlite.Connection, data: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    cur = await db.execute(
        "INSERT INTO templates (name, subject_template, body_template, tone, category, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (data["name"], data["subject_template"], data["body_template"],
         data.get("tone", "professional"), data.get("category", "cold"), now)
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM templates WHERE id = ?", (cur.lastrowid,))
    return _template_row(rows[0])


async def list_templates(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM templates ORDER BY times_used DESC")
    return [_template_row(r) for r in rows]


async def create_prospect_list(db: aiosqlite.Connection, data: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    cur = await db.execute(
        "INSERT INTO prospect_lists (name, description, created_at) VALUES (?, ?, ?)",
        (data["name"], data.get("description"), now)
    )
    await db.commit()
    list_id = cur.lastrowid
    count_rows = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM prospects WHERE list_id = ?", (list_id,))
    cnt = count_rows[0]["cnt"] if count_rows else 0
    rows = await db.execute_fetchall("SELECT * FROM prospect_lists WHERE id = ?", (list_id,))
    r = rows[0]
    return {"id": r["id"], "name": r["name"], "description": r["description"],
            "prospect_count": cnt, "created_at": r["created_at"]}


async def list_prospect_lists(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM prospect_lists ORDER BY created_at DESC")
    result = []
    for r in rows:
        count_rows = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM prospects WHERE list_id = ?", (r["id"],))
        cnt = count_rows[0]["cnt"] if count_rows else 0
        result.append({"id": r["id"], "name": r["name"], "description": r["description"],
                       "prospect_count": cnt, "created_at": r["created_at"]})
    return result


async def get_stats(db: aiosqlite.Connection) -> dict:
    total_p = (await db.execute_fetchall("SELECT COUNT(*) as cnt FROM prospects"))[0]["cnt"]
    total_l = (await db.execute_fetchall("SELECT COUNT(*) as cnt FROM prospect_lists"))[0]["cnt"]
    total_t = (await db.execute_fetchall("SELECT COUNT(*) as cnt FROM templates"))[0]["cnt"]
    stats_row = await db.execute_fetchall("SELECT * FROM usage_stats WHERE id = 1")
    total_drafts = stats_row[0]["total_drafts"] if stats_row else 0
    status_rows = await db.execute_fetchall("SELECT status, COUNT(*) as cnt FROM prospects GROUP BY status")
    by_status = {r["status"]: r["cnt"] for r in status_rows}
    tone_rows = await db.execute_fetchall("SELECT tone, COUNT(*) as cnt FROM draft_log GROUP BY tone ORDER BY cnt DESC LIMIT 1")
    most_used_tone = tone_rows[0]["tone"] if tone_rows else None
    return {
        "total_prospects": total_p, "total_drafts_generated": total_drafts,
        "total_lists": total_l, "total_templates": total_t,
        "by_status": by_status, "most_used_tone": most_used_tone,
    }

async def list_drafts(
    db: aiosqlite.Connection,
    prospect_id: int | None = None,
    tone: str | None = None,
    limit: int = 50,
) -> list[dict]:
    q = "SELECT * FROM draft_log WHERE 1=1"
    params: list = []
    if prospect_id is not None:
        q += " AND prospect_id = ?"
        params.append(prospect_id)
    if tone:
        q += " AND tone = ?"
        params.append(tone)
    q += f" ORDER BY created_at DESC LIMIT {limit}"
    rows = await db.execute_fetchall(q, params)
    return [_draft_log_row(r) for r in rows]


async def get_draft(db: aiosqlite.Connection, draft_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM draft_log WHERE id = ?", (draft_id,))
    return _draft_log_row(rows[0]) if rows else None


def _draft_log_row(r: aiosqlite.Row) -> dict:
    body = r["body"]
    return {
        "id": r["id"],
        "prospect_id": r["prospect_id"],
        "template_id": r["template_id"],
        "tone": r["tone"],
        "subject": r["subject"],
        "body": body,
        "word_count": len(body.split()) if body else 0,
        "created_at": r["created_at"],
    }
