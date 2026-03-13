from __future__ import annotations
import csv
import io
import json as jsonlib
import re
from datetime import datetime, timezone, timedelta

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

CREATE TABLE IF NOT EXISTS sequences (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    value_prop TEXT NOT NULL,
    cta TEXT NOT NULL DEFAULT 'book a 15-min call',
    steps TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS enrollments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sequence_id INTEGER NOT NULL REFERENCES sequences(id),
    prospect_id INTEGER NOT NULL REFERENCES prospects(id),
    current_step INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active',
    enrolled_at TEXT NOT NULL,
    last_advanced_at TEXT,
    UNIQUE(sequence_id, prospect_id)
);

CREATE TABLE IF NOT EXISTS campaign_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    prospect_id INTEGER NOT NULL REFERENCES prospects(id),
    sequence_id INTEGER,
    draft_id INTEGER,
    event_type TEXT NOT NULL,
    metadata TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dnc_list (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT,
    domain TEXT,
    reason TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS smart_lists (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    filters TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_prospect ON campaign_events(prospect_id);
CREATE INDEX IF NOT EXISTS idx_events_sequence ON campaign_events(sequence_id);
CREATE INDEX IF NOT EXISTS idx_dnc_email ON dnc_list(email);
CREATE INDEX IF NOT EXISTS idx_dnc_domain ON dnc_list(domain);
CREATE INDEX IF NOT EXISTS idx_prospects_email ON prospects(email);
CREATE INDEX IF NOT EXISTS idx_prospects_company ON prospects(company);

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

FOLLOWUP_OPENERS = {
    "professional": "Following up on my earlier note — I know inboxes get busy.",
    "friendly": "Hey again! Just bumping this up in case it got buried.",
    "direct": "Circling back briefly.",
    "witty": "My last email was so good it deserved a sequel.",
}


async def init_db(path: str) -> aiosqlite.Connection:
    db = await aiosqlite.connect(path)
    db.row_factory = aiosqlite.Row
    await db.executescript(SQL)
    # Migration: add tags column if missing
    try:
        await db.execute("SELECT tags FROM prospects LIMIT 1")
    except Exception:
        await db.execute("ALTER TABLE prospects ADD COLUMN tags TEXT NOT NULL DEFAULT '[]'")
    await db.commit()
    return db


def _prospect_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"], "first_name": r["first_name"], "last_name": r["last_name"],
        "email": r["email"], "company": r["company"], "job_title": r["job_title"],
        "website": r["website"], "linkedin_url": r["linkedin_url"],
        "notes": r["notes"], "list_id": r["list_id"],
        "tags": jsonlib.loads(r["tags"]) if r["tags"] else [],
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
    body = "\n".join(body_lines)

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


def generate_followup_draft(prospect: dict, tone: str, step_num: int,
                             subject_hint: str, value_prop: str, cta: str) -> dict:
    first = prospect.get("first_name", "there")
    company = prospect.get("company", "your company")
    opener = FOLLOWUP_OPENERS.get(tone, FOLLOWUP_OPENERS["professional"])
    close_line = TONE_CLOSES.get(tone, TONE_CLOSES["professional"])

    subject = f"Re: {subject_hint}"
    body_lines = [
        f"Hi {first},",
        "",
        opener,
        "",
        f"{value_prop}",
        "",
        f"Would you be open to {cta}? {close_line}",
        "",
        "Best,",
    ]
    body = "\n".join(body_lines)
    return {
        "subject": subject,
        "body": body,
        "word_count": len(body.split()),
    }


# ── Do-Not-Contact ──────────────────────────────────────────────────────

async def check_dnc(db: aiosqlite.Connection, email: str) -> dict | None:
    """Check if an email or its domain is on the DNC list. Returns the blocking entry or None."""
    rows = await db.execute_fetchall(
        "SELECT * FROM dnc_list WHERE email = ?", (email.lower(),))
    if rows:
        r = rows[0]
        return {"id": r["id"], "email": r["email"], "domain": r["domain"],
                "reason": r["reason"], "created_at": r["created_at"]}
    domain = email.lower().split("@")[-1] if "@" in email else None
    if domain:
        rows = await db.execute_fetchall(
            "SELECT * FROM dnc_list WHERE domain = ?", (domain,))
        if rows:
            r = rows[0]
            return {"id": r["id"], "email": r["email"], "domain": r["domain"],
                    "reason": r["reason"], "created_at": r["created_at"]}
    return None


async def add_dnc(db: aiosqlite.Connection, data: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    email = data.get("email", "").lower().strip() or None
    domain = data.get("domain", "").lower().strip() or None
    cur = await db.execute(
        "INSERT INTO dnc_list (email, domain, reason, created_at) VALUES (?,?,?,?)",
        (email, domain, data.get("reason"), now),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM dnc_list WHERE id = ?", (cur.lastrowid,))
    r = rows[0]
    return {"id": r["id"], "email": r["email"], "domain": r["domain"],
            "reason": r["reason"], "created_at": r["created_at"]}


async def list_dnc(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM dnc_list ORDER BY created_at DESC")
    return [{"id": r["id"], "email": r["email"], "domain": r["domain"],
             "reason": r["reason"], "created_at": r["created_at"]} for r in rows]


async def delete_dnc(db: aiosqlite.Connection, dnc_id: int) -> bool:
    cur = await db.execute("DELETE FROM dnc_list WHERE id = ?", (dnc_id,))
    await db.commit()
    return cur.rowcount > 0


# ── Prospects ─────────────────────────────────────────────────────────────

async def create_prospect(db: aiosqlite.Connection, data: dict) -> dict | str:
    # DNC check
    dnc = await check_dnc(db, data["email"])
    if dnc:
        blocked_by = dnc.get("email") or dnc.get("domain")
        return f"blocked_by_dnc:{blocked_by}"
    now = datetime.now(timezone.utc).isoformat()
    tags_json = jsonlib.dumps(data.get("tags", []))
    cur = await db.execute(
        """INSERT INTO prospects (first_name, last_name, email, company, job_title, website,
           linkedin_url, notes, list_id, tags, status, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'new', ?)""",
        (data["first_name"], data["last_name"], data["email"], data["company"],
         data.get("job_title"), data.get("website"), data.get("linkedin_url"),
         data.get("notes"), data.get("list_id"), tags_json, now)
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM prospects WHERE id = ?", (cur.lastrowid,))
    return _prospect_row(rows[0])


async def list_prospects(db: aiosqlite.Connection, list_id: int | None = None,
                         status: str | None = None, tag: str | None = None) -> list[dict]:
    q, params = "SELECT * FROM prospects", []
    conds = []
    if list_id is not None:
        conds.append("list_id = ?"); params.append(list_id)
    if status:
        conds.append("status = ?"); params.append(status)
    if tag:
        conds.append("tags LIKE ?"); params.append(f'%"{tag}"%')
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
    created, skipped, dnc_blocked = 0, 0, 0
    for p in prospects:
        if list_id:
            p["list_id"] = list_id
        result = await create_prospect(db, p)
        if isinstance(result, str) and result.startswith("blocked_by_dnc:"):
            dnc_blocked += 1
        elif isinstance(result, dict):
            created += 1
        else:
            skipped += 1
    return {"created": created, "skipped": skipped, "dnc_blocked": dnc_blocked}


async def create_draft(db: aiosqlite.Connection, prospect_id: int, template_id: int | None,
                       tone: str, context: str | None, value_prop: str, cta: str) -> dict | str:
    prospect = await get_prospect(db, prospect_id)
    if not prospect:
        return {}
    # DNC check before generating draft
    dnc = await check_dnc(db, prospect["email"])
    if dnc:
        return "dnc_blocked"
    draft = generate_draft(prospect, tone, context, value_prop, cta)

    if template_id:
        rows = await db.execute_fetchall("SELECT * FROM templates WHERE id = ?", (template_id,))
        if rows:
            t = rows[0]
            draft["subject"] = _render_template(t["subject_template"], prospect, value_prop, cta)
            draft["body"] = _render_template(t["body_template"], prospect, value_prop, cta)
            draft["word_count"] = len(draft["body"].split())
            await db.execute("UPDATE templates SET times_used = times_used + 1 WHERE id = ?", (template_id,))

    now = datetime.now(timezone.utc).isoformat()
    cur = await db.execute(
        "INSERT INTO draft_log (prospect_id, template_id, tone, subject, body, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (prospect_id, template_id, tone, draft["subject"], draft["body"], now)
    )
    await db.execute("UPDATE usage_stats SET total_drafts = total_drafts + 1 WHERE id = 1")
    await db.commit()
    return {"prospect_id": prospect_id, **draft, "tone": tone, "draft_id": cur.lastrowid}


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
    total_s = (await db.execute_fetchall("SELECT COUNT(*) as cnt FROM sequences"))[0]["cnt"]
    total_dnc = (await db.execute_fetchall("SELECT COUNT(*) as cnt FROM dnc_list"))[0]["cnt"]
    total_sl = (await db.execute_fetchall("SELECT COUNT(*) as cnt FROM smart_lists"))[0]["cnt"]
    stats_row = await db.execute_fetchall("SELECT * FROM usage_stats WHERE id = 1")
    total_drafts = stats_row[0]["total_drafts"] if stats_row else 0
    status_rows = await db.execute_fetchall("SELECT status, COUNT(*) as cnt FROM prospects GROUP BY status")
    by_status = {r["status"]: r["cnt"] for r in status_rows}
    tone_rows = await db.execute_fetchall("SELECT tone, COUNT(*) as cnt FROM draft_log GROUP BY tone ORDER BY cnt DESC LIMIT 1")
    most_used_tone = tone_rows[0]["tone"] if tone_rows else None
    return {
        "total_prospects": total_p, "total_drafts_generated": total_drafts,
        "total_lists": total_l, "total_templates": total_t,
        "total_sequences": total_s, "total_dnc_entries": total_dnc,
        "total_smart_lists": total_sl,
        "by_status": by_status, "most_used_tone": most_used_tone,
    }


async def list_drafts(db: aiosqlite.Connection, prospect_id: int | None = None,
                       tone: str | None = None, limit: int = 50) -> list[dict]:
    q = "SELECT * FROM draft_log WHERE 1=1"
    params: list = []
    if prospect_id is not None:
        q += " AND prospect_id = ?"; params.append(prospect_id)
    if tone:
        q += " AND tone = ?"; params.append(tone)
    q += f" ORDER BY created_at DESC LIMIT {limit}"
    rows = await db.execute_fetchall(q, params)
    return [_draft_log_row(r) for r in rows]


async def get_draft(db: aiosqlite.Connection, draft_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM draft_log WHERE id = ?", (draft_id,))
    return _draft_log_row(rows[0]) if rows else None


def _draft_log_row(r: aiosqlite.Row) -> dict:
    body = r["body"]
    return {
        "id": r["id"], "prospect_id": r["prospect_id"],
        "template_id": r["template_id"], "tone": r["tone"],
        "subject": r["subject"], "body": body,
        "word_count": len(body.split()) if body else 0,
        "created_at": r["created_at"],
    }


async def search_prospects(db: aiosqlite.Connection, q: str) -> list[dict]:
    pattern = f"%{q}%"
    rows = await db.execute_fetchall(
        """SELECT * FROM prospects
           WHERE first_name LIKE ? OR last_name LIKE ? OR email LIKE ?
              OR company LIKE ? OR job_title LIKE ?
           ORDER BY created_at DESC LIMIT 100""",
        (pattern, pattern, pattern, pattern, pattern)
    )
    return [_prospect_row(r) for r in rows]


async def export_prospects_csv(db: aiosqlite.Connection,
                                list_id: int | None = None,
                                status: str | None = None) -> str:
    rows = await list_prospects(db, list_id, status)
    buf = io.StringIO()
    fieldnames = ["id", "first_name", "last_name", "email", "company",
                  "job_title", "website", "linkedin_url", "notes",
                  "list_id", "status", "created_at"]
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    for r in rows:
        writer.writerow({k: r.get(k, "") for k in fieldnames})
    return buf.getvalue()


# ── Sequences ─────────────────────────────────────────────────────────────

async def create_sequence(db: aiosqlite.Connection, data: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    steps_json = jsonlib.dumps(data["steps"])
    cur = await db.execute(
        "INSERT INTO sequences (name, value_prop, cta, steps, created_at) VALUES (?,?,?,?,?)",
        (data["name"], data["value_prop"], data.get("cta", "book a 15-min call"), steps_json, now),
    )
    await db.commit()
    return await _get_sequence(db, cur.lastrowid)


async def list_sequences(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM sequences ORDER BY created_at DESC")
    result = []
    for r in rows:
        enrolled = (await db.execute_fetchall(
            "SELECT COUNT(*) as c FROM enrollments WHERE sequence_id=?", (r["id"],)))[0]["c"]
        result.append({
            "id": r["id"], "name": r["name"], "value_prop": r["value_prop"],
            "cta": r["cta"], "steps": jsonlib.loads(r["steps"]),
            "total_enrolled": enrolled, "created_at": r["created_at"],
        })
    return result


async def _get_sequence(db: aiosqlite.Connection, seq_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM sequences WHERE id=?", (seq_id,))
    if not rows:
        return None
    r = rows[0]
    enrolled = (await db.execute_fetchall(
        "SELECT COUNT(*) as c FROM enrollments WHERE sequence_id=?", (seq_id,)))[0]["c"]
    return {
        "id": r["id"], "name": r["name"], "value_prop": r["value_prop"],
        "cta": r["cta"], "steps": jsonlib.loads(r["steps"]),
        "total_enrolled": enrolled, "created_at": r["created_at"],
    }


async def clone_sequence(db: aiosqlite.Connection, seq_id: int,
                          new_name: str | None = None) -> dict | None:
    """Clone a sequence with all its steps. Enrollments are NOT copied."""
    src = await _get_sequence(db, seq_id)
    if not src:
        return None
    now = datetime.now(timezone.utc).isoformat()
    name = new_name or f"{src['name']} (copy)"
    cur = await db.execute(
        "INSERT INTO sequences (name, value_prop, cta, steps, created_at) VALUES (?,?,?,?,?)",
        (name, src["value_prop"], src["cta"], jsonlib.dumps(src["steps"]), now),
    )
    await db.commit()
    return await _get_sequence(db, cur.lastrowid)


async def enroll_prospect(db: aiosqlite.Connection, seq_id: int, prospect_id: int) -> dict | str | None:
    seq = await _get_sequence(db, seq_id)
    if not seq:
        return None
    prospect = await get_prospect(db, prospect_id)
    if not prospect:
        return "prospect_not_found"
    # DNC check
    dnc = await check_dnc(db, prospect["email"])
    if dnc:
        return "dnc_blocked"
    existing = await db.execute_fetchall(
        "SELECT id FROM enrollments WHERE sequence_id=? AND prospect_id=?", (seq_id, prospect_id))
    if existing:
        return "already_enrolled"
    now = datetime.now(timezone.utc).isoformat()
    cur = await db.execute(
        "INSERT INTO enrollments (sequence_id, prospect_id, current_step, status, enrolled_at) VALUES (?,?,0,'active',?)",
        (seq_id, prospect_id, now),
    )
    await db.commit()
    return await _get_enrollment(db, cur.lastrowid, seq)


async def list_enrollments(db: aiosqlite.Connection, seq_id: int) -> list[dict]:
    seq = await _get_sequence(db, seq_id)
    if not seq:
        return []
    rows = await db.execute_fetchall(
        "SELECT * FROM enrollments WHERE sequence_id=? ORDER BY enrolled_at DESC", (seq_id,))
    result = []
    for r in rows:
        prospect = await get_prospect(db, r["prospect_id"])
        result.append({
            "id": r["id"], "sequence_id": r["sequence_id"],
            "prospect_id": r["prospect_id"],
            "prospect_name": f"{prospect['first_name']} {prospect['last_name']}" if prospect else "unknown",
            "prospect_email": prospect["email"] if prospect else "",
            "current_step": r["current_step"],
            "total_steps": len(seq["steps"]),
            "status": r["status"],
            "enrolled_at": r["enrolled_at"],
            "last_advanced_at": r["last_advanced_at"],
        })
    return result


async def _get_enrollment(db: aiosqlite.Connection, enroll_id: int, seq: dict) -> dict:
    rows = await db.execute_fetchall("SELECT * FROM enrollments WHERE id=?", (enroll_id,))
    r = rows[0]
    prospect = await get_prospect(db, r["prospect_id"])
    return {
        "id": r["id"], "sequence_id": r["sequence_id"],
        "prospect_id": r["prospect_id"],
        "prospect_name": f"{prospect['first_name']} {prospect['last_name']}" if prospect else "unknown",
        "prospect_email": prospect["email"] if prospect else "",
        "current_step": r["current_step"],
        "total_steps": len(seq["steps"]),
        "status": r["status"],
        "enrolled_at": r["enrolled_at"],
        "last_advanced_at": r["last_advanced_at"],
    }


async def advance_sequence(db: aiosqlite.Connection, seq_id: int) -> dict | None:
    seq = await _get_sequence(db, seq_id)
    if not seq:
        return None
    steps = seq["steps"]
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    rows = await db.execute_fetchall(
        "SELECT * FROM enrollments WHERE sequence_id=? AND status='active'", (seq_id,))

    advanced = 0
    already_complete = 0
    not_due = 0
    drafts_generated = []

    for r in rows:
        step_idx = r["current_step"]
        if step_idx >= len(steps):
            await db.execute("UPDATE enrollments SET status='completed' WHERE id=?", (r["id"],))
            already_complete += 1
            continue

        step = steps[step_idx]
        ref_time = r["last_advanced_at"] or r["enrolled_at"]
        ref_dt = datetime.fromisoformat(ref_time)
        delay = timedelta(days=step["delay_days"])

        if now < ref_dt + delay:
            not_due += 1
            continue

        prospect = await get_prospect(db, r["prospect_id"])
        if not prospect:
            continue

        draft = generate_followup_draft(
            prospect, step["tone"], step_idx,
            step["subject_hint"], seq["value_prop"], seq["cta"],
        )
        cur = await db.execute(
            "INSERT INTO draft_log (prospect_id, template_id, tone, subject, body, created_at) VALUES (?,?,?,?,?,?)",
            (r["prospect_id"], None, step["tone"], draft["subject"], draft["body"], now_iso),
        )
        await db.execute("UPDATE usage_stats SET total_drafts = total_drafts + 1 WHERE id = 1")
        drafts_generated.append(cur.lastrowid)

        next_step = step_idx + 1
        new_status = "completed" if next_step >= len(steps) else "active"
        await db.execute(
            "UPDATE enrollments SET current_step=?, status=?, last_advanced_at=? WHERE id=?",
            (next_step, new_status, now_iso, r["id"]),
        )
        advanced += 1

    await db.commit()
    return {
        "advanced": advanced,
        "already_complete": already_complete,
        "not_due": not_due,
        "drafts_generated": drafts_generated,
    }


# ── Tags ─────────────────────────────────────────────────────────────────

async def add_prospect_tag(db: aiosqlite.Connection, prospect_id: int, tag: str) -> dict | None:
    rows = await db.execute_fetchall("SELECT tags FROM prospects WHERE id = ?", (prospect_id,))
    if not rows:
        return None
    current = jsonlib.loads(rows[0]["tags"]) if rows[0]["tags"] else []
    tag = tag.strip().lower()
    if tag not in current:
        current.append(tag)
        await db.execute("UPDATE prospects SET tags = ? WHERE id = ?", (jsonlib.dumps(current), prospect_id))
        await db.commit()
    return await get_prospect(db, prospect_id)


async def remove_prospect_tag(db: aiosqlite.Connection, prospect_id: int, tag: str) -> dict | None:
    rows = await db.execute_fetchall("SELECT tags FROM prospects WHERE id = ?", (prospect_id,))
    if not rows:
        return None
    current = jsonlib.loads(rows[0]["tags"]) if rows[0]["tags"] else []
    tag = tag.strip().lower()
    if tag in current:
        current.remove(tag)
        await db.execute("UPDATE prospects SET tags = ? WHERE id = ?", (jsonlib.dumps(current), prospect_id))
        await db.commit()
    return await get_prospect(db, prospect_id)


# ── Campaign Events ──────────────────────────────────────────────────────

async def record_event(db: aiosqlite.Connection, data: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    cur = await db.execute(
        "INSERT INTO campaign_events (prospect_id, sequence_id, draft_id, event_type, metadata, created_at) VALUES (?,?,?,?,?,?)",
        (data["prospect_id"], data.get("sequence_id"), data.get("draft_id"),
         data["event_type"], jsonlib.dumps(data.get("metadata")) if data.get("metadata") else None, now),
    )
    await db.commit()
    return {"id": cur.lastrowid, "event_type": data["event_type"], "created_at": now}


async def get_sequence_analytics(db: aiosqlite.Connection, seq_id: int) -> dict | None:
    seq = await _get_sequence(db, seq_id)
    if not seq:
        return None
    enrolled = (await db.execute_fetchall(
        "SELECT COUNT(*) as c FROM enrollments WHERE sequence_id=?", (seq_id,)))[0]["c"]
    completed = (await db.execute_fetchall(
        "SELECT COUNT(*) as c FROM enrollments WHERE sequence_id=? AND status='completed'", (seq_id,)))[0]["c"]
    event_rows = await db.execute_fetchall(
        "SELECT event_type, COUNT(*) as cnt FROM campaign_events WHERE sequence_id=? GROUP BY event_type",
        (seq_id,),
    )
    events = {r["event_type"]: r["cnt"] for r in event_rows}
    sent = events.get("sent", 0)
    opened = events.get("opened", 0)
    replied = events.get("replied", 0)
    bounced = events.get("bounced", 0)
    return {
        "sequence_id": seq_id,
        "sequence_name": seq["name"],
        "total_enrolled": enrolled,
        "completed": completed,
        "events": events,
        "sent": sent, "opened": opened, "replied": replied, "bounced": bounced,
        "open_rate": round(opened / max(sent, 1) * 100, 1),
        "reply_rate": round(replied / max(sent, 1) * 100, 1),
        "bounce_rate": round(bounced / max(sent, 1) * 100, 1),
    }


async def get_campaign_overview(db: aiosqlite.Connection) -> dict:
    total_events = (await db.execute_fetchall("SELECT COUNT(*) as c FROM campaign_events"))[0]["c"]
    event_rows = await db.execute_fetchall(
        "SELECT event_type, COUNT(*) as cnt FROM campaign_events GROUP BY event_type"
    )
    by_type = {r["event_type"]: r["cnt"] for r in event_rows}
    sent = by_type.get("sent", 0)
    opened = by_type.get("opened", 0)
    replied = by_type.get("replied", 0)
    seq_rows = await db.execute_fetchall(
        """SELECT s.id, s.name, COUNT(e.id) as event_count
           FROM sequences s LEFT JOIN campaign_events e ON s.id = e.sequence_id
           GROUP BY s.id ORDER BY event_count DESC LIMIT 5"""
    )
    top_sequences = [{"id": r["id"], "name": r["name"], "events": r["event_count"]} for r in seq_rows]
    return {
        "total_events": total_events,
        "by_type": by_type,
        "sent": sent, "opened": opened, "replied": replied,
        "overall_open_rate": round(opened / max(sent, 1) * 100, 1),
        "overall_reply_rate": round(replied / max(sent, 1) * 100, 1),
        "top_sequences": top_sequences,
    }


# ── Lead Scoring ─────────────────────────────────────────────────────────

async def compute_lead_score(db: aiosqlite.Connection, prospect_id: int) -> dict | None:
    prospect = await get_prospect(db, prospect_id)
    if not prospect:
        return None

    score = 0
    breakdown = {}

    fields = ["job_title", "website", "linkedin_url", "notes"]
    filled = sum(1 for f in fields if prospect.get(f))
    profile_pts = round(filled / len(fields) * 25)
    score += profile_pts
    breakdown["profile_completeness"] = profile_pts

    events = await db.execute_fetchall(
        "SELECT event_type, COUNT(*) as cnt FROM campaign_events WHERE prospect_id = ? GROUP BY event_type",
        (prospect_id,),
    )
    event_map = {r["event_type"]: r["cnt"] for r in events}
    sent = event_map.get("sent", 0)
    opened = event_map.get("opened", 0)
    replied = event_map.get("replied", 0)
    clicked = event_map.get("clicked", 0)
    bounced = event_map.get("bounced", 0)

    engagement_pts = 0
    if replied > 0:
        engagement_pts += min(replied * 15, 25)
    if clicked > 0:
        engagement_pts += min(clicked * 10, 10)
    if opened > 0:
        engagement_pts += min(opened * 3, 10)
    if bounced > 0:
        engagement_pts -= min(bounced * 10, 15)
    engagement_pts = max(0, min(45, engagement_pts))
    score += engagement_pts
    breakdown["engagement"] = engagement_pts

    last_event = await db.execute_fetchall(
        "SELECT MAX(created_at) as last_at FROM campaign_events WHERE prospect_id = ?",
        (prospect_id,),
    )
    recency_pts = 0
    if last_event and last_event[0]["last_at"]:
        last_dt = datetime.fromisoformat(last_event[0]["last_at"])
        days_ago = (datetime.now(timezone.utc) - last_dt).days
        if days_ago <= 7:
            recency_pts = 15
        elif days_ago <= 14:
            recency_pts = 10
        elif days_ago <= 30:
            recency_pts = 5
    score += recency_pts
    breakdown["recency"] = recency_pts

    tags = prospect.get("tags", [])
    tag_pts = min(len(tags) * 3, 10)
    score += tag_pts
    breakdown["tags"] = tag_pts

    enrollments = await db.execute_fetchall(
        "SELECT COUNT(*) as cnt FROM enrollments WHERE prospect_id = ?", (prospect_id,))
    enroll_pts = min(enrollments[0]["cnt"] * 5, 5) if enrollments else 0
    score += enroll_pts
    breakdown["enrollment"] = enroll_pts

    score = min(score, 100)

    if score >= 70:
        grade = "hot"
    elif score >= 40:
        grade = "warm"
    else:
        grade = "cold"

    return {
        "prospect_id": prospect_id,
        "score": score, "grade": grade,
        "breakdown": breakdown,
        "events_summary": {"sent": sent, "opened": opened, "replied": replied, "clicked": clicked, "bounced": bounced},
    }


async def get_top_leads(db: aiosqlite.Connection, limit: int = 20) -> list[dict]:
    rows = await db.execute_fetchall("SELECT id FROM prospects ORDER BY created_at DESC LIMIT 200")
    scored = []
    for r in rows:
        s = await compute_lead_score(db, r["id"])
        if s:
            p = await get_prospect(db, r["id"])
            scored.append({
                "prospect_id": r["id"],
                "name": f"{p['first_name']} {p['last_name']}",
                "email": p["email"], "company": p["company"],
                "score": s["score"], "grade": s["grade"], "status": p["status"],
            })
    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored[:limit]


# ── Pipeline Stages ──────────────────────────────────────────────────────

VALID_STAGES = {"new", "contacted", "interested", "qualified", "converted", "lost"}


async def update_prospect_stage(db: aiosqlite.Connection, prospect_id: int,
                                 stage: str, notes: str | None = None) -> dict | str | None:
    if stage not in VALID_STAGES:
        return f"Invalid stage. Must be one of: {', '.join(sorted(VALID_STAGES))}"
    prospect = await get_prospect(db, prospect_id)
    if not prospect:
        return None
    old_stage = prospect["status"]
    await db.execute("UPDATE prospects SET status = ? WHERE id = ?", (stage, prospect_id))
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        "INSERT INTO campaign_events (prospect_id, sequence_id, draft_id, event_type, metadata, created_at) VALUES (?,?,?,?,?,?)",
        (prospect_id, None, None, "stage_change",
         jsonlib.dumps({"from": old_stage, "to": stage, "notes": notes}), now),
    )
    await db.commit()
    return await get_prospect(db, prospect_id)


async def get_pipeline_summary(db: aiosqlite.Connection) -> dict:
    rows = await db.execute_fetchall(
        "SELECT status, COUNT(*) as cnt FROM prospects GROUP BY status ORDER BY cnt DESC"
    )
    stages = {r["status"]: r["cnt"] for r in rows}
    total = sum(stages.values())
    funnel = []
    for stage in ["new", "contacted", "interested", "qualified", "converted", "lost"]:
        count = stages.get(stage, 0)
        funnel.append({
            "stage": stage, "count": count,
            "pct": round(count / max(total, 1) * 100, 1),
        })
    converted = stages.get("converted", 0)
    contacted = stages.get("contacted", 0) + stages.get("interested", 0) + stages.get("qualified", 0) + converted
    return {
        "total_prospects": total,
        "stages": funnel,
        "conversion_rate": round(converted / max(contacted, 1) * 100, 1) if contacted else 0,
    }


# ── Tone A/B Analytics ──────────────────────────────────────────────────

async def get_tone_analytics(db: aiosqlite.Connection) -> list[dict]:
    tones = await db.execute_fetchall(
        "SELECT tone, COUNT(*) as drafts FROM draft_log GROUP BY tone ORDER BY drafts DESC"
    )
    result = []
    for t in tones:
        tone = t["tone"]
        draft_ids = await db.execute_fetchall(
            "SELECT id FROM draft_log WHERE tone = ?", (tone,))
        ids = [d["id"] for d in draft_ids]
        if not ids:
            result.append({"tone": tone, "drafts": t["drafts"],
                          "sent": 0, "opened": 0, "replied": 0,
                          "open_rate": 0, "reply_rate": 0})
            continue
        placeholders = ",".join("?" * len(ids))
        events = await db.execute_fetchall(
            f"SELECT event_type, COUNT(*) as cnt FROM campaign_events WHERE draft_id IN ({placeholders}) GROUP BY event_type",
            ids,
        )
        ev = {r["event_type"]: r["cnt"] for r in events}
        sent = ev.get("sent", 0)
        opened = ev.get("opened", 0)
        replied = ev.get("replied", 0)
        result.append({
            "tone": tone, "drafts": t["drafts"],
            "sent": sent, "opened": opened, "replied": replied,
            "open_rate": round(opened / max(sent, 1) * 100, 1),
            "reply_rate": round(replied / max(sent, 1) * 100, 1),
        })
    return result


# ── Prospect Activity Log ────────────────────────────────────────────────

async def get_prospect_activity(db: aiosqlite.Connection, prospect_id: int) -> dict | None:
    """Full timeline of all interactions with a prospect."""
    prospect = await get_prospect(db, prospect_id)
    if not prospect:
        return None

    activity = []

    # Prospect creation
    activity.append({
        "type": "prospect_created",
        "timestamp": prospect["created_at"],
        "detail": f"Added to system (status: {prospect['status']})",
        "metadata": None,
    })

    # Drafts generated
    drafts = await db.execute_fetchall(
        "SELECT * FROM draft_log WHERE prospect_id = ? ORDER BY created_at ASC",
        (prospect_id,),
    )
    for d in drafts:
        activity.append({
            "type": "draft_generated",
            "timestamp": d["created_at"],
            "detail": f"Draft #{d['id']}: \"{d['subject']}\" (tone: {d['tone']})",
            "metadata": {"draft_id": d["id"], "tone": d["tone"]},
        })

    # Campaign events
    events = await db.execute_fetchall(
        "SELECT * FROM campaign_events WHERE prospect_id = ? ORDER BY created_at ASC",
        (prospect_id,),
    )
    for e in events:
        meta = jsonlib.loads(e["metadata"]) if e["metadata"] else {}
        if e["event_type"] == "stage_change":
            detail = f"Stage: {meta.get('from', '?')} -> {meta.get('to', '?')}"
            if meta.get("notes"):
                detail += f" ({meta['notes']})"
        else:
            detail = f"Event: {e['event_type']}"
            if e["sequence_id"]:
                detail += f" (sequence #{e['sequence_id']})"
        activity.append({
            "type": e["event_type"],
            "timestamp": e["created_at"],
            "detail": detail,
            "metadata": meta or None,
        })

    # Enrollments
    enrollments = await db.execute_fetchall(
        "SELECT e.*, s.name as seq_name FROM enrollments e JOIN sequences s ON e.sequence_id = s.id WHERE e.prospect_id = ? ORDER BY e.enrolled_at ASC",
        (prospect_id,),
    )
    for en in enrollments:
        activity.append({
            "type": "sequence_enrolled",
            "timestamp": en["enrolled_at"],
            "detail": f"Enrolled in sequence \"{en['seq_name']}\" (status: {en['status']}, step {en['current_step']})",
            "metadata": {"sequence_id": en["sequence_id"], "status": en["status"]},
        })

    activity.sort(key=lambda a: a["timestamp"])

    return {
        "prospect_id": prospect_id,
        "prospect_name": f"{prospect['first_name']} {prospect['last_name']}",
        "total_events": len(activity),
        "activity": activity,
    }


# ── Smart Lists ──────────────────────────────────────────────────────────

async def _smart_list_count(db: aiosqlite.Connection, filters: dict) -> int:
    """Count prospects matching smart list filters."""
    q = "SELECT COUNT(*) as cnt FROM prospects WHERE 1=1"
    params: list = []
    if filters.get("status"):
        q += " AND status = ?"
        params.append(filters["status"])
    if filters.get("list_id") is not None:
        q += " AND list_id = ?"
        params.append(filters["list_id"])
    if filters.get("tag"):
        q += " AND tags LIKE ?"
        params.append(f'%"{filters["tag"]}"%')
    if filters.get("company_contains"):
        q += " AND company LIKE ?"
        params.append(f'%{filters["company_contains"]}%')
    if filters.get("job_title_contains"):
        q += " AND job_title LIKE ?"
        params.append(f'%{filters["job_title_contains"]}%')
    rows = await db.execute_fetchall(q, params)
    return rows[0]["cnt"] if rows else 0


async def _smart_list_prospects(db: aiosqlite.Connection, filters: dict) -> list[dict]:
    """Get prospects matching smart list filters."""
    q = "SELECT * FROM prospects WHERE 1=1"
    params: list = []
    if filters.get("status"):
        q += " AND status = ?"
        params.append(filters["status"])
    if filters.get("list_id") is not None:
        q += " AND list_id = ?"
        params.append(filters["list_id"])
    if filters.get("tag"):
        q += " AND tags LIKE ?"
        params.append(f'%"{filters["tag"]}"%')
    if filters.get("company_contains"):
        q += " AND company LIKE ?"
        params.append(f'%{filters["company_contains"]}%')
    if filters.get("job_title_contains"):
        q += " AND job_title LIKE ?"
        params.append(f'%{filters["job_title_contains"]}%')
    q += " ORDER BY created_at DESC"
    rows = await db.execute_fetchall(q, params)
    return [_prospect_row(r) for r in rows]


async def create_smart_list(db: aiosqlite.Connection, data: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    filters = data["filters"]
    cur = await db.execute(
        "INSERT INTO smart_lists (name, filters, created_at) VALUES (?,?,?)",
        (data["name"], jsonlib.dumps(filters), now),
    )
    await db.commit()
    count = await _smart_list_count(db, filters)
    return {
        "id": cur.lastrowid,
        "name": data["name"],
        "filters": filters,
        "matching_count": count,
        "created_at": now,
    }


async def list_smart_lists(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM smart_lists ORDER BY created_at DESC")
    result = []
    for r in rows:
        filters = jsonlib.loads(r["filters"])
        count = await _smart_list_count(db, filters)
        result.append({
            "id": r["id"],
            "name": r["name"],
            "filters": filters,
            "matching_count": count,
            "created_at": r["created_at"],
        })
    return result


async def get_smart_list(db: aiosqlite.Connection, smart_list_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM smart_lists WHERE id = ?", (smart_list_id,))
    if not rows:
        return None
    r = rows[0]
    filters = jsonlib.loads(r["filters"])
    count = await _smart_list_count(db, filters)
    return {
        "id": r["id"],
        "name": r["name"],
        "filters": filters,
        "matching_count": count,
        "created_at": r["created_at"],
    }


async def get_smart_list_prospects(db: aiosqlite.Connection, smart_list_id: int) -> list[dict] | None:
    rows = await db.execute_fetchall("SELECT * FROM smart_lists WHERE id = ?", (smart_list_id,))
    if not rows:
        return None
    filters = jsonlib.loads(rows[0]["filters"])
    return await _smart_list_prospects(db, filters)


async def delete_smart_list(db: aiosqlite.Connection, smart_list_id: int) -> bool:
    cur = await db.execute("DELETE FROM smart_lists WHERE id = ?", (smart_list_id,))
    await db.commit()
    return cur.rowcount > 0


# ── Enrollment Pause/Resume ──────────────────────────────────────────────

async def pause_enrollment(db: aiosqlite.Connection, enrollment_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM enrollments WHERE id = ?", (enrollment_id,))
    if not rows:
        return None
    r = rows[0]
    if r["status"] != "active":
        return {"enrollment_id": enrollment_id, "status": r["status"],
                "message": f"Cannot pause: enrollment is {r['status']}"}
    await db.execute("UPDATE enrollments SET status = 'paused' WHERE id = ?", (enrollment_id,))
    await db.commit()
    return {"enrollment_id": enrollment_id, "status": "paused", "message": "Enrollment paused"}


async def resume_enrollment(db: aiosqlite.Connection, enrollment_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM enrollments WHERE id = ?", (enrollment_id,))
    if not rows:
        return None
    r = rows[0]
    if r["status"] != "paused":
        return {"enrollment_id": enrollment_id, "status": r["status"],
                "message": f"Cannot resume: enrollment is {r['status']}"}
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        "UPDATE enrollments SET status = 'active', last_advanced_at = ? WHERE id = ?",
        (now, enrollment_id),
    )
    await db.commit()
    return {"enrollment_id": enrollment_id, "status": "active", "message": "Enrollment resumed"}


async def pause_all_enrollments(db: aiosqlite.Connection, seq_id: int) -> dict | None:
    seq = await _get_sequence(db, seq_id)
    if not seq:
        return None
    cur = await db.execute(
        "UPDATE enrollments SET status = 'paused' WHERE sequence_id = ? AND status = 'active'",
        (seq_id,),
    )
    await db.commit()
    return {"affected": cur.rowcount, "skipped": 0}


async def resume_all_enrollments(db: aiosqlite.Connection, seq_id: int) -> dict | None:
    seq = await _get_sequence(db, seq_id)
    if not seq:
        return None
    now = datetime.now(timezone.utc).isoformat()
    cur = await db.execute(
        "UPDATE enrollments SET status = 'active', last_advanced_at = ? WHERE sequence_id = ? AND status = 'paused'",
        (now, seq_id),
    )
    await db.commit()
    return {"affected": cur.rowcount, "skipped": 0}


# ── Prospect Merge ───────────────────────────────────────────────────────

async def find_duplicates(db: aiosqlite.Connection) -> list[dict]:
    """Find potential duplicate prospects by email."""
    rows = await db.execute_fetchall("""
        SELECT email, COUNT(*) as cnt
        FROM prospects
        GROUP BY LOWER(email)
        HAVING cnt > 1
        ORDER BY cnt DESC
        LIMIT 50
    """)
    groups = []
    for r in rows:
        prospects = await db.execute_fetchall(
            "SELECT id, first_name, last_name, email FROM prospects WHERE LOWER(email) = LOWER(?)",
            (r["email"],),
        )
        groups.append({
            "key": r["email"].lower(),
            "prospect_ids": [p["id"] for p in prospects],
            "names": [f"{p['first_name']} {p['last_name']}" for p in prospects],
            "emails": [p["email"] for p in prospects],
        })

    # Also check same first_name + last_name + company
    name_dupes = await db.execute_fetchall("""
        SELECT LOWER(first_name) || '|' || LOWER(last_name) || '|' || LOWER(company) as key,
               COUNT(*) as cnt
        FROM prospects
        GROUP BY LOWER(first_name), LOWER(last_name), LOWER(company)
        HAVING cnt > 1
        ORDER BY cnt DESC
        LIMIT 50
    """)
    seen_keys = {g["key"] for g in groups}
    for r in name_dupes:
        parts = r["key"].split("|")
        prospects = await db.execute_fetchall(
            "SELECT id, first_name, last_name, email FROM prospects WHERE LOWER(first_name) = ? AND LOWER(last_name) = ? AND LOWER(company) = ?",
            (parts[0], parts[1], parts[2]),
        )
        key = f"name:{r['key']}"
        if key not in seen_keys:
            groups.append({
                "key": key,
                "prospect_ids": [p["id"] for p in prospects],
                "names": [f"{p['first_name']} {p['last_name']}" for p in prospects],
                "emails": [p["email"] for p in prospects],
            })
    return groups


async def merge_prospects(db: aiosqlite.Connection, keep_id: int, merge_id: int) -> dict | str | None:
    """Merge merge_id prospect into keep_id. Transfers all data, deletes merge_id."""
    if keep_id == merge_id:
        return "same_prospect"
    keep = await get_prospect(db, keep_id)
    if not keep:
        return None
    merge = await get_prospect(db, merge_id)
    if not merge:
        return "merge_not_found"

    # Transfer draft_log
    cur_drafts = await db.execute(
        "UPDATE draft_log SET prospect_id = ? WHERE prospect_id = ?",
        (keep_id, merge_id),
    )
    drafts_transferred = cur_drafts.rowcount

    # Transfer campaign_events
    cur_events = await db.execute(
        "UPDATE campaign_events SET prospect_id = ? WHERE prospect_id = ?",
        (keep_id, merge_id),
    )
    events_transferred = cur_events.rowcount

    # Transfer enrollments (skip if would violate uniqueness)
    existing_seqs = await db.execute_fetchall(
        "SELECT sequence_id FROM enrollments WHERE prospect_id = ?", (keep_id,))
    existing_seq_ids = {r["sequence_id"] for r in existing_seqs}

    merge_enrollments = await db.execute_fetchall(
        "SELECT * FROM enrollments WHERE prospect_id = ?", (merge_id,))
    enrollments_transferred = 0
    for e in merge_enrollments:
        if e["sequence_id"] not in existing_seq_ids:
            await db.execute(
                "UPDATE enrollments SET prospect_id = ? WHERE id = ?",
                (keep_id, e["id"]),
            )
            enrollments_transferred += 1
    # Delete remaining duplicate enrollments
    await db.execute("DELETE FROM enrollments WHERE prospect_id = ?", (merge_id,))

    # Merge tags
    keep_tags = set(keep.get("tags", []))
    merge_tags = set(merge.get("tags", []))
    combined = list(keep_tags | merge_tags)
    await db.execute(
        "UPDATE prospects SET tags = ? WHERE id = ?",
        (jsonlib.dumps(combined), keep_id),
    )

    # Merge notes
    if merge.get("notes") and not keep.get("notes"):
        await db.execute("UPDATE prospects SET notes = ? WHERE id = ?", (merge["notes"], keep_id))
    elif merge.get("notes") and keep.get("notes"):
        combined_notes = f"{keep['notes']}\n[merged] {merge['notes']}"
        await db.execute("UPDATE prospects SET notes = ? WHERE id = ?", (combined_notes, keep_id))

    # Fill in missing fields from merge prospect
    for field in ("job_title", "website", "linkedin_url"):
        if not keep.get(field) and merge.get(field):
            await db.execute(f"UPDATE prospects SET {field} = ? WHERE id = ?", (merge[field], keep_id))

    # Delete the merged prospect
    await db.execute("DELETE FROM prospects WHERE id = ?", (merge_id,))
    await db.commit()

    return {
        "kept_prospect_id": keep_id,
        "merged_prospect_id": merge_id,
        "transferred_drafts": drafts_transferred,
        "transferred_events": events_transferred,
        "transferred_enrollments": enrollments_transferred,
    }
