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

CREATE TABLE IF NOT EXISTS prospect_notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    prospect_id INTEGER NOT NULL REFERENCES prospects(id) ON DELETE CASCADE,
    author TEXT NOT NULL,
    content TEXT NOT NULL,
    pinned INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snippets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    content TEXT NOT NULL,
    category TEXT NOT NULL DEFAULT 'general',
    times_used INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ab_tests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    sequence_id INTEGER REFERENCES sequences(id),
    variant_a_tone TEXT NOT NULL,
    variant_a_subject_hint TEXT NOT NULL,
    variant_b_tone TEXT NOT NULL,
    variant_b_subject_hint TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    winner TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS ab_test_assignments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    test_id INTEGER NOT NULL REFERENCES ab_tests(id),
    prospect_id INTEGER NOT NULL REFERENCES prospects(id),
    variant TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(test_id, prospect_id)
);

CREATE TABLE IF NOT EXISTS segments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    criteria TEXT NOT NULL,
    auto_assign INTEGER NOT NULL DEFAULT 0,
    prospect_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS automation_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    trigger_type TEXT NOT NULL,
    trigger_config TEXT NOT NULL,
    action_type TEXT NOT NULL,
    action_config TEXT NOT NULL,
    is_enabled INTEGER NOT NULL DEFAULT 1,
    times_fired INTEGER NOT NULL DEFAULT 0,
    last_fired_at TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_prospect ON campaign_events(prospect_id);
CREATE INDEX IF NOT EXISTS idx_events_sequence ON campaign_events(sequence_id);
CREATE INDEX IF NOT EXISTS idx_dnc_email ON dnc_list(email);
CREATE INDEX IF NOT EXISTS idx_dnc_domain ON dnc_list(domain);
CREATE INDEX IF NOT EXISTS idx_prospects_email ON prospects(email);
CREATE INDEX IF NOT EXISTS idx_prospects_company ON prospects(company);
CREATE INDEX IF NOT EXISTS idx_pnotes_prospect ON prospect_notes(prospect_id, created_at);
CREATE INDEX IF NOT EXISTS idx_snippets_category ON snippets(category);
CREATE INDEX IF NOT EXISTS idx_ab_assignments_test ON ab_test_assignments(test_id);
CREATE INDEX IF NOT EXISTS idx_segments_auto ON segments(auto_assign);
CREATE INDEX IF NOT EXISTS idx_automation_trigger ON automation_rules(trigger_type, is_enabled);

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


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def init_db(path: str) -> aiosqlite.Connection:
    db = await aiosqlite.connect(path)
    db.row_factory = aiosqlite.Row
    await db.executescript(SQL)
    # Migration: add tags column if missing
    try:
        await db.execute("SELECT tags FROM prospects LIMIT 1")
    except Exception:
        await db.execute("ALTER TABLE prospects ADD COLUMN tags TEXT NOT NULL DEFAULT '[]'")
    # Migration: ab_tests table (idempotent via CREATE TABLE IF NOT EXISTS in SQL above)
    # Migration: segments table (idempotent via CREATE TABLE IF NOT EXISTS in SQL above)
    # Migration: automation_rules table (idempotent via CREATE TABLE IF NOT EXISTS in SQL above)
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
    now = _now()
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
    now = _now()
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
    prospect = await get_prospect(db, prospect_id)
    if not prospect:
        return None
    old_status = prospect["status"]
    await db.execute("UPDATE prospects SET status = ? WHERE id = ?", (status, prospect_id))
    await db.commit()
    # Trigger automation for status_change
    await evaluate_automation_for_prospect(
        db, prospect_id, "status_change",
        {"from_status": old_status, "to_status": status},
    )
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

    now = _now()
    cur = await db.execute(
        "INSERT INTO draft_log (prospect_id, template_id, tone, subject, body, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (prospect_id, template_id, tone, draft["subject"], draft["body"], now)
    )
    await db.execute("UPDATE usage_stats SET total_drafts = total_drafts + 1 WHERE id = 1")
    await db.commit()
    return {"prospect_id": prospect_id, **draft, "tone": tone, "draft_id": cur.lastrowid}


async def create_template(db: aiosqlite.Connection, data: dict) -> dict:
    now = _now()
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
    now = _now()
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
    total_seg = (await db.execute_fetchall("SELECT COUNT(*) as cnt FROM segments"))[0]["cnt"]
    total_ab = (await db.execute_fetchall("SELECT COUNT(*) as cnt FROM ab_tests"))[0]["cnt"]
    total_auto = (await db.execute_fetchall("SELECT COUNT(*) as cnt FROM automation_rules"))[0]["cnt"]
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
        "total_segments": total_seg,
        "total_ab_tests": total_ab,
        "total_automation_rules": total_auto,
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
    now = _now()
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
    now = _now()
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
    now = _now()
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
            # Trigger automation: enrollment_completed
            await evaluate_automation_for_prospect(
                db, r["prospect_id"], "enrollment_completed",
                {"sequence_id": seq_id},
            )
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

        # If enrollment just completed, trigger automation
        if new_status == "completed":
            await evaluate_automation_for_prospect(
                db, r["prospect_id"], "enrollment_completed",
                {"sequence_id": seq_id},
            )

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
    now = _now()
    cur = await db.execute(
        "INSERT INTO campaign_events (prospect_id, sequence_id, draft_id, event_type, metadata, created_at) VALUES (?,?,?,?,?,?)",
        (data["prospect_id"], data.get("sequence_id"), data.get("draft_id"),
         data["event_type"], jsonlib.dumps(data.get("metadata")) if data.get("metadata") else None, now),
    )
    await db.commit()
    # Trigger automation: event_received
    await evaluate_automation_for_prospect(
        db, data["prospect_id"], "event_received",
        {"event_type": data["event_type"]},
    )
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
    now = _now()
    await db.execute(
        "INSERT INTO campaign_events (prospect_id, sequence_id, draft_id, event_type, metadata, created_at) VALUES (?,?,?,?,?,?)",
        (prospect_id, None, None, "stage_change",
         jsonlib.dumps({"from": old_stage, "to": stage, "notes": notes}), now),
    )
    await db.commit()
    # Trigger automation for status_change
    await evaluate_automation_for_prospect(
        db, prospect_id, "status_change",
        {"from_status": old_stage, "to_status": stage},
    )
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

    # Prospect notes
    notes = await db.execute_fetchall(
        "SELECT * FROM prospect_notes WHERE prospect_id = ? ORDER BY created_at ASC",
        (prospect_id,),
    )
    for n in notes:
        pin_label = " [pinned]" if n["pinned"] else ""
        activity.append({
            "type": "note_added",
            "timestamp": n["created_at"],
            "detail": f"Note by {n['author']}{pin_label}: {n['content'][:80]}{'...' if len(n['content']) > 80 else ''}",
            "metadata": {"note_id": n["id"], "author": n["author"], "pinned": bool(n["pinned"])},
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
    now = _now()
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
    now = _now()
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
    now = _now()
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

    # Transfer prospect_notes
    await db.execute(
        "UPDATE prospect_notes SET prospect_id = ? WHERE prospect_id = ?",
        (keep_id, merge_id),
    )

    # Transfer AB test assignments
    await db.execute(
        "UPDATE OR IGNORE ab_test_assignments SET prospect_id = ? WHERE prospect_id = ?",
        (keep_id, merge_id),
    )
    await db.execute(
        "DELETE FROM ab_test_assignments WHERE prospect_id = ?", (merge_id,),
    )

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


# ── Prospect Notes ───────────────────────────────────────────────────────

def _note_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"],
        "prospect_id": r["prospect_id"],
        "author": r["author"],
        "content": r["content"],
        "pinned": bool(r["pinned"]),
        "created_at": r["created_at"],
    }


async def add_prospect_note(db: aiosqlite.Connection, prospect_id: int,
                             author: str, content: str) -> dict | None:
    """Add a threaded note to a prospect. Returns None if prospect does not exist."""
    prospect = await get_prospect(db, prospect_id)
    if not prospect:
        return None
    now = _now()
    cur = await db.execute(
        "INSERT INTO prospect_notes (prospect_id, author, content, pinned, created_at) VALUES (?,?,?,0,?)",
        (prospect_id, author, content, now),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM prospect_notes WHERE id = ?", (cur.lastrowid,))
    return _note_row(rows[0])


async def list_prospect_notes(db: aiosqlite.Connection, prospect_id: int) -> list[dict]:
    """List notes for a prospect: pinned first, then by created_at DESC."""
    rows = await db.execute_fetchall(
        "SELECT * FROM prospect_notes WHERE prospect_id = ? ORDER BY pinned DESC, created_at DESC",
        (prospect_id,),
    )
    return [_note_row(r) for r in rows]


async def delete_prospect_note(db: aiosqlite.Connection, note_id: int) -> bool:
    cur = await db.execute("DELETE FROM prospect_notes WHERE id = ?", (note_id,))
    await db.commit()
    return cur.rowcount > 0


async def toggle_pin_note(db: aiosqlite.Connection, note_id: int) -> dict | None:
    """Toggle the pinned state of a note. Returns updated note or None if not found."""
    rows = await db.execute_fetchall("SELECT * FROM prospect_notes WHERE id = ?", (note_id,))
    if not rows:
        return None
    new_pinned = 0 if rows[0]["pinned"] else 1
    await db.execute("UPDATE prospect_notes SET pinned = ? WHERE id = ?", (new_pinned, note_id))
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM prospect_notes WHERE id = ?", (note_id,))
    return _note_row(rows[0])


# ── Email Snippets ───────────────────────────────────────────────────────

VALID_SNIPPET_CATEGORIES = {"general", "opener", "closer", "value_prop", "social_proof", "cta", "objection_handler"}


def _snippet_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"],
        "name": r["name"],
        "content": r["content"],
        "category": r["category"],
        "times_used": r["times_used"],
        "created_at": r["created_at"],
    }


async def create_snippet(db: aiosqlite.Connection, data: dict) -> dict:
    now = _now()
    category = data.get("category", "general")
    if category not in VALID_SNIPPET_CATEGORIES:
        category = "general"
    cur = await db.execute(
        "INSERT INTO snippets (name, content, category, times_used, created_at) VALUES (?,?,?,0,?)",
        (data["name"], data["content"], category, now),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM snippets WHERE id = ?", (cur.lastrowid,))
    return _snippet_row(rows[0])


async def list_snippets(db: aiosqlite.Connection, category: str | None = None) -> list[dict]:
    if category:
        rows = await db.execute_fetchall(
            "SELECT * FROM snippets WHERE category = ? ORDER BY times_used DESC, name ASC",
            (category,),
        )
    else:
        rows = await db.execute_fetchall(
            "SELECT * FROM snippets ORDER BY category ASC, times_used DESC, name ASC"
        )
    return [_snippet_row(r) for r in rows]


async def get_snippet(db: aiosqlite.Connection, snippet_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM snippets WHERE id = ?", (snippet_id,))
    return _snippet_row(rows[0]) if rows else None


async def delete_snippet(db: aiosqlite.Connection, snippet_id: int) -> bool:
    cur = await db.execute("DELETE FROM snippets WHERE id = ?", (snippet_id,))
    await db.commit()
    return cur.rowcount > 0


async def increment_snippet_usage(db: aiosqlite.Connection, snippet_id: int) -> dict | None:
    """Increment the times_used counter for a snippet. Returns updated snippet or None."""
    rows = await db.execute_fetchall("SELECT * FROM snippets WHERE id = ?", (snippet_id,))
    if not rows:
        return None
    await db.execute(
        "UPDATE snippets SET times_used = times_used + 1 WHERE id = ?", (snippet_id,)
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM snippets WHERE id = ?", (snippet_id,))
    return _snippet_row(rows[0])


# ── Outreach Calendar ────────────────────────────────────────────────────

async def get_outreach_calendar(db: aiosqlite.Connection,
                                 from_date: str, to_date: str) -> dict:
    """
    Return scheduled outreach activities between from_date and to_date (ISO date strings).
    Looks at active enrollments and computes the next advance date based on
    last_advanced_at + step delay_days.
    """
    from_dt = datetime.fromisoformat(from_date).replace(tzinfo=timezone.utc)
    # to_date is inclusive: extend to end of that day
    to_dt = datetime.fromisoformat(to_date).replace(tzinfo=timezone.utc) + timedelta(days=1)

    # Load all active enrollments with their sequence info
    enrollment_rows = await db.execute_fetchall(
        "SELECT e.*, s.name as seq_name, s.steps as seq_steps FROM enrollments e "
        "JOIN sequences s ON e.sequence_id = s.id "
        "WHERE e.status = 'active'"
    )

    # by_date: date_str -> list of prospect dicts
    by_date: dict[str, list[dict]] = {}
    # by_sequence: seq_id -> {id, name, count}
    by_sequence: dict[int, dict] = {}

    for r in enrollment_rows:
        steps = jsonlib.loads(r["seq_steps"])
        step_idx = r["current_step"]
        if step_idx >= len(steps):
            continue
        step = steps[step_idx]
        ref_time = r["last_advanced_at"] or r["enrolled_at"]
        ref_dt = datetime.fromisoformat(ref_time)
        if ref_dt.tzinfo is None:
            ref_dt = ref_dt.replace(tzinfo=timezone.utc)
        next_advance_dt = ref_dt + timedelta(days=step["delay_days"])

        # Check if next_advance_dt falls within [from_dt, to_dt)
        if next_advance_dt < from_dt or next_advance_dt >= to_dt:
            continue

        date_str = next_advance_dt.date().isoformat()

        prospect = await get_prospect(db, r["prospect_id"])
        if not prospect:
            continue

        entry = {
            "prospect_id": r["prospect_id"],
            "name": f"{prospect['first_name']} {prospect['last_name']}",
            "email": prospect["email"],
            "sequence_name": r["seq_name"],
            "step_num": step_idx + 1,
            "subject_hint": step.get("subject_hint", ""),
        }

        if date_str not in by_date:
            by_date[date_str] = []
        by_date[date_str].append(entry)

        seq_id = r["sequence_id"]
        if seq_id not in by_sequence:
            by_sequence[seq_id] = {"id": seq_id, "name": r["seq_name"], "scheduled_count": 0}
        by_sequence[seq_id]["scheduled_count"] += 1

    # Build sorted by_date list (fill in all dates in range with zero if empty is fine,
    # but only include dates that have prospects for compactness)
    sorted_dates = sorted(by_date.keys())
    by_date_list = [
        {"date": d, "count": len(by_date[d]), "prospects": by_date[d]}
        for d in sorted_dates
    ]

    total_scheduled = sum(len(v) for v in by_date.values())
    by_sequence_list = sorted(by_sequence.values(), key=lambda x: x["scheduled_count"], reverse=True)

    return {
        "from_date": from_date,
        "to_date": to_date,
        "total_scheduled": total_scheduled,
        "by_date": by_date_list,
        "by_sequence": by_sequence_list,
    }


# ══════════════════════════════════════════════════════════════════════════
# Feature 1: Email A/B Testing (v1.0.0)
# ══════════════════════════════════════════════════════════════════════════

def _ab_test_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"],
        "name": r["name"],
        "sequence_id": r["sequence_id"],
        "variant_a_tone": r["variant_a_tone"],
        "variant_a_subject_hint": r["variant_a_subject_hint"],
        "variant_b_tone": r["variant_b_tone"],
        "variant_b_subject_hint": r["variant_b_subject_hint"],
        "status": r["status"],
        "winner": r["winner"],
        "created_at": r["created_at"],
        "completed_at": r["completed_at"],
    }


async def _compute_variant_stats(db: aiosqlite.Connection, test_id: int, variant: str) -> dict:
    """Compute stats for a single variant of an A/B test from campaign_events."""
    # Get all prospect_ids assigned to this variant
    assignment_rows = await db.execute_fetchall(
        "SELECT prospect_id FROM ab_test_assignments WHERE test_id = ? AND variant = ?",
        (test_id, variant),
    )
    prospect_ids = [r["prospect_id"] for r in assignment_rows]
    if not prospect_ids:
        return {"sent": 0, "opened": 0, "replied": 0, "open_rate": 0.0, "reply_rate": 0.0}

    placeholders = ",".join("?" * len(prospect_ids))
    event_rows = await db.execute_fetchall(
        f"SELECT event_type, COUNT(*) as cnt FROM campaign_events "
        f"WHERE prospect_id IN ({placeholders}) GROUP BY event_type",
        prospect_ids,
    )
    ev = {r["event_type"]: r["cnt"] for r in event_rows}
    sent = ev.get("sent", 0)
    opened = ev.get("opened", 0)
    replied = ev.get("replied", 0)
    return {
        "sent": sent,
        "opened": opened,
        "replied": replied,
        "open_rate": round(opened / max(sent, 1) * 100, 1),
        "reply_rate": round(replied / max(sent, 1) * 100, 1),
    }


async def create_ab_test(db: aiosqlite.Connection, data: dict) -> dict:
    now = _now()
    cur = await db.execute(
        """INSERT INTO ab_tests (name, sequence_id, variant_a_tone, variant_a_subject_hint,
           variant_b_tone, variant_b_subject_hint, status, winner, created_at, completed_at)
           VALUES (?,?,?,?,?,?,'running',NULL,?,NULL)""",
        (data["name"], data.get("sequence_id"),
         data["variant_a_tone"], data["variant_a_subject_hint"],
         data["variant_b_tone"], data["variant_b_subject_hint"], now),
    )
    await db.commit()
    return await get_ab_test(db, cur.lastrowid)


async def list_ab_tests(db: aiosqlite.Connection, status: str | None = None) -> list[dict]:
    if status:
        rows = await db.execute_fetchall(
            "SELECT * FROM ab_tests WHERE status = ? ORDER BY created_at DESC", (status,))
    else:
        rows = await db.execute_fetchall("SELECT * FROM ab_tests ORDER BY created_at DESC")
    result = []
    for r in rows:
        test = _ab_test_row(r)
        test["variant_a_stats"] = await _compute_variant_stats(db, r["id"], "a")
        test["variant_b_stats"] = await _compute_variant_stats(db, r["id"], "b")
        result.append(test)
    return result


async def get_ab_test(db: aiosqlite.Connection, test_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM ab_tests WHERE id = ?", (test_id,))
    if not rows:
        return None
    test = _ab_test_row(rows[0])
    test["variant_a_stats"] = await _compute_variant_stats(db, test_id, "a")
    test["variant_b_stats"] = await _compute_variant_stats(db, test_id, "b")
    return test


async def assign_prospect_to_test(db: aiosqlite.Connection, test_id: int,
                                    prospect_id: int, variant: str | None = None) -> dict | str | None:
    """Assign a prospect to an A/B test variant. Auto round-robin if variant not specified."""
    rows = await db.execute_fetchall("SELECT * FROM ab_tests WHERE id = ?", (test_id,))
    if not rows:
        return None
    test = rows[0]
    if test["status"] != "running":
        return "test_not_running"

    # Check prospect exists
    prospect = await get_prospect(db, prospect_id)
    if not prospect:
        return "prospect_not_found"

    # DNC check
    dnc = await check_dnc(db, prospect["email"])
    if dnc:
        return "dnc_blocked"

    # Check for existing assignment
    existing = await db.execute_fetchall(
        "SELECT id FROM ab_test_assignments WHERE test_id = ? AND prospect_id = ?",
        (test_id, prospect_id),
    )
    if existing:
        return "already_assigned"

    # Auto round-robin if variant not specified
    if not variant:
        count_a = (await db.execute_fetchall(
            "SELECT COUNT(*) as c FROM ab_test_assignments WHERE test_id = ? AND variant = 'a'",
            (test_id,)))[0]["c"]
        count_b = (await db.execute_fetchall(
            "SELECT COUNT(*) as c FROM ab_test_assignments WHERE test_id = ? AND variant = 'b'",
            (test_id,)))[0]["c"]
        variant = "a" if count_a <= count_b else "b"

    if variant not in ("a", "b"):
        return "invalid_variant"

    now = _now()
    cur = await db.execute(
        "INSERT INTO ab_test_assignments (test_id, prospect_id, variant, created_at) VALUES (?,?,?,?)",
        (test_id, prospect_id, variant, now),
    )
    await db.commit()
    return {
        "id": cur.lastrowid,
        "test_id": test_id,
        "prospect_id": prospect_id,
        "variant": variant,
        "created_at": now,
    }


async def complete_ab_test(db: aiosqlite.Connection, test_id: int,
                             winner: str | None = None) -> dict | str | None:
    """Complete an A/B test. Auto-determine winner by reply_rate if not specified."""
    rows = await db.execute_fetchall("SELECT * FROM ab_tests WHERE id = ?", (test_id,))
    if not rows:
        return None
    test = rows[0]
    if test["status"] != "running":
        return "test_not_running"

    now = _now()

    if winner and winner in ("a", "b"):
        final_winner = winner
    else:
        # Auto-determine by reply_rate
        stats_a = await _compute_variant_stats(db, test_id, "a")
        stats_b = await _compute_variant_stats(db, test_id, "b")
        if stats_a["reply_rate"] > stats_b["reply_rate"]:
            final_winner = "a"
        elif stats_b["reply_rate"] > stats_a["reply_rate"]:
            final_winner = "b"
        else:
            final_winner = "tie"

    await db.execute(
        "UPDATE ab_tests SET status = 'completed', winner = ?, completed_at = ? WHERE id = ?",
        (final_winner, now, test_id),
    )
    await db.commit()
    return await get_ab_test(db, test_id)


async def list_test_assignments(db: aiosqlite.Connection, test_id: int) -> list[dict] | None:
    """List all prospect assignments for a test."""
    rows = await db.execute_fetchall("SELECT * FROM ab_tests WHERE id = ?", (test_id,))
    if not rows:
        return None
    assignment_rows = await db.execute_fetchall(
        "SELECT * FROM ab_test_assignments WHERE test_id = ? ORDER BY created_at DESC",
        (test_id,),
    )
    return [
        {
            "id": r["id"],
            "test_id": r["test_id"],
            "prospect_id": r["prospect_id"],
            "variant": r["variant"],
            "created_at": r["created_at"],
        }
        for r in assignment_rows
    ]


# ══════════════════════════════════════════════════════════════════════════
# Feature 2: Prospect Segments (v1.0.0)
# ══════════════════════════════════════════════════════════════════════════

def _segment_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"],
        "name": r["name"],
        "description": r["description"],
        "criteria": jsonlib.loads(r["criteria"]) if r["criteria"] else {},
        "auto_assign": bool(r["auto_assign"]),
        "prospect_count": r["prospect_count"],
        "created_at": r["created_at"],
    }


async def _evaluate_segment_criteria(db: aiosqlite.Connection, criteria: dict) -> list[dict]:
    """Evaluate segment criteria and return matching prospects with scores."""
    # Start with all prospects
    rows = await db.execute_fetchall("SELECT * FROM prospects ORDER BY created_at DESC")
    prospects = [_prospect_row(r) for r in rows]

    matched = []
    for p in prospects:
        # Compute lead score for this prospect
        score_data = await compute_lead_score(db, p["id"])
        score = score_data["score"] if score_data else 0
        grade = score_data["grade"] if score_data else "cold"

        # Apply criteria filters
        if "score_min" in criteria and criteria["score_min"] is not None:
            if score < criteria["score_min"]:
                continue
        if "score_max" in criteria and criteria["score_max"] is not None:
            if score > criteria["score_max"]:
                continue

        if "min_events" in criteria and criteria["min_events"] is not None:
            event_count_rows = await db.execute_fetchall(
                "SELECT COUNT(*) as cnt FROM campaign_events WHERE prospect_id = ?",
                (p["id"],),
            )
            event_count = event_count_rows[0]["cnt"] if event_count_rows else 0
            if event_count < criteria["min_events"]:
                continue

        if "status" in criteria and criteria["status"] is not None:
            if p["status"] != criteria["status"]:
                continue

        if "has_tag" in criteria and criteria["has_tag"] is not None:
            tag = criteria["has_tag"].strip().lower()
            tags = [t.lower() for t in p.get("tags", [])]
            if tag not in tags:
                continue

        if "company_contains" in criteria and criteria["company_contains"] is not None:
            if criteria["company_contains"].lower() not in p["company"].lower():
                continue

        if "enrolled_in_sequence" in criteria and criteria["enrolled_in_sequence"] is not None:
            enroll_rows = await db.execute_fetchall(
                "SELECT COUNT(*) as cnt FROM enrollments WHERE prospect_id = ?",
                (p["id"],),
            )
            has_enrollment = (enroll_rows[0]["cnt"] if enroll_rows else 0) > 0
            if criteria["enrolled_in_sequence"] and not has_enrollment:
                continue
            if not criteria["enrolled_in_sequence"] and has_enrollment:
                continue

        matched.append({
            "prospect_id": p["id"],
            "name": f"{p['first_name']} {p['last_name']}",
            "email": p["email"],
            "company": p["company"],
            "score": score,
            "grade": grade,
        })

    return matched


async def create_segment(db: aiosqlite.Connection, data: dict) -> dict:
    now = _now()
    criteria = data["criteria"]
    auto_assign = 1 if data.get("auto_assign") else 0
    # Evaluate initial prospect count
    matched = await _evaluate_segment_criteria(db, criteria)
    cur = await db.execute(
        """INSERT INTO segments (name, description, criteria, auto_assign, prospect_count, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?)""",
        (data["name"], data.get("description"), jsonlib.dumps(criteria),
         auto_assign, len(matched), now, now),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM segments WHERE id = ?", (cur.lastrowid,))
    return _segment_row(rows[0])


async def list_segments(db: aiosqlite.Connection) -> list[dict]:
    rows = await db.execute_fetchall("SELECT * FROM segments ORDER BY created_at DESC")
    return [_segment_row(r) for r in rows]


async def get_segment(db: aiosqlite.Connection, segment_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM segments WHERE id = ?", (segment_id,))
    return _segment_row(rows[0]) if rows else None


async def update_segment(db: aiosqlite.Connection, segment_id: int, data: dict) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM segments WHERE id = ?", (segment_id,))
    if not rows:
        return None
    now = _now()
    r = rows[0]
    name = data.get("name") or r["name"]
    description = data.get("description") if "description" in data else r["description"]
    criteria = data.get("criteria") or jsonlib.loads(r["criteria"])
    auto_assign = data.get("auto_assign") if "auto_assign" in data and data["auto_assign"] is not None else bool(r["auto_assign"])
    auto_assign_int = 1 if auto_assign else 0

    await db.execute(
        """UPDATE segments SET name=?, description=?, criteria=?, auto_assign=?, updated_at=?
           WHERE id=?""",
        (name, description, jsonlib.dumps(criteria), auto_assign_int, now, segment_id),
    )
    await db.commit()
    return await get_segment(db, segment_id)


async def delete_segment(db: aiosqlite.Connection, segment_id: int) -> bool:
    cur = await db.execute("DELETE FROM segments WHERE id = ?", (segment_id,))
    await db.commit()
    return cur.rowcount > 0


async def evaluate_segment(db: aiosqlite.Connection, segment_id: int) -> dict | None:
    """Re-compute matching prospects for a segment and update prospect_count."""
    rows = await db.execute_fetchall("SELECT * FROM segments WHERE id = ?", (segment_id,))
    if not rows:
        return None
    criteria = jsonlib.loads(rows[0]["criteria"])
    matched = await _evaluate_segment_criteria(db, criteria)
    now = _now()
    await db.execute(
        "UPDATE segments SET prospect_count = ?, updated_at = ? WHERE id = ?",
        (len(matched), now, segment_id),
    )
    await db.commit()
    return await get_segment(db, segment_id)


async def list_segment_prospects(db: aiosqlite.Connection, segment_id: int) -> list[dict] | None:
    """Get all prospects matching a segment's criteria with their scores."""
    rows = await db.execute_fetchall("SELECT * FROM segments WHERE id = ?", (segment_id,))
    if not rows:
        return None
    criteria = jsonlib.loads(rows[0]["criteria"])
    return await _evaluate_segment_criteria(db, criteria)


async def auto_assign_segments(db: aiosqlite.Connection) -> dict:
    """Re-evaluate all auto_assign segments and update their prospect_count."""
    rows = await db.execute_fetchall(
        "SELECT * FROM segments WHERE auto_assign = 1 ORDER BY id")
    updated = 0
    for r in rows:
        criteria = jsonlib.loads(r["criteria"])
        matched = await _evaluate_segment_criteria(db, criteria)
        now = _now()
        await db.execute(
            "UPDATE segments SET prospect_count = ?, updated_at = ? WHERE id = ?",
            (len(matched), now, r["id"]),
        )
        updated += 1
    await db.commit()
    return {"segments_updated": updated}


# ══════════════════════════════════════════════════════════════════════════
# Feature 3: Outreach Automation Rules (v1.0.0)
# ══════════════════════════════════════════════════════════════════════════

VALID_TRIGGER_TYPES = {"status_change", "event_received", "score_threshold", "enrollment_completed"}
VALID_ACTION_TYPES = {"enroll_in_sequence", "add_tag", "remove_tag", "change_status", "pause_enrollment"}


def _automation_rule_row(r: aiosqlite.Row) -> dict:
    return {
        "id": r["id"],
        "name": r["name"],
        "trigger_type": r["trigger_type"],
        "trigger_config": jsonlib.loads(r["trigger_config"]) if r["trigger_config"] else {},
        "action_type": r["action_type"],
        "action_config": jsonlib.loads(r["action_config"]) if r["action_config"] else {},
        "is_enabled": bool(r["is_enabled"]),
        "times_fired": r["times_fired"],
        "last_fired_at": r["last_fired_at"],
        "created_at": r["created_at"],
    }


async def create_automation_rule(db: aiosqlite.Connection, data: dict) -> dict:
    now = _now()
    is_enabled = 1 if data.get("is_enabled", True) else 0
    cur = await db.execute(
        """INSERT INTO automation_rules
           (name, trigger_type, trigger_config, action_type, action_config, is_enabled, times_fired, last_fired_at, created_at)
           VALUES (?,?,?,?,?,?,0,NULL,?)""",
        (data["name"], data["trigger_type"], jsonlib.dumps(data["trigger_config"]),
         data["action_type"], jsonlib.dumps(data["action_config"]), is_enabled, now),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM automation_rules WHERE id = ?", (cur.lastrowid,))
    return _automation_rule_row(rows[0])


async def list_automation_rules(db: aiosqlite.Connection,
                                  is_enabled: bool | None = None,
                                  trigger_type: str | None = None) -> list[dict]:
    q = "SELECT * FROM automation_rules WHERE 1=1"
    params: list = []
    if is_enabled is not None:
        q += " AND is_enabled = ?"
        params.append(1 if is_enabled else 0)
    if trigger_type:
        q += " AND trigger_type = ?"
        params.append(trigger_type)
    q += " ORDER BY created_at DESC"
    rows = await db.execute_fetchall(q, params)
    return [_automation_rule_row(r) for r in rows]


async def get_automation_rule(db: aiosqlite.Connection, rule_id: int) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM automation_rules WHERE id = ?", (rule_id,))
    return _automation_rule_row(rows[0]) if rows else None


async def update_automation_rule(db: aiosqlite.Connection, rule_id: int, data: dict) -> dict | None:
    rows = await db.execute_fetchall("SELECT * FROM automation_rules WHERE id = ?", (rule_id,))
    if not rows:
        return None
    r = rows[0]
    name = data.get("name") or r["name"]
    trigger_config = jsonlib.dumps(data["trigger_config"]) if "trigger_config" in data and data["trigger_config"] is not None else r["trigger_config"]
    action_config = jsonlib.dumps(data["action_config"]) if "action_config" in data and data["action_config"] is not None else r["action_config"]
    is_enabled = (1 if data["is_enabled"] else 0) if "is_enabled" in data and data["is_enabled"] is not None else r["is_enabled"]

    await db.execute(
        """UPDATE automation_rules SET name=?, trigger_config=?, action_config=?, is_enabled=?
           WHERE id=?""",
        (name, trigger_config, action_config, is_enabled, rule_id),
    )
    await db.commit()
    return await get_automation_rule(db, rule_id)


async def delete_automation_rule(db: aiosqlite.Connection, rule_id: int) -> bool:
    cur = await db.execute("DELETE FROM automation_rules WHERE id = ?", (rule_id,))
    await db.commit()
    return cur.rowcount > 0


async def _execute_automation_action(db: aiosqlite.Connection, prospect_id: int,
                                       action_type: str, action_config: dict) -> bool:
    """Execute a single automation action on a prospect. Returns True if action was executed."""
    if action_type == "enroll_in_sequence":
        seq_id = action_config.get("sequence_id")
        if seq_id:
            result = await enroll_prospect(db, seq_id, prospect_id)
            # Only count as fired if enrollment succeeded (not already enrolled, not blocked, etc.)
            return isinstance(result, dict)
        return False

    elif action_type == "add_tag":
        tag = action_config.get("tag")
        if tag:
            result = await add_prospect_tag(db, prospect_id, tag)
            return result is not None
        return False

    elif action_type == "remove_tag":
        tag = action_config.get("tag")
        if tag:
            result = await remove_prospect_tag(db, prospect_id, tag)
            return result is not None
        return False

    elif action_type == "change_status":
        status = action_config.get("status")
        if status:
            # Use direct SQL to avoid infinite recursion via update_prospect_status triggering automation
            await db.execute("UPDATE prospects SET status = ? WHERE id = ?", (status, prospect_id))
            await db.commit()
            return True
        return False

    elif action_type == "pause_enrollment":
        seq_id = action_config.get("sequence_id")
        if seq_id:
            # Pause enrollment for specific sequence
            enroll_rows = await db.execute_fetchall(
                "SELECT id FROM enrollments WHERE prospect_id = ? AND sequence_id = ? AND status = 'active'",
                (prospect_id, seq_id),
            )
            for er in enroll_rows:
                await pause_enrollment(db, er["id"])
            return len(enroll_rows) > 0
        else:
            # Pause all active enrollments for this prospect
            enroll_rows = await db.execute_fetchall(
                "SELECT id FROM enrollments WHERE prospect_id = ? AND status = 'active'",
                (prospect_id,),
            )
            for er in enroll_rows:
                await pause_enrollment(db, er["id"])
            return len(enroll_rows) > 0

    return False


async def evaluate_automation_for_prospect(db: aiosqlite.Connection, prospect_id: int,
                                             trigger_type: str, trigger_data: dict) -> list[int]:
    """
    Check all enabled rules matching trigger_type, execute matching actions,
    increment times_fired. Returns list of fired rule IDs.
    """
    rules = await db.execute_fetchall(
        "SELECT * FROM automation_rules WHERE trigger_type = ? AND is_enabled = 1",
        (trigger_type,),
    )

    fired_rule_ids = []

    for r in rules:
        config = jsonlib.loads(r["trigger_config"]) if r["trigger_config"] else {}
        match = False

        if trigger_type == "status_change":
            from_status = config.get("from_status")
            to_status = config.get("to_status")
            # Match if config conditions are met (None means any)
            from_ok = from_status is None or from_status == trigger_data.get("from_status")
            to_ok = to_status is None or to_status == trigger_data.get("to_status")
            match = from_ok and to_ok

        elif trigger_type == "event_received":
            event_type = config.get("event_type")
            match = event_type is None or event_type == trigger_data.get("event_type")

        elif trigger_type == "score_threshold":
            # Need to compute current score
            score_data = await compute_lead_score(db, prospect_id)
            if score_data:
                score = score_data["score"]
                min_score = config.get("min_score")
                max_score = config.get("max_score")
                min_ok = min_score is None or score >= min_score
                max_ok = max_score is None or score <= max_score
                match = min_ok and max_ok

        elif trigger_type == "enrollment_completed":
            seq_id = config.get("sequence_id")
            match = seq_id is None or seq_id == trigger_data.get("sequence_id")

        if not match:
            continue

        action_type = r["action_type"]
        action_config = jsonlib.loads(r["action_config"]) if r["action_config"] else {}

        executed = await _execute_automation_action(db, prospect_id, action_type, action_config)
        if executed:
            now = _now()
            await db.execute(
                "UPDATE automation_rules SET times_fired = times_fired + 1, last_fired_at = ? WHERE id = ?",
                (now, r["id"]),
            )
            await db.commit()
            fired_rule_ids.append(r["id"])

    return fired_rule_ids
