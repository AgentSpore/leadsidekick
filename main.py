from __future__ import annotations
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from models import (
    ProspectCreate, ProspectResponse,
    DraftRequest, DraftResponse,
    TemplateCreate, TemplateResponse,
    ProspectListCreate, ProspectListResponse,
    BulkImportRequest, UsageStats,
)
from engine import (
    init_db, create_prospect, list_prospects, get_prospect, update_prospect_status,
    bulk_import_prospects, create_draft, create_template, list_templates,
    create_prospect_list, list_prospect_lists, get_stats,
)

DB_PATH = os.getenv("DB_PATH", "leadsidekick.db")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await init_db(DB_PATH)
    yield
    await app.state.db.close()


app = FastAPI(
    title="LeadSidekick",
    description="Lead prospecting + personalised cold outreach drafter. Find prospects, generate tailored emails in seconds.",
    version="0.1.0",
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.1.0"}


# ── Prospect Lists ────────────────────────────────────────────────────────

@app.post("/lists", response_model=ProspectListResponse, status_code=201)
async def create_list(body: ProspectListCreate):
    """Create a named prospect list for organisation."""
    return await create_prospect_list(app.state.db, body.model_dump())


@app.get("/lists", response_model=list[ProspectListResponse])
async def get_lists():
    """List all prospect lists with prospect count."""
    return await list_prospect_lists(app.state.db)


# ── Prospects ─────────────────────────────────────────────────────────────

@app.post("/prospects", response_model=ProspectResponse, status_code=201)
async def add_prospect(body: ProspectCreate):
    """Add a single prospect."""
    return await create_prospect(app.state.db, body.model_dump())


@app.post("/prospects/bulk", status_code=201)
async def bulk_import(body: BulkImportRequest):
    """Bulk import up to 500 prospects at once."""
    return await bulk_import_prospects(
        app.state.db,
        [p.model_dump() for p in body.prospects],
        body.list_id,
    )


@app.get("/prospects", response_model=list[ProspectResponse])
async def get_prospects(
    list_id: int | None = Query(None),
    status: str | None = Query(None, description="new | emailed | replied | converted | unsubscribed"),
):
    """List prospects. Filter by list or status."""
    return await list_prospects(app.state.db, list_id, status)


@app.get("/prospects/{prospect_id}", response_model=ProspectResponse)
async def get_prospect_detail(prospect_id: int):
    """Get a single prospect."""
    p = await get_prospect(app.state.db, prospect_id)
    if not p:
        raise HTTPException(404, "Prospect not found")
    return p


@app.patch("/prospects/{prospect_id}/status", response_model=ProspectResponse)
async def patch_status(prospect_id: int, status: str = Query(...)):
    """Update prospect status: new | emailed | replied | converted | unsubscribed"""
    p = await update_prospect_status(app.state.db, prospect_id, status)
    if not p:
        raise HTTPException(404, "Prospect not found")
    return p


# ── Draft Generation ──────────────────────────────────────────────────────

@app.post("/draft", response_model=DraftResponse)
async def generate_draft(body: DraftRequest):
    """
    Generate a personalised cold email draft for a prospect.
    Uses tone, context, and value prop to craft a targeted subject + body.
    Optionally uses a saved template as base.
    """
    p = await get_prospect(app.state.db, body.prospect_id)
    if not p:
        raise HTTPException(404, "Prospect not found")
    result = await create_draft(
        app.state.db, body.prospect_id, body.template_id,
        body.tone, body.context, body.your_value_prop, body.cta,
    )
    if not result:
        raise HTTPException(500, "Draft generation failed")
    return result


# ── Templates ─────────────────────────────────────────────────────────────

@app.post("/templates", response_model=TemplateResponse, status_code=201)
async def add_template(body: TemplateCreate):
    """Save a reusable email template with {{placeholder}} variables."""
    return await create_template(app.state.db, body.model_dump())


@app.get("/templates", response_model=list[TemplateResponse])
async def get_templates():
    """List templates sorted by usage count."""
    return await list_templates(app.state.db)


# ── Stats ─────────────────────────────────────────────────────────────────

@app.get("/stats", response_model=UsageStats)
async def stats():
    """Usage stats: prospect counts by status, drafts generated, most-used tone."""
    return await get_stats(app.state.db)
