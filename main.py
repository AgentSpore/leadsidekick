from __future__ import annotations
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from models import (
    ProspectCreate, ProspectResponse,
    DraftRequest, DraftResponse, DraftLogResponse,
    TemplateCreate, TemplateResponse,
    ProspectListCreate, ProspectListResponse,
    BulkImportRequest, UsageStats,
    SequenceCreate, SequenceResponse,
    EnrollmentResponse, AdvanceResult,
)
from engine import (
    init_db, create_prospect, list_prospects, get_prospect, update_prospect_status,
    bulk_import_prospects, create_draft, list_drafts, get_draft,
    create_template, list_templates,
    create_prospect_list, list_prospect_lists, get_stats,
    search_prospects, export_prospects_csv,
    create_sequence, list_sequences, enroll_prospect,
    list_enrollments, advance_sequence,
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
    version="0.5.0",
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.5.0"}


# ── Prospect Lists ────────────────────────────────────────────────

@app.post("/lists", response_model=ProspectListResponse, status_code=201)
async def create_list(body: ProspectListCreate):
    return await create_prospect_list(app.state.db, body.model_dump())


@app.get("/lists", response_model=list[ProspectListResponse])
async def get_lists():
    return await list_prospect_lists(app.state.db)


# ── Prospects ─────────────────────────────────────────────────────

@app.post("/prospects", response_model=ProspectResponse, status_code=201)
async def add_prospect(body: ProspectCreate):
    return await create_prospect(app.state.db, body.model_dump())


@app.post("/prospects/bulk", status_code=201)
async def bulk_import(body: BulkImportRequest):
    return await bulk_import_prospects(
        app.state.db,
        [p.model_dump() for p in body.prospects],
        body.list_id,
    )


# search and export/csv BEFORE /{prospect_id} to avoid route conflicts
@app.get("/prospects/search", response_model=list[ProspectResponse])
async def search(q: str = Query(..., min_length=2, description="Search term (min 2 chars)")):
    """Full-text search across first_name, last_name, email, company, job_title."""
    return await search_prospects(app.state.db, q)


@app.get("/prospects/export/csv")
async def export_csv(
    list_id: int | None = Query(None, description="Filter by list"),
    status: str | None = Query(None, description="new | emailed | replied | converted | unsubscribed"),
):
    """Export matching prospects as a CSV file for use in CRM or outreach tools."""
    data = await export_prospects_csv(app.state.db, list_id, status)
    return StreamingResponse(
        iter([data]), media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=prospects.csv"}
    )


@app.get("/prospects", response_model=list[ProspectResponse])
async def get_prospects(
    list_id: int | None = Query(None),
    status: str | None = Query(None, description="new | emailed | replied | converted | unsubscribed"),
):
    return await list_prospects(app.state.db, list_id, status)


@app.get("/prospects/{prospect_id}", response_model=ProspectResponse)
async def get_prospect_detail(prospect_id: int):
    p = await get_prospect(app.state.db, prospect_id)
    if not p:
        raise HTTPException(404, "Prospect not found")
    return p


@app.patch("/prospects/{prospect_id}/status", response_model=ProspectResponse)
async def patch_status(prospect_id: int, status: str = Query(...)):
    p = await update_prospect_status(app.state.db, prospect_id, status)
    if not p:
        raise HTTPException(404, "Prospect not found")
    return p


# ── Draft Generation ──────────────────────────────────────────────

@app.post("/draft", response_model=DraftResponse)
async def generate_draft(body: DraftRequest):
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


@app.get("/drafts", response_model=list[DraftLogResponse])
async def list_draft_history(
    prospect_id: int | None = Query(None),
    tone: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
):
    return await list_drafts(app.state.db, prospect_id, tone, limit)


@app.get("/drafts/{draft_id}", response_model=DraftLogResponse)
async def get_draft_detail(draft_id: int):
    d = await get_draft(app.state.db, draft_id)
    if not d:
        raise HTTPException(404, "Draft not found")
    return d


# ── Templates ─────────────────────────────────────────────────────

@app.post("/templates", response_model=TemplateResponse, status_code=201)
async def add_template(body: TemplateCreate):
    return await create_template(app.state.db, body.model_dump())


@app.get("/templates", response_model=list[TemplateResponse])
async def get_templates():
    return await list_templates(app.state.db)


# ── Sequences ─────────────────────────────────────────────────────

@app.post("/sequences", response_model=SequenceResponse, status_code=201)
async def add_sequence(body: SequenceCreate):
    return await create_sequence(app.state.db, body.model_dump())


@app.get("/sequences", response_model=list[SequenceResponse])
async def get_sequences():
    return await list_sequences(app.state.db)


@app.post("/sequences/{sequence_id}/enroll/{prospect_id}", response_model=EnrollmentResponse, status_code=201)
async def enroll(sequence_id: int, prospect_id: int):
    p = await get_prospect(app.state.db, prospect_id)
    if not p:
        raise HTTPException(404, "Prospect not found")
    result = await enroll_prospect(app.state.db, sequence_id, prospect_id)
    if not result:
        raise HTTPException(409, "Prospect already enrolled in this sequence")
    return result


@app.get("/sequences/{sequence_id}/enrollments", response_model=list[EnrollmentResponse])
async def get_enrollments(sequence_id: int):
    return await list_enrollments(app.state.db, sequence_id)


@app.post("/sequences/{sequence_id}/advance", response_model=AdvanceResult)
async def advance(sequence_id: int):
    result = await advance_sequence(app.state.db, sequence_id)
    if result is None:
        raise HTTPException(404, "Sequence not found")
    return result


# ── Stats ─────────────────────────────────────────────────────────

@app.get("/stats", response_model=UsageStats)
async def stats():
    return await get_stats(app.state.db)



# ── Tags ─────────────────────────────────────────────────────────────────

@app.post("/prospects/{prospect_id}/tags", response_model=ProspectResponse)
async def tag_prospect(prospect_id: int, tag: str = Query(..., min_length=1, max_length=50)):
    result = await add_prospect_tag(app.state.db, prospect_id, tag)
    if not result:
        raise HTTPException(404, "Prospect not found")
    return result


@app.delete("/prospects/{prospect_id}/tags", response_model=ProspectResponse)
async def untag_prospect(prospect_id: int, tag: str = Query(..., min_length=1, max_length=50)):
    result = await remove_prospect_tag(app.state.db, prospect_id, tag)
    if not result:
        raise HTTPException(404, "Prospect not found")
    return result


# ── Campaign Analytics ───────────────────────────────────────────────────

@app.post("/events", status_code=201)
async def create_event(body: CampaignEventCreate):
    return await record_event(app.state.db, body.model_dump())


@app.get("/sequences/{sequence_id}/analytics", response_model=SequenceAnalytics)
async def sequence_analytics(sequence_id: int):
    result = await get_sequence_analytics(app.state.db, sequence_id)
    if not result:
        raise HTTPException(404, "Sequence not found")
    return result


@app.get("/analytics/campaigns", response_model=CampaignOverview)
async def campaign_overview():
    return await get_campaign_overview(app.state.db)
