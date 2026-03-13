from __future__ import annotations
import os
from contextlib import asynccontextmanager
from datetime import date, timedelta

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
    CampaignEventCreate, SequenceAnalytics, CampaignOverview,
    LeadScore, TopLead, StageUpdate, PipelineSummary, ToneAnalytics,
    ProspectActivity, SequenceCloneRequest,
    DncCreate, DncResponse,
    SmartListCreate, SmartListResponse,
    EnrollmentActionResult, BulkEnrollmentResult,
    DuplicateGroup, MergeRequest, MergeResult,
    ProspectNoteCreate, ProspectNoteResponse,
    SnippetCreate, SnippetResponse,
    OutreachCalendar,
    ABTestCreate, ABTestResponse, ABTestAssignment, ABTestComplete, ABTestAssignmentResponse,
    SegmentCreate, SegmentUpdate, SegmentResponse, SegmentProspectEntry,
    AutomationRuleCreate, AutomationRuleUpdate, AutomationRuleResponse, AutomationEvaluateRequest,
)
from engine import (
    init_db, create_prospect, list_prospects, get_prospect, update_prospect_status,
    bulk_import_prospects, create_draft, list_drafts, get_draft,
    create_template, list_templates,
    create_prospect_list, list_prospect_lists, get_stats,
    search_prospects, export_prospects_csv,
    create_sequence, list_sequences, enroll_prospect,
    list_enrollments, advance_sequence, clone_sequence,
    add_prospect_tag, remove_prospect_tag,
    record_event, get_sequence_analytics, get_campaign_overview,
    compute_lead_score, get_top_leads,
    update_prospect_stage, get_pipeline_summary,
    get_tone_analytics, get_prospect_activity,
    add_dnc, list_dnc, delete_dnc,
    create_smart_list, list_smart_lists, get_smart_list,
    get_smart_list_prospects, delete_smart_list,
    pause_enrollment, resume_enrollment,
    pause_all_enrollments, resume_all_enrollments,
    find_duplicates, merge_prospects,
    add_prospect_note, list_prospect_notes, delete_prospect_note, toggle_pin_note,
    create_snippet, list_snippets, get_snippet, delete_snippet, increment_snippet_usage,
    get_outreach_calendar,
    # v1.0.0: A/B Testing
    create_ab_test, list_ab_tests, get_ab_test,
    assign_prospect_to_test, complete_ab_test, list_test_assignments,
    # v1.0.0: Segments
    create_segment, list_segments, get_segment, update_segment, delete_segment,
    evaluate_segment, list_segment_prospects, auto_assign_segments,
    # v1.0.0: Automation Rules
    create_automation_rule, list_automation_rules, get_automation_rule,
    update_automation_rule, delete_automation_rule,
    evaluate_automation_for_prospect,
)

DB_PATH = os.getenv("DB_PATH", "leadsidekick.db")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await init_db(DB_PATH)
    yield
    await app.state.db.close()


app = FastAPI(
    title="LeadSidekick",
    description=(
        "Lead prospecting + personalised cold outreach drafter. "
        "Prospect management, sequences with pause/resume, campaign analytics, "
        "lead scoring, pipeline tracking, tone A/B testing, prospect activity log, "
        "sequence cloning, do-not-contact list, smart lists (saved filters), "
        "prospect deduplication with merge, threaded prospect notes with pinning, "
        "reusable email snippets, outreach calendar view, email A/B split-testing, "
        "dynamic prospect segments, and trigger-based outreach automation rules."
    ),
    version="1.0.0",
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health():
    return {"status": "ok", "version": "1.0.0"}


# ── Smart Lists ─────────────────────────────────────────────────────────

@app.post("/smart-lists", response_model=SmartListResponse, status_code=201)
async def create_smart(body: SmartListCreate):
    """Create a smart list with saved filter criteria. Matching count is computed dynamically."""
    return await create_smart_list(app.state.db, body.model_dump())


@app.get("/smart-lists", response_model=list[SmartListResponse])
async def get_smart_lists():
    return await list_smart_lists(app.state.db)


@app.get("/smart-lists/{smart_list_id}", response_model=SmartListResponse)
async def get_smart_list_detail(smart_list_id: int):
    sl = await get_smart_list(app.state.db, smart_list_id)
    if not sl:
        raise HTTPException(404, "Smart list not found")
    return sl


@app.get("/smart-lists/{smart_list_id}/prospects", response_model=list[ProspectResponse])
async def smart_list_prospects(smart_list_id: int):
    """Get all prospects matching this smart list's filters."""
    result = await get_smart_list_prospects(app.state.db, smart_list_id)
    if result is None:
        raise HTTPException(404, "Smart list not found")
    return result


@app.delete("/smart-lists/{smart_list_id}", status_code=204)
async def remove_smart_list(smart_list_id: int):
    if not await delete_smart_list(app.state.db, smart_list_id):
        raise HTTPException(404, "Smart list not found")


# ── Do-Not-Contact ───────────────────────────────────────────────────────

@app.post("/dnc", response_model=DncResponse, status_code=201)
async def create_dnc(body: DncCreate):
    """Add an email or domain to the do-not-contact blocklist."""
    if not body.email and not body.domain:
        raise HTTPException(422, "Must provide either email or domain")
    return await add_dnc(app.state.db, body.model_dump())


@app.get("/dnc", response_model=list[DncResponse])
async def get_dnc_list():
    return await list_dnc(app.state.db)


@app.delete("/dnc/{dnc_id}", status_code=204)
async def remove_dnc(dnc_id: int):
    if not await delete_dnc(app.state.db, dnc_id):
        raise HTTPException(404, "DNC entry not found")


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
    result = await create_prospect(app.state.db, body.model_dump())
    if isinstance(result, str) and result.startswith("blocked_by_dnc:"):
        blocked_by = result.split(":", 1)[1]
        raise HTTPException(422, f"Email blocked by DNC list: {blocked_by}")
    return result


@app.post("/prospects/bulk", status_code=201)
async def bulk_import(body: BulkImportRequest):
    return await bulk_import_prospects(
        app.state.db,
        [p.model_dump() for p in body.prospects],
        body.list_id,
    )


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


@app.get("/prospects/duplicates", response_model=list[DuplicateGroup])
async def duplicates():
    """Find potential duplicate prospects by email or by name+company match."""
    return await find_duplicates(app.state.db)


@app.post("/prospects/merge", response_model=MergeResult)
async def merge(body: MergeRequest):
    """Merge two prospects: keep one, transfer all data (drafts, events, enrollments) from the other."""
    result = await merge_prospects(app.state.db, body.keep_id, body.merge_id)
    if result is None:
        raise HTTPException(404, "Keep prospect not found")
    if result == "merge_not_found":
        raise HTTPException(404, "Merge prospect not found")
    if result == "same_prospect":
        raise HTTPException(422, "Cannot merge a prospect with itself")
    return result


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


# ── Prospect Activity Log ────────────────────────────────────────────────

@app.get("/prospects/{prospect_id}/activity", response_model=ProspectActivity)
async def prospect_activity(prospect_id: int):
    """Full timeline of all interactions with a prospect (includes notes)."""
    result = await get_prospect_activity(app.state.db, prospect_id)
    if not result:
        raise HTTPException(404, "Prospect not found")
    return result


# ── Prospect Notes ───────────────────────────────────────────────────────

@app.post("/prospects/{prospect_id}/notes", response_model=ProspectNoteResponse, status_code=201)
async def create_note(prospect_id: int, body: ProspectNoteCreate):
    """Add a threaded note to a prospect."""
    result = await add_prospect_note(app.state.db, prospect_id, body.author, body.content)
    if result is None:
        raise HTTPException(404, "Prospect not found")
    return result


@app.get("/prospects/{prospect_id}/notes", response_model=list[ProspectNoteResponse])
async def get_notes(prospect_id: int):
    """List notes for a prospect. Pinned notes appear first, then by newest first."""
    return await list_prospect_notes(app.state.db, prospect_id)


@app.delete("/prospect-notes/{note_id}", status_code=204)
async def remove_note(note_id: int):
    if not await delete_prospect_note(app.state.db, note_id):
        raise HTTPException(404, "Note not found")


@app.post("/prospect-notes/{note_id}/toggle-pin", response_model=ProspectNoteResponse)
async def toggle_pin(note_id: int):
    """Toggle the pinned state of a prospect note."""
    result = await toggle_pin_note(app.state.db, note_id)
    if result is None:
        raise HTTPException(404, "Note not found")
    return result


# ── Draft Generation ──────────────────────────────────────────────

@app.post("/draft", response_model=DraftResponse)
async def generate_draft_endpoint(body: DraftRequest):
    p = await get_prospect(app.state.db, body.prospect_id)
    if not p:
        raise HTTPException(404, "Prospect not found")
    result = await create_draft(
        app.state.db, body.prospect_id, body.template_id,
        body.tone, body.context, body.value_prop, body.cta,
    )
    if result == "dnc_blocked":
        raise HTTPException(422, "Prospect is on the do-not-contact list")
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


@app.post("/sequences/{sequence_id}/clone", response_model=SequenceResponse, status_code=201)
async def clone_seq(sequence_id: int, body: SequenceCloneRequest):
    """Clone a sequence with all steps. Enrollments are not copied."""
    result = await clone_sequence(app.state.db, sequence_id, body.new_name)
    if not result:
        raise HTTPException(404, "Sequence not found")
    return result


@app.post("/sequences/{sequence_id}/enroll/{prospect_id}", response_model=EnrollmentResponse, status_code=201)
async def enroll(sequence_id: int, prospect_id: int):
    p = await get_prospect(app.state.db, prospect_id)
    if not p:
        raise HTTPException(404, "Prospect not found")
    result = await enroll_prospect(app.state.db, sequence_id, prospect_id)
    if result == "dnc_blocked":
        raise HTTPException(422, "Prospect is on the do-not-contact list")
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


# ── Enrollment Pause/Resume ─────────────────────────────────────────────

@app.post("/enrollments/{enrollment_id}/pause", response_model=EnrollmentActionResult)
async def pause_enroll(enrollment_id: int):
    """Pause an active enrollment. Paused enrollments skip during sequence advance."""
    result = await pause_enrollment(app.state.db, enrollment_id)
    if result is None:
        raise HTTPException(404, "Enrollment not found")
    return result


@app.post("/enrollments/{enrollment_id}/resume", response_model=EnrollmentActionResult)
async def resume_enroll(enrollment_id: int):
    """Resume a paused enrollment. Delay timer resets from resume time."""
    result = await resume_enrollment(app.state.db, enrollment_id)
    if result is None:
        raise HTTPException(404, "Enrollment not found")
    return result


@app.post("/sequences/{sequence_id}/pause-all", response_model=BulkEnrollmentResult)
async def pause_all(sequence_id: int):
    """Pause all active enrollments in a sequence."""
    result = await pause_all_enrollments(app.state.db, sequence_id)
    if result is None:
        raise HTTPException(404, "Sequence not found")
    return result


@app.post("/sequences/{sequence_id}/resume-all", response_model=BulkEnrollmentResult)
async def resume_all(sequence_id: int):
    """Resume all paused enrollments in a sequence. Delay timers reset from now."""
    result = await resume_all_enrollments(app.state.db, sequence_id)
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


# ── Lead Scoring ─────────────────────────────────────────────────────────

@app.get("/prospects/{prospect_id}/score", response_model=LeadScore)
async def lead_score(prospect_id: int):
    """Compute engagement-based lead score (0-100) with grade and breakdown."""
    result = await compute_lead_score(app.state.db, prospect_id)
    if not result:
        raise HTTPException(404, "Prospect not found")
    return result


@app.get("/leads/top", response_model=list[TopLead])
async def top_leads(limit: int = Query(20, ge=1, le=100)):
    """Top prospects ranked by lead score."""
    return await get_top_leads(app.state.db, limit)


# ── Pipeline ─────────────────────────────────────────────────────────────

@app.put("/prospects/{prospect_id}/stage", response_model=ProspectResponse)
async def change_stage(prospect_id: int, body: StageUpdate):
    """Move a prospect to a new pipeline stage."""
    result = await update_prospect_stage(app.state.db, prospect_id, body.stage, body.notes)
    if result is None:
        raise HTTPException(404, "Prospect not found")
    if isinstance(result, str):
        raise HTTPException(422, result)
    return result


@app.get("/pipeline", response_model=PipelineSummary)
async def pipeline_summary():
    """Pipeline funnel with counts per stage and conversion rate."""
    return await get_pipeline_summary(app.state.db)


# ── Tone A/B Analytics ──────────────────────────────────────────────────

@app.get("/analytics/tones", response_model=list[ToneAnalytics])
async def tone_analytics():
    """Compare tone performance: drafts, open rate, reply rate per tone."""
    return await get_tone_analytics(app.state.db)


# ── Email Snippets ───────────────────────────────────────────────────────

@app.post("/snippets", response_model=SnippetResponse, status_code=201)
async def create_snippet_endpoint(body: SnippetCreate):
    """Create a reusable email snippet. Valid categories: general, opener, closer, value_prop, social_proof, cta, objection_handler."""
    from models import SNIPPET_CATEGORIES
    if body.category not in SNIPPET_CATEGORIES:
        raise HTTPException(422, f"Invalid category. Must be one of: {', '.join(sorted(SNIPPET_CATEGORIES))}")
    return await create_snippet(app.state.db, body.model_dump())


@app.get("/snippets", response_model=list[SnippetResponse])
async def list_snippets_endpoint(
    category: str | None = Query(None, description="Filter by category")
):
    """List snippets, optionally filtered by category."""
    return await list_snippets(app.state.db, category)


@app.get("/snippets/{snippet_id}", response_model=SnippetResponse)
async def get_snippet_endpoint(snippet_id: int):
    s = await get_snippet(app.state.db, snippet_id)
    if not s:
        raise HTTPException(404, "Snippet not found")
    return s


@app.delete("/snippets/{snippet_id}", status_code=204)
async def delete_snippet_endpoint(snippet_id: int):
    if not await delete_snippet(app.state.db, snippet_id):
        raise HTTPException(404, "Snippet not found")


@app.post("/snippets/{snippet_id}/use", response_model=SnippetResponse)
async def use_snippet(snippet_id: int):
    """Increment the usage counter for a snippet and return the updated snippet."""
    result = await increment_snippet_usage(app.state.db, snippet_id)
    if result is None:
        raise HTTPException(404, "Snippet not found")
    return result


# ── Outreach Calendar ────────────────────────────────────────────────────

@app.get("/calendar", response_model=OutreachCalendar)
async def outreach_calendar(
    from_date: str = Query(
        default=None,
        description="Start date (ISO format, e.g. 2026-03-13). Defaults to today.",
    ),
    to_date: str = Query(
        default=None,
        description="End date inclusive (ISO format). Defaults to today + 7 days.",
    ),
):
    """
    Calendar view of scheduled sequence outreach activities.
    Returns prospects grouped by the date their next sequence step is due.
    """
    today = date.today()
    if from_date is None:
        from_date = today.isoformat()
    if to_date is None:
        to_date = (today + timedelta(days=7)).isoformat()
    # Basic validation
    try:
        from datetime import datetime as _dt
        _dt.fromisoformat(from_date)
        _dt.fromisoformat(to_date)
    except ValueError:
        raise HTTPException(422, "Dates must be in ISO format (YYYY-MM-DD)")
    if from_date > to_date:
        raise HTTPException(422, "from_date must be <= to_date")
    return await get_outreach_calendar(app.state.db, from_date, to_date)


# ══════════════════════════════════════════════════════════════════════════
# Feature 1: Email A/B Testing (v1.0.0)
# ══════════════════════════════════════════════════════════════════════════

@app.post("/ab-tests", response_model=ABTestResponse, status_code=201)
async def create_ab_test_endpoint(body: ABTestCreate):
    """Create a new A/B test to split-test subject lines and tones."""
    return await create_ab_test(app.state.db, body.model_dump())


@app.get("/ab-tests", response_model=list[ABTestResponse])
async def list_ab_tests_endpoint(
    status: str | None = Query(None, description="running | completed"),
):
    """List all A/B tests, optionally filtered by status."""
    return await list_ab_tests(app.state.db, status)


@app.get("/ab-tests/{test_id}", response_model=ABTestResponse)
async def get_ab_test_endpoint(test_id: int):
    """Get A/B test details with computed variant stats."""
    result = await get_ab_test(app.state.db, test_id)
    if not result:
        raise HTTPException(404, "A/B test not found")
    return result


@app.post("/ab-tests/{test_id}/assign", response_model=ABTestAssignmentResponse, status_code=201)
async def assign_to_test(test_id: int, body: ABTestAssignment):
    """Assign a prospect to an A/B test variant. Auto round-robin if variant omitted."""
    result = await assign_prospect_to_test(
        app.state.db, test_id, body.prospect_id, body.variant,
    )
    if result is None:
        raise HTTPException(404, "A/B test not found")
    if result == "test_not_running":
        raise HTTPException(422, "Test is not running")
    if result == "prospect_not_found":
        raise HTTPException(404, "Prospect not found")
    if result == "dnc_blocked":
        raise HTTPException(422, "Prospect is on the do-not-contact list")
    if result == "already_assigned":
        raise HTTPException(409, "Prospect already assigned to this test")
    if result == "invalid_variant":
        raise HTTPException(422, "Variant must be 'a' or 'b'")
    return result


@app.get("/ab-tests/{test_id}/assignments", response_model=list[ABTestAssignmentResponse])
async def get_test_assignments(test_id: int):
    """List all prospect assignments for an A/B test."""
    result = await list_test_assignments(app.state.db, test_id)
    if result is None:
        raise HTTPException(404, "A/B test not found")
    return result


@app.post("/ab-tests/{test_id}/complete", response_model=ABTestResponse)
async def complete_test(test_id: int, body: ABTestComplete):
    """Complete an A/B test. Auto-determines winner by reply_rate if not specified."""
    result = await complete_ab_test(app.state.db, test_id, body.winner)
    if result is None:
        raise HTTPException(404, "A/B test not found")
    if result == "test_not_running":
        raise HTTPException(422, "Test is not running")
    return result


# ══════════════════════════════════════════════════════════════════════════
# Feature 2: Prospect Segments (v1.0.0)
# ══════════════════════════════════════════════════════════════════════════

@app.post("/segments", response_model=SegmentResponse, status_code=201)
async def create_segment_endpoint(body: SegmentCreate):
    """Create a dynamic segment based on criteria (score range, engagement, status, tags, etc.)."""
    return await create_segment(app.state.db, body.model_dump())


@app.get("/segments", response_model=list[SegmentResponse])
async def list_segments_endpoint():
    """List all segments with their prospect counts."""
    return await list_segments(app.state.db)


@app.get("/segments/{segment_id}", response_model=SegmentResponse)
async def get_segment_endpoint(segment_id: int):
    result = await get_segment(app.state.db, segment_id)
    if not result:
        raise HTTPException(404, "Segment not found")
    return result


@app.patch("/segments/{segment_id}", response_model=SegmentResponse)
async def update_segment_endpoint(segment_id: int, body: SegmentUpdate):
    """Update a segment's name, description, criteria, or auto_assign flag."""
    result = await update_segment(app.state.db, segment_id, body.model_dump(exclude_unset=True))
    if not result:
        raise HTTPException(404, "Segment not found")
    return result


@app.delete("/segments/{segment_id}", status_code=204)
async def delete_segment_endpoint(segment_id: int):
    if not await delete_segment(app.state.db, segment_id):
        raise HTTPException(404, "Segment not found")


@app.post("/segments/{segment_id}/evaluate", response_model=SegmentResponse)
async def evaluate_segment_endpoint(segment_id: int):
    """Re-evaluate a segment: recompute matching prospects and update prospect_count."""
    result = await evaluate_segment(app.state.db, segment_id)
    if not result:
        raise HTTPException(404, "Segment not found")
    return result


@app.get("/segments/{segment_id}/prospects", response_model=list[SegmentProspectEntry])
async def segment_prospects_endpoint(segment_id: int):
    """Get all prospects matching this segment's criteria with their scores."""
    result = await list_segment_prospects(app.state.db, segment_id)
    if result is None:
        raise HTTPException(404, "Segment not found")
    return result


# ══════════════════════════════════════════════════════════════════════════
# Feature 3: Outreach Automation Rules (v1.0.0)
# ══════════════════════════════════════════════════════════════════════════

@app.post("/automations", response_model=AutomationRuleResponse, status_code=201)
async def create_automation_endpoint(body: AutomationRuleCreate):
    """Create a trigger-based automation rule."""
    from models import VALID_TRIGGER_TYPES, VALID_ACTION_TYPES
    if body.trigger_type not in VALID_TRIGGER_TYPES:
        raise HTTPException(422, f"Invalid trigger_type. Must be one of: {', '.join(sorted(VALID_TRIGGER_TYPES))}")
    if body.action_type not in VALID_ACTION_TYPES:
        raise HTTPException(422, f"Invalid action_type. Must be one of: {', '.join(sorted(VALID_ACTION_TYPES))}")
    return await create_automation_rule(app.state.db, body.model_dump())


@app.get("/automations", response_model=list[AutomationRuleResponse])
async def list_automations_endpoint(
    enabled: bool | None = Query(None, description="Filter by enabled state"),
    trigger_type: str | None = Query(None, description="Filter by trigger type"),
):
    """List automation rules with optional filters."""
    return await list_automation_rules(app.state.db, enabled, trigger_type)


@app.get("/automations/{rule_id}", response_model=AutomationRuleResponse)
async def get_automation_endpoint(rule_id: int):
    result = await get_automation_rule(app.state.db, rule_id)
    if not result:
        raise HTTPException(404, "Automation rule not found")
    return result


@app.patch("/automations/{rule_id}", response_model=AutomationRuleResponse)
async def update_automation_endpoint(rule_id: int, body: AutomationRuleUpdate):
    """Update an automation rule's config or enabled state."""
    result = await update_automation_rule(app.state.db, rule_id, body.model_dump(exclude_unset=True))
    if not result:
        raise HTTPException(404, "Automation rule not found")
    return result


@app.delete("/automations/{rule_id}", status_code=204)
async def delete_automation_endpoint(rule_id: int):
    if not await delete_automation_rule(app.state.db, rule_id):
        raise HTTPException(404, "Automation rule not found")


@app.post("/automations/evaluate/{prospect_id}")
async def evaluate_automation_endpoint(prospect_id: int, body: AutomationEvaluateRequest):
    """Manually trigger automation evaluation for a prospect with specific trigger data."""
    p = await get_prospect(app.state.db, prospect_id)
    if not p:
        raise HTTPException(404, "Prospect not found")
    from models import VALID_TRIGGER_TYPES
    if body.trigger_type not in VALID_TRIGGER_TYPES:
        raise HTTPException(422, f"Invalid trigger_type. Must be one of: {', '.join(sorted(VALID_TRIGGER_TYPES))}")
    fired = await evaluate_automation_for_prospect(
        app.state.db, prospect_id, body.trigger_type, body.trigger_data,
    )
    return {"prospect_id": prospect_id, "rules_fired": fired, "count": len(fired)}
