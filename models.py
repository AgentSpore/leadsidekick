from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional


class ProspectCreate(BaseModel):
    first_name: str = Field(min_length=1, max_length=80)
    last_name: str = Field(min_length=1, max_length=80)
    email: str = Field(min_length=3, max_length=200)
    company: str = Field(min_length=1, max_length=200)
    job_title: Optional[str] = None
    website: Optional[str] = None
    linkedin_url: Optional[str] = None
    notes: Optional[str] = None
    list_id: Optional[int] = None
    tags: list[str] = Field(default_factory=list, description="Tags for segmentation")


class ProspectResponse(BaseModel):
    id: int
    first_name: str
    last_name: str
    email: str
    company: str
    job_title: Optional[str]
    website: Optional[str]
    linkedin_url: Optional[str]
    notes: Optional[str]
    list_id: Optional[int]
    tags: list[str]
    status: str
    created_at: str


class DraftRequest(BaseModel):
    prospect_id: int
    template_id: Optional[int] = None
    tone: str = Field("professional", description="professional | friendly | direct | witty")
    context: Optional[str] = Field(None, description="Custom context about the prospect")
    value_prop: str = Field(..., description="Value proposition to highlight")
    cta: str = Field("book a 15-min call", description="Call to action")


class DraftResponse(BaseModel):
    prospect_id: int
    subject: str
    body: str
    tone: str
    personalization_signals: list[str]
    word_count: int
    draft_id: int


class DraftLogResponse(BaseModel):
    id: int
    prospect_id: int
    template_id: Optional[int]
    tone: str
    subject: str
    body: str
    word_count: int
    created_at: str


class TemplateCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    subject_template: str
    body_template: str
    tone: str = Field("professional")
    category: str = Field("cold", description="cold | warm | followup | nurture")


class TemplateResponse(BaseModel):
    id: int
    name: str
    subject_template: str
    body_template: str
    tone: str
    category: str
    times_used: int
    created_at: str


class ProspectListCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    description: Optional[str] = None


class ProspectListResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    prospect_count: int
    created_at: str


class BulkImportRequest(BaseModel):
    prospects: list[ProspectCreate] = Field(min_length=1, max_length=500)
    list_id: Optional[int] = None


class UsageStats(BaseModel):
    total_prospects: int
    total_drafts_generated: int
    total_lists: int
    total_templates: int
    total_sequences: int
    total_dnc_entries: int
    total_smart_lists: int
    by_status: dict[str, int]
    most_used_tone: Optional[str]


class SequenceStep(BaseModel):
    delay_days: int = Field(ge=0, le=90)
    tone: str = Field("professional")
    subject_hint: str = Field(min_length=1)


class SequenceCreate(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    value_prop: str
    cta: str = Field("book a 15-min call")
    steps: list[SequenceStep] = Field(min_length=1, max_length=10)


class SequenceResponse(BaseModel):
    id: int
    name: str
    value_prop: str
    cta: str
    steps: list[dict]
    total_enrolled: int
    created_at: str


class EnrollmentResponse(BaseModel):
    id: int
    sequence_id: int
    prospect_id: int
    prospect_name: str
    prospect_email: str
    current_step: int
    total_steps: int
    status: str
    enrolled_at: str
    last_advanced_at: Optional[str]


class AdvanceResult(BaseModel):
    advanced: int
    already_complete: int
    not_due: int
    drafts_generated: list[int]


# ── Campaign Analytics ───────────────────────────────────────────────────

class CampaignEventCreate(BaseModel):
    prospect_id: int
    sequence_id: Optional[int] = None
    draft_id: Optional[int] = None
    event_type: str = Field(..., description="sent | opened | replied | bounced | clicked")
    metadata: Optional[dict] = None


class SequenceAnalytics(BaseModel):
    sequence_id: int
    sequence_name: str
    total_enrolled: int
    completed: int
    events: dict[str, int]
    sent: int
    opened: int
    replied: int
    bounced: int
    open_rate: float
    reply_rate: float
    bounce_rate: float


class CampaignOverview(BaseModel):
    total_events: int
    by_type: dict[str, int]
    sent: int
    opened: int
    replied: int
    overall_open_rate: float
    overall_reply_rate: float
    top_sequences: list[dict]


# ── Lead Scoring ────────────────────────────────────────────────────────

class LeadScore(BaseModel):
    prospect_id: int
    score: int
    grade: str  # hot | warm | cold
    breakdown: dict[str, int]
    events_summary: dict[str, int]


class TopLead(BaseModel):
    prospect_id: int
    name: str
    email: str
    company: str
    score: int
    grade: str
    status: str


# ── Pipeline ────────────────────────────────────────────────────────────

class StageUpdate(BaseModel):
    stage: str = Field(..., description="new | contacted | interested | qualified | converted | lost")
    notes: Optional[str] = None


class PipelineSummary(BaseModel):
    total_prospects: int
    stages: list[dict]
    conversion_rate: float


class ToneAnalytics(BaseModel):
    tone: str
    drafts: int
    sent: int
    opened: int
    replied: int
    open_rate: float
    reply_rate: float


# ── Prospect Activity Log ───────────────────────────────────────────────

class ActivityEntry(BaseModel):
    type: str
    timestamp: str
    detail: str
    metadata: Optional[dict] = None


class ProspectActivity(BaseModel):
    prospect_id: int
    prospect_name: str
    total_events: int
    activity: list[ActivityEntry]


# ── Sequence Clone ──────────────────────────────────────────────────────

class SequenceCloneRequest(BaseModel):
    new_name: Optional[str] = Field(None, min_length=1, max_length=80)


# ── Do-Not-Contact ──────────────────────────────────────────────────────

class DncCreate(BaseModel):
    email: Optional[str] = Field(None, description="Exact email to block")
    domain: Optional[str] = Field(None, description="Domain to block (e.g. example.com)")
    reason: Optional[str] = Field(None, max_length=200)


class DncResponse(BaseModel):
    id: int
    email: Optional[str]
    domain: Optional[str]
    reason: Optional[str]
    created_at: str


# ── Smart Lists ─────────────────────────────────────────────────────────

class SmartListCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    filters: dict = Field(
        ...,
        description="Filter criteria: status, list_id, tag, company_contains, job_title_contains",
    )


class SmartListResponse(BaseModel):
    id: int
    name: str
    filters: dict
    matching_count: int
    created_at: str


# ── Enrollment Pause/Resume ─────────────────────────────────────────────

class EnrollmentActionResult(BaseModel):
    enrollment_id: int
    status: str
    message: str


class BulkEnrollmentResult(BaseModel):
    affected: int
    skipped: int


# ── Prospect Merge ──────────────────────────────────────────────────────

class DuplicateGroup(BaseModel):
    key: str
    prospect_ids: list[int]
    names: list[str]
    emails: list[str]


class MergeRequest(BaseModel):
    keep_id: int = Field(..., description="Prospect ID to keep")
    merge_id: int = Field(..., description="Prospect ID to merge into keep_id and delete")


class MergeResult(BaseModel):
    kept_prospect_id: int
    merged_prospect_id: int
    transferred_drafts: int
    transferred_events: int
    transferred_enrollments: int


# ── Prospect Notes ──────────────────────────────────────────────────────

class ProspectNoteCreate(BaseModel):
    author: str = Field(min_length=1, max_length=120)
    content: str = Field(min_length=1)


class ProspectNoteResponse(BaseModel):
    id: int
    prospect_id: int
    author: str
    content: str
    pinned: bool
    created_at: str


# ── Email Snippets ──────────────────────────────────────────────────────

SNIPPET_CATEGORIES = {"general", "opener", "closer", "value_prop", "social_proof", "cta", "objection_handler"}


class SnippetCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    content: str = Field(min_length=1)
    category: str = Field("general", description="general | opener | closer | value_prop | social_proof | cta | objection_handler")


class SnippetResponse(BaseModel):
    id: int
    name: str
    content: str
    category: str
    times_used: int
    created_at: str


# ── Outreach Calendar ───────────────────────────────────────────────────

class CalendarProspect(BaseModel):
    prospect_id: int
    name: str
    email: str
    sequence_name: str
    step_num: int
    subject_hint: str


class CalendarDay(BaseModel):
    date: str
    count: int
    prospects: list[CalendarProspect]


class CalendarSequenceSummary(BaseModel):
    id: int
    name: str
    scheduled_count: int


class OutreachCalendar(BaseModel):
    from_date: str
    to_date: str
    total_scheduled: int
    by_date: list[CalendarDay]
    by_sequence: list[CalendarSequenceSummary]
