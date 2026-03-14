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
    total_segments: int
    total_ab_tests: int
    total_automation_rules: int
    total_email_accounts: int
    total_enrichment_lookups: int
    total_replies_analyzed: int
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


# ── Email A/B Testing (v1.0.0) ──────────────────────────────────────────

class VariantStats(BaseModel):
    sent: int
    opened: int
    replied: int
    open_rate: float
    reply_rate: float


class ABTestCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    sequence_id: Optional[int] = None
    variant_a_tone: str = Field(..., description="Tone for variant A")
    variant_a_subject_hint: str = Field(..., description="Subject hint for variant A")
    variant_b_tone: str = Field(..., description="Tone for variant B")
    variant_b_subject_hint: str = Field(..., description="Subject hint for variant B")


class ABTestResponse(BaseModel):
    id: int
    name: str
    sequence_id: Optional[int]
    variant_a_tone: str
    variant_a_subject_hint: str
    variant_b_tone: str
    variant_b_subject_hint: str
    status: str
    winner: Optional[str]
    variant_a_stats: VariantStats
    variant_b_stats: VariantStats
    created_at: str
    completed_at: Optional[str]


class ABTestAssignment(BaseModel):
    prospect_id: int
    variant: Optional[str] = Field(None, description="a | b  (auto round-robin if omitted)")


class ABTestComplete(BaseModel):
    winner: Optional[str] = Field(None, description="a | b  (auto-determine if omitted)")


class ABTestAssignmentResponse(BaseModel):
    id: int
    test_id: int
    prospect_id: int
    variant: str
    created_at: str


# ── Prospect Segments (v1.0.0) ──────────────────────────────────────────

class SegmentCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    description: Optional[str] = None
    criteria: dict = Field(
        ...,
        description="Criteria: score_min, score_max, min_events, status, has_tag, company_contains, enrolled_in_sequence",
    )
    auto_assign: bool = Field(False, description="Auto-evaluate on segment evaluate call")


class SegmentUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=120)
    description: Optional[str] = None
    criteria: Optional[dict] = None
    auto_assign: Optional[bool] = None


class SegmentResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    criteria: dict
    auto_assign: bool
    prospect_count: int
    created_at: str


class SegmentProspectEntry(BaseModel):
    prospect_id: int
    name: str
    email: str
    company: str
    score: int
    grade: str


# ── Outreach Automation Rules (v1.0.0) ──────────────────────────────────

VALID_TRIGGER_TYPES = {"status_change", "event_received", "score_threshold", "enrollment_completed"}
VALID_ACTION_TYPES = {"enroll_in_sequence", "add_tag", "remove_tag", "change_status", "pause_enrollment"}


class AutomationRuleCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    trigger_type: str = Field(..., description="status_change | event_received | score_threshold | enrollment_completed")
    trigger_config: dict = Field(..., description="Trigger-specific configuration")
    action_type: str = Field(..., description="enroll_in_sequence | add_tag | remove_tag | change_status | pause_enrollment")
    action_config: dict = Field(..., description="Action-specific configuration")
    is_enabled: bool = Field(True)


class AutomationRuleUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=120)
    trigger_config: Optional[dict] = None
    action_config: Optional[dict] = None
    is_enabled: Optional[bool] = None


class AutomationRuleResponse(BaseModel):
    id: int
    name: str
    trigger_type: str
    trigger_config: dict
    action_type: str
    action_config: dict
    is_enabled: bool
    times_fired: int
    last_fired_at: Optional[str]
    created_at: str


class AutomationEvaluateRequest(BaseModel):
    trigger_type: str = Field(..., description="status_change | event_received | score_threshold | enrollment_completed")
    trigger_data: dict = Field(default_factory=dict, description="Trigger-specific data for evaluation")


# ══════════════════════════════════════════════════════════════════════════
# v1.1.0 Feature 1: Email Warmup Tracking
# ══════════════════════════════════════════════════════════════════════════

class EmailAccountCreate(BaseModel):
    email: str = Field(min_length=3, max_length=200)
    provider: str = Field(min_length=1, max_length=80, description="e.g. gmail, outlook, sendgrid")
    daily_limit: int = Field(ge=1, le=10000, description="Max emails per day")
    warmup_target_limit: int = Field(ge=1, le=10000, description="Target daily limit after warmup")


class EmailAccountUpdate(BaseModel):
    daily_limit: Optional[int] = Field(None, ge=1, le=10000)
    status: Optional[str] = Field(None, description="warming | active | paused")


class EmailAccountResponse(BaseModel):
    id: int
    email: str
    provider: str
    daily_limit: int
    current_daily_sent: int
    warmup_start_date: str
    warmup_day: int
    warmup_target_limit: int
    status: str
    reputation_score: float
    bounce_rate: float
    spam_rate: float
    created_at: str
    updated_at: str


class WarmupLogCreate(BaseModel):
    sent_count: int = Field(ge=0)
    delivered_count: int = Field(ge=0)
    bounced_count: int = Field(ge=0, default=0)
    spam_count: int = Field(ge=0, default=0)


class WarmupLogResponse(BaseModel):
    id: int
    account_id: int
    date: str
    sent_count: int
    delivered_count: int
    bounced_count: int
    spam_count: int
    reputation_delta: float
    created_at: str


class WarmupProgress(BaseModel):
    account_id: int
    email: str
    status: str
    warmup_day: int
    current_daily_limit: int
    warmup_target_limit: int
    reputation_score: float
    bounce_rate: float
    spam_rate: float
    projected_completion_days: int
    health: str


# ══════════════════════════════════════════════════════════════════════════
# v1.1.0 Feature 2: Prospect Enrichment Log
# ══════════════════════════════════════════════════════════════════════════

class EnrichmentProviderCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    api_type: str = Field(min_length=1, max_length=80, description="e.g. rest, graphql, scraper")
    priority: int = Field(ge=1, le=100, default=10)
    is_enabled: bool = Field(True)


class EnrichmentProviderUpdate(BaseModel):
    priority: Optional[int] = Field(None, ge=1, le=100)
    is_enabled: Optional[bool] = None


class EnrichmentProviderResponse(BaseModel):
    id: int
    name: str
    api_type: str
    priority: int
    is_enabled: bool
    total_lookups: int
    successful_lookups: int
    created_at: str


class EnrichmentLogResponse(BaseModel):
    id: int
    prospect_id: int
    source: str
    fields_before: dict
    fields_after: dict
    fields_updated: list[str]
    status: str
    error_message: Optional[str]
    created_at: str


class BulkEnrichRequest(BaseModel):
    prospect_ids: list[int] = Field(min_length=1, max_length=100)


class BulkEnrichResult(BaseModel):
    total: int
    enriched: int
    failed: int
    skipped: int


class EnrichmentStats(BaseModel):
    total_lookups: int
    successful_lookups: int
    success_rate: float
    top_fields_enriched: dict[str, int]
    by_provider: list[dict]


# ══════════════════════════════════════════════════════════════════════════
# v1.1.0 Feature 3: Reply Sentiment Analysis
# ══════════════════════════════════════════════════════════════════════════

class ReplyCreate(BaseModel):
    prospect_id: int
    sequence_id: Optional[int] = None
    draft_id: Optional[int] = None
    reply_text: str = Field(min_length=1)


class ReplyAnalysisResponse(BaseModel):
    id: int
    prospect_id: int
    sequence_id: Optional[int]
    draft_id: Optional[int]
    reply_text: str
    sentiment: str
    confidence: float
    key_phrases: list[str]
    auto_action_taken: Optional[str]
    created_at: str


class ReplyAnalytics(BaseModel):
    total_replies: int
    by_sentiment: dict[str, int]
    avg_confidence: float
    sentiment_trend: list[dict]
    auto_actions_summary: dict[str, int]


class SequenceSentiment(BaseModel):
    sequence_id: int
    sequence_name: str
    total_replies: int
    reply_rate: float
    sentiment_distribution: dict[str, int]
    top_positive_phrases: list[str]
    top_negative_phrases: list[str]
