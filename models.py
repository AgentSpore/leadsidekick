from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional


class ProspectCreate(BaseModel):
    first_name: str
    last_name: str
    email: str
    company: str
    job_title: Optional[str] = None
    website: Optional[str] = None
    linkedin_url: Optional[str] = None
    notes: Optional[str] = None
    list_id: Optional[int] = None


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
    status: str
    created_at: str


class DraftRequest(BaseModel):
    prospect_id: int
    template_id: Optional[int] = None
    tone: str = Field("professional", description="Tone: professional | friendly | direct | witty")
    context: Optional[str] = Field(None, description="Extra context: their pain, recent news, shared interest")
    your_value_prop: str = Field(..., description="One-sentence value proposition of your product/service")
    cta: str = Field("book a 15-min call", description="Call to action")


class DraftResponse(BaseModel):
    prospect_id: int
    subject: str
    body: str
    tone: str
    word_count: int
    personalization_signals: list[str]


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
    name: str
    subject_template: str = Field(..., description="Subject with {{first_name}}, {{company}} placeholders")
    body_template: str = Field(..., description="Body with {{first_name}}, {{company}}, {{value_prop}}, {{cta}} placeholders")
    tone: str = Field("professional")
    category: str = Field("cold", description="cold | follow_up | referral | event")


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
    name: str
    description: Optional[str] = None


class ProspectListResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    prospect_count: int
    created_at: str


class BulkImportRequest(BaseModel):
    prospects: list[ProspectCreate] = Field(..., min_length=1, max_length=500)
    list_id: Optional[int] = None


class UsageStats(BaseModel):
    total_prospects: int
    total_drafts_generated: int
    total_lists: int
    total_templates: int
    total_sequences: int
    by_status: dict
    most_used_tone: Optional[str]


class SequenceStep(BaseModel):
    delay_days: int = Field(ge=0, le=90, description="Days to wait before sending this step")
    tone: str = Field("professional", description="professional | friendly | direct | witty")
    subject_hint: str = Field(..., min_length=1, max_length=200, description="Subject line hint for this step")


class SequenceCreate(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    value_prop: str = Field(..., description="Value proposition used in all steps")
    cta: str = Field("book a 15-min call", description="Call to action")
    steps: list[SequenceStep] = Field(min_length=1, max_length=10)


class SequenceResponse(BaseModel):
    id: int
    name: str
    value_prop: str
    cta: str
    steps: list[SequenceStep]
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
