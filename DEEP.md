# LeadSidekick — Architecture (DEEP.md)

## Overview
Lead prospecting + personalised cold outreach drafter. Find prospects, generate tailored emails, manage pipeline, score leads.

## Stack
- **Runtime**: Python 3.11+ / FastAPI / uvicorn
- **Database**: aiosqlite (SQLite WAL mode)
- **Models**: Pydantic v2 with Field validation

## API Endpoints (v0.6.0) — 30+ endpoints

### Prospect Lists
- POST /lists, GET /lists

### Prospects
- POST /prospects, POST /prospects/bulk, GET /prospects, GET /prospects/{id}
- PATCH /prospects/{id}/status, GET /prospects/search, GET /prospects/export/csv

### Lead Scoring
- GET /prospects/{id}/score — engagement-based score (0-100) with breakdown
- GET /leads/top — top prospects ranked by score

### Pipeline
- PUT /prospects/{id}/stage — move through stages (new/contacted/interested/qualified/converted/lost)
- GET /pipeline — funnel summary with conversion rate

### Draft Generation
- POST /draft, GET /drafts, GET /drafts/{id}

### Templates
- POST /templates, GET /templates

### Sequences
- POST /sequences, GET /sequences
- POST /sequences/{id}/enroll/{prospect_id}
- GET /sequences/{id}/enrollments
- POST /sequences/{id}/advance

### Tags
- POST /prospects/{id}/tags, DELETE /prospects/{id}/tags

### Analytics
- GET /analytics/campaigns — overall campaign stats
- GET /analytics/tones — A/B tone comparison (open/reply rate per tone)
- GET /sequences/{id}/analytics — per-sequence performance
- POST /events — record campaign event

### Stats & Health
- GET /stats — usage statistics
- GET /health

## Key Features
- **Lead Scoring**: 5 factors (profile completeness, engagement, recency, tags, enrollment) → hot/warm/cold grade
- **Pipeline Stages**: 6-stage funnel with automatic stage_change event logging
- **Tone A/B Analytics**: Compare drafts/open_rate/reply_rate across 4 tones
- **Sequences**: Multi-step follow-up with delay-based progression
- **Tags**: Freeform segmentation tags on prospects
- **Campaign Events**: Track sent/opened/replied/bounced/clicked

## Version History
- v0.1.0: Prospects, draft generation, templates
- v0.2.0: Draft history, prospect lists
- v0.3.0: Prospect search, CSV export
- v0.4.0: Follow-up sequences with enrollment
- v0.5.0: Tags, campaign events, sequence analytics
- v0.6.0: Lead scoring, pipeline stages, tone A/B analytics
