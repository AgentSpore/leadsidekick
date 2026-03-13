# LeadSidekick — Architecture (DEEP.md)

## Overview
Lead finder + personalized cold outreach drafter. API-first service for managing prospects, generating tone-aware email drafts, running multi-step sequences, and tracking campaign analytics.

## Data Model
- **prospect_lists** — named lists for organizing prospects
- **prospects** — contact info + job_title + tags (JSON array) + status
- **templates** — subject/body templates with {{placeholder}} support
- **draft_log** — generated drafts with tone and template reference
- **sequences** — multi-step outreach (steps with delay_days, tone, subject_hint)
- **enrollments** — prospect-to-sequence mapping with step tracking
- **campaign_events** — sent/opened/replied/bounced/clicked tracking
- **usage_stats** — singleton total_drafts counter

## Tone System
4 tones with distinct openers, closers, and followup openers:
- professional, friendly, direct, witty

## Draft Generation Pipeline
1. Load prospect data
2. Select tone-appropriate opener (deterministic hash-based selection)
3. Build subject line (tone-dependent format)
4. Compose body with value_prop, context, CTA
5. If template_id provided: render template with {{placeholders}} override
6. Log to draft_log, increment usage_stats

## Tag System
- Tags stored as JSON array on prospects
- Case-insensitive, deduplicated on add
- Filter: LIKE query on JSON text (simple, no FTS needed)

## Campaign Analytics
- Events table: prospect_id, sequence_id, draft_id, event_type, metadata
- Per-sequence analytics: enrolled/completed/sent/opened/replied/bounced + rates
- Campaign overview: totals + top sequences by event count

## API Surface (v0.5.0)
19 endpoints covering prospects, drafts, templates, lists, sequences, tags, events, analytics.
