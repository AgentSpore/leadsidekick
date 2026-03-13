# LeadSidekick — Development Log (MEMORY.md)

## v0.1.0 — Initial MVP
- Prospect CRUD (first_name, last_name, email, company, job_title)
- Draft generation with 4-tone system (professional/friendly/direct/witty)
- Template system with {{placeholder}} rendering

## v0.2.0 — Lists & Bulk Import
- Prospect lists for organization
- Bulk import (up to 500 prospects)
- Search across 5 fields with LIKE
- CSV export with list/status filters

## v0.3.0 — Sequences
- Multi-step outreach sequences (delay_days, tone, subject_hint)
- Prospect enrollment with step tracking
- Advance sequence: auto-generate followup drafts when due

## v0.4.0 — Draft History & Stats
- Draft log with prospect/template/tone tracking
- Usage stats with status breakdown and most-used tone
- Draft list with prospect_id and tone filters

## v0.5.0 — Tags & Campaign Analytics
- **Prospect tags**: JSON array, add/remove endpoints, filter by tag
- Tags stored lowercase, deduplicated on add
- **Campaign events**: sent/opened/replied/bounced/clicked tracking
- Per-sequence analytics: enrollment funnel + open/reply/bounce rates
- Campaign overview: totals by event type, top sequences
- DEEP.md + MEMORY.md added
