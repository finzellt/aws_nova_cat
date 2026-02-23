# DynamoDB Item Model (Epic 3)

This document defines high-level item types, fields, and example JSON items for Nova Cat persistence.
Design goals:
- Minimal, cost-conscious, **access-pattern driven**
- Primarily **per-nova partitioned**
- Deterministic dataset state transitions, retries, and validation outcomes
- Store only what workflows must query; keep large blobs in S3

## Single Table Overview

Table: `NovaCat` with `(PK, SK)`.

Namespaces:
- Per-nova partition: `PK = NOVA#<nova_id>`
- Name resolution: `PK = NAME#<normalized_name>`
- Optional additional namespaces later: `REF#...`, `DATASET#...` (not required initially)

Common fields:
- `entity_type` (string)
- `created_at` (ISO-8601 UTC)
- `updated_at` (ISO-8601 UTC)
- `schema_version` (string; for internal item evolution, not event schema version)

Indexes (see access patterns doc):
- `GSI1PK`, `GSI1SK` (eligibility)
- `GSI2PK`, `GSI2SK` (name resolution)
- `GSI3PK`, `GSI3SK` (S3 lookup)

---

## Item Types

### 1) Nova

Identity + minimal mutable state for a nova.

Key:
- `PK = NOVA#<nova_id>`
- `SK = META`

Suggested fields:
- `nova_id` (UUID string)
- `preferred_name` (string; human-readable; only set/changed at resolution boundary)
- `normalized_preferred_name` (string)
- `aliases` (list of strings; optional convenience cache)
- `discovery_date` (ISO-8601 date or datetime; derived from references)
- `status` (string; e.g., ACTIVE, MERGED, DEPRECATED)
- `last_ingest_at` (ISO-8601 UTC)
- `version_counter` (number; optional optimistic concurrency)

Example:
```json
{
  "PK": "NOVA#4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1",
  "SK": "META",
  "entity_type": "Nova",
  "schema_version": "1",
  "nova_id": "4e9b0e88-5d2b-4d1a-9a1a-4a4f6f0cb9b1",
  "preferred_name": "V1324 Sco",
  "normalized_preferred_name": "v1324 sco",
  "aliases": ["Nova Sco 2012", "V1324 Sco"],
  "discovery_date": "2012-06-01",
  "status": "ACTIVE",
  "last_ingest_at": "2026-02-21T20:05:11Z",
  "created_at": "2026-02-21T20:00:00Z",
  "updated_at": "2026-02-21T20:05:11Z"
}
