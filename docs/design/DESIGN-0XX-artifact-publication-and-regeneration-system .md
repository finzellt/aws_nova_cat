# DESIGN-XXX: Artifact Publication and Regeneration System (MVP)

## 1. Overview

This document defines the system responsible for transforming backend data products into frontend-consumable artifacts and publishing them to the public catalog.

The system operates on a **per-nova invalidation model** but executes work in **bulk batches** to optimize cost.

---

## 2. Core Principles

- **Per-nova correctness, bulk execution efficiency**
- **Artifacts are always fully regenerated (no partial updates)**
- **Frontend consumes only precomputed artifacts**
- **Cost optimization is a primary constraint**
- **Publication is eventual (not real-time)**

---

## 3. Artifact Model

### Per-nova artifacts
- `spectra.json`
- `photometry.json`
- `sparkline.svg`
- `references.json`
- `nova.json`
- `bundle.zip`

### Global artifact
- `catalog.json`

---

## 4. Invalidation Model

### Dirty flags (logical)
- spectra
- photometry
- color
- references

### Invalidation rules

| Change Type      | Artifacts Rebuilt |
|------------------|------------------|
| spectra          | spectra.json, nova.json, bundle.zip, catalog.json |
| photometry/color | photometry.json, sparkline.svg, nova.json, bundle.zip, catalog.json |
| references       | references.json, nova.json, catalog.json |

### Invariants

- `nova.json` is rebuilt on any change
- `bundle.zip` is rebuilt only for scientific data (spectra/photometry/color)
- `catalog.json` is updated for all changes

---

## 5. Work Item Model

Instead of boolean flags, regeneration is driven by **regen work items** stored in DynamoDB.

Each item represents a change that requires regeneration.

### Fields (conceptual)
- `nova_id`
- `change_type` (spectra | photometry | color | references)
- `data_product_type` (optional)
- `data_product_id` (optional)
- `created_at`

### Behavior
- Multiple items may exist per nova
- Items are **consumed and deleted** upon successful regeneration
- Items are grouped by `nova_id` during processing

---

## 6. Regeneration Model

### 6.1 Trigger (MVP)

- A **daily cron job** initiates regeneration
- If no work items exist, the job exits immediately

---

### 6.2 Coordinator Phase

The regeneration coordinator:

1. Scans DynamoDB for pending work items
2. Groups items by `nova_id`
3. Computes required artifact families per nova
4. Constructs a **batch execution plan**

---

### 6.3 Execution Strategy

Execution is split by workload size:

- **Small workload** → Lambda execution
- **Large workload** → Single Fargate task

The goal is to:
- minimize container startup overhead
- avoid Lambda execution limits
- batch expensive operations (especially bundle generation)

---

### 6.4 Batch Execution Model (Fargate)

A single long-running task executes the full publication pipeline:

#### Phase 1: Load Plan
- Load all work items
- Build per-nova requirements
- Build per-artifact-family batches

#### Phase 2: Heavy Artifact Generation (Bulk)
- Spectra visualizations (`spectra.json`)
- Photometry visualizations (`photometry.json`, `sparkline.svg`)
- Bundles (`bundle.zip`)

Execution is batched across many novas for efficiency.

---

#### Phase 3: Manifest Writes

Each artifact-family operation writes a **per-nova manifest record**:

Fields:
- `nova_id`
- `artifact_family`
- `status` (SUCCEEDED | FAILED)
- `outputs`
- `completed_at`

---

#### Phase 4: Finalization (Per-Nova)

For each nova:

If all required artifact families have reached terminal state:

- regenerate `nova.json`
- regenerate `references.json` (if applicable)

---

#### Phase 5: Global Update

- regenerate or patch `catalog.json`

---

#### Phase 6: Cleanup

- delete satisfied work items
- retain failed items for retry
- optionally record failure metadata

---

## 7. Execution Model Summary

| Layer              | Responsibility |
|--------------------|--------------|
| Work Items         | Define what changed |
| Coordinator        | Build execution plan |
| Batch Executor     | Perform heavy work (bulk) |
| Manifest Layer     | Record per-nova completion |
| Finalizer          | Produce `nova.json` + update catalog |

---

## 8. Key Design Decisions

- Bulk execution via **single Fargate task**
- Per-nova correctness enforced via **manifest + finalization**
- Separation of:
  - heavy artifact generation
  - lightweight aggregation (`nova.json`, `catalog.json`)
- No dependency on previously generated artifacts

---

## 9. Open Questions / Future ADRs

### 9.1 Trigger Strategy
- Cron (MVP)
- Event-driven queue
- Hybrid batching model

### 9.2 Lambda vs Fargate Threshold
- When to switch execution modes

### 9.3 Work Item Storage
- Same DynamoDB table vs dedicated table

### 9.4 Manifest Retention
- Temporary vs persistent audit record

### 9.5 Catalog Update Strategy
- Full rebuild vs incremental patching

---

## 10. Non-Goals (MVP)

- Real-time frontend updates
- Fine-grained partial artifact updates
- Cross-nova transactional guarantees
