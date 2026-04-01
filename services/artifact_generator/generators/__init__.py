"""Artifact generators for the regeneration pipeline (DESIGN-003 Epic 3).

Each module implements a single per-nova generator that reads from DynamoDB
and/or S3, applies all computation, and returns a frontend-ready artifact
conforming to the ADR-014 schemas.

Modules
-------
shared
    Pure-computation utilities consumed by multiple generators: outburst
    MJD resolution, coordinate formatting, timestamp helper, LTTB
    downsampling.
references
    ``references.json`` generator (§6).
spectra
    ``spectra.json`` generator (§7).
photometry
    ``photometry.json`` generator (§8).
sparkline
    ``sparkline.svg`` generator (§9).
nova
    ``nova.json`` generator (§5).
bundle
    ``bundle.zip`` generator (§10).
"""
