# ADR-033 Amendment: Compositing Purpose Reframing

**Date:** 2026-04-14
**Author:** TF
**Amends:** ADR-033 (Spectra Compositing Pipeline)

---

## Motivation

ADR-033 as originally written frames compositing primarily as an SNR-boosting
operation. This is inaccurate. The compositing pipeline's actual purpose is to
produce a **single representative spectrum per instrument per night** by stitching
disjoint wavelength ranges and stacking overlapping ones. The SNR improvement
in overlap regions is a welcome side effect, not the motivation.

The original framing led to confusion about whether compositing should be limited
to spectra with wavelength overlap (it should not) and whether SNR improvement is
a precondition for compositing to be worthwhile (it is not). A group of two
non-overlapping spectra covering 300–500 nm and 500–900 nm benefits from
compositing just as much as two identical-range spectra — the result is a single
spectrum spanning the full 300–900 nm range, which is a more complete picture of
the nova's state on that night than either spectrum alone.

This amendment corrects the framing in four locations.

---

## Amended §1 Context — Second Paragraph

**Original text:**

> These spectra are independently valid, but when displayed individually in the
> waterfall plot they add visual clutter without adding scientific information.
> Compositing them — resampling onto a common wavelength grid and averaging —
> produces a single higher-SNR spectrum per instrument per night that better
> represents the data.

**Replaced with:**

> These spectra are independently valid, but when displayed individually in the
> waterfall plot they add visual clutter without adding scientific information.
> Compositing them — resampling onto a common wavelength grid and combining —
> produces a single representative spectrum per instrument per night: disjoint
> wavelength ranges are stitched into continuous coverage, and overlapping ranges
> are stacked. The result is a unified view of the nova's spectral state on that
> night, replacing N redundant or partial entries in the waterfall plot with one
> coherent spectrum.

---

## Amended §1.1 Scope — Last Sentence

**Original text:**

> Different instruments have independent flux calibrations, and normalizing across
> instruments introduces systematic uncertainty that outweighs the SNR benefit
> for a display-oriented catalog.

**Replaced with:**

> Different instruments have independent flux calibrations, and normalizing across
> instruments introduces systematic uncertainty that undermines the goal of
> producing a faithful per-night representation.

---

## Amended §3 Consequences — Positive Bullet 1

**Original text:**

> **Improved waterfall plot readability:** Dense same-night observations collapse
> into single high-SNR composites, reducing visual clutter while preserving
> temporal evolution between nights.

**Replaced with:**

> **Improved waterfall plot readability:** Dense same-night observations collapse
> into single representative composites, reducing visual clutter while preserving
> temporal evolution between nights. Each composite presents the most complete
> wavelength coverage available for that instrument on that night.

---

## Amended §3 Consequences — Positive Bullet 2

**Original text:**

> **Higher SNR:** Compositing multiple exposures produces a combined spectrum with
> SNR_combined ≈ √(Σ SNR²_i) in overlap regions.

**Replaced with:**

> **SNR improvement in overlap regions:** Where constituent spectra overlap in
> wavelength, combination improves SNR (approximately √(Σ SNR²_i) for
> inverse-variance weighting, somewhat less for median combination). This is a
> secondary benefit of the stacking step, not the primary purpose of compositing.

---

## Downstream Impact

No code changes required. The compositing implementation already handles both
the stitching case (disjoint wavelength ranges combined via NaN-aware averaging
on the union grid) and the stacking case (overlapping ranges combined at each
grid point). This amendment corrects the documentation framing to match what
the code has always done.
