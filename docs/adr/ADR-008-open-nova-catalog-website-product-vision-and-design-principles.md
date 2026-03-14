# ADR-008: Open Nova Catalog Website Product Vision and Design Principles

Status: Proposed
Date: 2026-03-14

---

## Context

The Open Nova Catalog (ONC) is intended to provide a centralized, curated collection of observational data for classical novae. Historically, assembling such datasets has required researchers to manually search through multiple astronomical archives, identify relevant observations, download heterogeneous datasets, and perform significant effort to normalize and combine them.

For example, a graduate student beginning work on a nova project may spend months locating and aggregating observational data from disparate sources before any scientific analysis can begin. This process often produces little scientific value in itself and can inadvertently encourage researchers to focus narrowly on a single object simply because assembling the dataset required such effort.

At the same time, novae produce rich and diverse observational datasets, including spectra, photometry, and multi-wavelength observations. Despite this richness, novae are often underrepresented in broader astrophysical discussions compared to other transient phenomena.

The Open Nova Catalog aims to address these issues by providing a centralized resource that enables astronomers to quickly discover, inspect, and access curated datasets for many novae. By lowering the barrier to accessing nova data, the project seeks to make it easier for researchers to explore the diversity of nova behavior and to encourage broader participation in nova research.

The Open Nova Catalog website serves as the primary public interface for interacting with this data.

---

## Decision

The Open Nova Catalog website will serve as a curated scientific interface for discovering, exploring, and accessing observational datasets for classical novae.

The website should enable astronomers to rapidly locate nova data, visually inspect available observations, and download curated datasets without requiring extensive interaction with multiple external archives.

The interface should emphasize discoverability, scientific clarity, and transparency of data provenance while supporting future expansion toward richer exploration and comparison tools.

The interface should also encourage exploration of multiple novae, helping users understand how individual events relate to the broader population.

---

## Mission Statement

The Open Nova Catalog website provides a simple and elegant interface for exploring classical novae and accessing curated observational datasets, enabling astronomers to rapidly discover, inspect, and download rich multiwavelength observations.

---

## Product Goals

### Rapid Data Access

Astronomers should be able to rapidly obtain a curated dataset for a nova without manually aggregating observations from multiple archives.

### Exploration of Nova Diversity

Users should be able to browse the catalog to understand how different novae compare to one another and to explore the diversity of nova behavior.

### Showcase the Richness of Nova Observations

The interface should make the depth and diversity of available observations visible and easy to inspect, highlighting the scientific value of nova datasets across wavelengths and instruments.

### Lower the Barrier to Nova Research

Astronomers unfamiliar with nova research should be able to easily explore and understand nova datasets, reducing the effort required to begin working with nova observations.

---

## Design Principles

### Scientific Clarity

The interface should prioritize readability, clear presentation of data, and scientific interpretability. Features should support understanding of the data rather than obscure it.

### Transparency of Data Provenance

Metadata and references should be clearly visible so users can understand the origin, context, and reliability of the data.

### Immediate Visual Insight

Where appropriate, visualizations should help users quickly understand the observational characteristics of a nova and the nature of the available data.

### Extensibility

Although initially focused on classical novae, the architecture and conceptual model of the Open Nova Catalog should support extension to other classes of astronomical transient events.

---

## Long-Term Vision

While the initial focus of the Open Nova Catalog is classical novae, the broader goal is to establish a robust and extensible framework for curated transient-event catalogs.

Future versions of the system may support:

- comparison of observational data across multiple novae
- richer visualization tools
- programmatic access through APIs
- integration with external analysis tools
- community data contributions
- extension to other classes of transient astronomical phenomena

---

## Success Criteria

The website will be considered successful if it enables astronomers to:

- quickly locate and access curated datasets for specific novae
- browse the catalog to discover novae with rich observational data
- visually inspect key observational characteristics of a nova
- understand the provenance and sources of the data
- obtain datasets in a form suitable for scientific analysis with minimal additional processing

---

## Consequences

Adopting this product vision implies that:

- the website should prioritize clarity, discoverability, and data access over complex interface features
- the catalog interface will play a central role in the user experience
- visualization tools will be used to help users interpret available observations
- metadata and references will be treated as first-class elements of the interface
- future architectural decisions should preserve extensibility beyond nova-specific applications

---

## Relationship to Other ADRs

This ADR defines the product vision and guiding design principles for the Open Nova Catalog website.

Subsequent ADRs describing specific architectural or implementation decisions for the website should align with the goals and principles described here.

In particular, future ADRs may address:

- the minimum viable product (MVP) implementation strategy
- catalog navigation and browsing models
- visualization tools and interaction patterns
- frontend architecture and technology choices
- programmatic data access mechanisms

These decisions should be evaluated in light of the product goals and design principles established in this document.
