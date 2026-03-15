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

## Intended Audience

The Open Nova Catalog is designed primarily as a **research data tool for observational astronomy**.

Its primary users are researchers who need to quickly discover, interpret, and obtain observational data related to classical novae. These users may include professional astronomers, graduate students, and others conducting observational research.

Typical research tasks include:

• identifying novae relevant to a research question
• determining what observational data exist for a given object
• locating spectra, photometry, and associated literature
• obtaining datasets suitable for immediate scientific inspection or analysis

The catalog prioritizes **rapid discovery and contextual understanding of observational data**, rather than access to raw archive holdings.

A secondary audience includes **data producers and observers**, such as professional observatories, survey teams, and citizen scientists who collect nova observations. While the MVP focuses on data discovery and research use, a longer-term goal of the project is to help observational data reach researchers who can make scientific use of it.

---

## Core Product Objectives

The Open Nova Catalog is intended to function as a research data interface for classical novae. Its core objectives are to:

1. **Enable rapid discovery of nova objects and their observational records**

   Users should be able to quickly locate novae by name, alias, or catalog browsing, and immediately see what observations and references exist for the object.

2. **Provide contextual understanding of nova observations**

   Observational datasets should be presented in the context of the specific nova to which they belong, along with key metadata such as coordinates, eruption year, and relevant literature.

3. **Reduce the effort required to identify and obtain useful datasets**

   Researchers should be able to determine what spectra, photometry, and other observational products exist for a nova without needing to independently search multiple archives.

4. **Preserve scientific provenance**

   All data and metadata should clearly identify their originating archives, instruments, and publications so that users can trace information back to authoritative sources.

Together, these objectives aim to make the catalog a central entry point for understanding and accessing observational information about classical novae.

---

## Curated Scientific Resource

The Open Nova Catalog is not intended to replicate astronomical data archives.

Large archives are designed to provide access to **raw observational products and full data holdings**. These systems support detailed reprocessing, specialized reductions, and precision analysis workflows.

The Open Nova Catalog serves a different role.

It is a **curated, object-centered resource** that organizes observational information around individual nova objects. Data products, references, and metadata are aggregated and presented in context so that users can quickly understand what is known about a given nova and what observations exist.

Key characteristics of this approach include:

• Emphasizing observational products that are immediately informative
• Organizing data in the context of specific nova objects
• Linking datasets to their originating archives and publications
• Prioritizing discoverability and scientific interpretation over archive completeness

Researchers seeking raw observations or specialized reductions should access the originating archives. The catalog instead focuses on **discoverability, context, and curated access to observational data.**

---

## Design Principles

### Scientific Interpretability

The catalog should present information in ways that support scientific reasoning and interpretation. Interface elements and data summaries should help users quickly understand the observational characteristics of a nova and the nature of the available data.

### Transparency of Data Provenance

All datasets and metadata should clearly indicate their originating archives, instruments, and associated literature. Users should be able to trace catalog information back to authoritative sources.

### Rapid Observational Understanding

Users should be able to quickly determine the observational state of a nova: what data exist, what instruments observed it, and where the observations originate. The interface should minimize the effort required to answer these questions.

### Immediate Visual Insight

Where possible, the catalog should provide visual representations of observational data that allow users to quickly understand the characteristics of a nova and the nature of the available observations.

Visual summaries (for example spectra or light curves) help transform the catalog from a simple listing of datasets into a contextual scientific resource. These visualizations should prioritize rapid comprehension over exhaustive analytical capability.

### Object-Centered Organization

Information in the catalog should be organized around individual nova objects rather than around individual datasets or archive holdings. Observations, references, and metadata should be presented in the context of the specific nova to which they belong.

### Generalizable Transient Model

Although the catalog initially focuses on classical novae, the underlying conceptual model should be capable of representing other classes of transient astronomical objects. Extending the system to new transient types should not require rethinking the core object-centered architecture.

---

## Consequences

Adopting this product vision implies that:

- The website should prioritize clarity, discoverability, and data access over complex interface features
- The catalog interface will play a central role in the user experience
- Visualization tools will be used to help users interpret available observations
- Metadata and references will be treated as first-class elements of the interface
- Future architectural decisions should preserve extensibility beyond nova-specific applications

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
