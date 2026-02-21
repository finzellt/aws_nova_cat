# Diagram Rendering Tool

> Note: Diagram rendering is not part of CI.
> If you modify Mermaid source files, please re-run `npm run render`
> before committing changes.

This tool renders Mermaid diagrams from Markdown files in `docs/` into image artifacts for easy viewing in GitHub and PRs.

## Inputs / Outputs
- **Source:** `docs/diagrams/**/*.md` (Mermaid code fences)
- **Output:** `docs/diagrams/rendered/` (PNG or SVG)

## Prerequisites
- Node.js 20+ (for `@mermaid-js/mermaid-cli`)
- Python 3.11+ (for the wrapper script)

## Install
From this directory:

```bash
npm ci
```
