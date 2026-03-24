#!/usr/bin/env python3
"""
Run astronomy table metadata extraction with the OpenAI Responses API.

Usage examples:

  # VizieR mode
  export OPENAI_API_KEY="your-key-here"
  python extract_table_metadata.py \
      --template /path/to/full_file_info.json \
      --vizier-table "J/ApJ/899/162/fig3a" \
      --bibcode "2019ApJ...899..162S" \
      --output result.json

  # CSV mode
  export OPENAI_API_KEY="your-key-here"
  python extract_table_metadata.py \
      --template /path/to/full_file_info.json \
      --csv /path/to/table.csv \
      --output result.json

Notes:
- Requires: pip install openai
- The model name below is configurable; replace if needed.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import openai
from openai import OpenAI

api_key = subprocess.run(
    ["op", "read", "op://Personal/OpenAI API Key/password"], capture_output=True, text=True
).stdout.strip()

openai.api_key = api_key


DEFAULT_MODEL = "gpt-5"


def load_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception as exc:
        raise RuntimeError(f"Failed to read file {path}: {exc}") from exc


def build_system_prompt() -> str:
    return """You are an astronomy table-metadata extraction agent.

Your task is to fill in EXACTLY the provided JSON template and return ONLY valid JSON.

You will receive EITHER:
1. a VizieR table name plus the bibcode for the paper it came from
OR
2. an uploaded CSV file.

Your job is to infer as much as you reasonably can about the table and its columns, while being conservative and avoiding hallucinations.

PRIMARY REQUIREMENTS
- Return exactly one JSON object matching the provided template structure.
- Do NOT add keys.
- Do NOT remove keys.
- Do NOT rename keys.
- Do NOT change nesting.
- Return JSON only.
- If a value cannot be determined safely, use null.
- Do not guess aggressively.

GENERAL RULES
- Prefer explicit evidence over inference.
- Use column names, units, descriptions, sample values, and UCDs as primary evidence.
- Use paper/table context only as supporting evidence.
- Classify the specific table, not just the paper.
- Dwarf novae and nova-like variables are NOT acceptable nova relevance.
- If the table is about dwarf novae or nova-like variables rather than actual novae, set nova relevance to false.

TABLE-LEVEL GUIDANCE
- data_type should be one of: "photometry", "spectroscopy", "metadata", or null if unsupported by the template.
- table_format should be "long", "wide", or null.
- wavelength_regime should be conservative: e.g. "optical", "nir", "mir", "uv", "xray", "radio", "fir", "mixed", or null.
- telescope/instrument should only be filled at table level if a single value is supported for the whole table.
- time coverage should be conservative.

COLUMN-LEVEL GUIDANCE
- Create one column entry per actual source column.
- Infer semantic role conservatively.
- Preserve any UCD information if present.
- Mark likely error columns appropriately.
- Fill filter/band only when clearly supported.
- If telescope/instrument apply only to the whole table and your template has per-column fields, only populate them when clearly appropriate.

CONFIDENCE GUIDANCE
- High confidence only for explicit or near-explicit evidence.
- Lower confidence for inference from naming/value patterns.
- Prefer null over weak guesses.

OUTPUT RULES
- Return ONLY valid JSON.
- No markdown.
- No prose.
- Preserve the template structure exactly.
"""


def build_user_prompt_for_vizier(template_json: str, vizier_table: str, bibcode: str) -> str:
    return f"""Here is the exact JSON template you must fill:

{template_json}

Input mode: VizieR

VizieR table name: {vizier_table}
Bibcode: {bibcode}

Fill the template exactly and return JSON only.
"""


def build_user_prompt_for_csv(template_json: str, csv_filename: str) -> str:
    return f"""Here is the exact JSON template you must fill:

{template_json}

Input mode: CSV

Use the uploaded CSV file named "{csv_filename}" as the source table.

Fill the template exactly and return JSON only.
"""


def extract_text_output(response: Any) -> str:
    """
    Try a few common places where text may be exposed by the SDK.
    """
    if hasattr(response, "output_text") and response.output_text:
        return response.output_text

    # Fallback for structured output objects
    try:
        chunks: list[str] = []
        for item in getattr(response, "output", []) or []:
            for content in getattr(item, "content", []) or []:
                text = getattr(content, "text", None)
                if text:
                    chunks.append(text)
        if chunks:
            return "".join(chunks)
    except Exception:
        pass

    raise RuntimeError("Could not extract text output from the API response.")


def parse_json_strict(text: str) -> Any:
    text = text.strip()

    # Remove accidental code fences if the model ignored instructions.
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Model output was not valid JSON.\nRaw output:\n{text}\n\nJSON error: {exc}"
        ) from exc


def upload_csv_if_needed(client: OpenAI, csv_path: Path) -> str:
    with csv_path.open("rb") as f:
        uploaded = client.files.create(file=f, purpose="user_data")
    # Files API supports upload/retrieval for document use. :contentReference[oaicite:1]{index=1}
    return uploaded.id


def run_vizier_mode(
    client: OpenAI,
    model: str,
    template_json: str,
    vizier_table: str,
    bibcode: str,
) -> dict[str, Any]:
    response = client.responses.create(
        model=model,
        input=[
            {
                "role": "system",
                "content": [{"type": "input_text", "text": build_system_prompt()}],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": build_user_prompt_for_vizier(
                            template_json=template_json,
                            vizier_table=vizier_table,
                            bibcode=bibcode,
                        ),
                    }
                ],
            },
        ],
    )
    text = extract_text_output(response)
    return parse_json_strict(text)


def run_csv_mode(
    client: OpenAI,
    model: str,
    template_json: str,
    csv_path: Path,
) -> dict[str, Any]:
    file_id = upload_csv_if_needed(client, csv_path)

    response = client.responses.create(
        model=model,
        input=[
            {
                "role": "system",
                "content": [{"type": "input_text", "text": build_system_prompt()}],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": build_user_prompt_for_csv(
                            template_json=template_json,
                            csv_filename=csv_path.name,
                        ),
                    },
                    {
                        "type": "input_file",
                        "file_id": file_id,
                    },
                ],
            },
        ],
    )
    text = extract_text_output(response)
    return parse_json_strict(text)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract astronomy table metadata into a fixed JSON template."
    )
    parser.add_argument(
        "--template",
        required=True,
        type=Path,
        help="Path to the fixed JSON template file.",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Path to save the completed JSON output.",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help="OpenAI model name to use.",
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--csv",
        type=Path,
        help="Path to the CSV file to analyze.",
    )
    mode.add_argument(
        "--vizier-table",
        type=str,
        help="VizieR table name, e.g. J/ApJ/899/162/fig3a",
    )

    parser.add_argument(
        "--bibcode",
        type=str,
        help="Bibcode for VizieR mode.",
    )

    args = parser.parse_args()

    if args.vizier_table and not args.bibcode:
        parser.error("--bibcode is required when using --vizier-table")

    return args


def main() -> int:
    args = parse_args()

    if not os.environ.get("OPENAI_API_KEY"):
        print("Error: OPENAI_API_KEY is not set.", file=sys.stderr)
        return 2

    template_json = load_text(args.template)
    client = OpenAI()

    try:
        if args.csv:
            result = run_csv_mode(
                client=client,
                model=args.model,
                template_json=template_json,
                csv_path=args.csv,
            )
        else:
            result = run_vizier_mode(
                client=client,
                model=args.model,
                template_json=template_json,
                vizier_table=args.vizier_table,
                bibcode=args.bibcode,
            )

        args.output.write_text(
            json.dumps(result, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        print(f"Wrote JSON to {args.output}")
        return 0

    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
