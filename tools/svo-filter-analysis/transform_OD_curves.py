#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="Convert optical density (OD) to transmission and output a clean 2-column CSV."
    )
    parser.add_argument(
        "input_file",
        help="Path to input file (.csv or .xlsx)"
    )
    parser.add_argument(
        "--od-column",
        default="OD",
        help="Name of the OD column (default: OD)"
    )
    parser.add_argument(
        "--wavelength-column",
        default=None,
        help="Name of wavelength column (e.g., 'Lambda', 'λ (nm)'). If not provided, first column is used."
    )
    parser.add_argument(
        "--sheet",
        default=0,
        help="Excel sheet name or index (default: 0)"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    input_path = Path(args.input_file)

    if not input_path.exists():
        print(f"Error: file '{input_path}' does not exist.", file=sys.stderr)
        sys.exit(1)

    output_path = input_path.with_name(f"{input_path.stem}_TRANSFORMED.csv")

    try:
        # Load file
        if input_path.suffix.lower() == ".csv":
            df = pd.read_csv(input_path)
        elif input_path.suffix.lower() in [".xlsx", ".xls"]:
            df = pd.read_excel(input_path, sheet_name=args.sheet)
        else:
            raise ValueError("Unsupported file type. Use .csv or .xlsx")

        # Determine wavelength column
        if args.wavelength_column:
            wl_col = args.wavelength_column
        else:
            wl_col = df.columns[0]  # assume first column

        if wl_col not in df.columns:
            raise ValueError(f"Wavelength column '{wl_col}' not found.")

        if args.od_column not in df.columns:
            raise ValueError(
                f"OD column '{args.od_column}' not found. Columns: {list(df.columns)}"
            )

        # Convert OD → transmission
        od_values = pd.to_numeric(df[args.od_column], errors="coerce")
        transmission = 10 ** (-od_values)

        # Build clean output DataFrame
        out_df = pd.DataFrame({
            wl_col: pd.to_numeric(df[wl_col], errors="coerce"),
            "transmission": transmission
        })

        # Drop rows with NaNs (optional but usually desirable)
        out_df = out_df.dropna()

        # Write output
        out_df.to_csv(output_path, index=False)

        print(f"✅ Wrote cleaned transmission file to: {output_path}")

    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
