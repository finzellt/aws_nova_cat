#!/usr/bin/env python3
"""
Transmission Curve Digitizer
=============================

Extracts transmission curve data from PDF images (e.g., filter manufacturer
datasheets) by letting you interactively calibrate the axes and trace/extract
the curve.

Usage
-----
    # Basic usage — opens an interactive matplotlib window
    python digitize_curve.py filter_datasheet.pdf

    # Specify which page of the PDF to use (default: 1)
    python digitize_curve.py filter_datasheet.pdf --page 2

    # Specify output file
    python digitize_curve.py filter_datasheet.pdf -o astrodon_V.csv

    # Specify DPI for PDF rendering (higher = more precision, slower)
    python digitize_curve.py filter_datasheet.pdf --dpi 300

    # If you already have a PNG/JPG image instead of a PDF
    python digitize_curve.py curve_screenshot.png

Workflow
--------
The tool opens an interactive window and walks you through four steps:

1. CALIBRATE AXES — Click four points on the plot axes:
   - X-axis minimum (e.g., the leftmost tick mark)
   - X-axis maximum (e.g., the rightmost tick mark)
   - Y-axis minimum (usually 0% transmission)
   - Y-axis maximum (usually 100% transmission)
   After each click, you'll enter the corresponding data value in the terminal.

2. SELECT CURVE COLOR — Click directly on the curve line you want to extract.
   The tool samples the color at that point and uses it to find all matching
   pixels.

3. EXTRACT & REVIEW — The tool finds all pixels matching the curve color,
   converts them to data coordinates, and overlays the extracted curve on the
   image so you can visually verify.

4. SAVE — If it looks good, press Enter to save. If not, you can adjust
   the color tolerance and re-extract.

Dependencies
------------
    pip install matplotlib numpy Pillow scipy

    # For PDF rendering (only needed if input is PDF, not PNG/JPG):
    pip install pdf2image
    # Plus system dependency: poppler (brew install poppler / apt install poppler-utils)

    # Alternative PDF renderer (no system deps):
    pip install pymupdf

Output Format
-------------
CSV with two columns: wavelength (nm by default), transmission (0-1 scale).
Suitable for direct import into the SVO database via svo_aws.py add-custom.

Notes
-----
- Works best with clean, high-contrast plots (colored line on white background).
- For complex plots with multiple curves, you select which curve to extract
  by clicking on its specific color.
- The color tolerance parameter controls how strictly the tool matches the
  curve color. Increase it if the curve is anti-aliased or gradient-colored.
"""

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from scipy.signal import savgol_filter

# Try to import PDF renderers
PDF_RENDERER = None
try:
    import fitz  # PyMuPDF

    PDF_RENDERER = "pymupdf"
except ImportError:
    try:
        from pdf2image import convert_from_path

        PDF_RENDERER = "pdf2image"
    except ImportError:
        pass

# Try to import PIL for image loading
from PIL import Image

# ============================================================================
# PDF / Image loading
# ============================================================================


def load_image(input_path: str, page: int = 1, dpi: int = 300) -> np.ndarray:
    """
    Load an image from a PDF or image file. Returns an RGB numpy array.
    """
    path = Path(input_path)
    suffix = path.suffix.lower()

    if suffix == ".pdf":
        return _render_pdf(input_path, page, dpi)
    elif suffix in (".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp"):
        img = Image.open(input_path).convert("RGB")
        return np.array(img)
    else:
        # Try opening as image anyway
        try:
            img = Image.open(input_path).convert("RGB")
            return np.array(img)
        except Exception:
            print(f"ERROR: Cannot load {input_path} — unsupported format.")
            sys.exit(1)


def _render_pdf(pdf_path: str, page: int, dpi: int) -> np.ndarray:
    """Render a PDF page to a numpy RGB array."""
    if PDF_RENDERER == "pymupdf":
        doc = fitz.open(pdf_path)
        if page > len(doc):
            print(f"ERROR: PDF has {len(doc)} pages, requested page {page}.")
            sys.exit(1)
        pg = doc[page - 1]
        zoom = dpi / 72.0
        mat = fitz.Matrix(zoom, zoom)
        pix = pg.get_pixmap(matrix=mat)
        img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
        doc.close()
        return np.array(img)

    elif PDF_RENDERER == "pdf2image":
        images = convert_from_path(pdf_path, dpi=dpi, first_page=page, last_page=page)
        if not images:
            print(f"ERROR: Could not render page {page} from {pdf_path}.")
            sys.exit(1)
        return np.array(images[0].convert("RGB"))

    else:
        print(
            "ERROR: No PDF renderer available. Install one of:\n"
            "  pip install pymupdf       (recommended, no system deps)\n"
            "  pip install pdf2image     (requires poppler system package)"
        )
        sys.exit(1)


# ============================================================================
# Interactive calibration
# ============================================================================


class AxisCalibrator:
    """
    Interactive tool: user clicks 4 points to define the data coordinate
    system of the plot in the image.

    Flow:
      1. Collect axis data values in the TERMINAL first (no GUI interaction).
      2. Open the image and collect 4 pixel clicks (no terminal interaction).
    This avoids mixing input() with the matplotlib event loop, which
    deadlocks on macOS.
    """

    def __init__(self, img: np.ndarray):
        self.img = img
        self.click_steps = [
            "Click the LEFT edge of the X-axis (minimum wavelength)",
            "Click the RIGHT edge of the X-axis (maximum wavelength)",
            "Click the BOTTOM edge of the Y-axis (minimum transmission)",
            "Click the TOP edge of the Y-axis (maximum transmission)",
        ]
        self.pixel_clicks = []  # List of (px_x, px_y)
        self.current_step = 0
        self._fig = None
        self._ax = None

    def run(self, wavelength_unit: str = "nm") -> dict:
        """
        Collect calibration data. Returns a calibration dict.
        """
        # --- Phase 1: Collect axis values in terminal (no GUI) ---
        print("\n  First, enter the axis range values by reading them from the plot.")
        print(f"  (Wavelength unit: {wavelength_unit})\n")

        data_x_min = _prompt_float(f"  X-axis minimum wavelength ({wavelength_unit}): ")
        data_x_max = _prompt_float(f"  X-axis maximum wavelength ({wavelength_unit}): ")
        data_y_min = _prompt_float("  Y-axis minimum transmission (e.g. 0): ")
        data_y_max = _prompt_float("  Y-axis maximum transmission (e.g. 1 or 100): ")

        print("\n  Now click the 4 corresponding points on the plot image.")
        print("  Click order:")
        for i, desc in enumerate(self.click_steps, 1):
            print(f"    {i}. {desc}")
        print()

        # --- Phase 2: Collect pixel clicks in GUI (no terminal I/O) ---
        self._fig, self._ax = plt.subplots(1, 1, figsize=(14, 10))
        self._ax.imshow(self.img)
        self._ax.set_title(
            f"Click 1/4: {self.click_steps[0]}",
            fontsize=11,
        )
        self._fig.canvas.mpl_connect("button_press_event", self._on_click)
        plt.tight_layout()
        plt.show(block=True)

        if len(self.pixel_clicks) < 4:
            print("ERROR: Calibration incomplete — need 4 clicks.")
            sys.exit(1)

        px_x_min = self.pixel_clicks[0][0]
        px_x_max = self.pixel_clicks[1][0]
        px_y_min = self.pixel_clicks[2][1]  # Bottom of plot = high pixel Y value
        px_y_max = self.pixel_clicks[3][1]  # Top of plot = low pixel Y value

        cal = {
            "px_x_min": px_x_min,
            "px_x_max": px_x_max,
            "px_y_min": px_y_min,
            "px_y_max": px_y_max,
            "data_x_min": data_x_min,
            "data_x_max": data_x_max,
            "data_y_min": data_y_min,
            "data_y_max": data_y_max,
        }

        print("\nCalibration complete:")
        print(f"  X: pixels [{px_x_min:.0f}, {px_x_max:.0f}] -> data [{data_x_min}, {data_x_max}]")
        print(f"  Y: pixels [{px_y_min:.0f}, {px_y_max:.0f}] -> data [{data_y_min}, {data_y_max}]")

        return cal

    def _on_click(self, event):
        if event.inaxes != self._ax or self.current_step >= 4:
            return

        px, py = event.xdata, event.ydata
        self.pixel_clicks.append((px, py))

        # Mark the click
        self._ax.plot(px, py, "r+", markersize=15, markeredgewidth=2)
        self._ax.annotate(
            f"  {self.current_step + 1}", (px, py),
            color="red", fontsize=12, fontweight="bold",
        )

        self.current_step += 1

        if self.current_step < 4:
            self._ax.set_title(
                f"Click {self.current_step + 1}/4: {self.click_steps[self.current_step]}",
                fontsize=11,
            )
        else:
            self._ax.set_title(
                "All 4 points collected! Close this window to continue.",
                fontsize=11, color="green",
            )

        self._fig.canvas.draw()


def _prompt_float(prompt: str) -> float:
    """Prompt for a float value in the terminal, retrying on bad input."""
    while True:
        try:
            return float(input(prompt).strip())
        except (ValueError, EOFError):
            print("    Invalid number, try again.")


# ============================================================================
# Curve color selection
# ============================================================================


class ColorSelector:
    """Let the user click on the curve to sample its color."""

    def __init__(self, img: np.ndarray):
        self.img = img
        self.selected_color = None
        self._fig = None
        self._ax = None

    def run(self) -> np.ndarray:
        """Display image and let user click on the curve. Returns RGB color array."""
        self._fig, self._ax = plt.subplots(1, 1, figsize=(14, 10))
        self._ax.imshow(self.img)
        self._ax.set_title(
            "Click directly ON the curve line you want to extract\n"
            "(try to click on a thick/solid part of the line)",
            fontsize=11,
        )
        self._fig.canvas.mpl_connect("button_press_event", self._on_click)
        plt.tight_layout()
        plt.show(block=True)

        if self.selected_color is None:
            print("ERROR: No color selected.")
            sys.exit(1)

        return self.selected_color

    def _on_click(self, event):
        if event.inaxes != self._ax:
            return

        px, py = int(round(event.xdata)), int(round(event.ydata))

        # Sample a small neighborhood to get a stable color
        h, w = self.img.shape[:2]
        r = 2  # Sample radius
        y0, y1 = max(0, py - r), min(h, py + r + 1)
        x0, x1 = max(0, px - r), min(w, px + r + 1)
        patch = self.img[y0:y1, x0:x1]
        self.selected_color = np.median(patch.reshape(-1, 3), axis=0).astype(np.uint8)

        print(f"  Selected color: RGB({self.selected_color[0]}, {self.selected_color[1]}, {self.selected_color[2]})")

        # Show the selected color
        self._ax.plot(px, py, "wo", markersize=10, markeredgewidth=2)
        self._ax.set_title(
            f"Color sampled: RGB({self.selected_color[0]}, {self.selected_color[1]}, {self.selected_color[2]})\n"
            f"Close this window to continue.",
            fontsize=11,
        )
        self._fig.canvas.draw()


# ============================================================================
# Named color presets & parsing
# ============================================================================

NAMED_COLORS = {
    "blue":       np.array([0, 0, 255]),
    "darkblue":   np.array([0, 0, 180]),
    "lightblue":  np.array([100, 150, 255]),
    "red":        np.array([255, 0, 0]),
    "darkred":    np.array([180, 0, 0]),
    "green":      np.array([0, 180, 0]),
    "darkgreen":  np.array([0, 128, 0]),
    "orange":     np.array([255, 165, 0]),
    "purple":     np.array([128, 0, 128]),
    "magenta":    np.array([255, 0, 255]),
    "cyan":       np.array([0, 255, 255]),
    "yellow":     np.array([255, 255, 0]),
    "brown":      np.array([139, 69, 19]),
    "pink":       np.array([255, 105, 180]),
    "teal":       np.array([0, 128, 128]),
}


def parse_color(color_str: str) -> np.ndarray:
    """
    Parse a color string into an RGB array.
    Accepts named colors ('blue', 'red', ...) or RGB triplets ('0,0,255').
    """
    color_str = color_str.strip().lower()

    if color_str in NAMED_COLORS:
        return NAMED_COLORS[color_str]

    # Try parsing as R,G,B
    parts = color_str.replace(" ", "").split(",")
    if len(parts) == 3:
        try:
            rgb = np.array([int(p) for p in parts], dtype=np.uint8)
            return rgb
        except ValueError:
            pass

    # Try parsing as hex (#0000FF or 0000FF)
    hex_str = color_str.lstrip("#")
    if len(hex_str) == 6:
        try:
            r = int(hex_str[0:2], 16)
            g = int(hex_str[2:4], 16)
            b = int(hex_str[4:6], 16)
            return np.array([r, g, b], dtype=np.uint8)
        except ValueError:
            pass

    available = ", ".join(sorted(NAMED_COLORS.keys()))
    print(f"ERROR: Cannot parse color '{color_str}'")
    print(f"  Use a named color ({available}),")
    print("  an RGB triplet (e.g. '0,0,255'),")
    print("  or hex (e.g. '#0000FF').")
    sys.exit(1)


# ============================================================================
# Curve extraction
# ============================================================================


def extract_curve_pixels(
    img: np.ndarray,
    target_color: np.ndarray,
    tolerance: float = 40.0,
    cal: dict = None,
    exclude_dark: float = 0.0,
    exclude_gray: float = 0.0,
) -> np.ndarray:
    """
    Find all pixels matching the target color within the calibrated region.

    Args:
        img: RGB image array
        target_color: RGB color to match
        tolerance: Euclidean distance threshold in RGB space
        cal: Calibration dict (to crop search to the plot area)
        exclude_dark: Exclude pixels darker than this brightness (0-255).
                      Use ~80 to filter out black gridlines/text.
        exclude_gray: Exclude pixels with color saturation below this level.
                      Grayscale pixels (black, gray, white gridlines) have
                      near-zero saturation. Use ~30 to filter them out.

    Returns:
        Array of shape (N, 2) with (pixel_x, pixel_y) of matched pixels.
    """
    # Crop to the calibrated plot region (with a small margin)
    if cal:
        margin = 10
        x_lo = max(0, int(min(cal["px_x_min"], cal["px_x_max"]) - margin))
        x_hi = min(img.shape[1], int(max(cal["px_x_min"], cal["px_x_max"]) + margin))
        y_lo = max(0, int(min(cal["px_y_min"], cal["px_y_max"]) - margin))
        y_hi = min(img.shape[0], int(max(cal["px_y_min"], cal["px_y_max"]) + margin))
    else:
        x_lo, x_hi = 0, img.shape[1]
        y_lo, y_hi = 0, img.shape[0]

    region = img[y_lo:y_hi, x_lo:x_hi].astype(np.float64)
    target = target_color.astype(np.float64)

    # Compute color distance
    dist = np.sqrt(np.sum((region - target) ** 2, axis=2))
    mask = dist < tolerance

    # Exclude dark pixels (gridlines, axis lines, text)
    if exclude_dark > 0:
        brightness = np.mean(region, axis=2)
        mask &= brightness > exclude_dark

    # Exclude grayscale pixels by filtering on color saturation
    # Saturation = max(R,G,B) - min(R,G,B); grayscale pixels have ~0
    if exclude_gray > 0:
        chan_max = np.max(region, axis=2)
        chan_min = np.min(region, axis=2)
        saturation = chan_max - chan_min
        mask &= saturation > exclude_gray

    # Get pixel coordinates (in full-image space)
    ys, xs = np.where(mask)
    xs = xs + x_lo
    ys = ys + y_lo

    return np.column_stack([xs, ys])


def pixels_to_data(
    pixels: np.ndarray,
    cal: dict,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Convert pixel coordinates to data coordinates using the calibration.
    Returns (wavelength_array, transmission_array).
    """
    px_x = pixels[:, 0].astype(np.float64)
    px_y = pixels[:, 1].astype(np.float64)

    # Linear interpolation from pixel space to data space
    x_scale = (cal["data_x_max"] - cal["data_x_min"]) / (cal["px_x_max"] - cal["px_x_min"])
    y_scale = (cal["data_y_max"] - cal["data_y_min"]) / (cal["px_y_max"] - cal["px_y_min"])

    data_x = cal["data_x_min"] + (px_x - cal["px_x_min"]) * x_scale
    data_y = cal["data_y_min"] + (px_y - cal["px_y_min"]) * y_scale

    return data_x, data_y


def reduce_curve(
    wavelengths: np.ndarray,
    transmissions: np.ndarray,
    n_bins: int = 500,
    smooth_window: int = 0,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Reduce a cloud of extracted pixels into a clean 1D curve.

    For each wavelength bin, takes the median transmission. Optionally
    applies Savitzky-Golay smoothing.

    Returns (wavelength, transmission) arrays.
    """
    if len(wavelengths) == 0:
        return np.array([]), np.array([])

    wl_min, wl_max = wavelengths.min(), wavelengths.max()
    bins = np.linspace(wl_min, wl_max, n_bins + 1)
    bin_centers = 0.5 * (bins[:-1] + bins[1:])
    bin_indices = np.digitize(wavelengths, bins) - 1

    wl_out = []
    tr_out = []
    for i in range(n_bins):
        mask = bin_indices == i
        if mask.sum() > 0:
            wl_out.append(bin_centers[i])
            tr_out.append(np.median(transmissions[mask]))

    wl_out = np.array(wl_out)
    tr_out = np.array(tr_out)

    if smooth_window > 0 and len(tr_out) > smooth_window:
        # Savitzky-Golay smoothing
        window = min(smooth_window, len(tr_out))
        if window % 2 == 0:
            window -= 1
        if window >= 5:
            tr_out = savgol_filter(tr_out, window, polyorder=3)

    return wl_out, tr_out


# ============================================================================
# Review and save
# ============================================================================


def review_and_save(
    img: np.ndarray,
    cal: dict,
    wavelengths: np.ndarray,
    transmissions: np.ndarray,
    raw_pixels: np.ndarray,
    output_path: str,
    wavelength_unit: str,
):
    """Show extracted curve overlaid on the image. Prompt to save."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))

    # Left: image with extracted pixels highlighted
    ax1.imshow(img)
    ax1.scatter(raw_pixels[:, 0], raw_pixels[:, 1], c="lime", s=0.5, alpha=0.3)
    ax1.set_title(f"Extracted pixels ({len(raw_pixels):,} points)", fontsize=11)

    # Right: clean extracted curve in data space
    ax2.plot(wavelengths, transmissions, "b-", linewidth=1.5)
    ax2.set_xlabel(f"Wavelength ({wavelength_unit})")
    ax2.set_ylabel("Transmission")
    ax2.set_title("Extracted Transmission Curve")
    ax2.set_ylim(bottom=max(0, transmissions.min() - 0.02))
    ax2.grid(True, alpha=0.3)

    plt.suptitle(f"Output: {output_path}", fontsize=12, fontweight="bold")
    plt.tight_layout()
    plt.show(block=False)

    # Prompt
    print(f"\nExtracted {len(wavelengths)} data points.")
    print(f"Wavelength range: {wavelengths.min():.1f} – {wavelengths.max():.1f} {wavelength_unit}")
    print(f"Transmission range: {transmissions.min():.4f} – {transmissions.max():.4f}")

    response = input("\nSave to CSV? [Y/n/t=adjust tolerance] ").strip().lower()
    plt.close(fig)

    if response in ("", "y", "yes"):
        _save_csv(wavelengths, transmissions, output_path, wavelength_unit)
        return True
    elif response.startswith("t"):
        return False  # Signal to retry with different tolerance
    else:
        print("Discarded.")
        return True  # Done, but didn't save


def _save_csv(
    wavelengths: np.ndarray,
    transmissions: np.ndarray,
    output_path: str,
    wavelength_unit: str,
):
    """Save the extracted curve to CSV."""
    # Clip transmission to [0, 1] range
    transmissions = np.clip(transmissions, 0.0, 1.0)

    with open(output_path, "w") as f:
        f.write("# Transmission curve extracted by digitize_curve.py\n")
        f.write(f"# Wavelength unit: {wavelength_unit}\n")
        f.write(f"wavelength_{wavelength_unit},transmission\n")
        for wl, tr in zip(wavelengths, transmissions, strict=False):
            f.write(f"{wl:.2f},{tr:.6f}\n")

    print(f"Saved {len(wavelengths)} points to {output_path}")

    # Also note the Angstrom conversion if needed
    if wavelength_unit == "nm":
        print(
            "  Note: to convert to Angstroms (for SVO database), multiply wavelength by 10."
        )


# ============================================================================
# Main workflow
# ============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Extract transmission curves from PDF/image plots",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("input", help="PDF or image file containing the plot")
    parser.add_argument("-o", "--output", help="Output CSV path (default: auto-generated)")
    parser.add_argument("--page", type=int, default=1, help="PDF page number (default: 1)")
    parser.add_argument("--dpi", type=int, default=300, help="PDF render DPI (default: 300)")
    parser.add_argument(
        "--tolerance",
        type=float,
        default=40.0,
        help="Color matching tolerance in RGB space (default: 40)",
    )
    parser.add_argument(
        "--bins",
        type=int,
        default=500,
        help="Number of wavelength bins for curve reduction (default: 500)",
    )
    parser.add_argument(
        "--smooth",
        type=int,
        default=0,
        help="Savitzky-Golay smoothing window (0=off, try 11 or 21)",
    )
    parser.add_argument(
        "--unit",
        choices=["nm", "A", "um"],
        default="nm",
        help="Wavelength unit of the plot (default: nm)",
    )
    parser.add_argument(
        "--color",
        type=str,
        default=None,
        help=(
            "Curve color — skip interactive color picker. "
            "Accepts named colors (blue, red, green, darkblue, ...), "
            "RGB triplets (e.g. '0,0,255'), or hex (e.g. '#0000FF')."
        ),
    )
    parser.add_argument(
        "--exclude-dark",
        type=float,
        default=0.0,
        metavar="BRIGHTNESS",
        help=(
            "Exclude pixels darker than this brightness (0-255). "
            "Use ~80 to filter out black gridlines, axes, and text. (default: 0 = off)"
        ),
    )
    parser.add_argument(
        "--exclude-gray",
        type=float,
        default=0.0,
        metavar="SATURATION",
        help=(
            "Exclude grayscale pixels with color saturation below this value. "
            "Gridlines are typically gray/black with ~0 saturation. "
            "Use ~30 to filter them out. (default: 0 = off)"
        ),
    )

    args = parser.parse_args()

    # Default output path
    if not args.output:
        stem = Path(args.input).stem
        args.output = f"{stem}_transmission.csv"

    print("=" * 60)
    print("Transmission Curve Digitizer")
    print("=" * 60)
    print(f"Input:  {args.input}")
    print(f"Output: {args.output}")
    print()

    # 1. Load image
    print("Loading image ...")
    img = load_image(args.input, page=args.page, dpi=args.dpi)
    print(f"  Image size: {img.shape[1]} x {img.shape[0]} pixels")

    # 2. Calibrate axes
    print("\n--- STEP 1: AXIS CALIBRATION ---")
    print("You'll first enter the axis range values in the terminal,")
    print("then click the 4 corresponding points on the plot image.")

    calibrator = AxisCalibrator(img)
    cal = calibrator.run(wavelength_unit=args.unit)

    # 3. Select curve color
    if args.color:
        target_color = parse_color(args.color)
        print("\n--- STEP 2: USING SPECIFIED COLOR ---")
        print(f"  Color: RGB({target_color[0]}, {target_color[1]}, {target_color[2]})")
        if args.exclude_dark > 0:
            print(f"  Excluding dark pixels below brightness {args.exclude_dark:.0f}")
        if args.exclude_gray > 0:
            print(f"  Excluding grayscale pixels below saturation {args.exclude_gray:.0f}")
    else:
        print("\n--- STEP 2: CURVE COLOR SELECTION ---")
        print("Click on the curve line you want to extract.")
        print()
        selector = ColorSelector(img)
        target_color = selector.run()

    # 4. Extract curve (with tolerance retry loop)
    tolerance = args.tolerance
    while True:
        print(f"\n--- STEP 3: EXTRACTING CURVE (tolerance={tolerance:.0f}) ---")

        raw_pixels = extract_curve_pixels(
            img, target_color, tolerance=tolerance, cal=cal,
            exclude_dark=args.exclude_dark,
            exclude_gray=args.exclude_gray,
        )
        print(f"  Found {len(raw_pixels):,} matching pixels")

        if len(raw_pixels) < 10:
            print("  Too few pixels found. Try increasing tolerance.")
            try:
                tolerance = float(input("  New tolerance (or Enter to quit): ").strip() or "0")
                if tolerance <= 0:
                    print("Aborting.")
                    return
            except ValueError:
                print("Aborting.")
                return
            continue

        # Convert to data coordinates
        data_x, data_y = pixels_to_data(raw_pixels, cal)

        # Normalize transmission if it's in percentage
        if data_y.max() > 1.5:
            print(f"  Detected percentage scale (max={data_y.max():.1f}), dividing by 100")
            data_y = data_y / 100.0

        # Reduce to clean curve
        wl, tr = reduce_curve(data_x, data_y, n_bins=args.bins, smooth_window=args.smooth)

        # 5. Review
        done = review_and_save(img, cal, wl, tr, raw_pixels, args.output, args.unit)

        if done:
            break
        else:
            try:
                tolerance = float(input(f"  New tolerance (current={tolerance:.0f}): ").strip())
            except (ValueError, EOFError):
                print("Keeping current tolerance.")

    print("\nDone!")


if __name__ == "__main__":
    main()
