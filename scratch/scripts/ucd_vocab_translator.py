from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass, field

# =========================
# Data classes
# =========================


@dataclass(frozen=True)
class UCDMatch:
    ucd: str
    score: float
    reasons: tuple[str, ...] = field(default_factory=tuple)

    def add(self, delta: float, reason: str) -> UCDMatch:
        return UCDMatch(
            ucd=self.ucd,
            score=self.score + delta,
            reasons=self.reasons + (reason,),
        )


@dataclass(frozen=True)
class Concept:
    ucd: str
    pref_label: str
    alt_labels: tuple[str, ...] = ()
    hidden_labels: tuple[str, ...] = ()
    broader: tuple[str, ...] = ()


# =========================
# Translator
# =========================


class UCDTranslator:
    """
    Hybrid free-text -> candidate UCD translator.

    Design:
      1. Normalize text
      2. Apply regex/pattern recognizers
      3. Apply SKOS-like label matching
      4. Compose compound UCDs when appropriate
      5. Rank and return likely candidates
    """

    def __init__(self) -> None:
        self.concepts = self._build_concepts()
        self.label_index = self._build_label_index(self.concepts)

        # band aliases used for composition
        self.band_aliases: dict[str, tuple[str, ...]] = {
            "em.opt.U": ("u", "u band", "u-band", "johnson u"),
            "em.opt.B": ("b", "b band", "b-band", "johnson b", "blue"),
            "em.opt.V": ("v", "v band", "v-band", "visual"),
            "em.opt.R": ("r band", "r-band", "johnson r", "cousins r"),
            "em.opt.I": ("i band", "i-band", "johnson i", "cousins i"),
            "em.opt.g": ("g", "g band", "g-band", "sdss g"),
            "em.opt.r": ("r", "r prime", "r'", "sdss r"),
            "em.opt.i": ("i", "i prime", "i'", "sdss i"),
            "em.opt.z": ("z", "z band", "z-band", "sdss z"),
            "em.IR.J": ("j", "j band", "j-band"),
            "em.IR.H": ("h", "h band", "h-band"),
            "em.IR.K": ("k", "k band", "k-band"),
            "em.IR.Ks": ("ks", "k s", "k_s", "ks band", "k-short"),
            "em.radio.4-8GHz": ("c band", "c-band", "cband"),
            "em.radio.8-12GHz": ("x band", "x-band", "xband"),
            "em.radio.12-18GHz": ("ku band", "ku-band"),
            "em.radio.18-27GHz": ("k band radio", "k-band radio"),
            "em.radio.26.5-40GHz": ("ka band", "ka-band"),
        }

        self.band_index = self._build_band_index()

        self.radio_ranges_ghz: list[tuple[float, float, str]] = [
            (0.02, 0.1, "em.radio.20-100MHz"),
            (0.1, 0.2, "em.radio.100-200MHz"),
            (0.2, 0.4, "em.radio.200-400MHz"),
            (0.4, 0.75, "em.radio.400-750MHz"),
            (0.75, 1.5, "em.radio.750-1500MHz"),
            (1.5, 3.0, "em.radio.1500-3000MHz"),
            (3.0, 6.0, "em.radio.3-6GHz"),
            (6.0, 12.0, "em.radio.6-12GHz"),
            (12.0, 30.0, "em.radio.12-30GHz"),
        ]

    # -------------------------
    # Public API
    # -------------------------

    def translate(self, text: str, top_n: int = 15) -> list[UCDMatch]:
        raw = text.strip()
        norm = self._normalize(raw)
        store: dict[str, UCDMatch] = {}

        def add(ucd: str, score: float, reason: str) -> None:
            if ucd in store:
                store[ucd] = store[ucd].add(score, reason)
            else:
                store[ucd] = UCDMatch(ucd=ucd, score=score, reasons=(reason,))

        # 1) Pattern recognizers
        self._recognize_bibcode(raw, norm, add)
        self._recognize_doi(raw, norm, add)
        self._recognize_arxiv(raw, norm, add)
        self._recognize_color_index(raw, norm, add)
        self._recognize_frequency(raw, norm, add)
        self._recognize_wavelength(raw, norm, add)
        self._recognize_coordinates(raw, norm, add)
        self._recognize_time(raw, norm, add)
        self._recognize_error_terms(raw, norm, add)

        # 2) SKOS-like label matching
        self._match_labels(norm, add)

        # 3) Band detection
        detected_bands = self._detect_bands(norm)
        for band_ucd, matched_alias in detected_bands:
            add(band_ucd, 2.5, f"matched band alias '{matched_alias}'")

        # 4) Composition
        self._compose(norm, detected_bands, add)

        ranked = sorted(store.values(), key=lambda m: (-m.score, m.ucd))
        return ranked[:top_n]

    # -------------------------
    # Recognizers
    # -------------------------

    def _recognize_bibcode(self, raw: str, norm: str, add) -> None:
        # Typical ADS bibcode: YYYYJJJJJVVVVMPPPPA
        # e.g. 2018ApJ...852..108F
        if re.fullmatch(r"\d{4}[A-Za-z\.\&]{5}\S{10}", raw.strip()):
            add("meta.bib.bibcode", 12.0, "matched ADS-style bibcode pattern")
            add("meta.bib", 2.0, "bibliographic identifier implies bibliography metadata")

    def _recognize_doi(self, raw: str, norm: str, add) -> None:
        if re.search(r"\b10\.\d{4,9}/[-._;()/:A-Za-z0-9]+\b", raw):
            add("meta.ref.url", 4.0, "matched DOI pattern")
            add("meta.bib", 1.5, "DOI suggests bibliographic metadata")

    def _recognize_arxiv(self, raw: str, norm: str, add) -> None:
        if re.search(r"\barxiv:\s*\d{4}\.\d{4,5}(v\d+)?\b", norm) or re.search(
            r"\b\d{4}\.\d{4,5}(v\d+)?\b", raw
        ):
            add("meta.bib", 3.5, "matched arXiv-style identifier")

    def _recognize_color_index(self, raw: str, norm: str, add) -> None:
        # B-V, U-B, g-r, J-K, etc.
        if re.fullmatch(r"\s*[A-Za-z][A-Za-z']?\s*-\s*[A-Za-z][A-Za-z']?\s*", raw.strip()):
            add("phot.color", 10.0, "matched color-index pattern")
            left, right = [x.strip().casefold() for x in re.split(r"-", raw.strip())]
            left_band = self._lookup_simple_band(left)
            right_band = self._lookup_simple_band(right)
            if left_band:
                add(left_band, 1.5, f"left side of color index resembles band '{left}'")
            if right_band:
                add(right_band, 1.5, f"right side of color index resembles band '{right}'")

    def _recognize_frequency(self, raw: str, norm: str, add) -> None:
        m = re.search(r"\b(\d+(?:\.\d+)?)\s*(ghz|mhz|khz|hz)\b", norm)
        if not m:
            return
        value = float(m.group(1))
        unit = m.group(2)
        ghz = self._to_ghz(value, unit)

        add("em.freq", 8.0, f"parsed frequency {value:g} {unit}")
        add("em.radio", 2.0, "frequency expression suggests radio regime")

        for lo, hi, ucd in self.radio_ranges_ghz:
            if lo <= ghz < hi:
                add(ucd, 4.0, f"frequency lies in {lo:g}-{hi:g} GHz interval")
                break

    def _recognize_wavelength(self, raw: str, norm: str, add) -> None:
        if re.search(
            r"\b(\d+(?:\.\d+)?)\s*(angstrom|angstroms|aa|nm|um|micron|microns|mm|cm|m)\b", norm
        ):
            add("em.wl", 8.0, "parsed wavelength-like expression")

    def _recognize_coordinates(self, raw: str, norm: str, add) -> None:
        if any(tok in norm for tok in ("ra", "right ascension")):
            add("pos.eq.ra", 7.0, "matched RA keyword")
        if any(tok in norm for tok in ("dec", "declination")):
            add("pos.eq.dec", 7.0, "matched Dec keyword")

    def _recognize_time(self, raw: str, norm: str, add) -> None:
        time_aliases = {
            "MJD": "time.epoch",
            "JD": "time.epoch",
            "HJD": "time.epoch",
            "BJD": "time.epoch",
            "date": "time",
            "time": "time",
            "epoch": "time.epoch",
        }
        for alias, ucd in time_aliases.items():
            if self._contains_term(norm, alias.casefold()):
                add(ucd, 4.0, f"matched time alias '{alias}'")

    def _recognize_error_terms(self, raw: str, norm: str, add) -> None:
        if any(tok in norm for tok in ("err", "error", "sigma", "uncertainty", "stddev", "stdev")):
            add("stat.error", 5.0, "matched uncertainty/error terminology")

    # -------------------------
    # SKOS-like label matching
    # -------------------------

    def _match_labels(self, norm: str, add) -> None:
        for label, ucds in self.label_index.items():
            if self._contains_term(norm, label):
                for ucd in ucds:
                    add(ucd, 3.0, f"matched vocabulary label '{label}'")

    # -------------------------
    # Composition
    # -------------------------

    def _compose(self, norm: str, detected_bands: list[tuple[str, str]], add) -> None:
        has_mag = self._has_any(norm, ("mag", "magnitude", "brightness"))
        has_flux = self._has_any(norm, ("flux",))
        has_flux_density = self._has_any(
            norm, ("flux density", "fnu", "f_lambda", "jy", "mjy", "ujy")
        )
        has_filter = self._has_any(norm, ("filter", "bandpass"))
        has_color = self._has_any(norm, ("color", "colour"))

        for band_ucd, _alias in detected_bands:
            if has_mag and band_ucd.startswith("em."):
                add(f"phot.mag;{band_ucd}", 6.0, "composed magnitude with detected band")
            if has_flux and band_ucd.startswith("em."):
                add(f"phot.flux;{band_ucd}", 5.0, "composed flux with detected band")
            if has_flux_density and band_ucd.startswith("em."):
                add(
                    f"phot.flux.density;{band_ucd}", 6.0, "composed flux density with detected band"
                )
            if has_filter and band_ucd.startswith("em."):
                add(f"instr.filter;{band_ucd}", 5.0, "composed filter with detected band")
            if has_color and band_ucd.startswith("em."):
                add("phot.color", 2.0, "color language present near band expression")

    # -------------------------
    # Helpers
    # -------------------------

    def _detect_bands(self, norm: str) -> list[tuple[str, str]]:
        matches: list[tuple[str, str]] = []
        for ucd, aliases in self.band_aliases.items():
            for alias in aliases:
                if self._contains_term(norm, self._normalize(alias)):
                    matches.append((ucd, alias))
                    break
        return matches

    def _lookup_simple_band(self, token: str) -> str | None:
        token = self._normalize(token)
        for ucd, aliases in self.band_aliases.items():
            for alias in aliases:
                if token == self._normalize(alias):
                    return ucd
        return None

    @staticmethod
    def _contains_term(text: str, term: str) -> bool:
        pattern = r"(?<!\w)" + re.escape(term) + r"(?!\w)"
        return re.search(pattern, text) is not None

    @staticmethod
    def _has_any(text: str, terms: Iterable[str]) -> bool:
        return any(UCDTranslator._contains_term(text, UCDTranslator._normalize(t)) for t in terms)

    @staticmethod
    def _normalize(text: str) -> str:
        text = unicodedata.normalize("NFKC", text)
        text = text.casefold()
        text = text.replace("μ", "u")
        text = text.replace("_", " ")
        text = re.sub(r"\s+", " ", text).strip()
        return text

    @staticmethod
    def _to_ghz(value: float, unit: str) -> float:
        unit = unit.casefold()
        if unit == "ghz":
            return value
        if unit == "mhz":
            return value / 1_000.0
        if unit == "khz":
            return value / 1_000_000.0
        if unit == "hz":
            return value / 1_000_000_000.0
        raise ValueError(f"Unsupported frequency unit: {unit}")

    def _build_label_index(self, concepts: list[Concept]) -> dict[str, list[str]]:
        index: dict[str, list[str]] = {}
        for c in concepts:
            labels = [c.pref_label, *c.alt_labels, *c.hidden_labels]
            for label in labels:
                norm = self._normalize(label)
                index.setdefault(norm, []).append(c.ucd)
        return index

    def _build_band_index(self) -> dict[str, str]:
        out: dict[str, str] = {}
        for ucd, aliases in self.band_aliases.items():
            for alias in aliases:
                out[self._normalize(alias)] = ucd
        return out

    def _build_concepts(self) -> list[Concept]:
        # This is intentionally small/starter-sized.
        # Expand it over time by adding more UCD concepts and aliases.
        return [
            Concept(
                ucd="phot.color",
                pref_label="color index",
                alt_labels=("color", "colour index", "colour", "b-v", "u-b", "g-r", "r-i", "j-k"),
            ),
            Concept(
                ucd="meta.bib.bibcode",
                pref_label="bibcode",
                alt_labels=("ads bibcode", "bibliographic code"),
                hidden_labels=("apj bibcode", "mnras bibcode", "a&a bibcode"),
                broader=("meta.bib",),
            ),
            Concept(
                ucd="meta.bib",
                pref_label="bibliographic metadata",
                alt_labels=("bibliography", "bibliographic reference", "reference metadata"),
            ),
            Concept(
                ucd="phot.mag",
                pref_label="magnitude",
                alt_labels=("mag", "brightness"),
            ),
            Concept(
                ucd="phot.flux",
                pref_label="flux",
                alt_labels=("energy flux",),
            ),
            Concept(
                ucd="phot.flux.density",
                pref_label="flux density",
                alt_labels=("fnu", "f_lambda", "jy", "mjy", "ujy"),
            ),
            Concept(
                ucd="em.freq",
                pref_label="frequency",
                alt_labels=("freq", "ghz", "mhz", "khz", "hz"),
            ),
            Concept(
                ucd="em.wl",
                pref_label="wavelength",
                alt_labels=("lambda", "angstrom", "angstroms", "nm", "micron", "microns", "um"),
            ),
            Concept(
                ucd="em.energy",
                pref_label="energy",
                alt_labels=("ev", "kev", "mev"),
            ),
            Concept(
                ucd="instr.filter",
                pref_label="filter",
                alt_labels=("bandpass",),
            ),
            Concept(
                ucd="pos.eq.ra",
                pref_label="right ascension",
                alt_labels=("ra",),
            ),
            Concept(
                ucd="pos.eq.dec",
                pref_label="declination",
                alt_labels=("dec",),
            ),
            Concept(
                ucd="time",
                pref_label="time",
                alt_labels=("date",),
            ),
            Concept(
                ucd="time.epoch",
                pref_label="epoch",
                alt_labels=("mjd", "jd", "hjd", "bjd"),
            ),
            Concept(
                ucd="stat.error",
                pref_label="error",
                alt_labels=("uncertainty", "sigma", "err", "stdev", "stddev"),
            ),
            Concept(
                ucd="meta.ref.url",
                pref_label="reference URL",
                alt_labels=("url", "doi"),
            ),
            Concept(
                ucd="em.radio",
                pref_label="radio",
                alt_labels=("radio band",),
            ),
            # A few EM band concepts for composition
            Concept(ucd="em.opt.U", pref_label="U band"),
            Concept(ucd="em.opt.B", pref_label="B band"),
            Concept(ucd="em.opt.V", pref_label="V band"),
            Concept(ucd="em.opt.R", pref_label="R band"),
            Concept(ucd="em.opt.I", pref_label="I band"),
            Concept(ucd="em.opt.g", pref_label="g band"),
            Concept(ucd="em.opt.r", pref_label="r band"),
            Concept(ucd="em.opt.i", pref_label="i band"),
            Concept(ucd="em.opt.z", pref_label="z band"),
            Concept(ucd="em.IR.J", pref_label="J band"),
            Concept(ucd="em.IR.H", pref_label="H band"),
            Concept(ucd="em.IR.K", pref_label="K band"),
            Concept(ucd="em.IR.Ks", pref_label="Ks band"),
            Concept(ucd="em.radio.4-8GHz", pref_label="C band"),
            Concept(ucd="em.radio.8-12GHz", pref_label="X band"),
            Concept(ucd="em.radio.12-18GHz", pref_label="Ku band"),
            Concept(ucd="em.radio.18-27GHz", pref_label="K band radio"),
            Concept(ucd="em.radio.26.5-40GHz", pref_label="Ka band"),
            Concept(ucd="em.radio.20-100MHz", pref_label="20-100 MHz radio"),
            Concept(ucd="em.radio.100-200MHz", pref_label="100-200 MHz radio"),
            Concept(ucd="em.radio.200-400MHz", pref_label="200-400 MHz radio"),
            Concept(ucd="em.radio.400-750MHz", pref_label="400-750 MHz radio"),
            Concept(ucd="em.radio.750-1500MHz", pref_label="750-1500 MHz radio"),
            Concept(ucd="em.radio.1500-3000MHz", pref_label="1500-3000 MHz radio"),
            Concept(ucd="em.radio.3-6GHz", pref_label="3-6 GHz radio"),
            Concept(ucd="em.radio.6-12GHz", pref_label="6-12 GHz radio"),
            Concept(ucd="em.radio.12-30GHz", pref_label="12-30 GHz radio"),
        ]


# =========================
# Demo
# =========================

if __name__ == "__main__":
    translator = UCDTranslator()

    tests = [
        "B-V",
        "2018ApJ...852..108F",
        "g mag",
        "C band",
        "1.2 GHz",
        "flux density at 4.5 GHz",
        "RA",
        "MJD",
        "uncertainty in g mag",
        "doi:10.3847/1538-4357/aaa123",
    ]

    for text in tests:
        print(f"\nINPUT: {text}")
        results = translator.translate(text)
        for r in results[:8]:
            print(f"  {r.ucd:28s} score={r.score:4.1f} reasons={list(r.reasons)}")
