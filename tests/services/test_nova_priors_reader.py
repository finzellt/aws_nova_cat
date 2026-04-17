"""Unit tests for services/nova_resolver/nova_priors/reader.py.

Scope
-----
- The private ``_load_priors(path)`` loader.  Called directly with
  synthetic bundles written to ``tmp_path`` so load-time error paths can
  be exercised without reimporting the module.
- The four public API functions (``lookup``, ``get_entry``,
  ``is_known_non_nova``, ``list_entries``).  The module-level singletons
  are monkeypatched with synthetic indexes so behavior is decoupled from
  the real ``nova_priors.json`` artifact.

Implicit smoke check
---------------------
Importing the reader at collection time triggers ``_load_priors`` against
the committed ``nova_priors.json``.  If that file is ever malformed,
every test in this file fails at collection — which is the intended CI
signal.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest
from nova_resolver.nova_priors import reader
from pydantic import ValidationError

from contracts.models.priors import NovaPriorsEntry

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# Three entries covering every public-API permutation:
#   CK Vul — is_nova=True, no peak_mag, rich aliases
#   FU Ori — is_nova=False (exercises rejection flow)
#   T CrB  — is_nova=True, is_recurrent=True, peak_mag present
_VALID_BUNDLE: dict[str, Any] = {
    "_schema_version": "1.0.0",
    "_generated_at": "2026-04-17T00:00:00Z",
    "_source_csv": "test",
    "_source_sha256": "deadbeef",
    "_note": "synthetic",
    "entries": {
        "ck vul": {
            "primary_name": "CK Vul",
            "simbad_main_id": "CK Vul",
            "aliases": ["NOVA Vul 1670", "CK Vul", "TIC 452248097"],
            "discovery_date": "1670-06-20",
            "otypes": ["*", "CV*", "No*"],
            "is_nova": True,
            "is_recurrent": False,
            "peak_mag": None,
            "peak_mag_band": None,
            "peak_mag_uncertain": False,
        },
        "fu ori": {
            "primary_name": "FU Ori",
            "simbad_main_id": None,
            "aliases": ["NOVA Ori 1939"],
            "discovery_date": "1939-01-18",
            "otypes": ["Or*", "V*"],
            "is_nova": False,
            "is_recurrent": False,
            "peak_mag": None,
            "peak_mag_band": None,
            "peak_mag_uncertain": False,
        },
        "t crb": {
            "primary_name": "T CrB",
            "simbad_main_id": "T CrB",
            "aliases": ["NOVA CrB 1866", "NOVA CrB 1946", "HR 5958"],
            "discovery_date": "1866-05-12",
            "otypes": ["*", "No*", "Sy*"],
            "is_nova": True,
            "is_recurrent": True,
            "peak_mag": 2.0,
            "peak_mag_band": "V",
            "peak_mag_uncertain": False,
        },
    },
}


def _write_bundle(tmp_path: Path, doc: dict[str, Any]) -> Path:
    """Write a bundle dict to a temp file and return its path."""
    path = tmp_path / "nova_priors.json"
    path.write_text(json.dumps(doc), encoding="utf-8")
    return path


def _deep_copy_bundle() -> dict[str, Any]:
    """Return a fresh deep copy of _VALID_BUNDLE so tests can mutate freely."""
    return cast(dict[str, Any], json.loads(json.dumps(_VALID_BUNDLE)))


@pytest.fixture
def valid_bundle_path(tmp_path: Path) -> Path:
    """Write the happy-path bundle and return its path."""
    return _write_bundle(tmp_path, _VALID_BUNDLE)


@pytest.fixture
def patched_reader(valid_bundle_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Load the happy-path bundle into the module-level singletons.

    Public-API tests use this fixture so they exercise the real
    ``_load_priors`` pipeline against a known synthetic bundle, without
    depending on the committed ``nova_priors.json``.
    """
    entry_index, alias_index = reader._load_priors(valid_bundle_path)
    monkeypatch.setattr(reader, "_ENTRY_INDEX", entry_index)
    monkeypatch.setattr(reader, "_ALIAS_INDEX", alias_index)


# ---------------------------------------------------------------------------
# TestLoadPriors — direct calls to _load_priors with synthetic bundles
# ---------------------------------------------------------------------------


class TestLoadPriors:
    """Exercise every load-time check defined by ADR-036 Decision 4."""

    def test_happy_path_builds_both_indexes(self, valid_bundle_path: Path) -> None:
        entry_index, alias_index = reader._load_priors(valid_bundle_path)

        # Three primary-name entries
        assert set(entry_index) == {"ck vul", "fu ori", "t crb"}
        assert all(isinstance(e, NovaPriorsEntry) for e in entry_index.values())

        # Every primary name self-aliases
        for key in entry_index:
            assert alias_index[key] == key

        # Operator-curated aliases resolve to their primary
        assert alias_index["nova vul 1670"] == "ck vul"
        assert alias_index["tic 452248097"] == "ck vul"
        assert alias_index["hr 5958"] == "t crb"
        assert alias_index["nova crb 1866"] == "t crb"
        assert alias_index["nova crb 1946"] == "t crb"
        assert alias_index["nova ori 1939"] == "fu ori"

    def test_major_version_mismatch_raises(self, tmp_path: Path) -> None:
        doc = _deep_copy_bundle()
        doc["_schema_version"] = "2.0.0"
        path = _write_bundle(tmp_path, doc)
        with pytest.raises(RuntimeError, match="major version 2 is not supported"):
            reader._load_priors(path)

    def test_unparseable_schema_version_raises(self, tmp_path: Path) -> None:
        doc = _deep_copy_bundle()
        doc["_schema_version"] = "banana"
        path = _write_bundle(tmp_path, doc)
        with pytest.raises(RuntimeError, match="cannot parse _schema_version"):
            reader._load_priors(path)

    def test_minor_version_bump_accepted(self, tmp_path: Path) -> None:
        doc = _deep_copy_bundle()
        doc["_schema_version"] = "1.1.0"
        path = _write_bundle(tmp_path, doc)
        entry_index, _ = reader._load_priors(path)
        assert "ck vul" in entry_index

    def test_patch_version_bump_accepted(self, tmp_path: Path) -> None:
        doc = _deep_copy_bundle()
        doc["_schema_version"] = "1.0.42"
        path = _write_bundle(tmp_path, doc)
        entry_index, _ = reader._load_priors(path)
        assert "ck vul" in entry_index

    def test_entries_not_a_dict_raises(self, tmp_path: Path) -> None:
        doc = _deep_copy_bundle()
        doc["entries"] = [doc["entries"]["ck vul"]]  # list instead of dict
        path = _write_bundle(tmp_path, doc)
        with pytest.raises(RuntimeError, match="'entries' must be a mapping"):
            reader._load_priors(path)

    def test_entry_value_not_an_object_raises(self, tmp_path: Path) -> None:
        doc = _deep_copy_bundle()
        doc["entries"]["ck vul"] = "not an object"
        path = _write_bundle(tmp_path, doc)
        with pytest.raises(RuntimeError, match="must be an object"):
            reader._load_priors(path)

    def test_key_mismatch_with_primary_name_raises(self, tmp_path: Path) -> None:
        """Key must equal normalize(primary_name) — catches hand-edits."""
        doc = _deep_copy_bundle()
        ck_vul_data = doc["entries"].pop("ck vul")
        doc["entries"]["wrong key"] = ck_vul_data
        path = _write_bundle(tmp_path, doc)
        with pytest.raises(RuntimeError, match="does not match normalize"):
            reader._load_priors(path)

    def test_alias_collision_raises(self, tmp_path: Path) -> None:
        """Two entries claiming the same alias → RuntimeError."""
        doc = _deep_copy_bundle()
        # Give T CrB an alias that collides with CK Vul's "NOVA Vul 1670".
        doc["entries"]["t crb"]["aliases"].append("NOVA Vul 1670")
        path = _write_bundle(tmp_path, doc)
        with pytest.raises(RuntimeError, match="alias collision"):
            reader._load_priors(path)

    def test_primary_name_in_own_aliases_is_noop(self, valid_bundle_path: Path) -> None:
        """A primary name listed in its own aliases list is not a collision.

        CK Vul's fixture has ``"CK Vul"`` in its aliases list alongside the
        self-alias the reader inserts.  This test makes the no-op invariant
        explicit so it can't silently regress to a collision.
        """
        entry_index, alias_index = reader._load_priors(valid_bundle_path)
        assert "ck vul" in entry_index
        assert alias_index["ck vul"] == "ck vul"

    def test_pydantic_cofield_violation_raises(self, tmp_path: Path) -> None:
        """peak_mag without peak_mag_band — caught during entry construction."""
        doc = _deep_copy_bundle()
        doc["entries"]["ck vul"]["peak_mag"] = 5.0
        doc["entries"]["ck vul"]["peak_mag_band"] = None
        path = _write_bundle(tmp_path, doc)
        with pytest.raises(ValidationError, match="peak_mag"):
            reader._load_priors(path)

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            reader._load_priors(tmp_path / "does_not_exist.json")


# ---------------------------------------------------------------------------
# TestLookup — the normalize-and-resolve primary API
# ---------------------------------------------------------------------------


class TestLookup:
    def test_primary_name_exact(self, patched_reader: None) -> None:
        entry = reader.lookup("CK Vul")
        assert entry is not None
        assert entry.primary_name == "CK Vul"

    def test_primary_name_already_normalized(self, patched_reader: None) -> None:
        assert reader.lookup("ck vul") is not None

    def test_alias_hit(self, patched_reader: None) -> None:
        entry = reader.lookup("NOVA Vul 1670")
        assert entry is not None
        assert entry.primary_name == "CK Vul"

    def test_normalization_case(self, patched_reader: None) -> None:
        assert reader.lookup("CK VUL") is not None
        assert reader.lookup("ck vul") is not None

    def test_normalization_underscores(self, patched_reader: None) -> None:
        entry = reader.lookup("CK_Vul")
        assert entry is not None
        assert entry.primary_name == "CK Vul"

    def test_normalization_whitespace(self, patched_reader: None) -> None:
        entry = reader.lookup("  CK   Vul  ")
        assert entry is not None
        assert entry.primary_name == "CK Vul"

    def test_miss_returns_none(self, patched_reader: None) -> None:
        """ADR-036 Decision 9: miss is neutral, not a rejection."""
        assert reader.lookup("V9999 Unknown") is None


# ---------------------------------------------------------------------------
# TestGetEntry — direct primary-name-only lookup
# ---------------------------------------------------------------------------


class TestGetEntry:
    def test_direct_primary_key_hit(self, patched_reader: None) -> None:
        entry = reader.get_entry("t crb")
        assert entry is not None
        assert entry.primary_name == "T CrB"

    def test_alias_does_not_resolve(self, patched_reader: None) -> None:
        """get_entry bypasses the alias index by design."""
        assert reader.get_entry("hr 5958") is None
        assert reader.get_entry("nova vul 1670") is None

    def test_non_normalized_input_misses(self, patched_reader: None) -> None:
        """get_entry does NOT normalize — callers are responsible."""
        assert reader.get_entry("T CrB") is None
        assert reader.get_entry("CK Vul") is None


# ---------------------------------------------------------------------------
# TestIsKnownNonNova — rejection-flow convenience
# ---------------------------------------------------------------------------


class TestIsKnownNonNova:
    def test_non_nova_hit_returns_true(self, patched_reader: None) -> None:
        assert reader.is_known_non_nova("FU Ori") is True

    def test_non_nova_hit_via_alias(self, patched_reader: None) -> None:
        assert reader.is_known_non_nova("NOVA Ori 1939") is True

    def test_non_nova_hit_with_normalization(self, patched_reader: None) -> None:
        assert reader.is_known_non_nova("fu_ori") is True

    def test_is_nova_true_returns_false(self, patched_reader: None) -> None:
        """Known classical novae are not known-non-novae."""
        assert reader.is_known_non_nova("CK Vul") is False
        assert reader.is_known_non_nova("T CrB") is False

    def test_miss_returns_false(self, patched_reader: None) -> None:
        """ADR-036 Decision 9: miss is not a rejection.

        This is the single most important invariant of the rejection
        flow.  If a miss returned True, every new discovery that an
        operator tried to ingest would be rejected at the front door
        until they hand-curated a CSV row.
        """
        assert reader.is_known_non_nova("V9999 Unknown") is False


# ---------------------------------------------------------------------------
# TestListEntries — iterator over all entries
# ---------------------------------------------------------------------------


class TestListEntries:
    def test_yields_all_entries(self, patched_reader: None) -> None:
        entries = list(reader.list_entries())
        assert len(entries) == 3

    def test_returns_novapriors_entries(self, patched_reader: None) -> None:
        for entry in reader.list_entries():
            assert isinstance(entry, NovaPriorsEntry)

    def test_in_load_order(self, patched_reader: None) -> None:
        """Load order is the JSON's insertion order, which the build
        script emits alphabetically."""
        names = [e.primary_name for e in reader.list_entries()]
        assert names == ["CK Vul", "FU Ori", "T CrB"]

    def test_fresh_iterator_per_call(self, patched_reader: None) -> None:
        """Each call returns a new iterator, not a shared one."""
        iter_a = reader.list_entries()
        iter_b = reader.list_entries()
        assert list(iter_a) != []
        assert list(iter_b) != []  # iter_a was not consumed by the list() of iter_b
