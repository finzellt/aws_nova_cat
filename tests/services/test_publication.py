"""Unit tests for publication.py (DESIGN-003 §12).

Uses moto to mock S3.  ``ReleasePublisher`` takes a low-level S3 client
and bucket name, so no module-reload pattern is needed.

Covers:
  Release ID (§12.3):
  - Format is YYYYMMDD-HHMMSS

  Pointer read (§12.3, §12.6):
  - Returns previous release ID when current.json exists
  - Returns None on bootstrap (NoSuchKey)
  - Re-raises non-NoSuchKey errors

  Phase 1 — Upload artifacts (§12.5):
  - JSON artifact written with correct key and Content-Type
  - SVG artifact written with correct key and Content-Type
  - Bundle artifact written with Content-Disposition: attachment
  - Decimal values in JSON are serialized correctly

  Phase 2 — Copy forward (§12.5):
  - Copies all artifacts for non-swept novae
  - Skips Phase 2 on bootstrap (no previous release)
  - Empty set of novae to copy is a no-op
  - Copy failure tracked; returns False
  - Raises if read_previous_pointer not called first

  Phase 3 — Write catalog (§12.5):
  - catalog.json written to release prefix

  Phase 4 — Update pointer (§12.5):
  - current.json updated with new release ID
  - Blocked when Phase 2 had copy failures
"""

from __future__ import annotations

import json
import re
from decimal import Decimal
from typing import Any

import boto3
import pytest
from moto import mock_aws
from release_publisher import ReleasePublisher

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BUCKET = "nova-cat-public-test"
_REGION = "us-east-1"

_NOVA_A = "aaaaaaaa-0000-0000-0000-000000000001"
_NOVA_B = "bbbbbbbb-0000-0000-0000-000000000002"
_NOVA_C = "cccccccc-0000-0000-0000-000000000003"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_DEFAULT_REGION", _REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_s3() -> Any:
    """Create moto S3 client and bucket."""
    s3 = boto3.client("s3", region_name=_REGION)
    s3.create_bucket(Bucket=_BUCKET)
    return s3


def _seed_pointer(s3: Any, release_id: str) -> None:
    """Write a current.json pointer to the bucket."""
    pointer = {"release_id": release_id, "generated_at": "2026-03-30T14:00:00Z"}
    s3.put_object(
        Bucket=_BUCKET,
        Key="current.json",
        Body=json.dumps(pointer).encode(),
        ContentType="application/json",
    )


def _seed_nova_artifacts(
    s3: Any,
    release_id: str,
    nova_id: str,
    filenames: list[str],
) -> None:
    """Write stub artifacts under a release prefix for a nova."""
    for fname in filenames:
        key = f"releases/{release_id}/nova/{nova_id}/{fname}"
        s3.put_object(Bucket=_BUCKET, Key=key, Body=b"stub-content")


def _get_object_body(s3: Any, key: str) -> bytes:
    """Read an S3 object's body."""
    resp = s3.get_object(Bucket=_BUCKET, Key=key)
    body: bytes = resp["Body"].read()
    return body


def _list_keys(s3: Any, prefix: str) -> list[str]:
    """List all keys under a prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return sorted(keys)


# ===========================================================================
# Release ID (§12.3)
# ===========================================================================


class TestReleaseId:
    def test_format_is_yyyymmdd_hhmmss(self) -> None:
        with mock_aws():
            s3 = _make_s3()
            pub = ReleasePublisher(s3, _BUCKET)
            assert re.match(r"^\d{8}-\d{6}$", pub.release_id)

    def test_release_id_stable_across_reads(self) -> None:
        with mock_aws():
            s3 = _make_s3()
            pub = ReleasePublisher(s3, _BUCKET)
            assert pub.release_id == pub.release_id


# ===========================================================================
# Pointer read (§12.3, §12.6)
# ===========================================================================


class TestReadPreviousPointer:
    def test_returns_previous_release_id(self) -> None:
        with mock_aws():
            s3 = _make_s3()
            _seed_pointer(s3, "20260329-140000")
            pub = ReleasePublisher(s3, _BUCKET)
            result = pub.read_previous_pointer()
            assert result == "20260329-140000"
            assert pub.previous_release_id == "20260329-140000"

    def test_returns_none_on_bootstrap(self) -> None:
        """§12.6: first sweep, no current.json."""
        with mock_aws():
            s3 = _make_s3()
            pub = ReleasePublisher(s3, _BUCKET)
            result = pub.read_previous_pointer()
            assert result is None
            assert pub.previous_release_id is None


# ===========================================================================
# Phase 1 — Upload artifacts (§12.5)
# ===========================================================================


class TestUploadJsonArtifact:
    def test_writes_to_correct_key(self) -> None:
        with mock_aws():
            s3 = _make_s3()
            pub = ReleasePublisher(s3, _BUCKET)
            pub.upload_json_artifact(_NOVA_A, "references.json", {"test": True})
            key = f"releases/{pub.release_id}/nova/{_NOVA_A}/references.json"
            body = json.loads(_get_object_body(s3, key))
            assert body == {"test": True}

    def test_content_type_is_application_json(self) -> None:
        with mock_aws():
            s3 = _make_s3()
            pub = ReleasePublisher(s3, _BUCKET)
            pub.upload_json_artifact(_NOVA_A, "nova.json", {})
            key = f"releases/{pub.release_id}/nova/{_NOVA_A}/nova.json"
            head = s3.head_object(Bucket=_BUCKET, Key=key)
            assert head["ContentType"] == "application/json"

    def test_decimal_values_serialized(self) -> None:
        with mock_aws():
            s3 = _make_s3()
            pub = ReleasePublisher(s3, _BUCKET)
            data: dict[str, Any] = {
                "count": Decimal("7"),
                "ratio": Decimal("3.14"),
            }
            pub.upload_json_artifact(_NOVA_A, "test.json", data)
            key = f"releases/{pub.release_id}/nova/{_NOVA_A}/test.json"
            body = json.loads(_get_object_body(s3, key))
            assert body["count"] == 7
            assert isinstance(body["count"], int)
            assert body["ratio"] == pytest.approx(3.14)


class TestUploadSvgArtifact:
    def test_writes_to_correct_key(self) -> None:
        with mock_aws():
            s3 = _make_s3()
            pub = ReleasePublisher(s3, _BUCKET)
            svg = '<svg xmlns="http://www.w3.org/2000/svg"><polyline/></svg>'
            pub.upload_svg_artifact(_NOVA_A, "sparkline.svg", svg)
            key = f"releases/{pub.release_id}/nova/{_NOVA_A}/sparkline.svg"
            body = _get_object_body(s3, key).decode("utf-8")
            assert "<polyline/>" in body

    def test_content_type_is_svg(self) -> None:
        with mock_aws():
            s3 = _make_s3()
            pub = ReleasePublisher(s3, _BUCKET)
            pub.upload_svg_artifact(_NOVA_A, "sparkline.svg", "<svg/>")
            key = f"releases/{pub.release_id}/nova/{_NOVA_A}/sparkline.svg"
            head = s3.head_object(Bucket=_BUCKET, Key=key)
            assert head["ContentType"] == "image/svg+xml"


class TestUploadBundleArtifact:
    def test_writes_to_stable_key(self) -> None:
        with mock_aws():
            s3 = _make_s3()
            pub = ReleasePublisher(s3, _BUCKET)
            pub.upload_bundle_artifact(_NOVA_A, b"PK\x03\x04fake")
            key = f"releases/{pub.release_id}/nova/{_NOVA_A}/bundle.zip"
            body = _get_object_body(s3, key)
            assert body == b"PK\x03\x04fake"

    def test_content_type_is_application_zip(self) -> None:
        with mock_aws():
            s3 = _make_s3()
            pub = ReleasePublisher(s3, _BUCKET)
            pub.upload_bundle_artifact(_NOVA_A, b"data")
            key = f"releases/{pub.release_id}/nova/{_NOVA_A}/bundle.zip"
            head = s3.head_object(Bucket=_BUCKET, Key=key)
            assert head["ContentType"] == "application/zip"

    def test_content_disposition_with_dated_filename(self) -> None:
        with mock_aws():
            s3 = _make_s3()
            pub = ReleasePublisher(s3, _BUCKET)
            dated = "v1674-her_bundle_20260330.zip"
            pub.upload_bundle_artifact(
                _NOVA_A,
                b"data",
                disposition_filename=dated,
            )
            key = f"releases/{pub.release_id}/nova/{_NOVA_A}/bundle.zip"
            head = s3.head_object(Bucket=_BUCKET, Key=key)
            assert head["ContentDisposition"] == f'attachment; filename="{dated}"'

    def test_content_disposition_defaults_to_bundle_zip(self) -> None:
        with mock_aws():
            s3 = _make_s3()
            pub = ReleasePublisher(s3, _BUCKET)
            pub.upload_bundle_artifact(_NOVA_A, b"data")
            key = f"releases/{pub.release_id}/nova/{_NOVA_A}/bundle.zip"
            head = s3.head_object(Bucket=_BUCKET, Key=key)
            assert head["ContentDisposition"] == 'attachment; filename="bundle.zip"'


# ===========================================================================
# Phase 2 — Copy forward (§12.5)
# ===========================================================================


class TestCopyForward:
    def test_copies_all_artifacts_for_unchanged_novae(self) -> None:
        with mock_aws():
            s3 = _make_s3()
            prev = "20260329-140000"
            _seed_pointer(s3, prev)
            _seed_nova_artifacts(
                s3,
                prev,
                _NOVA_A,
                ["references.json", "spectra.json", "nova.json"],
            )

            pub = ReleasePublisher(s3, _BUCKET)
            pub.read_previous_pointer()
            result = pub.copy_forward_unchanged_novae({_NOVA_A})

            assert result is True
            new_prefix = f"releases/{pub.release_id}/nova/{_NOVA_A}/"
            keys = _list_keys(s3, new_prefix)
            filenames = [k.split("/")[-1] for k in keys]
            assert sorted(filenames) == [
                "nova.json",
                "references.json",
                "spectra.json",
            ]

    def test_copies_multiple_novae(self) -> None:
        with mock_aws():
            s3 = _make_s3()
            prev = "20260329-140000"
            _seed_pointer(s3, prev)
            _seed_nova_artifacts(s3, prev, _NOVA_A, ["nova.json"])
            _seed_nova_artifacts(s3, prev, _NOVA_B, ["nova.json", "spectra.json"])

            pub = ReleasePublisher(s3, _BUCKET)
            pub.read_previous_pointer()
            result = pub.copy_forward_unchanged_novae({_NOVA_A, _NOVA_B})

            assert result is True
            assert len(_list_keys(s3, f"releases/{pub.release_id}/nova/{_NOVA_A}/")) == 1
            assert len(_list_keys(s3, f"releases/{pub.release_id}/nova/{_NOVA_B}/")) == 2

    def test_skips_on_bootstrap(self) -> None:
        """§12.6: no previous release → Phase 2 is a no-op."""
        with mock_aws():
            s3 = _make_s3()
            pub = ReleasePublisher(s3, _BUCKET)
            pub.read_previous_pointer()
            result = pub.copy_forward_unchanged_novae({_NOVA_A})
            assert result is True
            # Nothing written — no release prefix exists.
            keys = _list_keys(s3, f"releases/{pub.release_id}/")
            assert keys == []

    def test_empty_set_is_noop(self) -> None:
        with mock_aws():
            s3 = _make_s3()
            _seed_pointer(s3, "20260329-140000")
            pub = ReleasePublisher(s3, _BUCKET)
            pub.read_previous_pointer()
            result = pub.copy_forward_unchanged_novae(set())
            assert result is True

    def test_nova_with_no_previous_artifacts(self) -> None:
        """A nova in the copy set but absent from previous release."""
        with mock_aws():
            s3 = _make_s3()
            _seed_pointer(s3, "20260329-140000")
            # No artifacts seeded for NOVA_A under previous release.
            pub = ReleasePublisher(s3, _BUCKET)
            pub.read_previous_pointer()
            result = pub.copy_forward_unchanged_novae({_NOVA_A})
            assert result is True
            # Nothing to copy, but no failure either.
            keys = _list_keys(s3, f"releases/{pub.release_id}/nova/{_NOVA_A}/")
            assert keys == []

    def test_raises_if_pointer_not_read(self) -> None:
        with mock_aws():
            s3 = _make_s3()
            pub = ReleasePublisher(s3, _BUCKET)
            with pytest.raises(RuntimeError, match="read_previous_pointer"):
                pub.copy_forward_unchanged_novae({_NOVA_A})

    def test_has_copy_failures_false_on_success(self) -> None:
        with mock_aws():
            s3 = _make_s3()
            pub = ReleasePublisher(s3, _BUCKET)
            pub.read_previous_pointer()
            pub.copy_forward_unchanged_novae(set())
            assert pub.has_copy_failures is False


# ===========================================================================
# Phase 1.5 — Copy forward missing artifacts for swept novae
# ===========================================================================


class TestCopyForwardMissingArtifacts:
    def test_partial_manifest_copies_missing(self) -> None:
        """Artifacts not in generated_artifacts are copied from previous release."""
        with mock_aws():
            s3 = _make_s3()
            prev = "20260329-140000"
            _seed_pointer(s3, prev)
            _seed_nova_artifacts(
                s3,
                prev,
                _NOVA_A,
                ["spectra.json", "references.json", "photometry.json", "nova.json"],
            )

            pub = ReleasePublisher(s3, _BUCKET)
            pub.read_previous_pointer()

            # Simulate that photometry.json and nova.json were freshly generated.
            generated = {"photometry.json", "nova.json"}
            result = pub.copy_forward_missing_artifacts(_NOVA_A, generated)

            assert result == 2  # spectra.json + references.json copied

            new_prefix = f"releases/{pub.release_id}/nova/{_NOVA_A}/"
            keys = _list_keys(s3, new_prefix)
            filenames = sorted(k.split("/")[-1] for k in keys)
            assert filenames == ["references.json", "spectra.json"]

    def test_bootstrap_returns_zero(self) -> None:
        """No previous release → nothing to copy, no S3 calls."""
        with mock_aws():
            s3 = _make_s3()
            pub = ReleasePublisher(s3, _BUCKET)
            pub.read_previous_pointer()
            assert pub.previous_release_id is None

            result = pub.copy_forward_missing_artifacts(_NOVA_A, {"nova.json"})
            assert result == 0

    def test_all_generated_copies_nothing(self) -> None:
        """When generated_artifacts covers everything, 0 copied."""
        with mock_aws():
            s3 = _make_s3()
            prev = "20260329-140000"
            _seed_pointer(s3, prev)
            _seed_nova_artifacts(s3, prev, _NOVA_A, ["photometry.json", "nova.json"])

            pub = ReleasePublisher(s3, _BUCKET)
            pub.read_previous_pointer()

            result = pub.copy_forward_missing_artifacts(_NOVA_A, {"photometry.json", "nova.json"})
            assert result == 0

    def test_new_nova_nothing_in_previous(self) -> None:
        """Nova has no artifacts in the previous release — 0 copied, no errors."""
        with mock_aws():
            s3 = _make_s3()
            prev = "20260329-140000"
            _seed_pointer(s3, prev)
            # No artifacts seeded for _NOVA_A.

            pub = ReleasePublisher(s3, _BUCKET)
            pub.read_previous_pointer()

            result = pub.copy_forward_missing_artifacts(_NOVA_A, {"nova.json"})
            assert result == 0


# ===========================================================================
# Phase 3 — Write catalog (§12.5)
# ===========================================================================


class TestWriteCatalog:
    def test_writes_to_release_prefix(self) -> None:
        with mock_aws():
            s3 = _make_s3()
            pub = ReleasePublisher(s3, _BUCKET)
            catalog = {"schema_version": "1.1", "novae": []}
            pub.write_catalog(catalog)
            key = f"releases/{pub.release_id}/catalog.json"
            body = json.loads(_get_object_body(s3, key))
            assert body["schema_version"] == "1.1"

    def test_content_type_is_json(self) -> None:
        with mock_aws():
            s3 = _make_s3()
            pub = ReleasePublisher(s3, _BUCKET)
            pub.write_catalog({"novae": []})
            key = f"releases/{pub.release_id}/catalog.json"
            head = s3.head_object(Bucket=_BUCKET, Key=key)
            assert head["ContentType"] == "application/json"


# ===========================================================================
# Phase 4 — Update pointer (§12.5)
# ===========================================================================


class TestUpdatePointer:
    def test_writes_current_json(self) -> None:
        with mock_aws():
            s3 = _make_s3()
            pub = ReleasePublisher(s3, _BUCKET)
            pub.read_previous_pointer()
            pub.update_pointer()
            body = json.loads(_get_object_body(s3, "current.json"))
            assert body["release_id"] == pub.release_id
            assert "generated_at" in body

    def test_pointer_generated_at_is_iso_8601(self) -> None:
        with mock_aws():
            s3 = _make_s3()
            pub = ReleasePublisher(s3, _BUCKET)
            pub.read_previous_pointer()
            pub.update_pointer()
            body = json.loads(_get_object_body(s3, "current.json"))
            assert re.match(
                r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$",
                body["generated_at"],
            )

    def test_blocked_by_copy_failures(self) -> None:
        """§12.9: pointer must NOT be updated if Phase 2 had failures."""
        with mock_aws():
            s3 = _make_s3()
            pub = ReleasePublisher(s3, _BUCKET)
            # Simulate a copy failure by poking the internal ledger.
            pub._pointer_read = True
            pub._copy_failures.append(_NOVA_A)
            with pytest.raises(RuntimeError, match="copy failures"):
                pub.update_pointer()


# ===========================================================================
# End-to-end: full four-phase lifecycle
# ===========================================================================


class TestFullLifecycle:
    def test_two_successive_sweeps(self) -> None:
        """Simulate two sweeps: first is bootstrap, second copies forward."""
        with mock_aws():
            s3 = _make_s3()

            # --- Sweep 1 (bootstrap) ---
            pub1 = ReleasePublisher(s3, _BUCKET)
            # Force a distinct release ID so sweep 2 doesn't collide.
            pub1._release_id = "20260329-140000"
            pub1.read_previous_pointer()
            assert pub1.previous_release_id is None

            # Phase 1: generate artifacts for NOVA_A.
            pub1.upload_json_artifact(_NOVA_A, "nova.json", {"nova_id": _NOVA_A})
            pub1.upload_json_artifact(_NOVA_A, "references.json", {"references": []})

            # Phase 2: bootstrap — nothing to copy.
            assert pub1.copy_forward_unchanged_novae(set()) is True

            # Phase 3: catalog.
            pub1.write_catalog({"schema_version": "1.1", "novae": [_NOVA_A]})

            # Phase 4: pointer.
            pub1.update_pointer()

            release_1 = pub1.release_id

            # --- Sweep 2 (NOVA_B is new; NOVA_A copied forward) ---
            pub2 = ReleasePublisher(s3, _BUCKET)
            prev = pub2.read_previous_pointer()
            assert prev == release_1

            # Phase 1: generate artifacts for NOVA_B only.
            pub2.upload_json_artifact(_NOVA_B, "nova.json", {"nova_id": _NOVA_B})

            # Phase 2: copy NOVA_A from release_1.
            assert pub2.copy_forward_unchanged_novae({_NOVA_A}) is True

            # Verify NOVA_A's artifacts exist in release 2.
            nova_a_keys = _list_keys(s3, f"releases/{pub2.release_id}/nova/{_NOVA_A}/")
            assert len(nova_a_keys) == 2  # nova.json + references.json

            # Verify NOVA_B's artifacts exist in release 2.
            nova_b_keys = _list_keys(s3, f"releases/{pub2.release_id}/nova/{_NOVA_B}/")
            assert len(nova_b_keys) == 1  # nova.json

            # Phase 3 + 4.
            pub2.write_catalog({"schema_version": "1.1", "novae": [_NOVA_A, _NOVA_B]})
            pub2.update_pointer()

            # Final pointer points to release 2.
            final = json.loads(_get_object_body(s3, "current.json"))
            assert final["release_id"] == pub2.release_id
            assert final["release_id"] != release_1
