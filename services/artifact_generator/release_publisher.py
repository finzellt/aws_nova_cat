"""Release publication logic (DESIGN-003 §12).

Manages the immutable release lifecycle for the artifact regeneration
pipeline.  Each sweep writes all artifacts — freshly generated and
unchanged — to a new S3 prefix (``releases/<YYYYMMDD-HHMMSS>/``).
A stable pointer file (``current.json``) at the bucket root identifies
the active release.

Four-phase publication (§12.5):
    Phase 1 — Write swept novae artifacts to the new release prefix.
    Phase 2 — Copy unchanged novae from the previous release.
    Phase 3 — Write ``catalog.json`` to the new release prefix.
    Phase 4 — Update ``current.json`` pointer (atomic switchover).

Error handling (§12.9):
    - Per-artifact upload failure → caller records nova as failed.
    - Copy failure → tracked; pointer NOT updated if any copy fails.
    - catalog.json failure → pointer NOT updated.
    - Pointer failure → logged; release exists but is unreferenced.

Bootstrap (§12.6):
    First sweep has no ``current.json``.  Phase 2 is skipped entirely.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from botocore.exceptions import ClientError

_logger = logging.getLogger("artifact_generator")

_POINTER_KEY = "current.json"


# ---------------------------------------------------------------------------
# JSON serialization
# ---------------------------------------------------------------------------


class _DecimalEncoder(json.JSONEncoder):
    """JSON encoder that handles DynamoDB ``Decimal`` values."""

    def default(self, o: object) -> int | float:
        if isinstance(o, Decimal):
            return int(o) if o == int(o) else float(o)
        msg = f"Object of type {type(o).__name__} is not JSON serializable"
        raise TypeError(msg)


def _serialize_json(data: dict[str, Any]) -> bytes:
    """Serialize a dict to UTF-8 JSON bytes with Decimal support."""
    return json.dumps(
        data,
        cls=_DecimalEncoder,
        ensure_ascii=False,
    ).encode("utf-8")


# ---------------------------------------------------------------------------
# Release ID
# ---------------------------------------------------------------------------


def _generate_release_id() -> str:
    """Generate a ``YYYYMMDD-HHMMSS`` release ID from the current UTC time.

    Generated once at the start of publication and used consistently for
    all writes in a sweep (§12.3).
    """
    now = datetime.now(UTC)
    return now.strftime("%Y%m%d-%H%M%S")


# ---------------------------------------------------------------------------
# ReleasePublisher
# ---------------------------------------------------------------------------


class ReleasePublisher:
    """Manages the four-phase immutable release lifecycle (§12).

    Instantiated once per Fargate task execution.  The caller uses it
    across all four phases:

    1. Call :meth:`upload_json_artifact`, :meth:`upload_svg_artifact`,
       and :meth:`upload_bundle_artifact` during per-nova generation
       (Phase 1).
    2. Call :meth:`copy_forward_unchanged_novae` after all per-nova
       generation completes (Phase 2).
    3. Call :meth:`write_catalog` after Phase 2 (Phase 3).
    4. Call :meth:`update_pointer` after Phase 3 (Phase 4).

    Parameters
    ----------
    s3_client
        Low-level boto3 S3 client (``boto3.client("s3")``).
    bucket
        Name of the public site S3 bucket.
    """

    def __init__(self, s3_client: Any, bucket: str) -> None:
        self._s3: Any = s3_client
        self._bucket = bucket
        self._release_id = _generate_release_id()
        self._previous_release_id: str | None = None
        self._pointer_read = False
        self._copy_failures: list[str] = []

    @property
    def release_id(self) -> str:
        """The ``YYYYMMDD-HHMMSS`` release ID for this sweep."""
        return self._release_id

    @property
    def previous_release_id(self) -> str | None:
        """The previous release ID, or ``None`` on bootstrap.

        Only available after :meth:`read_previous_pointer` has been called.
        """
        return self._previous_release_id

    @property
    def has_copy_failures(self) -> bool:
        """Whether any Phase 2 copy-forward operations failed."""
        return len(self._copy_failures) > 0

    # ------------------------------------------------------------------
    # Pointer — read (§12.3, §12.6)
    # ------------------------------------------------------------------

    def read_previous_pointer(self) -> str | None:
        """Read ``current.json`` to discover the previous release ID.

        Returns ``None`` on bootstrap (first sweep — ``current.json``
        does not exist).  Must be called before Phase 2.

        Returns
        -------
        str | None
            Previous release ID string, or ``None``.
        """
        try:
            response = self._s3.get_object(
                Bucket=self._bucket,
                Key=_POINTER_KEY,
            )
            body = json.loads(response["Body"].read())
            self._previous_release_id = body.get("release_id")
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code == "NoSuchKey":
                _logger.info(
                    "No current.json found — bootstrap sweep",
                    extra={"phase": "publication"},
                )
                self._previous_release_id = None
            else:
                raise

        self._pointer_read = True

        _logger.info(
            "Previous pointer resolved",
            extra={
                "previous_release_id": self._previous_release_id,
                "phase": "publication",
            },
        )
        return self._previous_release_id

    # ------------------------------------------------------------------
    # Phase 1 — Write swept nova artifacts (§12.5)
    # ------------------------------------------------------------------

    def _nova_key(self, nova_id: str, filename: str) -> str:
        """Build the S3 key for a per-nova artifact in this release."""
        return f"releases/{self._release_id}/nova/{nova_id}/{filename}"

    def upload_json_artifact(
        self,
        nova_id: str,
        filename: str,
        data: dict[str, Any],
    ) -> None:
        """Write a JSON artifact to the release prefix (Phase 1).

        Raises on S3 failure — the caller catches and records the nova
        as failed (§12.9).
        """
        key = self._nova_key(nova_id, filename)
        self._s3.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=_serialize_json(data),
            ContentType="application/json",
        )
        _logger.info(
            "Uploaded JSON artifact",
            extra={"nova_id": nova_id, "artifact": filename, "s3_key": key},
        )

    def upload_svg_artifact(
        self,
        nova_id: str,
        filename: str,
        svg: str,
    ) -> None:
        """Write an SVG artifact to the release prefix (Phase 1)."""
        key = self._nova_key(nova_id, filename)
        self._s3.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=svg.encode("utf-8"),
            ContentType="image/svg+xml",
        )
        _logger.info(
            "Uploaded SVG artifact",
            extra={"nova_id": nova_id, "artifact": filename, "s3_key": key},
        )

    def upload_bundle_artifact(
        self,
        nova_id: str,
        filename: str,
        body: bytes,
    ) -> None:
        """Write a bundle ZIP to the release prefix (Phase 1).

        Sets ``Content-Disposition: attachment`` so browsers trigger a
        download dialog with the human-readable filename (§12.5).
        """
        key = self._nova_key(nova_id, filename)
        self._s3.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=body,
            ContentType="application/zip",
            ContentDisposition=f'attachment; filename="{filename}"',
        )
        _logger.info(
            "Uploaded bundle artifact",
            extra={"nova_id": nova_id, "artifact": filename, "s3_key": key},
        )

    # ------------------------------------------------------------------
    # Phase 1.5 — Copy forward missing artifacts for swept novae
    # ------------------------------------------------------------------

    def copy_forward_missing_artifacts(
        self,
        nova_id: str,
        generated_artifacts: set[str],
    ) -> int:
        """Copy artifacts from the previous release that were not regenerated.

        After a swept nova's manifest-specified artifacts are generated,
        this method copies any *other* artifacts that exist in the
        previous release but were not part of the current manifest.

        Parameters
        ----------
        nova_id
            The nova whose missing artifacts should be copied forward.
        generated_artifacts
            Set of artifact filenames that were freshly generated in
            this sweep (e.g. ``{"photometry.json", "nova.json"}``).
            These will be skipped — they already exist in the new release.

        Returns
        -------
        int
            Number of artifacts copied forward.
        """
        if self._previous_release_id is None:
            return 0

        source_prefix = f"releases/{self._previous_release_id}/nova/{nova_id}/"
        target_prefix = f"releases/{self._release_id}/nova/{nova_id}/"

        paginator = self._s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self._bucket, Prefix=source_prefix)

        copied = 0
        skipped = 0
        for page in pages:
            for obj in page.get("Contents", []):
                source_key: str = obj["Key"]
                filename = source_key[len(source_prefix) :]
                if not filename:
                    continue

                if filename in generated_artifacts:
                    skipped += 1
                    _logger.debug(
                        "Skipping already-generated artifact",
                        extra={
                            "nova_id": nova_id,
                            "artifact": filename,
                            "phase": "copy_forward_missing",
                        },
                    )
                    continue

                target_key = target_prefix + filename
                self._s3.copy_object(
                    Bucket=self._bucket,
                    CopySource={"Bucket": self._bucket, "Key": source_key},
                    Key=target_key,
                )
                copied += 1
                _logger.debug(
                    "Copied forward missing artifact",
                    extra={
                        "nova_id": nova_id,
                        "artifact": filename,
                        "phase": "copy_forward_missing",
                    },
                )

        _logger.info(
            "Copy-forward missing artifacts complete",
            extra={
                "nova_id": nova_id,
                "copied": copied,
                "skipped": skipped,
                "phase": "copy_forward_missing",
            },
        )
        return copied

    # ------------------------------------------------------------------
    # Phase 2 — Copy forward unchanged novae (§12.5)
    # ------------------------------------------------------------------

    def copy_forward_unchanged_novae(
        self,
        nova_ids_to_copy: set[str],
    ) -> bool:
        """Copy artifacts for non-swept ACTIVE novae from previous release.

        For each nova in *nova_ids_to_copy*, lists all objects under the
        nova's prefix in the previous release and copies each to the new
        release prefix.

        On bootstrap (no previous release), this is a no-op.

        Parameters
        ----------
        nova_ids_to_copy
            Set of nova IDs whose artifacts should be copied forward.
            Typically: all ACTIVE nova IDs minus the swept nova IDs.

        Returns
        -------
        bool
            ``True`` if all copies succeeded (or nothing to copy).
            ``False`` if any copy failed — the pointer must NOT be
            updated (§12.9).
        """
        if not self._pointer_read:
            raise RuntimeError("read_previous_pointer() must be called before copy_forward")

        if self._previous_release_id is None:
            _logger.info(
                "Bootstrap sweep — skipping Phase 2 copy-forward",
                extra={"phase": "publication"},
            )
            return True

        if not nova_ids_to_copy:
            _logger.info(
                "No unchanged novae to copy forward",
                extra={"phase": "publication"},
            )
            return True

        _logger.info(
            "Phase 2: copying forward unchanged novae",
            extra={
                "novae_to_copy": len(nova_ids_to_copy),
                "previous_release": self._previous_release_id,
                "phase": "publication",
            },
        )

        for nova_id in sorted(nova_ids_to_copy):
            try:
                self._copy_nova_artifacts(nova_id)
            except Exception:
                _logger.exception(
                    "Copy-forward failed for nova",
                    extra={"nova_id": nova_id, "phase": "publication"},
                )
                self._copy_failures.append(nova_id)

        if self._copy_failures:
            _logger.error(
                "Phase 2 copy-forward had failures — pointer will NOT be updated",
                extra={
                    "failed_novae": len(self._copy_failures),
                    "phase": "publication",
                },
            )
            return False

        _logger.info(
            "Phase 2 copy-forward complete",
            extra={
                "novae_copied": len(nova_ids_to_copy),
                "phase": "publication",
            },
        )
        return True

    def _copy_nova_artifacts(self, nova_id: str) -> None:
        """Copy all artifacts for a single nova from previous → new release.

        Lists keys under the nova's prefix in the previous release and
        issues ``copy_object`` for each.
        """
        source_prefix = f"releases/{self._previous_release_id}/nova/{nova_id}/"
        target_prefix = f"releases/{self._release_id}/nova/{nova_id}/"

        # Paginated listing — unlikely to exceed one page per nova, but
        # correct by construction.
        paginator = self._s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=self._bucket,
            Prefix=source_prefix,
        )

        copied = 0
        for page in pages:
            for obj in page.get("Contents", []):
                source_key: str = obj["Key"]
                # Derive the artifact filename from the source key.
                relative = source_key[len(source_prefix) :]
                if not relative:
                    continue  # Skip the prefix placeholder itself.
                target_key = target_prefix + relative

                self._s3.copy_object(
                    Bucket=self._bucket,
                    CopySource={"Bucket": self._bucket, "Key": source_key},
                    Key=target_key,
                )
                copied += 1

        _logger.info(
            "Copied nova artifacts",
            extra={
                "nova_id": nova_id,
                "artifacts_copied": copied,
                "phase": "publication",
            },
        )

    # ------------------------------------------------------------------
    # Phase 3 — Write catalog.json (§12.5)
    # ------------------------------------------------------------------

    def write_catalog(self, catalog_data: dict[str, Any]) -> None:
        """Write ``catalog.json`` to the new release prefix (Phase 3).

        This is the last artifact written before the pointer update.
        """
        key = f"releases/{self._release_id}/catalog.json"
        self._s3.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=_serialize_json(catalog_data),
            ContentType="application/json",
        )
        _logger.info(
            "Wrote catalog.json",
            extra={"s3_key": key, "phase": "publication"},
        )

    # ------------------------------------------------------------------
    # Phase 4 — Update pointer (§12.5)
    # ------------------------------------------------------------------

    def update_pointer(self) -> None:
        """Update ``current.json`` to point to this release (Phase 4).

        This is the atomic switchover — the single operation that makes
        the new release visible to users.  Must only be called after
        Phases 1–3 complete successfully and :attr:`has_copy_failures`
        is ``False``.

        Raises on S3 failure — the caller logs and exits with failure
        status (§12.9).
        """
        if self._copy_failures:
            raise RuntimeError(
                "Cannot update pointer: Phase 2 had copy failures "
                f"({len(self._copy_failures)} novae). "
                "Previous release remains active."
            )

        pointer = {
            "release_id": self._release_id,
            "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        self._s3.put_object(
            Bucket=self._bucket,
            Key=_POINTER_KEY,
            Body=json.dumps(pointer).encode("utf-8"),
            ContentType="application/json",
        )
        _logger.info(
            "Updated current.json pointer",
            extra={
                "release_id": self._release_id,
                "phase": "publication",
            },
        )
