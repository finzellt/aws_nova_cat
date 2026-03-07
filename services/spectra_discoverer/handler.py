"""
spectra_discoverer — Lambda handler

Description: Provider adapter dispatch, data_product_id assignment, DataProduct stub persistence
Workflows:   discover_spectra_products
Tasks:       QueryProviderForProducts, NormalizeProviderProducts,
             DeduplicateAndAssignDataProductIds, PersistDataProductMetadata

Step Functions passes a `task_name` field in the event payload so this single
Lambda serves multiple state machine task states.

This handler is intentionally thin. All provider-specific logic lives in
the adapters package:
    adapters/base.py     — SpectraDiscoveryAdapter Protocol (the contract)
    adapters/eso.py      — ESO SSAP implementation
    adapters/__init__.py — registry: provider string → adapter instance

Adding a new provider requires no changes to this file. See adapters/__init__.py.

data_product_id derivation (see ADR-003):
    NATIVE_ID    (preferred): uuid5(NAMESPACE, f"{provider}:{provider_product_key}")
    METADATA_KEY (fallback):  uuid5(NAMESPACE, f"{provider}:{locator_identity}")
    WEAK:                     uuid4() assigned; deferred to byte-fingerprint
                              resolution in acquire_and_validate_spectra.

    _DATA_PRODUCT_ID_NAMESPACE is a fixed UUID5 namespace seed.
    IT MUST NEVER CHANGE — any change invalidates all previously minted IDs.
    See ADR-003 for the full specification.

Environment variables (injected by CDK):
    NOVA_CAT_TABLE_NAME           — DynamoDB table name
    NOVA_CAT_PRIVATE_BUCKET       — private data S3 bucket name
    NOVA_CAT_PUBLIC_SITE_BUCKET   — public site S3 bucket name
    NOVA_CAT_QUARANTINE_TOPIC_ARN — quarantine notifications SNS topic ARN
    LOG_LEVEL                     — logging level (default INFO)
    POWERTOOLS_SERVICE_NAME       — AWS Lambda Powertools service name
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import boto3
from adapters import _PROVIDER_ADAPTERS, SpectraDiscoveryAdapter  # type: ignore[import-not-found]
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError
from nova_common.errors import RetryableError
from nova_common.logging import configure_logging, logger
from nova_common.tracing import tracer

_TABLE_NAME = os.environ["NOVA_CAT_TABLE_NAME"]
_SCHEMA_VERSION = "1"

# Fixed UUID5 namespace for deterministic data_product_id derivation.
# Value = uuid.NAMESPACE_URL. MUST NEVER CHANGE. See ADR-003.
_DATA_PRODUCT_ID_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

_dynamodb = boto3.resource("dynamodb")
_table = _dynamodb.Table(_TABLE_NAME)


# ---------------------------------------------------------------------------
# Startup validation
# ---------------------------------------------------------------------------


def _validate_adapters() -> None:
    """
    Verify all registered adapters satisfy the SpectraDiscoveryAdapter Protocol.
    Called once at module load time — catches misconfigured adapters before
    the first live invocation rather than at query time.
    """
    for provider, adapter in _PROVIDER_ADAPTERS.items():
        if not isinstance(adapter, SpectraDiscoveryAdapter):
            raise TypeError(
                f"Adapter registered for provider {provider!r} does not satisfy "
                f"SpectraDiscoveryAdapter Protocol: {type(adapter)}"
            )


_validate_adapters()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


@tracer.capture_lambda_handler
def handle(event: dict[str, Any], context: object) -> dict[str, Any]:
    configure_logging(event)
    task_name = event.get("task_name")
    if not task_name:
        raise ValueError("Missing required field: task_name")
    handler_fn = _TASK_HANDLERS.get(task_name)
    if handler_fn is None:
        raise ValueError(f"Unknown task_name: {task_name!r}. Known tasks: {list(_TASK_HANDLERS)}")
    logger.info(
        "Dispatching task",
        extra={
            "task_name": task_name,
            "correlation_id": event.get("correlation_id"),
            "nova_id": event.get("nova_id"),
        },
    )
    return handler_fn(event, context)


# ---------------------------------------------------------------------------
# Task: QueryProviderForProducts
# ---------------------------------------------------------------------------


@tracer.capture_method
def _handle_query_provider_for_products(event: dict[str, Any], context: object) -> dict[str, Any]:
    """
    Fetch nova coordinates from DynamoDB, then delegate to the provider
    adapter to query the archive.

    Raises ValueError (terminal) for missing nova or missing coordinates.
    Raises RetryableError for transient DynamoDB or provider failures.

    Returns:
        raw_products — list of provider-native record dicts (JSON-safe)
    """
    provider: str = event["provider"]
    nova_id: str = event["nova_id"]

    adapter = _resolve_adapter(provider)

    try:
        response = _table.get_item(Key={"PK": nova_id, "SK": "NOVA"})
    except ClientError as exc:
        raise RetryableError(f"DynamoDB get_item failed fetching Nova {nova_id!r}: {exc}") from exc

    nova_item = response.get("Item")
    if not nova_item:
        raise ValueError(f"Nova not found in DynamoDB: nova_id={nova_id!r}")
    ra_deg_raw = nova_item.get("ra_deg")
    dec_deg_raw = nova_item.get("dec_deg")
    if ra_deg_raw is None or dec_deg_raw is None:
        raise ValueError(
            f"Nova {nova_id!r} is missing coordinates — cannot query positional archive."
        )

    ra_deg = float(str(ra_deg_raw))
    dec_deg = float(str(dec_deg_raw))

    logger.info(
        "Querying provider for spectra products",
        extra={"provider": provider, "nova_id": nova_id, "ra_deg": ra_deg, "dec_deg": dec_deg},
    )

    raw_products = adapter.query(nova_id=nova_id, ra_deg=ra_deg, dec_deg=dec_deg)

    logger.info(
        "Provider query complete",
        extra={"provider": provider, "nova_id": nova_id, "raw_count": len(raw_products)},
    )
    return {"raw_products": raw_products}


# ---------------------------------------------------------------------------
# Task: NormalizeProviderProducts
# ---------------------------------------------------------------------------


@tracer.capture_method
def _handle_normalize_provider_products(event: dict[str, Any], context: object) -> dict[str, Any]:
    """
    Delegate normalization of each raw record to the provider adapter.

    Malformed records (adapter returns None) are dropped with a warning.
    The task does not fail for individual skipped records — this is
    intentional per the workflow spec's item-level quarantine policy.

    Returns:
        normalized_products — list of normalized product dicts
    """
    provider: str = event["provider"]
    nova_id: str = event["nova_id"]
    raw_products: list[dict[str, Any]] = event["raw_products"]

    adapter = _resolve_adapter(provider)

    normalized: list[dict[str, Any]] = []
    skipped = 0
    for raw in raw_products:
        result = adapter.normalize(nova_id=nova_id, raw=raw)
        if result is None:
            skipped += 1
        else:
            normalized.append(result)

    if skipped:
        logger.warning(
            "Skipped malformed provider records during normalization",
            extra={"provider": provider, "nova_id": nova_id, "skipped": skipped},
        )
    logger.info(
        "Normalization complete",
        extra={
            "provider": provider,
            "nova_id": nova_id,
            "normalized": len(normalized),
            "skipped": skipped,
        },
    )
    return {"normalized_products": normalized}


# ---------------------------------------------------------------------------
# Task: DeduplicateAndAssignDataProductIds
# ---------------------------------------------------------------------------


@tracer.capture_method
def _handle_deduplicate_and_assign_data_product_ids(
    event: dict[str, Any], context: object
) -> dict[str, Any]:
    """
    For each normalized product:
      1. Check LocatorAlias for an existing data_product_id (dedup authority).
      2. If found: use the existing ID; check if product is already VALID
         (flag skip_acquisition=True if so).
      3. If not found: derive a deterministic UUID via uuid5 (or uuid4 for WEAK).

    Identity strategies (per ADR-003):
        NATIVE_ID    — uuid5(NAMESPACE, f"{provider}:{provider_product_key}")
        METADATA_KEY — uuid5(NAMESPACE, f"{provider}:{locator_identity}")
        WEAK         — uuid4(); deferred to byte-fingerprint resolution

    Returns:
        products_with_ids — normalized products enriched with data_product_id,
                            is_new, and skip_acquisition flags
    """
    provider: str = event["provider"]
    nova_id: str = event["nova_id"]
    normalized_products: list[dict[str, Any]] = event["normalized_products"]

    products_with_ids: list[dict[str, Any]] = []

    for product in normalized_products:
        locator_identity: str = product["locator_identity"]
        identity_strategy: str = product["identity_strategy"]

        # Step 1: check LocatorAlias — the deduplication authority.
        existing_id = _lookup_locator_alias(provider=provider, locator_identity=locator_identity)

        if existing_id:
            data_product_id = existing_id
            is_new = False
            skip_acquisition = _is_product_valid(
                nova_id=nova_id, provider=provider, data_product_id=data_product_id
            )
            logger.info(
                "Resolved existing data_product_id from LocatorAlias",
                extra={
                    "data_product_id": data_product_id,
                    "skip_acquisition": skip_acquisition,
                },
            )
        else:
            is_new = True
            skip_acquisition = False

            if identity_strategy == "NATIVE_ID":
                key_material = f"{provider}:{product['provider_product_key']}"
                data_product_id = str(uuid.uuid5(_DATA_PRODUCT_ID_NAMESPACE, key_material))
            elif identity_strategy == "METADATA_KEY":
                key_material = f"{provider}:{locator_identity}"
                data_product_id = str(uuid.uuid5(_DATA_PRODUCT_ID_NAMESPACE, key_material))
            else:
                # WEAK — cannot construct a stable key.
                data_product_id = str(uuid.uuid4())
                logger.warning(
                    "WEAK identity strategy — data_product_id is not stable across runs",
                    extra={"provider": provider, "locator_identity": locator_identity},
                )

            logger.info(
                "Minted new data_product_id",
                extra={
                    "data_product_id": data_product_id,
                    "identity_strategy": identity_strategy,
                },
            )

        products_with_ids.append(
            {
                **product,
                "data_product_id": data_product_id,
                "is_new": is_new,
                "skip_acquisition": skip_acquisition,
            }
        )

    logger.info(
        "DeduplicateAndAssign complete",
        extra={
            "provider": provider,
            "total": len(products_with_ids),
            "new": sum(1 for p in products_with_ids if p["is_new"]),
            "already_valid_skipped": sum(1 for p in products_with_ids if p["skip_acquisition"]),
        },
    )
    return {"products_with_ids": products_with_ids}


# ---------------------------------------------------------------------------
# Task: PersistDataProductMetadata
# ---------------------------------------------------------------------------


@tracer.capture_method
def _handle_persist_data_product_metadata(event: dict[str, Any], context: object) -> dict[str, Any]:
    """
    For each product with an assigned data_product_id:

      - skip_acquisition=True (already VALID): log and skip entirely.
      - is_new=True: write LocatorAlias + insert DataProduct stub.
      - is_new=False, not VALID: write LocatorAlias only (additional locator
        alias rule from workflow spec). Preserves existing acquisition state.

    DataProduct stub initial state:
        acquisition_status = STUB
        validation_status  = UNVALIDATED
        eligibility        = ACQUIRE
        attempt_count      = 0
        GSI1 attributes written → product appears in EligibilityIndex

    Returns:
        persisted_products — newly stubbed products eligible for acquisition,
                             fed to PublishAcquireAndValidateSpectraRequests
    """
    provider: str = event["provider"]
    nova_id: str = event["nova_id"]
    products_with_ids: list[dict[str, Any]] = event["products_with_ids"]

    now = _now()
    persisted_products: list[dict[str, Any]] = []

    for product in products_with_ids:
        data_product_id: str = product["data_product_id"]
        locator_identity: str = product["locator_identity"]
        is_new: bool = product["is_new"]
        skip_acquisition: bool = product["skip_acquisition"]

        if skip_acquisition:
            logger.info(
                "Skipping already-VALID product — no stub write, no acquisition event",
                extra={"data_product_id": data_product_id},
            )
            continue

        # Always attempt LocatorAlias write — conditional put; first writer wins.
        _write_locator_alias(
            provider=provider,
            locator_identity=locator_identity,
            data_product_id=data_product_id,
            nova_id=nova_id,
            now=now,
        )

        if is_new:
            _insert_data_product_stub(
                nova_id=nova_id,
                provider=provider,
                data_product_id=data_product_id,
                locator_identity=locator_identity,
                locators=product["locators"],
                hints=product.get("hints", {}),
                identity_strategy=product["identity_strategy"],
                provider_product_key=product.get("provider_product_key"),
                now=now,
            )
            persisted_products.append(
                {
                    "data_product_id": data_product_id,
                    "provider": provider,
                    "nova_id": nova_id,
                }
            )
            logger.info(
                "DataProduct stub persisted",
                extra={"data_product_id": data_product_id, "provider": provider},
            )
        else:
            logger.info(
                "Existing non-VALID product — LocatorAlias ensured, no stub re-write",
                extra={"data_product_id": data_product_id},
            )

    logger.info(
        "PersistDataProductMetadata complete",
        extra={
            "nova_id": nova_id,
            "provider": provider,
            "newly_persisted": len(persisted_products),
        },
    )
    return {"persisted_products": persisted_products}


# ---------------------------------------------------------------------------
# DynamoDB helpers
# ---------------------------------------------------------------------------


def _lookup_locator_alias(*, provider: str, locator_identity: str) -> str | None:
    """
    Query the LocatorAlias partition for an existing data_product_id.

    PK = "LOCATOR#<provider>#<locator_identity>"
    A given provider+locator_identity maps to exactly one product by design.
    Limit=1 is correct. Raises RetryableError on DynamoDB transient failures.
    """
    pk = f"LOCATOR#{provider}#{locator_identity}"
    try:
        response = _table.query(
            KeyConditionExpression=Key("PK").eq(pk),
            Limit=1,
        )
        items = response.get("Items", [])
        return str(items[0]["data_product_id"]) if items else None
    except ClientError as exc:
        raise RetryableError(f"DynamoDB query failed for LocatorAlias pk={pk!r}: {exc}") from exc


def _is_product_valid(*, nova_id: str, provider: str, data_product_id: str) -> bool:
    """
    Return True if the DataProduct already has validation_status == 'VALID'.
    Raises RetryableError on DynamoDB transient failures.
    """
    try:
        response = _table.get_item(
            Key={
                "PK": nova_id,
                "SK": f"PRODUCT#SPECTRA#{provider}#{data_product_id}",
            },
            ProjectionExpression="validation_status",
        )
        item = response.get("Item")
        return bool(item and item.get("validation_status") == "VALID")
    except ClientError as exc:
        raise RetryableError(
            f"DynamoDB get_item failed checking validation_status "
            f"for DataProduct {data_product_id!r}: {exc}"
        ) from exc


def _write_locator_alias(
    *,
    provider: str,
    locator_identity: str,
    data_product_id: str,
    nova_id: str,
    now: str,
) -> None:
    """
    Write a LocatorAlias item with attribute_not_exists(PK) condition.

    First writer wins — concurrent or repeated calls for the same alias
    are silent no-ops. Raises RetryableError on DynamoDB transient failures.
    """
    pk = f"LOCATOR#{provider}#{locator_identity}"
    sk = f"DATA_PRODUCT#{data_product_id}"
    try:
        _table.put_item(
            Item={
                "PK": pk,
                "SK": sk,
                "entity_type": "LocatorAlias",
                "schema_version": _SCHEMA_VERSION,
                "provider": provider,
                "locator_identity": locator_identity,
                "data_product_id": data_product_id,
                "nova_id": nova_id,
                "created_at": now,
                "updated_at": now,
            },
            ConditionExpression=Attr("PK").not_exists(),
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return  # Already exists — idempotent no-op.
        raise RetryableError(f"DynamoDB put_item failed for LocatorAlias pk={pk!r}: {exc}") from exc


def _insert_data_product_stub(
    *,
    nova_id: str,
    provider: str,
    data_product_id: str,
    locator_identity: str,
    locators: list[dict[str, Any]],
    hints: dict[str, Any],
    identity_strategy: str,
    provider_product_key: str | None,
    now: str,
) -> None:
    """
    Insert a new DataProduct stub with attribute_not_exists(PK) condition.

    Idempotent — if the stub already exists from a prior run, the conditional
    write is a silent no-op that preserves any existing acquisition state.

    GSI1 attributes are written at insert time so the product immediately
    appears in the EligibilityIndex with eligibility=ACQUIRE.
    Raises RetryableError on DynamoDB transient failures.
    """
    sk = f"PRODUCT#SPECTRA#{provider}#{data_product_id}"
    gsi1_sk = f"ELIG#ACQUIRE#SPECTRA#{provider}#{data_product_id}"

    item: dict[str, Any] = {
        "PK": nova_id,
        "SK": sk,
        "entity_type": "DataProduct",
        "schema_version": _SCHEMA_VERSION,
        "data_product_id": data_product_id,
        "nova_id": nova_id,
        "product_type": "SPECTRA",
        "provider": provider,
        "locator_identity": locator_identity,
        "locators": locators,
        "hints": hints,
        "identity_strategy": identity_strategy,
        "acquisition_status": "STUB",
        "validation_status": "UNVALIDATED",
        "eligibility": "ACQUIRE",
        "attempt_count": 0,
        "GSI1PK": nova_id,
        "GSI1SK": gsi1_sk,
        "created_at": now,
        "updated_at": now,
    }

    if provider_product_key:
        item["provider_product_key"] = provider_product_key

    try:
        _table.put_item(
            Item=item,
            ConditionExpression=Attr("PK").not_exists(),
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.info(
                "DataProduct stub already exists — skipping insert (idempotent)",
                extra={"data_product_id": data_product_id},
            )
            return
        raise RetryableError(
            f"DynamoDB put_item failed for DataProduct stub {data_product_id!r}: {exc}"
        ) from exc


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _resolve_adapter(provider: str) -> SpectraDiscoveryAdapter:
    """
    Look up the registered adapter for the given provider string.
    Raises ValueError (terminal) for unrecognised providers.
    """
    adapter = _PROVIDER_ADAPTERS.get(provider)
    if adapter is None:
        raise ValueError(
            f"No adapter registered for provider {provider!r}. "
            f"Known providers: {list(_PROVIDER_ADAPTERS)}"
        )
    return adapter


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

_TASK_HANDLERS: dict[str, Callable[[dict[str, Any], object], dict[str, Any]]] = {
    "QueryProviderForProducts": _handle_query_provider_for_products,
    "NormalizeProviderProducts": _handle_normalize_provider_products,
    "DeduplicateAndAssignDataProductIds": _handle_deduplicate_and_assign_data_product_ids,
    "PersistDataProductMetadata": _handle_persist_data_product_metadata,
}
