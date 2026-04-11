"""
adapters/base.py

Defines the SpectraDiscoveryAdapter Protocol — the explicit contract that all
provider adapters must satisfy.

Each adapter is responsible for exactly two things:
  1. query()     — ask a provider archive "what spectra exist for this nova?"
                   and return raw provider-native records
  2. normalize() — map one raw record into Nova Cat's internal
                   discovered-product shape

Everything else (deduplication, data_product_id assignment, DynamoDB writes,
downstream fan-out) is the responsibility of spectra_discoverer/handler.py.

Adding a new provider:
  1. Create services/spectra_discoverer/adapters/<provider_name>.py
  2. Implement SpectraDiscoveryAdapter
  3. Add the adapter instance to _PROVIDER_ADAPTERS in handler.py
  No changes to the ASL or to this file are required.

Normalized product shape (returned by normalize()):
  Required fields:
    provider              str   — must match adapter.provider
    nova_id               str   — passed through from the task event
    locator_identity      str   — normalized stable identity key:
                                  "provider_product_id:<id>" (preferred)
                                  "url:<normalized_url>" (fallback)
    identity_strategy     str   — "NATIVE_ID" | "METADATA_KEY" | "WEAK"
    locators              list  — [{"kind": "URL"|"S3"|"OTHER",
                                    "role": "PRIMARY"|"MIRROR",
                                    "value": "<url>"}]

  Optional fields:
    provider_product_key  str | None  — provider-native product ID,
                                        present when identity_strategy == "NATIVE_ID"
    hints                 dict        — provider metadata useful for downstream
                                        FITS profile selection, e.g.:
                                          collection, specrp, snr,
                                          t_min_mjd, t_max_mjd,
                                          em_min_m, em_max_m

  normalize() MUST return None for records that cannot be safely normalized
  (missing access URL, unparseable identity, etc.). The handler logs and
  skips None returns — it does NOT raise.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class SpectraDiscoveryAdapter(Protocol):
    """
    Protocol for provider-specific spectra discovery adapters.

    runtime_checkable enables isinstance() validation at handler startup,
    catching misconfigured adapters before the first live query rather than
    at query time.
    """

    provider: str
    """
    Provider identifier string. Must match the value used in:
      - PrepareProviderList in the ASL (discover_spectra_products.asl.json)
      - DataProduct.provider
      - LocatorAlias PK construction ("LOCATOR#<provider>#...")
      - DataProduct SK construction ("PRODUCT#SPECTRA#<provider>#...")
    Example: "ESO", "MAST"
    """

    def query(
        self,
        *,
        nova_id: str,
        ra_deg: float,
        dec_deg: float,
        primary_name: str | None = None,
        aliases: list[str] | None = None,
    ) -> list[dict]:
        """
        Query the provider archive for spectra products associated with
        the given sky position and/or target names.

        Args:
            nova_id:      Nova UUID string (for logging/tracing only — not sent
                          to the provider; the provider does not know nova_ids).
            ra_deg:       Right ascension in decimal degrees (ICRS).
            dec_deg:      Declination in decimal degrees (ICRS).
            primary_name: Nova primary name (e.g. "V339 Del"). Optional —
                          coordinate-based adapters (ESO) ignore this.
                          Name-based adapters (MAST) use it for query_object.
            aliases:      Additional known names / designations for the nova
                          (e.g. ["Nova Del 2013", "NOVADEL2013"]). Used as
                          fallback query terms when primary_name yields no
                          results. Optional — may be None or empty.

        Returns:
            List of raw provider-native record dicts. Each dict contains
            whatever fields the provider returned, in their original form.
            The list may be empty if no products are found.

        Raises:
            RetryableError: for transient network or service failures.
            ValueError:     for terminal failures (invalid coordinates, etc.).
        """
        ...

    def normalize(
        self,
        *,
        nova_id: str,
        raw: dict,
    ) -> dict | None:
        """
        Normalize one raw provider record into Nova Cat's internal
        discovered-product shape.

        Args:
            nova_id:  Nova UUID string (for logging/tracing).
            raw:      One raw record dict as returned by query().

        Returns:
            Normalized product dict conforming to the shape described in
            this module's docstring, or None if the record cannot be
            safely normalized (missing required fields, unparseable identity,
            etc.). Callers log and skip None — they do not raise.
        """
        ...
