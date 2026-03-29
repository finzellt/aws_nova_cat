"""file_io — S3-aware path resolution for Lambda handlers.

Lambda handlers receive file paths from Step Functions event payloads.  In
local / integration-test contexts these are real filesystem paths.  In the
deployed stack they are bare S3 keys; the handler resolves the correct
bucket from its environment and delegates to the helpers here.

Convention
----------
``ticket_path``  — bare S3 key of the ticket .txt file, e.g.
                   ``"raw/tickets/V4739_Sgr_Livingston_optical_Photometry.txt"``
``data_dir``     — bare S3 prefix of the data directory, e.g.
                   ``"raw/data/v4739_sgr/"``  (trailing slash optional)

Both fields also accept ordinary filesystem paths, which are returned
unchanged.  This dual-mode behaviour preserves full compatibility with the
integration test suite, which supplies real local paths.

Public API
----------
resolve_file(path_spec, *, s3_client, bucket) -> Path
    Resolve a single file to a local Path.

resolve_dir(dir_spec, *, s3_client, bucket) -> Path
    Resolve a directory (or S3 prefix) to a local directory Path whose
    contents have been populated by downloading every object directly under
    the prefix.

Caching
-------
Downloaded files are cached in ``/tmp`` for the lifetime of the Lambda
execution context.  A retry within the same warm container skips files that
are already present on disk.  The cache key is deterministic (MD5 of the
S3 key/prefix), so a given key always maps to the same local path.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any


def resolve_file(path_spec: str, *, s3_client: Any, bucket: str) -> Path:
    """Resolve a single file path spec to a local Path.

    If ``path_spec`` refers to an existing local file it is returned
    unchanged (integration-test path).  Otherwise it is treated as a bare
    S3 key and downloaded to ``/tmp/<cache_dir>/<filename>``.

    Parameters
    ----------
    path_spec:
        Filesystem path or bare S3 key.
    s3_client:
        Injected boto3 S3 client.  Passed by the caller so that unit tests
        can substitute a mock without patching module-level state here.
    bucket:
        Name of the S3 bucket to download from when ``path_spec`` is a key.

    Returns
    -------
    Path
        A locally accessible file path.

    Raises
    ------
    botocore.exceptions.ClientError
        Propagated unchanged from S3 on any AWS error (NoSuchKey, etc.).
    OSError
        If the local filesystem path exists but cannot be read.
    """
    local = Path(path_spec)
    if local.exists():
        return local

    # Treat path_spec as a bare S3 key.
    #
    # Use an MD5 hash of the full key as the cache directory so that two
    # different keys sharing the same basename don't collide in /tmp.
    cache_dir = Path("/tmp") / hashlib.md5(path_spec.encode()).hexdigest()
    cache_dir.mkdir(parents=True, exist_ok=True)

    dest = cache_dir / Path(path_spec).name
    if not dest.exists():
        s3_client.download_file(bucket, path_spec, str(dest))

    return dest


def resolve_dir(dir_spec: str, *, s3_client: Any, bucket: str) -> Path:
    """Resolve a directory path spec to a local directory Path.

    If ``dir_spec`` refers to an existing local directory it is returned
    unchanged.  Otherwise it is treated as a bare S3 prefix; every object
    whose key begins with the prefix and contains no further path separators
    (i.e. objects directly in the "directory", not in sub-prefixes) is
    downloaded into a stable ``/tmp`` subdirectory.

    Sub-prefix objects (keys of the form ``prefix/subdir/file``) are
    silently skipped — handlers are expected to be given the most specific
    prefix containing the files they need.

    Parameters
    ----------
    dir_spec:
        Local directory path or bare S3 prefix (trailing slash is optional;
        one will be appended if absent).
    s3_client:
        Injected boto3 S3 client.
    bucket:
        Name of the S3 bucket to download from.

    Returns
    -------
    Path
        A local directory containing all downloaded files.  The directory
        is guaranteed to exist even if the S3 prefix contained no objects.

    Raises
    ------
    botocore.exceptions.ClientError
        Propagated unchanged from S3 on any AWS error.
    """
    local = Path(dir_spec)
    if local.is_dir():
        return local

    # Normalise to a prefix with exactly one trailing slash.
    prefix = dir_spec.rstrip("/") + "/"

    # Stable cache directory: the same prefix always maps to the same /tmp
    # subdir across retries within a warm Lambda container.
    cache_dir = Path("/tmp") / hashlib.md5(prefix.encode()).hexdigest()
    cache_dir.mkdir(parents=True, exist_ok=True)

    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key: str = obj["Key"]

            # Strip the prefix to obtain the relative filename.
            relative = key[len(prefix):]

            # Skip the prefix placeholder object itself (empty relative part)
            # and any objects inside sub-prefixes (relative contains a slash).
            if not relative or "/" in relative:
                continue

            dest = cache_dir / relative
            if not dest.exists():
                s3_client.download_file(bucket, key, str(dest))

    return cache_dir
