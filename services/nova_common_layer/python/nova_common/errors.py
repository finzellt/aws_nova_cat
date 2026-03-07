"""
nova_common.errors

Shared exception types for all Nova Cat Lambda functions.

These exceptions map directly to the error classification taxonomy in
execution-governance.md and are used by Step Functions ASL retry/catch
policies via ErrorEquals matching on the exception class name.

Step Functions captures the Python exception class __name__ (not the fully
qualified module path), so "RetryableError" in an ASL ErrorEquals clause
matches nova_common.errors.RetryableError correctly.

Error taxonomy (from execution-governance.md):

  RetryableError  — transient; Step Functions will retry with backoff
                    e.g. DynamoDB throttling, network timeouts, 5xx responses

  TerminalError   — unrecoverable; routes to TerminalFailHandler
                    e.g. schema mismatch, missing required fields,
                    internal invariant violations

  QuarantineError — requires human review; routes to QuarantineHandler
                    e.g. ambiguous resolver results, conflicting sources,
                    coordinate match in ambiguous band
"""

from __future__ import annotations


class RetryableError(Exception):
    """
    Transient error — Step Functions should retry with backoff.

    Raise when the failure is likely to resolve on its own:
      - DynamoDB ConditionalCheckFailedException (lock contention)
      - Network timeouts or connection errors
      - HTTP 429 / 503 from external services
      - AWS service throttling
    """


class TerminalError(Exception):
    """
    Unrecoverable error — routes to TerminalFailHandler then FinalizeJobRunFailed.

    Raise when retrying cannot possibly help:
      - Schema or event version mismatch
      - Missing or invalid required fields
      - Internal invariant violations
      - Resolver returned coordinates when none were expected (or vice versa)
    """


class QuarantineError(Exception):
    """
    Ambiguous or suspicious condition requiring human review.
    Routes to QuarantineHandler then FinalizeJobRunQuarantined.

    Raise when the data is not clearly wrong but cannot be safely processed:
      - Ambiguous archive resolver results
      - Conflicting authoritative sources
      - Coordinate match in the ambiguous 2"-10" band
    """
