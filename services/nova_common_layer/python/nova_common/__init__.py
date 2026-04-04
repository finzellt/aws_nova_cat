"""
nova_common — shared utilities for all Nova Cat Lambda functions.

Provides a pre-configured Powertools Logger and Tracer, and a
configure_logging() helper that injects standard structured fields
(correlation_id, job_run_id, workflow_name, state_name) from the
Step Functions event payload into every subsequent log line.

Usage in a handler:

    from nova_common.logging import logger, configure_logging
    from nova_common.timing import log_duration
    from nova_common.tracing import tracer

    def handle(event, context):
        configure_logging(event)
        with log_duration("my_operation", key="value"):
            ...
"""
