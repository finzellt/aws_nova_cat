"""
Nova Cat CDK Stack

Single-stack deployment for MVP. Composes storage and compute constructs.
Stack is designed to be split into separate stacks (storage / compute / workflows)
in a future epic when Step Functions state machines are added.

Removal policy is environment-aware:
  - DESTROY for dev (safe to tear down and redeploy)
  - RETAIN for prod (never accidentally delete scientific data)

Usage:
  cdk deploy -c account=<AWS_ACCOUNT_ID>
  cdk deploy -c account=<AWS_ACCOUNT_ID> -c env=prod
"""

from __future__ import annotations

from typing import Any

import aws_cdk as cdk
from constructs import Construct
from constructs.compute import NovaCatCompute
from constructs.storage import NovaCatStorage


class NovaCatStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs: Any) -> None:
        super().__init__(scope, construct_id, **kwargs)

        is_prod = self.node.try_get_context("env") == "prod"
        removal_policy = cdk.RemovalPolicy.RETAIN if is_prod else cdk.RemovalPolicy.DESTROY

        # ------------------------------------------------------------------
        # Storage layer
        # ------------------------------------------------------------------
        self.storage = NovaCatStorage(
            self,
            "Storage",
            enable_pitr=is_prod,
            removal_policy=removal_policy,
        )

        # ------------------------------------------------------------------
        # Compute layer
        # ------------------------------------------------------------------
        self.compute = NovaCatCompute(
            self,
            "Compute",
            table=self.storage.table,
            private_bucket=self.storage.private_bucket,
            public_site_bucket=self.storage.public_site_bucket,
            quarantine_topic=self.storage.quarantine_topic,
            # services/ lives two levels up from infra/
            services_root="../../services",
        )

        # ------------------------------------------------------------------
        # Stack-level tags applied to all resources
        # ------------------------------------------------------------------
        cdk.Tags.of(self).add("Project", "NovaCat")
        cdk.Tags.of(self).add("ManagedBy", "CDK")
        cdk.Tags.of(self).add("Environment", "prod" if is_prod else "dev")
