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
import aws_cdk.aws_secretsmanager as secretsmanager
from constructs import Construct
from nova_constructs.compute import NovaCatCompute
from nova_constructs.storage import NovaCatStorage
from nova_constructs.workflows import NovaCatWorkflows


class NovaCatStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_prefix: str = "nova-cat",
        cf_prefix: str = "NovaCat",
        **kwargs: Any,
    ) -> None:
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
            env_prefix=env_prefix,
            cf_prefix=cf_prefix,
        )

        # ------------------------------------------------------------------
        # ADS API token — pre-created in Secrets Manager, not managed by CDK.
        # Created once per account:
        #   aws secretsmanager create-secret \
        #     --name ADSQueryToken \
        #     --secret-string '{"token":"<your_ads_token>"}'
        # ------------------------------------------------------------------
        ads_secret = secretsmanager.Secret.from_secret_name_v2(
            self, "AdsApiSecret", "ADSQueryToken"
        )

        # ------------------------------------------------------------------
        # Compute layer
        # ------------------------------------------------------------------
        self.compute = NovaCatCompute(
            self,
            "Compute",
            table=self.storage.table,
            photometry_table=self.storage.photometry_table,
            private_bucket=self.storage.private_bucket,
            public_site_bucket=self.storage.public_site_bucket,
            quarantine_topic=self.storage.quarantine_topic,
            ads_secret=ads_secret,
            env_prefix=env_prefix,
        )

        # ------------------------------------------------------------------
        # Workflows layer
        # ------------------------------------------------------------------
        self.workflows = NovaCatWorkflows(
            self,
            "Workflows",
            compute=self.compute,
            env_prefix=env_prefix,
            cf_prefix=cf_prefix,
        )

        # ------------------------------------------------------------------
        # Stack-level tags applied to all resources
        # ------------------------------------------------------------------
        cdk.Tags.of(self).add("Project", "NovaCat")
        cdk.Tags.of(self).add("ManagedBy", "CDK")
        cdk.Tags.of(self).add("Environment", "prod" if is_prod else "dev")
