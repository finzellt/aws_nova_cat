"""
Nova Cat Storage Construct

Provisions:
  - Single DynamoDB table (NovaCat) with EligibilityIndex GSI
  - Private data S3 bucket (raw bytes, quarantine, derived artifacts, bundles)
  - Public site S3 bucket (static site releases)
  - Quarantine notifications SNS topic

Design decisions:
  - PAY_PER_REQUEST billing: expected dataset is small (<250 GB, <1000 novae).
    Cost-aware architecture; no need for provisioned capacity.
  - BEST_EFFORT PITR disabled by default (cost); enable in prod via parameter.
  - S3 versioning not relied upon for application semantics (per s3-layout.md),
    but enabled on private bucket as an operational safety net.
  - Public site bucket has static website hosting disabled at infra level;
    content is served via CloudFront (future) or direct S3 URLs for MVP.
  - Both buckets block all public access by default. Public site access
    will be granted via bucket policy to a CloudFront OAC (future epic).
  - SNS quarantine topic: one shared topic; workflow name + reason in message body
    allows subscribers to filter. Per-workflow topics deferred as unnecessary
    complexity at current scale.
"""

from __future__ import annotations

import aws_cdk as cdk
import aws_cdk.aws_dynamodb as dynamodb
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_sns as sns
from constructs import Construct


class NovaCatStorage(Construct):
    """
    Storage layer for Nova Cat.

    Exposes:
      table               — the single NovaCat DynamoDB table
      private_bucket      — nova-cat-private-data (raw bytes, derived, quarantine, bundles)
      public_site_bucket  — nova-cat-public-site (static site releases)
      quarantine_topic    — SNS topic for quarantine notifications (all workflows)
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        enable_pitr: bool = False,
        removal_policy: cdk.RemovalPolicy = cdk.RemovalPolicy.RETAIN,
    ) -> None:
        super().__init__(scope, construct_id)

        # ------------------------------------------------------------------
        # DynamoDB — single table, namespaced PK design
        #
        # Primary key:
        #   PK (String) — per-nova: "<nova_id>"
        #                  global:  "NAME#<normalized_name>"
        #                           "LOCATOR#<provider>#<locator_identity>"
        #   SK (String) — item type discriminator; see dynamodb-item-model.md
        #
        # GSI1 (EligibilityIndex):
        #   GSI1PK (String) = "<nova_id>"
        #   GSI1SK (String) = "ELIG#<eligibility>#SPECTRA#<provider>#<data_product_id>"
        #   Projection: ALL (eligibility queries need most product fields for cooldown
        #   enforcement; avoids extra reads on the hot acquisition path)
        #   Items that are no longer eligible have GSI1PK/GSI1SK set to null,
        #   which removes them from the index automatically.
        # ------------------------------------------------------------------
        self.table = dynamodb.Table(
            self,
            "NovaCatTable",
            table_name="NovaCat",
            partition_key=dynamodb.Attribute(
                name="PK",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="SK",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery=enable_pitr,
            removal_policy=removal_policy,
        )

        self.table.add_global_secondary_index(
            index_name="EligibilityIndex",
            partition_key=dynamodb.Attribute(
                name="GSI1PK",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="GSI1SK",
                type=dynamodb.AttributeType.STRING,
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # ------------------------------------------------------------------
        # S3 — Private data bucket
        #
        # Contains: raw spectra bytes, quarantine objects, derived artifacts,
        # per-nova bundles, optional workflow payload snapshots.
        # Never directly exposed to the public.
        #
        # Versioning enabled as an operational safety net (not relied upon
        # for application semantics per s3-layout.md). Raw bytes are logically
        # immutable; versioning protects against accidental overwrites during
        # re-acquisition before validation succeeds.
        #
        # Lifecycle rules:
        #   - quarantine/ prefix: expire after 365 days (human review window)
        #   - workflow-payloads/ prefix: expire after 30 days (transient debugging)
        # ------------------------------------------------------------------
        self.private_bucket = s3.Bucket(
            self,
            "PrivateDataBucket",
            bucket_name=None,  # CDK-generated name; avoids global naming conflicts
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=removal_policy,
            auto_delete_objects=removal_policy == cdk.RemovalPolicy.DESTROY,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="ExpireQuarantineObjects",
                    prefix="quarantine/",
                    expiration=cdk.Duration.days(365),
                    noncurrent_version_expiration=cdk.Duration.days(30),
                ),
                s3.LifecycleRule(
                    id="ExpireWorkflowPayloadSnapshots",
                    prefix="workflow-payloads/",
                    expiration=cdk.Duration.days(30),
                ),
            ],
        )

        # ------------------------------------------------------------------
        # S3 — Public site bucket
        #
        # Contains: immutable static site releases under releases/<release_id>/
        # All public access blocked at bucket level; a future CloudFront
        # distribution with OAC will be granted read access via bucket policy.
        # No versioning needed: releases are immutable by convention.
        # ------------------------------------------------------------------
        self.public_site_bucket = s3.Bucket(
            self,
            "PublicSiteBucket",
            bucket_name=None,  # CDK-generated name
            versioned=False,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=removal_policy,
            auto_delete_objects=removal_policy == cdk.RemovalPolicy.DESTROY,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="ExpireOldReleases",
                    prefix="releases/",
                    # Keep releases for 2 years; adjust as publication cadence matures
                    expiration=cdk.Duration.days(730),
                ),
            ],
        )

        # ------------------------------------------------------------------
        # SNS — Quarantine notifications topic
        #
        # Used by all workflows when a QUARANTINE outcome occurs.
        # Notifications are best-effort: Lambda catch blocks publish here
        # and swallow any SNS errors to avoid masking the real quarantine result.
        #
        # Message structure (enforced by Lambda convention, not SNS):
        #   workflow_name, nova_id / data_product_id, correlation_id,
        #   error_fingerprint, quarantine_reason_code
        # ------------------------------------------------------------------
        self.quarantine_topic = sns.Topic(
            self,
            "QuarantineNotificationsTopic",
            topic_name="nova-cat-quarantine-notifications",
            display_name="Nova Cat — Quarantine Notifications",
        )

        # ------------------------------------------------------------------
        # Stack outputs — makes physical names available after deploy
        # ------------------------------------------------------------------
        cdk.CfnOutput(
            self,
            "TableName",
            value=self.table.table_name,
            description="NovaCat DynamoDB table name",
            export_name="NovaCat-TableName",
        )
        cdk.CfnOutput(
            self,
            "PrivateBucketName",
            value=self.private_bucket.bucket_name,
            description="Nova Cat private data S3 bucket name",
            export_name="NovaCat-PrivateBucketName",
        )
        cdk.CfnOutput(
            self,
            "PublicSiteBucketName",
            value=self.public_site_bucket.bucket_name,
            description="Nova Cat public site S3 bucket name",
            export_name="NovaCat-PublicSiteBucketName",
        )
        cdk.CfnOutput(
            self,
            "QuarantineTopicArn",
            value=self.quarantine_topic.topic_arn,
            description="Nova Cat quarantine notifications SNS topic ARN",
            export_name="NovaCat-QuarantineTopicArn",
        )
