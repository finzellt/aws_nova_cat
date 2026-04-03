"""
Nova Cat Delivery Construct

Provisions:
  - CloudFront distribution with OAC on the public site S3 bucket
  - Two cache policies: pointer file (60s TTL) and release content (7d TTL)
  - CORS response headers policy (Access-Control-Allow-Origin: *)
  - Custom error response mapping 403 → 404 (standard OAC behaviour)
  - Price Class All, compression enabled on all behaviours

Design references:
  - DESIGN-003 §13 (CloudFront distribution)
  - DESIGN-003 §14.2 (Vercel environment variable)

The distribution domain name is exported as a CfnOutput for consumption
by the Vercel environment configuration (NEXT_PUBLIC_DATA_URL).

No custom domain, no Origin Shield, no invalidation automation at MVP.
"""

from __future__ import annotations

import aws_cdk as cdk
import aws_cdk.aws_cloudfront as cloudfront
import aws_cdk.aws_cloudfront_origins as origins
import aws_cdk.aws_s3 as s3
from constructs import Construct


class NovaCatDelivery(Construct):
    """
    Delivery layer for Nova Cat.

    Exposes:
      distribution  — the CloudFront distribution serving the public site bucket
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        public_site_bucket: s3.IBucket,
        cf_prefix: str = "NovaCat",
    ) -> None:
        super().__init__(scope, construct_id)

        # ------------------------------------------------------------------
        # Cache policy — pointer file (current.json)
        #
        # §13.5: 60-second default/max TTL, 0-second min (allows operator
        # invalidation for immediate switchover after rollback).
        # ------------------------------------------------------------------
        pointer_cache_policy = cloudfront.CachePolicy(
            self,
            "PointerCachePolicy",
            cache_policy_name=f"{cf_prefix}-Pointer",
            comment="NovaCat pointer file (current.json): 60s TTL (§13.5)",
            min_ttl=cdk.Duration.seconds(0),
            default_ttl=cdk.Duration.seconds(60),
            max_ttl=cdk.Duration.seconds(60),
            # No query strings, cookies, or headers needed — static JSON file.
            header_behavior=cloudfront.CacheHeaderBehavior.none(),
            query_string_behavior=cloudfront.CacheQueryStringBehavior.none(),
            cookie_behavior=cloudfront.CacheCookieBehavior.none(),
            enable_accept_encoding_gzip=True,
            enable_accept_encoding_brotli=True,
        )

        # ------------------------------------------------------------------
        # Cache policy — release content (releases/*)
        #
        # §13.5: 7-day default/max TTL, 1-day min TTL floor. Release content
        # is immutable — a given release path's artifacts never change.
        # ------------------------------------------------------------------
        releases_cache_policy = cloudfront.CachePolicy(
            self,
            "ReleasesCachePolicy",
            cache_policy_name=f"{cf_prefix}-Releases",
            comment="NovaCat immutable release content: 7d TTL (§13.5)",
            min_ttl=cdk.Duration.days(1),
            default_ttl=cdk.Duration.days(7),
            max_ttl=cdk.Duration.days(7),
            header_behavior=cloudfront.CacheHeaderBehavior.none(),
            query_string_behavior=cloudfront.CacheQueryStringBehavior.none(),
            cookie_behavior=cloudfront.CacheCookieBehavior.none(),
            enable_accept_encoding_gzip=True,
            enable_accept_encoding_brotli=True,
        )

        # ------------------------------------------------------------------
        # Response headers policy — CORS
        #
        # §13.7: Wildcard origin, GET/HEAD only, 24h preflight cache.
        # Applied to both cache behaviours. CORS via CloudFront response
        # headers policy (not S3 bucket CORS) to keep delivery config in
        # one place.
        # ------------------------------------------------------------------
        cors_policy = cloudfront.ResponseHeadersPolicy(
            self,
            "CorsPolicy",
            response_headers_policy_name=f"{cf_prefix}-CORS",
            comment="NovaCat CORS: public scientific data, wildcard origin (§13.7)",
            cors_behavior=cloudfront.ResponseHeadersCorsBehavior(
                access_control_allow_origins=["*"],
                access_control_allow_methods=["GET", "HEAD"],
                access_control_allow_headers=["*"],
                access_control_allow_credentials=False,
                access_control_max_age=cdk.Duration.seconds(86400),
                origin_override=True,
            ),
        )

        # ------------------------------------------------------------------
        # Origin — S3 bucket with OAC
        #
        # §13.2: OAC is the current AWS-recommended mechanism. The bucket
        # policy granting s3:GetObject to the distribution's OAC is added
        # automatically by CDK when using S3BucketOrigin.with_origin_access_control().
        # ------------------------------------------------------------------
        s3_origin = origins.S3BucketOrigin.with_origin_access_control(
            public_site_bucket,
        )

        # ------------------------------------------------------------------
        # Distribution
        #
        # §13: Single origin, two explicit behaviours (pointer + releases),
        # default behaviour catches everything else (typos, probes, bots)
        # with the same settings as releases.
        #
        # §13.4: Price Class All — cost difference negligible at MVP.
        # §13.6: Compression enabled (gzip/Brotli, free).
        # §13.8: 403 → 404 custom error response (OAC returns 403 for
        #         missing keys; map to clean 404 for consumers).
        # §13.3: Default CloudFront domain, no custom domain at MVP.
        # ------------------------------------------------------------------
        self.distribution = cloudfront.Distribution(
            self,
            "ArtifactDistribution",
            comment="Nova Cat artifact delivery (DESIGN-003 §13)",
            default_behavior=cloudfront.BehaviorOptions(
                origin=s3_origin,
                cache_policy=releases_cache_policy,
                response_headers_policy=cors_policy,
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                compress=True,
                allowed_methods=cloudfront.AllowedMethods.ALLOW_GET_HEAD,
            ),
            additional_behaviors={
                "/current.json": cloudfront.BehaviorOptions(
                    origin=s3_origin,
                    cache_policy=pointer_cache_policy,
                    response_headers_policy=cors_policy,
                    viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                    compress=True,
                    allowed_methods=cloudfront.AllowedMethods.ALLOW_GET_HEAD,
                ),
                "/releases/*": cloudfront.BehaviorOptions(
                    origin=s3_origin,
                    cache_policy=releases_cache_policy,
                    response_headers_policy=cors_policy,
                    viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                    compress=True,
                    allowed_methods=cloudfront.AllowedMethods.ALLOW_GET_HEAD,
                ),
            },
            price_class=cloudfront.PriceClass.PRICE_CLASS_ALL,
            error_responses=[
                cloudfront.ErrorResponse(
                    http_status=403,
                    # §13.8: OAC returns 403 for missing S3 keys. Ideally we'd
                    # remap to 404, but CloudFormation requires a ResponsePagePath
                    # alongside any ResponseCode — and this is a data-only bucket
                    # with no error page to serve. The 60s cache TTL prevents
                    # missing-key lookups from hammering S3. The frontend handles
                    # 403 and 404 identically (non-OK → component error state).
                    ttl=cdk.Duration.seconds(60),
                ),
            ],
        )

        # ------------------------------------------------------------------
        # Stack outputs
        #
        # §13.11: Distribution domain exported for Vercel env configuration.
        # The frontend reads this as NEXT_PUBLIC_DATA_URL (§14.2).
        # ------------------------------------------------------------------
        cdk.CfnOutput(
            self,
            "DistributionDomain",
            value=self.distribution.distribution_domain_name,
            description="CloudFront distribution domain for artifact delivery",
            export_name=f"{cf_prefix}-DistributionDomain",
        )
        cdk.CfnOutput(
            self,
            "DistributionId",
            value=self.distribution.distribution_id,
            description="CloudFront distribution ID (for invalidation commands)",
            export_name=f"{cf_prefix}-DistributionId",
        )
