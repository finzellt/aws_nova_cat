#!/usr/bin/env python3
"""
Nova Cat CDK Application Entry Point
"""

import aws_cdk as cdk
from nova_cat.nova_cat_stack import NovaCatStack

app = cdk.App()

NovaCatStack(
    app,
    "NovaCat",
    env=cdk.Environment(
        account=app.node.try_get_context("account"),
        region="us-east-1",
    ),
    description="Nova Cat: serverless platform for aggregating and publishing classical nova data",
)

# Smoke test stack — identical to NovaCat but with namespaced resources and
# always-DESTROY removal policy. Smoke tests run against this stack so that
# full table wipes never touch production data.
NovaCatStack(
    app,
    "NovaCatSmoke",
    env=cdk.Environment(
        account=app.node.try_get_context("account"),
        region="us-east-1",
    ),
    env_prefix="nova-cat-smoke",
    cf_prefix="NovaCatSmoke",
    description="Nova Cat smoke test stack — ephemeral, always DESTROY",
)

app.synth()
