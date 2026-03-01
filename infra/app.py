#!/usr/bin/env python3
"""
Nova Cat CDK Application Entry Point
"""

import aws_cdk as cdk

from infra.nova_cat.nova_cat_stack import NovaCatStack

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

app.synth()
