#!/usr/bin/env python3
import aws_cdk as cdk
from nova_cat_stack import NovaCatStack

app = cdk.App()

NovaCatStack(
    app,
    "NovaCatEpic5",
    # set env explicitly if you prefer:
    # env=cdk.Environment(account="123456789012", region="us-east-1"),
)

app.synth()
