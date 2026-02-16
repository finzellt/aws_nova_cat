#!/usr/bin/env python3
import aws_cdk as cdk

from infra.stacks.app_stack import AppStack

app = cdk.App()

AppStack(app, "ServerlessMonorepoApp")

app.synth()
