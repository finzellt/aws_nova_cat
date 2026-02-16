import aws_cdk as cdk
from aws_cdk.assertions import Template

from infra.stacks.app_stack import AppStack


def test_stack_synthesizes() -> None:
    app = cdk.App()
    stack = AppStack(app, "TestStack")
    template = Template.from_stack(stack)
    template.resource_count_is("AWS::Lambda::Function", 1)
