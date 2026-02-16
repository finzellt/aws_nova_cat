from __future__ import annotations

from pathlib import Path
from typing import Any

from aws_cdk import Duration, Stack
from aws_cdk import aws_lambda as _lambda
from constructs import Construct


class AppStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs: Any) -> None:
        super().__init__(scope, construct_id, **kwargs)

        service_path = Path(__file__).resolve().parents[2] / "services" / "hello"

        _lambda.Function(
            self,
            "HelloFunction",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="app.lambda_handler",
            code=_lambda.Code.from_asset(str(service_path)),
            timeout=Duration.seconds(10),
            memory_size=128,
        )
