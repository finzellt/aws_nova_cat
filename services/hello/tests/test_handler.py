from services.hello.app import lambda_handler


def test_handler_default() -> None:
    assert lambda_handler({}, None)["message"] == "hello world"


def test_handler_name() -> None:
    assert lambda_handler({"name": "Ada"}, None)["message"] == "hello Ada"
