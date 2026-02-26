from lambdas.task_router.handler import main


def test_router_rejects_missing_context() -> None:
    try:
        main({"input": {}}, None)
        raise AssertionError("expected KeyError")
    except KeyError:
        pass
