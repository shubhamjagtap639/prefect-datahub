from prefect import flow

from prefect_datahub.tasks import (
    goodbye_prefect_datahub,
    hello_prefect_datahub,
)


def test_hello_prefect_datahub():
    @flow
    def test_flow():
        return hello_prefect_datahub()

    result = test_flow()
    assert result == "Hello, prefect-datahub!"


def goodbye_hello_prefect_datahub():
    @flow
    def test_flow():
        return goodbye_prefect_datahub()

    result = test_flow()
    assert result == "Goodbye, prefect-datahub!"
