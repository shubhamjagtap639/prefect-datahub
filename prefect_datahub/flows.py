"""This is an example flows module"""
from prefect import flow

from prefect_datahub.blocks import DatahubBlock
from prefect_datahub.tasks import (
    goodbye_prefect_datahub,
    hello_prefect_datahub,
)


@flow
def hello_and_goodbye():
    """
    Sample flow that says hello and goodbye!
    """
    DatahubBlock.seed_value_for_example()
    block = DatahubBlock.load("sample-block")

    print(hello_prefect_datahub())
    print(f"The block's value: {block.value}")
    print(goodbye_prefect_datahub())
    return "Done"


if __name__ == "__main__":
    hello_and_goodbye()
