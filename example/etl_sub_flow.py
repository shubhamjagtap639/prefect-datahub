from datahub_provider.entities import Dataset
from prefect import flow, task

from prefect_datahub import DatahubEmitter

# datahub_emitter = DatahubEmitter.load("datahub-emitter-block")
datahub_emitter = DatahubEmitter()


@task(name="Extract", description="Extract the actual data")
def extract():
    data = "This is data"
    datahub_emitter.emit_task()
    return data


@task(description="Transform the actual data")
def transform(actual_data):
    actual_data = actual_data.split(" ")
    datahub_emitter.emit_task(
        inputs=[Dataset("snowflake", "mydb.schema.tableA")],
        outputs=[Dataset("snowflake", "mydb.schema.tableC")],
    )
    return actual_data


@task(name="Load_task", description="Load the actual data")
def load(data):
    # datahub_emitter.emit_task()
    print(data)


@flow(log_prints=True)
def tl(data):
    print("Flow started")
    data = transform(data)
    load(data)
    datahub_emitter.emit_flow()
    print("")


@flow(log_prints=True)
def etl():
    print("Flow started")
    data = extract()
    tl(data)
    datahub_emitter.emit_flow()
    print("")


if __name__ == "__main__":
    etl()
    print("s")
