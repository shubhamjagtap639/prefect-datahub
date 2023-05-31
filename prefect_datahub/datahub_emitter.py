"""Module for emit metadata to Datahub REST. """

import asyncio
from typing import List, Optional

from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.utilities.urns.data_flow_urn import DataFlowUrn
from datahub.utilities.urns.data_job_urn import DataJobUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub_provider.entities import _Entity
from prefect.blocks.core import Block
from prefect.client.cloud import get_cloud_client
from prefect.client.orchestration import get_client
from prefect.context import FlowRunContext, TaskRunContext
from pydantic import Field


class DatahubEmitter(Block):
    """
    Block used to emit prefect task and flow related metadata to Datahub REST

    Attributes:
        datahub_rest_url (str): The value to store.
        cluster (str): The value to store.
        capture_tags_info (boolean): The value to store.

    Example:
        Load a stored value:
        ```python
        from prefect_datahub import DatahubEmitter
        block = DatahubEmitter.load("BLOCK_NAME")
        ```
    """

    _block_type_name = "datahub emitter"
    # replace this with a relevant logo; defaults to Prefect logo
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/08yCE6xpJMX9Kjl5VArDS/c2ede674c20f90b9b6edeab71feffac9/prefect-200x200.png?h=250"  # noqa
    _documentation_url = "https://shubhamjagtap639.github.io/prefect-datahub/datahub_emitter/#prefect-datahub.datahub_emitter.DatahubEmitter"  # noqa

    datahub_rest_url: Optional[str] = Field(
        default="http://localhost:8080",
        title="Datahub rest url",
        description="Datahub gms rest url.",
    )

    cluster: Optional[str] = Field(
        default="prod",
        title="Cluster",
        description="Name of the prefect cluster.",
    )

    capture_tags_info: Optional[bool] = Field(
        default=True,
        title="Capture tags info",
        description="If true, the tags field of the task and flow will be captured as DataHub tags.",
    )

    graceful_exceptions: Optional[bool] = Field(
        default=True,
        title="Graceful Exceptions",
        description="If set to true, most runtime errors in the emit task or flow will be suppressed and will not cause the overall flow to fail. Note that configuration issues will still throw exceptions..",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.emitter = DatahubRestEmitter(gms_server=self.datahub_rest_url)
        self.emitter.test_connection()
        self.prefect_client = get_client()
        self.prefect_cloud_client = get_cloud_client()

    def _entities_to_urn_list(self, iolets: List[_Entity]) -> List[DatasetUrn]:
        return [DatasetUrn.create_from_string(let.urn) for let in iolets]

    def emit_task(self, inputs: List = None, outputs: List = None):
        flow_run_ctx = FlowRunContext.get()
        task_run_ctx = TaskRunContext.get()

        if flow_run_ctx is None or task_run_ctx is None:
            return

        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator="prefect", env=self.cluster, flow_id=flow_run_ctx.flow.name
        )

        datajob = DataJob(
            id=task_run_ctx.task.task_key,
            flow_urn=dataflow_urn,
            name=task_run_ctx.task.name,
        )
        datajob.description = task_run_ctx.task.description
        datajob.tags = task_run_ctx.task.tags
        if inputs is not None:
            datajob.inlets.extend(self._entities_to_urn_list(inputs))
        if outputs is not None:
            datajob.outlets.extend(self._entities_to_urn_list(outputs))

        # Add upstrem urns
        if task_run_ctx.task_run.task_inputs:
            task_run_key_map = {
                str(prefect_future.task_run.id): prefect_future.task_run.task_key
                for prefect_future in flow_run_ctx.task_run_futures
            }
            for key in task_run_ctx.task_run.task_inputs.keys():
                upstream_task_urn = DataJobUrn.create_from_ids(
                    data_flow_urn=str(dataflow_urn),
                    job_id=task_run_key_map[
                        str(task_run_ctx.task_run.task_inputs[key][0].id)
                    ],
                )
                datajob.upstream_urns.extend([upstream_task_urn])
        datajob.emit(self.emitter)

    def emit_flow(self):
        flow_run_ctx = FlowRunContext.get()

        if flow_run_ctx is None:
            return

        dataflow = DataFlow(
            cluster=self.cluster, id=flow_run_ctx.flow.name, orchestrator="prefect"
        )
        dataflow.description = flow_run_ctx.flow.description
        dataflow.emit(self.emitter)

        dpi = DataProcessInstance.from_dataflow(
            dataflow=dataflow, id=flow_run_ctx.flow_run.name
        )

        dpi.emit_process_start(
            emitter=self.emitter,
            start_timestamp_millis=int(
                flow_run_ctx.flow_run.start_time.timestamp() * 1000
            ),
        )

        dpi.emit_process_end(
            emitter=self.emitter,
            end_timestamp_millis=int(
                flow_run_ctx.flow_run.start_time.timestamp() * 1000
            )
            + 5000,
            result=InstanceRunResult.SUCCESS,
            result_type="prefect",
        )

        for prefect_future in flow_run_ctx.task_run_futures:
            task_run = asyncio.run(
                self.prefect_client.read_task_run(prefect_future.task_run.id)
            )
            datajob = DataJob(id=task_run.task_key, flow_urn=dataflow.urn)

            if task_run.state_name == "Completed":
                result = InstanceRunResult.SUCCESS
            elif task_run.state_name == "Failed":
                result = InstanceRunResult.FAILURE
            elif task_run.state_name == "Cancelled":
                result = InstanceRunResult.SKIPPED
            else:
                raise Exception(
                    f"Result should be either success or failure and it was {task_run.state_name}"
                )

            dpi = DataProcessInstance.from_datajob(
                datajob=datajob,
                id=f"{flow_run_ctx.flow_run.name}.{task_run.name}",
                clone_inlets=True,
                clone_outlets=True,
            )
            dpi.emit_process_start(
                emitter=self.emitter,
                start_timestamp_millis=int(task_run.start_time.timestamp() * 1000),
                emit_template=False,
            )
            dpi.emit_process_end(
                emitter=self.emitter,
                end_timestamp_millis=int(task_run.end_time.timestamp() * 1000),
                result=result,
                result_type="prefect",
            )
