"""Module for emit metadata to Datahub REST. """

from prefect.blocks.core import Block
from pydantic import Field
from typing import Dict, List, Tuple, Optional
import asyncio
from prefect.blocks.core import Block
from prefect.context import FlowRunContext, TaskRunContext
from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub_provider.entities import _Entity
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub_provider.entities import Dataset
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.utilities.urns.data_flow_urn import DataFlowUrn
from datahub.utilities.urns.data_job_urn import DataJobUrn
from prefect.client.orchestration import get_client


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
    _documentation_url = "https://GS lab.github.io/prefect-datahub/blocks/#prefect-datahub.blocks.DatahubBlock"  # noqa

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
        title="Capture tags infor",
        description="If true, the tags field of the task and flow will be captured as DataHub tags.",
    )

    @classmethod
    def seed_value_for_example(cls):
        """
        Seeds the field, value, so the block can be loaded.
        """
        block = cls(value="A sample value")
        block.save("sample-block", overwrite=True)
    
    def _get_config(self) -> Tuple[str, Optional[str], Optional[int]]:
        host = "http://localhost:8080"
        password = None
        timeout_sec = None
        return (host, password, timeout_sec)

    def make_emitter(self) -> "DatahubRestEmitter":
        import datahub.emitter.rest_emitter

        return datahub.emitter.rest_emitter.DatahubRestEmitter(*self._get_config())
    
    async def _get_flow_run_graph(self, flow_run_id):
        response = await get_client()._client.get(f"/flow_runs/{flow_run_id}/graph")
        return response.json()

    async def _get_task_run(self, task_run_id):
        return await get_client().read_task_run(task_run_id)
    
    def ingest_task(self, inputs: List = None, outputs: List = None):
    
        flow_run_ctx = FlowRunContext.get()
        task_run_ctx = TaskRunContext.get()
        
        emitter = self.make_emitter()

        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator="prefect", env='prod', flow_id=flow_run_ctx.flow.name
        )
        
        datajob = DataJob(id=task_run_ctx.task.task_key, flow_urn=dataflow_urn, name=task_run_ctx.task.name)
        datajob.description = task_run_ctx.task.description
        datajob.tags = task_run_ctx.task.tags
        if inputs is not None:
            datajob.inlets.extend(_entities_to_urn_list(inputs))
        if outputs is not None:
            datajob.outlets.extend(_entities_to_urn_list(outputs))

        if task_run_ctx.task_run.task_inputs:
            task_run_key_map = {str(prefect_future.task_run.id):prefect_future.task_run.task_key for prefect_future in flow_run_ctx.task_run_futures}
            for inputs in task_run_ctx.task_run.task_inputs['actual_data']:
                upstream_task_urn = DataJobUrn.create_from_ids(
                        data_flow_urn=str(dataflow_urn), job_id=task_run_key_map[str(inputs.id)]
                    )
                datajob.upstream_urns.extend([upstream_task_urn])
        datajob.emit(emitter)
    
    def ingest_flow(self):
        flow_run_ctx = FlowRunContext.get()

        emitter = self.make_emitter()

        dataflow = DataFlow(
                cluster='prod', id=flow_run_ctx.flow.name, orchestrator="prefect"
            )
        dataflow.description = flow_run_ctx.flow.description
        dataflow.emit(emitter)

        dpi = DataProcessInstance.from_dataflow(dataflow=dataflow, id=flow_run_ctx.flow_run.name)

        dpi.emit_process_start(
            emitter=emitter, start_timestamp_millis=int(flow_run_ctx.flow_run.start_time.timestamp() * 1000)
        )
        
        dpi.emit_process_end(
            emitter=emitter,
            end_timestamp_millis=int(flow_run_ctx.flow_run.start_time.timestamp() * 1000)+5000,
            result=InstanceRunResult.SUCCESS,
            result_type="prefect",
        )

        for prefect_future in flow_run_ctx.task_run_futures:
            task_run = asyncio.run(self._get_task_run(prefect_future.task_run.id))
            datajob = DataJob(id=task_run.task_key, flow_urn=dataflow.urn)
            
            if task_run.state_name == "Completed":
                result = InstanceRunResult.SUCCESS
            elif task_run.state_name == "Failed":
                result = InstanceRunResult.FAILURE
            elif task_run.state_name == "Cancelled":
                result = InstanceRunResult.SKIPPED
            else:
                raise Exception(
                    f"Result should be either success or failure and it was {ti.state}"
                )
            
            dpi = DataProcessInstance.from_datajob(
                datajob=datajob,
                id=f"{flow_run_ctx.flow_run.name}.{task_run.name}",
                clone_inlets=True,
                clone_outlets=True,
            )
            dpi.emit_process_start(
                emitter=emitter,
                start_timestamp_millis=int(task_run.start_time.timestamp() * 1000),
                emit_template=False,
            )
            dpi.emit_process_end(
                emitter=emitter,
                end_timestamp_millis=int(task_run.end_time.timestamp() * 1000),
                result=result,
                result_type="prefect",
            )
