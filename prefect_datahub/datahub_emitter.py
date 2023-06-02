"""Module for emit metadata to Datahub REST. """

import asyncio
from typing import Dict, List, Optional

from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.mce_builder import make_user_urn
from datahub.emitter.mcp_builder import PlatformKey, gen_containers
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.utilities.urns.data_flow_urn import DataFlowUrn
from datahub.utilities.urns.data_job_urn import DataJobUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub_provider.entities import _Entity
from prefect import get_run_logger
from prefect.blocks.core import Block
from prefect.client.cloud import get_cloud_client
from prefect.client.orchestration import get_client
from prefect.client.schemas import TaskRun
from prefect.context import FlowRunContext, TaskRunContext
from pydantic import Field


class WorkspaceKey(PlatformKey):
    workspace_name: str


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

    env: Optional[str] = Field(
        default="prod",
        title="Environment",
        description="Name of the prefect environment.",
    )

    platform_instance: Optional[str] = Field(
        default=None,
        title="Platform instance",
        description="Name of the prefect platform instance.",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.datajob_to_emit = {}
        self.emitter = DatahubRestEmitter(gms_server=self.datahub_rest_url)
        self.emitter.test_connection()

    def _entities_to_urn_list(self, iolets: List[_Entity]) -> List[DatasetUrn]:
        return [DatasetUrn.create_from_string(let.urn) for let in iolets]

    async def _get_flow_run_graph(self, flow_run_id):
        response = await get_client()._client.get(f"/flow_runs/{flow_run_id}/graph")
        return response.json()

    def generate_datajob(
        self,
        flow_run_ctx: FlowRunContext,
        task_run_ctx: TaskRunContext = None,
        task_key: str = None,
    ) -> Optional[DataJob]:
        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator="prefect",
            flow_id=flow_run_ctx.flow.name,
            env=self.env,
            platform_instance=self.platform_instance,
        )
        if task_run_ctx is not None:
            datajob = DataJob(
                id=task_run_ctx.task.task_key,
                flow_urn=dataflow_urn,
                name=task_run_ctx.task.name,
            )

            datajob.description = task_run_ctx.task.description
            datajob.tags = task_run_ctx.task.tags
            job_property_bag: Dict[str, str] = {}

            allowed_task_keys = [
                "version",
                "cache_expiration",
                "task_run_name",
                "retries",
                "timeout_seconds",
                "log_prints",
                "refresh_cache",
                "task_key",
                "on_completion",
                "on_failure",
            ]
            for key in allowed_task_keys:
                if (
                    hasattr(task_run_ctx.task, key)
                    and getattr(task_run_ctx.task, key) is not None
                ):
                    job_property_bag[key] = repr(getattr(task_run_ctx.task, key))
            datajob.properties = job_property_bag
            return datajob
        elif task_key is not None:
            datajob = DataJob(
                id=task_key,
                flow_urn=dataflow_urn,
            )
            return datajob
        return None

    def generate_dataflow(self, flow_run_ctx: FlowRunContext) -> DataFlow:
        flow = asyncio.run(
            get_client().read_flow(flow_id=flow_run_ctx.flow_run.flow_id)
        )
        dataflow = DataFlow(
            orchestrator="prefect",
            id=flow_run_ctx.flow.name,
            env=self.env,
            name=flow_run_ctx.flow.name,
            platform_instance=self.platform_instance,
        )
        dataflow.description = flow_run_ctx.flow.description
        dataflow.tags = flow.tags
        flow_property_bag: Dict[str, str] = {}
        flow_property_bag["id"] = str(flow.id)
        flow_property_bag["created"] = str(flow.created)
        flow_property_bag["updated"] = str(flow.updated)

        allowed_flow_keys = [
            "version",
            "flow_run_name",
            "retries",
            "task_runner",
            "timeout_seconds",
            "persist_result",
            "log_prints",
            "on_completion",
            "on_failure",
            "on_cancellation",
            "on_crashed",
        ]
        for key in allowed_flow_keys:
            if (
                hasattr(flow_run_ctx.flow, key)
                and getattr(flow_run_ctx.flow, key) is not None
            ):
                flow_property_bag[key] = repr(getattr(flow_run_ctx.flow, key))
        dataflow.properties = flow_property_bag

        return dataflow

    def run_dataflow(self, dataflow: DataFlow, flow_run_ctx: FlowRunContext) -> None:
        flow_run = asyncio.run(
            get_client().read_flow_run(flow_run_id=flow_run_ctx.flow_run.id)
        )
        dpi = DataProcessInstance.from_dataflow(
            dataflow=dataflow, id=flow_run_ctx.flow_run.name
        )

        dpi_property_bag: Dict[str, str] = {}
        dpi_property_bag["id"] = str(flow_run.id)
        dpi_property_bag["created"] = str(flow_run.created)
        dpi_property_bag["created_by"] = str(flow_run.created_by)
        dpi_property_bag["auto_scheduled"] = str(flow_run.auto_scheduled)
        dpi_property_bag["estimated_run_time"] = str(flow_run.estimated_run_time)
        dpi_property_bag["start_time"] = str(flow_run.start_time)
        dpi_property_bag["total_run_time"] = str(flow_run.total_run_time)
        dpi_property_bag["next_scheduled_start_time"] = str(
            flow_run.next_scheduled_start_time
        )
        dpi_property_bag["tags"] = str(flow_run.tags)
        dpi_property_bag["updated"] = str(flow_run.updated)
        dpi_property_bag["run_count"] = str(flow_run.run_count)
        dpi.properties.update(dpi_property_bag)

        dpi.emit_process_start(
            emitter=self.emitter,
            start_timestamp_millis=int(
                flow_run_ctx.flow_run.start_time.timestamp() * 1000
            ),
        )

    def run_datajob(
        self, datajob: DataJob, flow_run_name: str, task_run: TaskRun
    ) -> None:
        dpi = DataProcessInstance.from_datajob(
            datajob=datajob,
            id=f"{flow_run_name}.{task_run.name}",
            clone_inlets=True,
            clone_outlets=True,
        )

        dpi_property_bag: Dict[str, str] = {}
        dpi_property_bag["id"] = str(task_run.id)
        dpi_property_bag["flow_run_id"] = str(task_run.flow_run_id)
        dpi_property_bag["created"] = str(task_run.created)
        dpi_property_bag["estimated_run_time"] = str(task_run.estimated_run_time)
        dpi_property_bag["expected_start_time"] = str(task_run.expected_start_time)
        dpi_property_bag["start_time"] = str(task_run.start_time)
        dpi_property_bag["end_time"] = str(task_run.end_time)
        dpi_property_bag["total_run_time"] = str(task_run.total_run_time)
        dpi_property_bag["next_scheduled_start_time"] = str(
            task_run.next_scheduled_start_time
        )
        dpi_property_bag["tags"] = str(task_run.tags)
        dpi_property_bag["updated"] = str(task_run.updated)
        dpi_property_bag["run_count"] = str(task_run.run_count)
        dpi.properties.update(dpi_property_bag)

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

    def emit_workspaces(self) -> None:
        try:
            asyncio.run(get_cloud_client().api_healthcheck())
        except Exception as e:
            get_run_logger().info(
                "Cannot emit workspaces. Please set correct 'PREFECT_API_KEY'."
            )
            return

        workspaces = asyncio.run(get_cloud_client().read_workspaces())
        for workspace in workspaces:
            container_key = WorkspaceKey(
                workspace_name=workspace.workspace_name,
                platform="prefect",
                instance=self.platform_instance,
                env=self.env,
            )
            container_work_units = gen_containers(
                container_key=container_key,
                name=workspace.workspace_name,
                sub_types=["Workspace"],
                description=workspace.workspace_description,
                owner_urn=make_user_urn(workspace.account_name),
            )
            for workunit in container_work_units:
                self.emitter.emit(workunit.metadata)

    def emit_task(self, inputs: List = None, outputs: List = None):
        flow_run_ctx = FlowRunContext.get()
        task_run_ctx = TaskRunContext.get()
        assert flow_run_ctx
        assert task_run_ctx

        datajob = self.generate_datajob(
            flow_run_ctx=flow_run_ctx, task_run_ctx=task_run_ctx
        )
        if inputs is not None:
            datajob.inlets.extend(self._entities_to_urn_list(inputs))
        if outputs is not None:
            datajob.outlets.extend(self._entities_to_urn_list(outputs))
        self.datajob_to_emit[str(datajob.urn)] = datajob

    def emit_flow(self):
        flow_run_ctx = FlowRunContext.get()
        assert flow_run_ctx
        # Emit flow
        dataflow = self.generate_dataflow(flow_run_ctx=flow_run_ctx)
        dataflow.emit(self.emitter)

        # Emit task, task run and add upstream task if present for each task
        graph_json = asyncio.run(
            self._get_flow_run_graph(str(flow_run_ctx.flow_run.id))
        )
        task_run_key_map = {
            str(prefect_future.task_run.id): prefect_future.task_run.task_key
            for prefect_future in flow_run_ctx.task_run_futures
        }
        for node in graph_json:
            task_run = asyncio.run(get_client().read_task_run(node["id"]))
            # Emit task
            datajob_urn = DataJobUrn.create_from_ids(
                data_flow_urn=str(dataflow.urn),
                job_id=task_run.task_key,
            )
            if str(datajob_urn) in self.datajob_to_emit:
                datajob = self.datajob_to_emit[str(datajob_urn)]
            else:
                datajob = self.generate_datajob(
                    flow_run_ctx=flow_run_ctx, task_key=task_run.task_key
                )
            # Add upstrem urns
            for each in node["upstream_dependencies"]:
                upstream_task_urn = DataJobUrn.create_from_ids(
                    data_flow_urn=str(dataflow.urn),
                    job_id=task_run_key_map[each["id"]],
                )
                datajob.upstream_urns.extend([upstream_task_urn])
            datajob.emit(self.emitter)

            self.run_dataflow(dataflow, flow_run_ctx)
            self.run_datajob(
                datajob=datajob,
                flow_run_name=flow_run_ctx.flow_run.name,
                task_run=task_run,
            )

        self.emit_workspaces()
