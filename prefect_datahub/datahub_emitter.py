"""Datahub Emitter classes used to emit prefect metadata to Datahub REST. """

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
from prefect.client import cloud, orchestration
from prefect.client.schemas import TaskRun
from prefect.context import FlowRunContext, TaskRunContext
from pydantic import Field

from prefect_datahub import constants


class WorkspaceKey(PlatformKey):
    workspace_name: str


class DatahubEmitter(Block):
    """
    Block used to emit prefect task and flow related metadata to Datahub REST

    Attributes:
        datahub_rest_url (str): The Datahub GMS Rest URL.
        env (str): The environment that all assets produced by this orchestrator \
            belong to. For more detail and possible values refer \
            https://datahubproject.io/docs/graphql/enums/#fabrictype.
        platform_instance (str): The instance of the platform that all assets \
            produced by this recipe belong to. For more detail please refer to \
            https://datahubproject.io/docs/platform-instances/.

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
        description="The environment that all assets produced by this orchestrator "
        "belong to. For more detail and possible values refer "
        "https://datahubproject.io/docs/graphql/enums/#fabrictype.",
    )

    platform_instance: Optional[str] = Field(
        default=None,
        title="Platform instance",
        description="The instance of the platform that all assets produced by this "
        "recipe belong to. For more detail please refer to "
        "https://datahubproject.io/docs/platform-instances/.",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.datajobs_to_emit = {}
        self.emitter = DatahubRestEmitter(gms_server=self.datahub_rest_url)
        self.emitter.test_connection()

    def _entities_to_urn_list(self, iolets: List[_Entity]) -> List[DatasetUrn]:
        return [DatasetUrn.create_from_string(let.urn) for let in iolets]

    async def _get_flow_run_graph(self, flow_run_id):
        response = await orchestration.get_client()._client.get(
            f"/flow_runs/{flow_run_id}/graph"
        )
        return response.json()

    def _generate_datajob(
        self,
        flow_run_ctx: FlowRunContext,
        task_run_ctx: Optional[TaskRunContext] = None,
        task_key: Optional[str] = None,
    ) -> Optional[DataJob]:
        """
        Create datajob entity using task run ctx and flow run ctx.
        Assign description, tags, and properties to created datajob.

        Args:
            flow_run_ctx: The prefect current running flow run context.
            task_run_ctx: The prefect current running task run context.
            task_key: The task key.

        Returns:
            The datajob entity.
        """
        dataflow_urn = DataFlowUrn.create_from_ids(
            orchestrator=constants.ORCHESTRATOR,
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
                constants.VERSION,
                constants.CACHE_EXPIRATION,
                constants.TASK_RUN_NAME,
                constants.RETRIES,
                constants.TIMEOUT_SECONDS,
                constants.LOG_PRINTS,
                constants.REFRESH_CACHE,
                constants.TASK_KEY,
                constants.ON_COMPLETION,
                constants.ON_FAILURE,
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

    def _generate_dataflow(self, flow_run_ctx: FlowRunContext) -> DataFlow:
        """
        Create dataflow entity using flow run ctx.
        Assign description, tags, and properties to created dataflow.

        Args:
            flow_run_ctx: The prefect current running flow run context.

        Returns:
            The dataflow entity.
        """
        flow = asyncio.run(
            orchestration.get_client().read_flow(flow_id=flow_run_ctx.flow_run.flow_id)
        )
        dataflow = DataFlow(
            orchestrator=constants.ORCHESTRATOR,
            id=flow_run_ctx.flow.name,
            cluster=self.env,
            env=self.env,
            name=flow_run_ctx.flow.name,
            platform_instance=self.platform_instance,
        )
        dataflow.description = flow_run_ctx.flow.description
        dataflow.tags = flow.tags
        flow_property_bag: Dict[str, str] = {}
        flow_property_bag[constants.ID] = str(flow.id)
        flow_property_bag[constants.CREATED] = str(flow.created)
        flow_property_bag[constants.UPDATED] = str(flow.updated)

        allowed_flow_keys = [
            constants.VERSION,
            constants.FLOW_RUN_NAME,
            constants.RETRIES,
            constants.TASK_RUNNER,
            constants.TIMEOUT_SECONDS,
            constants.PERSIST_RESULT,
            constants.LOG_PRINTS,
            constants.ON_COMPLETION,
            constants.ON_FAILURE,
            constants.ON_CANCELLATION,
            constants.ON_CRASHED,
        ]
        for key in allowed_flow_keys:
            if (
                hasattr(flow_run_ctx.flow, key)
                and getattr(flow_run_ctx.flow, key) is not None
            ):
                flow_property_bag[key] = repr(getattr(flow_run_ctx.flow, key))
        dataflow.properties = flow_property_bag

        return dataflow

    def _run_dataflow(self, dataflow: DataFlow, flow_run_ctx: FlowRunContext) -> None:
        """
        Emit prefect flow run to datahub rest. Prefect flow run get mapped with datahub
        data process instance entity which get's generate from provided dataflow entity.
        Assign flow run properties to data process instance properties.

        Args:
            dataflow: The datahub dataflow entity used to create data process instance.
            flow_run_ctx: The prefect current running flow run context.
        """
        flow_run = asyncio.run(
            orchestration.get_client().read_flow_run(
                flow_run_id=flow_run_ctx.flow_run.id
            )
        )
        dpi = DataProcessInstance.from_dataflow(
            dataflow=dataflow, id=flow_run_ctx.flow_run.name
        )

        dpi_property_bag: Dict[str, str] = {}
        allowed_flow_run_keys = [
            constants.ID,
            constants.CREATED,
            constants.UPDATED,
            constants.CREATED_BY,
            constants.AUTO_SCHEDULED,
            constants.ESTIMATED_RUN_TIME,
            constants.START_TIME,
            constants.TOTAL_RUN_TIME,
            constants.NEXT_SCHEDULED_START_TIME,
            constants.TAGS,
            constants.RUN_COUNT,
        ]
        for key in allowed_flow_run_keys:
            if hasattr(flow_run, key) and getattr(flow_run, key) is not None:
                dpi_property_bag[key] = str(getattr(flow_run, key))
        dpi.properties.update(dpi_property_bag)

        dpi.emit_process_start(
            emitter=self.emitter,
            start_timestamp_millis=int(
                flow_run_ctx.flow_run.start_time.timestamp() * 1000
            ),
        )

    def _run_datajob(
        self, datajob: DataJob, flow_run_name: str, task_run: TaskRun
    ) -> None:
        """
        Emit prefect task run to datahub rest. Prefect task run get mapped with datahub
        data process instance entity which get's generate from provided datajob entity.
        Assign task run properties to data process instance properties.

        Args:
            datajob: The datahub datajob entity used to create data process instance.
            flow_run_name: The prefect current running flow run name.
            task_run: The prefect task run entity.
        """
        dpi = DataProcessInstance.from_datajob(
            datajob=datajob,
            id=f"{flow_run_name}.{task_run.name}",
            clone_inlets=True,
            clone_outlets=True,
        )

        dpi_property_bag: Dict[str, str] = {}
        allowed_task_run_keys = [
            constants.ID,
            constants.FLOW_RUN_ID,
            constants.CREATED,
            constants.UPDATED,
            constants.ESTIMATED_RUN_TIME,
            constants.START_TIME,
            constants.END_TIME,
            constants.TOTAL_RUN_TIME,
            constants.NEXT_SCHEDULED_START_TIME,
            constants.TAGS,
            constants.RUN_COUNT,
        ]
        for key in allowed_task_run_keys:
            if hasattr(task_run, key) and getattr(task_run, key) is not None:
                dpi_property_bag[key] = str(getattr(task_run, key))
        dpi.properties.update(dpi_property_bag)

        if task_run.state_name == constants.COMPLETE:
            result = InstanceRunResult.SUCCESS
        elif task_run.state_name == constants.FAILED:
            result = InstanceRunResult.FAILURE
        elif task_run.state_name == constants.CANCELLED:
            result = InstanceRunResult.SKIPPED
        else:
            raise Exception(
                f"Result should be either success or failure and it was "
                f"{task_run.state_name}"
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
            result_type=constants.ORCHESTRATOR,
        )

    def _emit_workspaces(self) -> None:
        """
        Emit prefect workspace metadata to datahub rest.
        Prefect workspce get mapped with datahub container entity.
        Workspace account name also get emit as owner of continer.
        """
        try:
            asyncio.run(cloud.get_cloud_client().api_healthcheck())
        except Exception:
            get_run_logger().info(
                "Cannot emit workspaces. Please set correct 'PREFECT_API_KEY'."
            )
            return
        SUB_TYPE = "Workspace"
        workspaces = asyncio.run(cloud.get_cloud_client().read_workspaces())
        for workspace in workspaces:
            container_key = WorkspaceKey(
                workspace_name=workspace.workspace_name,
                platform=constants.ORCHESTRATOR,
                instance=self.platform_instance,
                env=self.env,
            )
            container_work_units = gen_containers(
                container_key=container_key,
                name=workspace.workspace_name,
                sub_types=[SUB_TYPE],
                description=workspace.workspace_description,
                owner_urn=make_user_urn(workspace.account_name),
            )
            for workunit in container_work_units:
                self.emitter.emit(workunit.metadata)

    def emit_task(
        self,
        inputs: Optional[List[_Entity]] = None,
        outputs: Optional[List[_Entity]] = None,
    ) -> None:
        """
        Emit prefect task metadata to datahub rest. Prefect task get mapped with datahub
        datajob entity. Assign provided inputs and outputs as datajob inlets and outlets
        respectively. To emit task metadata it is compulsory to emit flow as well
        otherwise task will not get emit.

        Args:
            inputs (list): The list of task inputs.
            outputs (list): The list of task outputs.

        Example:
            Emit the task metadata as show below:
            ```python
            from datahub_provider.entities import Dataset
            from prefect import flow, task

            from prefect_datahub import DatahubEmitter

            datahub_emitter = DatahubEmitter.load("MY_BLOCK_NAME")

            @task(name="Transform", description="Transform the data")
            def transform(data):
                data = data.split(" ")
                datahub_emitter.emit_task(
                    inputs=[Dataset("snowflake", "mydb.schema.tableA")],
                    outputs=[Dataset("snowflake", "mydb.schema.tableC")],
                )
                return data

            @flow(name="ETL flow", description="Extract transform load flow")
            def etl():
                data = transform("This is data")
                datahub_emitter.emit_flow()
            ```
        """
        flow_run_ctx = FlowRunContext.get()
        task_run_ctx = TaskRunContext.get()
        assert flow_run_ctx
        assert task_run_ctx

        datajob = self._generate_datajob(
            flow_run_ctx=flow_run_ctx, task_run_ctx=task_run_ctx
        )
        if inputs is not None:
            datajob.inlets.extend(self._entities_to_urn_list(inputs))
        if outputs is not None:
            datajob.outlets.extend(self._entities_to_urn_list(outputs))
        self.datajobs_to_emit[str(datajob.urn)] = datajob

    def emit_flow(self) -> None:
        """
        Emit prefect flow metadata to datahub rest. Prefect flow get mapped with datahub
        dataflow entity. Add upstream dependencies if present for each task.
        Emit the prefect task run metadata as well. If user haven't called emit_task in
        task function still emit_flow will emit task but without task name, description,
        tags and properties.
        Emit the prefect workspace metadata as well.


        Example:
            Emit the flow metadata as show below:
            ```python
            from prefect import flow, task

            from prefect_datahub import DatahubEmitter

            datahub_emitter = DatahubEmitter.load("MY_BLOCK_NAME")

            @flow(name="ETL flow", description="Extract transform load flow")
            def etl():
                data = extract()
                data = transform(data)
                load(data)
                datahub_emitter.emit_flow()
            ```
        """
        flow_run_ctx = FlowRunContext.get()
        assert flow_run_ctx
        # Emit flow
        dataflow = self._generate_dataflow(flow_run_ctx=flow_run_ctx)
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
            task_run = asyncio.run(
                orchestration.get_client().read_task_run(node[constants.ID])
            )
            # Emit task
            datajob_urn = DataJobUrn.create_from_ids(
                data_flow_urn=str(dataflow.urn),
                job_id=task_run.task_key,
            )
            if str(datajob_urn) in self.datajobs_to_emit:
                datajob = self.datajobs_to_emit[str(datajob_urn)]
            else:
                datajob = self._generate_datajob(
                    flow_run_ctx=flow_run_ctx, task_key=task_run.task_key
                )
            # Add upstrem urns
            for each in node[constants.UPSTREAM_DEPENDENCIES]:
                upstream_task_urn = DataJobUrn.create_from_ids(
                    data_flow_urn=str(dataflow.urn),
                    job_id=task_run_key_map[each["id"]],
                )
                datajob.upstream_urns.extend([upstream_task_urn])
            datajob.emit(self.emitter)

            # self._run_dataflow(dataflow,flow_run_ctx)
            self._run_datajob(
                datajob=datajob,
                flow_run_name=flow_run_ctx.flow_run.name,
                task_run=task_run,
            )
