import asyncio
from unittest.mock import Mock, patch

from datahub.api.entities.datajob import DataJob
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub_provider.entities import Dataset
from prefect.context import FlowRunContext, TaskRunContext

from prefect_datahub import constants
from prefect_datahub.datahub_emitter import DatahubEmitter, WorkspaceKey


def test_constants():
    assert constants.ORCHESTRATOR == "prefect"


def test_workspace_key():
    container_key = WorkspaceKey(
        workspace_name="datahub",
        platform="prefect",
        env="PROD",
    )
    assert container_key.guid() == "bf46b065c6816616f35e83d8be976c62"
    assert container_key.workspace_name == "datahub"


@patch("prefect_datahub.datahub_emitter.DatahubRestEmitter", autospec=True)
def test_entities_to_urn_list(mock_emit):
    dataset_urn_list = DatahubEmitter()._entities_to_urn_list(
        [Dataset("snowflake", "mydb.schema.tableA")]
    )
    for dataset_urn in dataset_urn_list:
        assert isinstance(dataset_urn, DatasetUrn)


@patch("prefect_datahub.datahub_emitter.DatahubRestEmitter", autospec=True)
def test_get_flow_run_graph(mock_emit, mock_prefect_client):
    graph_json = asyncio.run(
        DatahubEmitter()._get_flow_run_graph("c3b947e5-3fa1-4b46-a2e2-58d50c938f2e")
    )
    assert isinstance(graph_json, list)


@patch("prefect_datahub.datahub_emitter.DatahubRestEmitter", autospec=True)
def test_add_task(mock_emit, mock_run_context):
    mock_emitter = Mock()
    mock_emit.return_value = mock_emitter

    datahub_emitter = DatahubEmitter()
    inputs = [Dataset("snowflake", "mydb.schema.tableA")]
    outputs = [Dataset("snowflake", "mydb.schema.tableC")]
    datahub_emitter.add_task(
        inputs=inputs,
        outputs=outputs,
    )

    task_run_ctx: TaskRunContext = mock_run_context[0]
    flow_run_ctx: FlowRunContext = mock_run_context[1]

    expected_datajob_urn = (
        f"urn:li:dataJob:(urn:li:dataFlow:"
        f"(prefect,{flow_run_ctx.flow.name},prod),{task_run_ctx.task.task_key})"
    )

    assert expected_datajob_urn in datahub_emitter.datajobs_to_emit.keys()
    actual_datajob = datahub_emitter.datajobs_to_emit[expected_datajob_urn]
    assert isinstance(actual_datajob, DataJob)
    assert str(actual_datajob.flow_urn) == "urn:li:dataFlow:(prefect,etl,prod)"
    assert actual_datajob.name == task_run_ctx.task.name
    assert actual_datajob.description == task_run_ctx.task.description
    assert actual_datajob.tags == task_run_ctx.task.tags
    assert (
        str(actual_datajob.inlets[0])
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableA,PROD)"
    )
    assert (
        str(actual_datajob.outlets[0])
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableC,PROD)"
    )
    assert mock_emit.emit.call_count == 0


@patch("prefect_datahub.datahub_emitter.DatahubRestEmitter", autospec=True)
def test_emit_flow(
    mock_emit, mock_run_context, mock_prefect_client, mock_prefect_cloud_client
):
    mock_emitter = Mock()
    mock_emit.return_value = mock_emitter

    datahub_emitter = DatahubEmitter()
    datahub_emitter.emit_flow()

    flow_run_ctx: FlowRunContext = mock_run_context[1]

    expected_dataflow_urn = f"urn:li:dataFlow:(prefect,{flow_run_ctx.flow.name},prod)"

    assert mock_emitter.method_calls[1].args[0].aspectName == "containerProperties"
    assert (
        mock_emitter.method_calls[1].args[0].entityUrn
        == "urn:li:container:bf46b065c6816616f35e83d8be976c62"
    )
    assert mock_emitter.method_calls[2].args[0].aspectName == "status"
    assert (
        mock_emitter.method_calls[2].args[0].entityUrn
        == "urn:li:container:bf46b065c6816616f35e83d8be976c62"
    )
    assert mock_emitter.method_calls[3].args[0].aspectName == "dataPlatformInstance"
    assert (
        mock_emitter.method_calls[3].args[0].entityUrn
        == "urn:li:container:bf46b065c6816616f35e83d8be976c62"
    )
    assert mock_emitter.method_calls[4].args[0].aspectName == "subTypes"
    assert (
        mock_emitter.method_calls[4].args[0].entityUrn
        == "urn:li:container:bf46b065c6816616f35e83d8be976c62"
    )
    assert mock_emitter.method_calls[5].args[0].aspectName == "ownership"
    assert (
        mock_emitter.method_calls[5].args[0].entityUrn
        == "urn:li:container:bf46b065c6816616f35e83d8be976c62"
    )
    assert (
        mock_emitter.method_calls[5].args[0].aspect.owners[0].owner
        == "urn:li:corpuser:shubhamjagtapgslabcom"
    )

    assert mock_emitter.method_calls[6].args[0].aspectName == "dataFlowInfo"
    assert mock_emitter.method_calls[6].args[0].entityUrn == expected_dataflow_urn
    assert mock_emitter.method_calls[7].args[0].aspectName == "ownership"
    assert mock_emitter.method_calls[7].args[0].entityUrn == expected_dataflow_urn
    assert mock_emitter.method_calls[8].args[0].aspectName == "globalTags"
    assert mock_emitter.method_calls[8].args[0].entityUrn == expected_dataflow_urn
    assert mock_emitter.method_calls[9].args[0].aspectName == "browsePaths"
    assert mock_emitter.method_calls[9].args[0].entityUrn == expected_dataflow_urn
    assert (
        mock_emitter.method_calls[13].args[0].aspectName
        == "dataProcessInstanceProperties"
    )
    assert (
        mock_emitter.method_calls[13].args[0].entityUrn
        == "urn:li:dataProcessInstance:1c61330602200cac15fe044b3698c176"
    )
    assert (
        mock_emitter.method_calls[14].args[0].aspectName
        == "dataProcessInstanceRelationships"
    )
    assert (
        mock_emitter.method_calls[14].args[0].entityUrn
        == "urn:li:dataProcessInstance:1c61330602200cac15fe044b3698c176"
    )
    assert (
        mock_emitter.method_calls[15].args[0].aspectName
        == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[15].args[0].entityUrn
        == "urn:li:dataProcessInstance:1c61330602200cac15fe044b3698c176"
    )
    assert mock_emitter.method_calls[16].args[0].aspectName == "dataJobInfo"
    assert (
        mock_emitter.method_calls[16].args[0].entityUrn
        == f"urn:li:dataJob:(urn:li:dataFlow:(prefect,"
        f"{flow_run_ctx.flow.name},prod),__main__.extract)"
    )
    assert mock_emitter.method_calls[17].args[0].aspectName == "dataJobInputOutput"
    assert (
        mock_emitter.method_calls[17].args[0].entityUrn
        == f"urn:li:dataJob:(urn:li:dataFlow:(prefect,"
        f"{flow_run_ctx.flow.name},prod),__main__.extract)"
    )
    assert mock_emitter.method_calls[18].args[0].aspectName == "ownership"
    assert (
        mock_emitter.method_calls[18].args[0].entityUrn
        == f"urn:li:dataJob:(urn:li:dataFlow:(prefect,"
        f"{flow_run_ctx.flow.name},prod),__main__.extract)"
    )
    assert mock_emitter.method_calls[19].args[0].aspectName == "globalTags"
    assert (
        mock_emitter.method_calls[19].args[0].entityUrn
        == f"urn:li:dataJob:(urn:li:dataFlow:(prefect,"
        f"{flow_run_ctx.flow.name},prod),__main__.extract)"
    )
    assert mock_emitter.method_calls[20].args[0].aspectName == "browsePaths"
    assert (
        mock_emitter.method_calls[20].args[0].entityUrn
        == f"urn:li:dataJob:(urn:li:dataFlow:(prefect,"
        f"{flow_run_ctx.flow.name},prod),__main__.extract)"
    )
    assert (
        mock_emitter.method_calls[21].args[0].aspectName
        == "dataProcessInstanceProperties"
    )
    assert (
        mock_emitter.method_calls[21].args[0].entityUrn
        == "urn:li:dataProcessInstance:77a8ea575ff6976d37cd1a60caf98a95"
    )
    assert (
        mock_emitter.method_calls[22].args[0].aspectName
        == "dataProcessInstanceRelationships"
    )
    assert (
        mock_emitter.method_calls[22].args[0].entityUrn
        == "urn:li:dataProcessInstance:77a8ea575ff6976d37cd1a60caf98a95"
    )
    assert (
        mock_emitter.method_calls[23].args[0].aspectName
        == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[23].args[0].entityUrn
        == "urn:li:dataProcessInstance:77a8ea575ff6976d37cd1a60caf98a95"
    )
    assert (
        mock_emitter.method_calls[24].args[0].aspectName
        == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[24].args[0].entityUrn
        == "urn:li:dataProcessInstance:77a8ea575ff6976d37cd1a60caf98a95"
    )
    assert mock_emitter.method_calls[25].args[0].aspectName == "dataJobInfo"
    assert (
        mock_emitter.method_calls[25].args[0].entityUrn
        == f"urn:li:dataJob:(urn:li:dataFlow:(prefect,"
        f"{flow_run_ctx.flow.name},prod),__main__.load)"
    )
    assert mock_emitter.method_calls[26].args[0].aspectName == "dataJobInputOutput"
    assert (
        mock_emitter.method_calls[26].args[0].entityUrn
        == f"urn:li:dataJob:(urn:li:dataFlow:(prefect,"
        f"{flow_run_ctx.flow.name},prod),__main__.load)"
    )
    assert mock_emitter.method_calls[27].args[0].aspectName == "ownership"
    assert (
        mock_emitter.method_calls[27].args[0].entityUrn
        == f"urn:li:dataJob:(urn:li:dataFlow:(prefect,"
        f"{flow_run_ctx.flow.name},prod),__main__.load)"
    )
    assert mock_emitter.method_calls[28].args[0].aspectName == "globalTags"
    assert (
        mock_emitter.method_calls[28].args[0].entityUrn
        == f"urn:li:dataJob:(urn:li:dataFlow:(prefect,"
        f"{flow_run_ctx.flow.name},prod),__main__.load)"
    )
    assert mock_emitter.method_calls[29].args[0].aspectName == "browsePaths"
    assert (
        mock_emitter.method_calls[29].args[0].entityUrn
        == f"urn:li:dataJob:(urn:li:dataFlow:(prefect,"
        f"{flow_run_ctx.flow.name},prod),__main__.load)"
    )
    assert (
        mock_emitter.method_calls[30].args[0].aspectName
        == "dataProcessInstanceProperties"
    )
    assert (
        mock_emitter.method_calls[30].args[0].entityUrn
        == "urn:li:dataProcessInstance:6efec88dd6d26cb85e8592baf38e42b9"
    )
    assert (
        mock_emitter.method_calls[31].args[0].aspectName
        == "dataProcessInstanceRelationships"
    )
    assert (
        mock_emitter.method_calls[31].args[0].entityUrn
        == "urn:li:dataProcessInstance:6efec88dd6d26cb85e8592baf38e42b9"
    )
    assert (
        mock_emitter.method_calls[32].args[0].aspectName
        == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[32].args[0].entityUrn
        == "urn:li:dataProcessInstance:6efec88dd6d26cb85e8592baf38e42b9"
    )
    assert (
        mock_emitter.method_calls[33].args[0].aspectName
        == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[33].args[0].entityUrn
        == "urn:li:dataProcessInstance:6efec88dd6d26cb85e8592baf38e42b9"
    )
    assert mock_emitter.method_calls[34].args[0].aspectName == "dataJobInfo"
    assert (
        mock_emitter.method_calls[34].args[0].entityUrn
        == f"urn:li:dataJob:(urn:li:dataFlow:(prefect,"
        f"{flow_run_ctx.flow.name},prod),__main__.transform)"
    )
    assert mock_emitter.method_calls[35].args[0].aspectName == "dataJobInputOutput"
    assert (
        mock_emitter.method_calls[35].args[0].entityUrn
        == f"urn:li:dataJob:(urn:li:dataFlow:(prefect,"
        f"{flow_run_ctx.flow.name},prod),__main__.transform)"
    )
    assert mock_emitter.method_calls[36].args[0].aspectName == "ownership"
    assert (
        mock_emitter.method_calls[36].args[0].entityUrn
        == f"urn:li:dataJob:(urn:li:dataFlow:(prefect,"
        f"{flow_run_ctx.flow.name},prod),__main__.transform)"
    )
    assert mock_emitter.method_calls[37].args[0].aspectName == "globalTags"
    assert (
        mock_emitter.method_calls[37].args[0].entityUrn
        == f"urn:li:dataJob:(urn:li:dataFlow:(prefect,"
        f"{flow_run_ctx.flow.name},prod),__main__.transform)"
    )
    assert mock_emitter.method_calls[38].args[0].aspectName == "browsePaths"
    assert (
        mock_emitter.method_calls[38].args[0].entityUrn
        == f"urn:li:dataJob:(urn:li:dataFlow:(prefect,"
        f"{flow_run_ctx.flow.name},prod),__main__.transform)"
    )
    assert (
        mock_emitter.method_calls[39].args[0].aspectName
        == "dataProcessInstanceProperties"
    )
    assert (
        mock_emitter.method_calls[39].args[0].entityUrn
        == "urn:li:dataProcessInstance:c4458dec616b26ad64e2c520614ef6b7"
    )
    assert (
        mock_emitter.method_calls[40].args[0].aspectName
        == "dataProcessInstanceRelationships"
    )
    assert (
        mock_emitter.method_calls[40].args[0].entityUrn
        == "urn:li:dataProcessInstance:c4458dec616b26ad64e2c520614ef6b7"
    )
    assert (
        mock_emitter.method_calls[41].args[0].aspectName
        == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[41].args[0].entityUrn
        == "urn:li:dataProcessInstance:c4458dec616b26ad64e2c520614ef6b7"
    )
    assert (
        mock_emitter.method_calls[42].args[0].aspectName
        == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[42].args[0].entityUrn
        == "urn:li:dataProcessInstance:c4458dec616b26ad64e2c520614ef6b7"
    )
