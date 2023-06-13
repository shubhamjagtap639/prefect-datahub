from prefect_datahub.datahub_emitter import WorkspaceKey

def test_workspace_key():
    container_key = WorkspaceKey(
        workspace_name="datahub",
        platform="prefect",
        env="PROD",
    )
    assert container_key.guid() == "bf46b065c6816616f35e83d8be976c62"
    assert container_key.workspace_name == "datahub"