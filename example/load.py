from prefect_datahub import DatahubEmitter

emitter = DatahubEmitter(
    datahub_rest_url="http://localhost:8080", capture_tags_info=False
)

emitter.save("datahub-emitter-block", overwrite=True)
