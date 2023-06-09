ORCHESTRATOR = "prefect"

# Flow and task common constants
VERSION = "version"
RETRIES = "retries"
TIMEOUT_SECONDS = "timeout_seconds"
LOG_PRINTS = "log_prints"
ON_COMPLETION = "on_completion"
ON_FAILURE = "on_failure"

# Flow constants
FLOW_RUN_NAME = "flow_run_name"
TASK_RUNNER = "task_runner"
PERSIST_RESULT = "persist_result"
ON_CANCELLATION = "on_cancellation"
ON_CRASHED = "on_crashed"

# Task constants
CACHE_EXPIRATION = "cache_expiration"
TASK_RUN_NAME = "task_run_name"
REFRESH_CACHE = "refresh_cache"
TASK_KEY = "task_key"

# Flow run and task run common constants
ID = "id"
CREATED = "created"
UPDATED = "updated"
TAGS = "tags"
ESTIMATED_RUN_TIME = "estimated_run_time"
START_TIME = "start_time"
END_TIME = "end_time"
TOTAL_RUN_TIME = "total_run_time"
NEXT_SCHEDULED_START_TIME = "next_scheduled_start_time"

# Fask run constants
CREATED_BY = "created_by"
AUTO_SCHEDULED = "auto_scheduled"

# Task run constants
FLOW_RUN_ID = "flow_run_id"
RUN_COUNT = "run_count"
UPSTREAM_DEPENDENCIES = "upstream_dependencies"

# States constants
COMPLETE = "Completed"
FAILED = "Failed"
CANCELLED = "Cancelled"
