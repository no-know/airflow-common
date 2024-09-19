from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Text

from google.cloud.logging_v2 import client as cloud_logging
from google.cloud.logging_v2.logger import Logger as cloud_logger
from google.cloud.logging_v2.resource import Resource


class AirflowLoggerName(Enum):
    Task = "task_events_v001"
    Dag = "dag_events_v001"


class AirflowLogEventLevel(Enum):
    Task = "airflow-task"
    Dag = "airflow-dagrun"


class AirflowTaskEventType(Enum):
    Success = "task-success"
    Failure = "task-failure"
    Retry = "task-retry"


class AirflowDagEventType(Enum):
    Success = "dag-success"
    Failure = "dag-failure"


class AirflowCloudLogger(object):
    def __init__(
        self,
        project_id: Text,
    ):
        assert project_id, "Project Id must be specified if client isn't specified"
        self.project_id = project_id
        self.client = cloud_logging.Client(project=project_id)

    def create_task_logger(self, default_labels: Dict = {}):
        return cloud_logger(
            name=AirflowLoggerName.Task.value,
            client=self.client,
            labels={**default_labels, "event_level": AirflowLogEventLevel.Task.value},
        )

    def create_dag_logger(self, default_labels: Dict = {}):
        return cloud_logger(
            name=AirflowLoggerName.Dag.value,
            client=self.client,
            labels={**default_labels, "event_level": AirflowLogEventLevel.Dag.value},
        )

    def create_task_log(
        self, event_type: AirflowTaskEventType, context: Dict, default_labels: Dict = {}
    ):
        task_logger = self.create_task_logger(default_labels=default_labels)

        ti = context["task_instance"]

        now = datetime.now(timezone.utc)
        end_date = ti.end_date if ti.end_date else now

        payload = {
            "event_type": event_type.value,
            "dag_id": ti.dag_id,
            "task_id": ti.task_id,
            "execution_date": ti.execution_date.isoformat(),
            "duration_s": (end_date - ti.start_date).total_seconds(),
            "start_date": ti.start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "try_number": ti.try_number - 1,
            "state": str(ti.state),
            "queue": ti.task.queue,
            "pool": ti.task.pool,
            "priority_weight": ti.task.priority_weight_total,
            "run_as_user": ti.task.run_as_user,
            "max_tries": ti.task.retries,
            "operator": ti.task.task_type,
            "hostname": ti.hostname,
            "unixname": ti.unixname,
            "job_id": ti.job_id,
            "queued_dttm": ti.queued_dttm.isoformat() if ti.queued_dttm else None,
            "pid": ti.pid,
        }
        res = Resource(
            type="generic_task",
            labels={
                "project_id": self.project_id,
                "namespace": default_labels.get("COMPOSER_ENVIRONMENT", ""),
                "job": ti.dag_id,
                "task_id": ti.task_id,
                "location": default_labels.get("COMPOSER_LOCATION", ""),
            },
        )
        task_logger.log_struct(payload, resource=res)

    def create_dag_log(
        self, event_type: AirflowDagEventType, context: Dict, default_labels: Dict = {}
    ):
        dag_logger = self.create_dag_logger(default_labels=default_labels)

        now = datetime.now(timezone.utc)
        ti = context["task_instance"]
        dag_run = context["dag_run"]
        payload = {
            "event_type": event_type.value,
            "dag_id": ti.dag_id,
            "execution_date": ti.execution_date.isoformat(),
            "start_date": dag_run.start_date.isoformat(),
            "end_date": dag_run.end_date.isoformat(),
            "duration_s": (now - dag_run.start_date).total_seconds(),
        }
        res = Resource(
            type="generic_task",
            labels={
                "project_id": self.project_id,
                "namespace": default_labels.get("COMPOSER_ENVIRONMENT", ""),
                "job": ti.dag_id,
                "task_id": ti.dag_id + "-" + ti.execution_date.isoformat(),
                "location": default_labels.get("COMPOSER_LOCATION", ""),
            },
        )
        dag_logger.log_struct(payload, resource=res)
