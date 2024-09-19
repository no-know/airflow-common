import os
from abc import abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional, Text

from airflow_common.cloud_logger import (
    AirflowCloudLogger,
    AirflowDagEventType,
    AirflowTaskEventType,
)
from airflow_common.constants import (
    BEAM_PARAMS_KEY,
    BQ_PARAMS_KEY,
    GRAPH_INFO_KEY,
    RECURRING_RUN_FLAG,
    StandardGraphs,
)
from tapad_common.config.gcp import Location, ProjectID
from tapad_common.dates import GraphDate
from tapad_common.graph_info import GraphInfo
from tapad_common.orchestration.config import PipelineConfig


@dataclass
class AirflowPipelineConfig(PipelineConfig):
    # pipeline configs
    pipeline_name: Optional[Text] = None
    pipeline_description: Optional[Text] = None
    # project id and location of where the resources are going to run
    project_id: Text = ProjectID.DGX_DEV
    location: Text = Location.US
    graph_info: GraphInfo = GraphInfo(
        dg5_graph_type=StandardGraphs.QUIQ_US,
        graph_date=GraphDate(3),
    )
    row_limit: int = 100
    is_recurring: bool = False

    # airflow specific configurations for local runs
    # For more info on params:
    # https://airflow.apache.org/docs/apache-airflow/2.1.1/_api/airflow/
    # models/index.html#airflow.models.BaseOperator
    owner: Optional[Text] = "dgx"
    depends_on_past: Optional[bool] = False
    start_date: Optional[datetime] = GraphDate(3).at_midnight()
    email_on_failure: Optional[bool] = False
    email_on_retry: Optional[bool] = False
    notify_channels: Optional[bool] = False
    schedule_interval: Optional[Text] = None
    retries: Optional[int] = 0
    retry_delay: Optional[int] = 5
    catch_up: Optional[bool] = False
    max_active_runs: Optional[int] = 1  # Max active runs per dag
    concurrency: Optional[int] = 4  # Max task instances allowed to run concurrently
    log_success_task: Optional[bool] = False
    log_failure_task: Optional[bool] = False
    log_retry_task: Optional[bool] = False
    log_success_dag: Optional[bool] = False
    log_failure_dag: Optional[bool] = False

    custom_config: Optional[Dict] = field(
        default_factory=lambda: {
            RECURRING_RUN_FLAG: False,
            GRAPH_INFO_KEY: {},
            BQ_PARAMS_KEY: {},
            BEAM_PARAMS_KEY: {},
        }
    )

    def __post_init__(self):
        super().__post_init__()
        self.custom_config.update(
            {
                RECURRING_RUN_FLAG: self.is_recurring,
                GRAPH_INFO_KEY: self.graph_info.runtime_params(),
            }
        )
        self.extra_custom_config_values()

    def task_success_callback(self, context):
        if self.log_success_task:
            AirflowCloudLogger(
                project_id=self.project_id,
            ).create_task_log(
                event_type=AirflowTaskEventType.Success,
                context=context,
                default_labels=self.logger_default_labels,
            )

    def task_retry_callback(self, context):
        if self.log_retry_task:
            AirflowCloudLogger(
                project_id=self.project_id,
            ).create_task_log(
                event_type=AirflowTaskEventType.Retry,
                context=context,
                default_labels=self.logger_default_labels,
            )

    @property
    def logger_default_labels(self):
        return {
            "composer_dag": self.pipeline_name,
            "composer_environment": os.environ.get("COMPOSER_ENVIRONMENT", ""),
            "composer_region": os.environ.get("COMPOSER_LOCATION", ""),
            "market": self.location,
            "team": self.owner,
        }

    def dag_success_callback(self, context):
        if self.log_success_dag:
            AirflowCloudLogger(
                project_id=self.project_id,
            ).create_dag_log(
                event_type=AirflowDagEventType.Success,
                context=context,
                default_labels=self.logger_default_labels,
            )

    def dag_failure_callback(self, context):
        if self.log_failure_dag:
            AirflowCloudLogger(
                project_id=self.project_id,
            ).create_dag_log(
                event_type=AirflowDagEventType.Failure,
                context=context,
                default_labels=self.logger_default_labels,
            )

    @property
    def default_args(self):
        return {
            "owner": self.owner,
            "depends_on_past": self.depends_on_past,
            "start_date": self.start_date,
            "email_on_failure": self.email_on_failure,
            "email_on_retry": self.email_on_retry,
            "schedule_interval": self.schedule_interval,
            "retries": self.retries,
            "retry_delay": self.retry_delay,
            "on_success_callback": self.task_success_callback,
            "on_retry_callback": self.task_retry_callback,
        }

    def runtime_parameters(self):
        return self.custom_config

    @staticmethod
    def deserialize_runtime_params_from_airflow(params: Dict):
        """To deserialize the custom config bug in airflow <2.2.0

        :param params: custom config runtime parameter outputted by airflow
        :type params: Dict
        """

        def _deserialize(d: Dict):
            """Checks if the dictionary have keys `__type` and `__var`
               and returns `__var`. If dict does not have keys, return the same dict

            :param d: `__type` and `__var`
            :type d: Dict
            """
            if "__type" in d and "__var" in d:
                return d["__var"]
            return d

        params = _deserialize(params)
        for k, v in params.items():
            if isinstance(v, dict):
                params[k] = (
                    AirflowPipelineConfig.deserialize_runtime_params_from_airflow(
                        params=v
                    )
                )
        return params

    @abstractmethod
    def extra_custom_config_values(self):
        pass
