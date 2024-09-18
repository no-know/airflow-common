import json
import logging
from abc import ABC
from typing import Dict, Text, Tuple

from airflow.models.dag import DAG

from airflow_common.config import AirflowPipelineConfig
from tapad_common.config.gcp import Location, ProjectID
from tapad_common.orchestration.pipeline import DGXPipeline
from tapad_common.utils.json import DGXJSONEncoder

logger = logging.getLogger(__name__)


class AirflowPipeline(DGXPipeline, ABC):
    DEFAULT_PIPELINE_NAME = ""
    DEFAULT_PIPELINE_DESCRIPTION = ""
    TARGET_REGIONS: Tuple[Text] = (Location.US,)

    def __init__(
        self,
        config: AirflowPipelineConfig,
    ):
        self.config: AirflowPipelineConfig
        super().__init__(config=config)
        self.dag = DAG(
            dag_id=config.pipeline_name or self.DEFAULT_PIPELINE_NAME,
            description=config.pipeline_description
            or self.DEFAULT_PIPELINE_DESCRIPTION,
            default_args=self.config.default_args,
            params=json.loads(
                json.dumps(self.config.runtime_parameters(), cls=DGXJSONEncoder)
            ),
            schedule_interval=config.schedule_interval,
            max_active_runs=config.max_active_runs,
            concurrency=config.concurrency,
            on_success_callback=config.dag_success_callback,  # for dag level callback
            on_failure_callback=config.dag_failure_callback,
        )

    @classmethod
    def spawn_dags(cls):
        pass

    @staticmethod
    def set_project_id_and_config(project_id):
        config = "default_%s_config"
        if project_id == ProjectID.DGX_PRD:
            config = config % "prod"
        elif project_id == ProjectID.DGX_DEV:
            config = config % "dev"
        elif project_id == ProjectID.DGX_STG:
            config = config % "stg"
        else:
            config = config % "experimental"
            project_id = ProjectID.DGX_DEV
        return project_id, config

    def extra_labels(self) -> Dict[Text, Text]:
        return {}
