import logging
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Text

import tapad_common.gcp.labels as gcp_labels
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.trigger_rule import TriggerRule
from dateutil.parser import parse
from google.cloud.bigquery import CreateDisposition, Table, WriteDisposition
from google.cloud.bigquery.table import TimePartitioning, TimePartitioningType
from google.cloud.exceptions import NotFound
from tapad_common.bigquery.job import DGXSqlJob
from tapad_common.config.gcp import Location
from tapad_common.dates import GraphDate
from tapad_common.gcp.resources.bigquery.client import BqClient
from tapad_common.graph_id_info import GraphIdInfo
from tapad_common.utils.config import extract_graph_id_info_from_custom_config

from airflow_common.constants import (
    BQ_PARAMS_KEY,
    COMPUTE_LOCATION_KEY,
    COMPUTE_PROJECT_ID_KEY,
    CURRENT_SHARD_KEY,
    DATA_LOCATION_KEY,
    DATA_PROJECT_ID_KEY,
    EXECUTION_DATE_KEY,
    GRAPH_DATE_KEY,
    GRAPH_ID_INFO_KEY,
    GRAPH_ID_KEY,
    GRAPH_INFO_KEY,
    INPUT_AGG_ARTIFACTS_KEY,
    INPUT_TABLE_KEY,
    INPUT_XCOM_KEY,
    OUTPUT_ALIAS_KEY,
    RECURRING_RUN_FLAG,
    RUN_ID_KEY,
    ZERO_INDEX_FUTURE_WEEK,
)
from airflow_common.config import AirflowPipelineConfig

logger = logging.getLogger(__name__)


def wild_card_key(key: Text):
    return f"{key}_WILDCARD"


class BaseBQInsertJobOperator(BigQueryInsertJobOperator):
    def __init__(
        self,
        module_file: Optional[Text] = None,
        custom_config: Optional[Dict] = None,
        output_splitter: Optional[Text] = ".",
        wild_card_key: Optional[bool] = False,
        write_disposition: Optional[Text] = WriteDisposition.WRITE_EMPTY,
        create_disposition: Optional[Text] = CreateDisposition.CREATE_IF_NEEDED,
        time_partition_type: Optional[Text] = None,
        time_partition_field: Optional[Text] = None,
        clustering_fields: Optional[List[str]] = None,
        labels: Optional[Dict[Text, Text]] = None,
        default_xcom_keys: Optional[Dict[Text, Text]] = None,
        **kwargs,
    ):
        assert (
            BQ_PARAMS_KEY in custom_config
        ), f"{BQ_PARAMS_KEY} must be in custom_config {custom_config}"

        if time_partition_field:
            assert time_partition_type, "Time Partition Type must be specified"

        self.compute_project_id = kwargs.get("project_id", None)
        self.compute_location = kwargs.get("location", None)

        graph_info = custom_config.get(GRAPH_INFO_KEY, None)
        if not self.compute_project_id:
            self.compute_project_id = graph_info.get(DATA_PROJECT_ID_KEY, None)
            assert self.compute_project_id, "Project Id must be specified"

        if not self.compute_location:
            self.compute_location = graph_info.get(DATA_LOCATION_KEY, None)
            assert self.compute_location, "Location must be specified"
        self.module_file = module_file
        self.output_splitter = output_splitter
        self.custom_config = custom_config or {}
        self.wild_card_key = wild_card_key
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.time_partition_type = time_partition_type
        self.clustering_fields = clustering_fields
        self.time_partition_field = time_partition_field
        self.default_xcom_keys = default_xcom_keys
        self.labels = labels

        super().__init__(
            task_id=kwargs["task_id"],
            dag=kwargs["dag"],
            project_id=self.compute_project_id,
            location=Location.cloud_location(self.compute_location),
            configuration={},
            trigger_rule=kwargs.get("trigger_rule", TriggerRule.ALL_SUCCESS),
        )

    def _resolve_xcom_input_tables(self, context: Any, custom_config: Dict):
        """Gets the custom config xcom variable if needed

        :param context: DAG runtime context
        :param custom_config: custom config of the pipeline; it contains
        all the input table key or agg artifacts key
        """
        # Get the list of input tables from xcom
        input_aliases = custom_config[BQ_PARAMS_KEY].get(INPUT_XCOM_KEY, [None])
        assert isinstance(
            input_aliases, list
        ), f"INPUT_XCOM_KEY must be list; instead got {input_aliases}"
        input_tables = {}
        for alias in input_aliases:
            if alias:
                input_tables[alias] = self.xcom_pull(context, key=alias)
        logger.info(f"Input Aliases: {input_tables}")

        # update input tables:
        # - if input table alias => getting table from intermediate table
        # - if a list of input tables alias => getting table from multiple
        # intermediate tables and need to make sure only 1 key gets passed
        # - if no input table alias => use config's input table key
        job_params = deepcopy(custom_config)
        if len(input_tables) == 1:
            input_key = list(input_tables.keys())[0]
            if input_tables[input_key]:
                job_params[BQ_PARAMS_KEY].update(
                    {INPUT_TABLE_KEY: input_tables[input_key].get("value")}
                )
            else:
                job_params[BQ_PARAMS_KEY].update(
                    {INPUT_TABLE_KEY: self.default_xcom_keys[input_key].get("value")}
                )
        elif len(input_tables) > 1:
            input_tables = {
                k: (v if v else self.default_xcom_keys[k])
                for k, v in input_tables.items()
            }
            job_params[BQ_PARAMS_KEY].update({INPUT_AGG_ARTIFACTS_KEY: input_tables})
            # cleanup if exist
            if INPUT_TABLE_KEY in job_params:
                job_params[BQ_PARAMS_KEY].pop(INPUT_TABLE_KEY)
        return job_params

    def _resolve_runtime_config(self, context: Any, run_id: Optional[Text] = ""):
        """Update custom config if a custom json was passed in the run

        :param context: DAG run context
        :param run_id: This is to help distinguish the run id of the pipeline run,
        defaults to ""
        """
        runtime_custom_config = (
            AirflowPipelineConfig.deserialize_runtime_params_from_airflow(
                params=context["params"]
            )
        )
        runtime_bq_params = runtime_custom_config.get(BQ_PARAMS_KEY, None)
        runtime_graph_params = runtime_custom_config.get(GRAPH_INFO_KEY, None)
        runtime_graph_id_params = runtime_custom_config.get(GRAPH_ID_INFO_KEY, None)

        bq_custom_config, graph_custom_config, graph_id_custom_config = {}, {}, {}

        logger.info(f"Default Custom Config: {self.custom_config}")
        # Construct new config for preparation to be passed into DGXSqlJob
        # Get static config's bq params with project id, location, runid, GraphInfo
        # Then update

        # update bq params
        if runtime_bq_params:
            bq_custom_config = {
                **self.custom_config[BQ_PARAMS_KEY],
                COMPUTE_PROJECT_ID_KEY: self.project_id,
                COMPUTE_LOCATION_KEY: self.location,
                RUN_ID_KEY: run_id,
            }

            bq_custom_config.update(**runtime_bq_params)

        # update graph info
        if runtime_graph_params:
            graph_custom_config = {**self.custom_config[GRAPH_INFO_KEY]}
            graph_custom_config.update(**runtime_graph_params)

        # update graph id info
        if runtime_graph_id_params:
            graph_id_custom_config = {**self.custom_config[GRAPH_ID_INFO_KEY]}
            graph_id_custom_config.update(**runtime_graph_id_params)

        self.custom_config.update(
            {
                BQ_PARAMS_KEY: bq_custom_config or self.custom_config[BQ_PARAMS_KEY],
                GRAPH_INFO_KEY: graph_custom_config
                or self.custom_config[GRAPH_INFO_KEY],
                RECURRING_RUN_FLAG: runtime_custom_config.get(RECURRING_RUN_FLAG, None)
                or self.custom_config[RECURRING_RUN_FLAG],
                GRAPH_ID_INFO_KEY: runtime_graph_id_params
                or self.custom_config[GRAPH_ID_INFO_KEY],
            }
        )
        # Update custom config bq_params with runtime custom config
        logger.info(f"Updated custom config from runtime config:{self.custom_config}")
        return self.custom_config

    def _update_next_execution_date_to_graph_date(
        self, context: Any, custom_config: Dict
    ):
        """Update graph date with execution date if it's a recurring run

        :param context: DAG runtime context
        :param custom_config: custom config dictionary with graph_info_key and
        recurring run key
        """

        # replace execution_date with graph_info, only if it's a recurring run
        if custom_config[RECURRING_RUN_FLAG]:
            graph_date = (
                GraphDate.from_date(
                    context[EXECUTION_DATE_KEY].strftime("%Y%m%d"),
                )
                .next_output_graph_date()
                .strftime("%Y%m%d")
            )
            _dow = GraphDate._to_dow(datetime.now())

            custom_config[GRAPH_INFO_KEY].update(
                {
                    # 2 Tuesdays after relative to execution date
                    GRAPH_DATE_KEY: graph_date,
                    ZERO_INDEX_FUTURE_WEEK: _dow <= parse(graph_date),
                }
            )
        return custom_config

    def job_configuration(self, *args, **kwargs):
        raise NotImplementedError

    def handle_bq_outputs(
        self,
        job: DGXSqlJob,
        bq_client: BqClient,
        graph_id_info: GraphIdInfo,
        run_id: Optional[str] = None,
        output_alias: Optional[str] = None,
    ):
        """Writes to bq destination table handler

        :param output_table: output table name
        :param output_splitter: sql splitter, defaults to "."

        :return: configuration for query, regular table id, wild card table id
        """
        query = job.union_shards_query() if job.union_shards_flag else job.query()
        logger.info(f"Query: {query}")
        (
            output_table_id,
            _,
            _,
        ) = job.resolve_output_properties(write_disposition=self.write_disposition)
        # check if partition exist in output_table_id
        partition = None

        if "$" in output_table_id:
            output_table_id, partition = output_table_id.split("$")
        tbl = Table(output_table_id)
        project_id, dataset, output_table = tbl.project, tbl.dataset_id, tbl.table_id

        # Create graph_id dataset if needed
        bq_client.create_dataset_for_graph_id(
            graph_id_info=graph_id_info,
            labels=self.labels,
        )

        wild_output_table_id = None
        if (job.is_prod or job.is_recurring) and partition:
            output_table = f"{output_table_id}${partition}"
            if self.wild_card_key:
                wild_output_table_id = f"{project_id}.{dataset}.{output_alias}*"
        else:
            if self.wild_card_key:
                output_table = (
                    f"{project_id}.{dataset}.{output_alias}_{run_id}_{output_table}"
                )
                wild_output_table_id = (
                    f"{project_id}.{dataset}.{output_alias}_{run_id}_*"
                )
            else:
                output_table = f"{project_id}.{dataset}.{output_table}"
                if run_id and not (job.is_prod or job.is_recurring):
                    output_table += f"_{run_id}"

        return (
            query,
            output_table,
            wild_output_table_id,
            partition,
        )

    def airflow_prep(self, context: Any):
        run_id = self.xcom_pull(context, key=RUN_ID_KEY)
        # update custom config with runtime config
        custom_config = self._resolve_runtime_config(context=context, run_id=run_id)
        # Get all input xcom tables
        job_params = self._resolve_xcom_input_tables(
            context=context, custom_config=custom_config
        )
        # update next execution date to graph date in graph info (for recurring runs)
        job_params = self._update_next_execution_date_to_graph_date(
            context=context, custom_config=job_params
        )
        logger.info(f"Updated job parameters: {job_params}")
        # update graph id info
        graph_id_info: GraphIdInfo = extract_graph_id_info_from_custom_config(
            custom_config
        )
        job_params[BQ_PARAMS_KEY].update({GRAPH_ID_KEY: graph_id_info.graph_id})
        if self.labels:
            self.labels = gcp_labels.dict_cleaner(
                {GRAPH_ID_KEY: graph_id_info.graph_id, **self.labels}
            )

        return job_params, custom_config, graph_id_info

    def write_xcom_keys(
        self,
        job: DGXSqlJob,
        custom_config: Dict,
        context: Dict,
        output_table_id: str = None,
        wildcard_output_table_id=None,
    ):

        output_alias = custom_config[BQ_PARAMS_KEY].get(OUTPUT_ALIAS_KEY, None)
        if output_alias:
            xcom_key = output_alias
            # regular or shard {key: output_table_id}
            curr_shard = custom_config[BQ_PARAMS_KEY].get(CURRENT_SHARD_KEY, None)
            if curr_shard is not None and curr_shard >= 0:
                xcom_key += f"_shard_{job.current_shard}"
            self.xcom_push(
                context=context,
                key=xcom_key,
                value={
                    "value": output_table_id,
                    "type": INPUT_TABLE_KEY,
                },
            )
            if wildcard_output_table_id:
                xcom_key = wild_card_key(output_alias)
                self.xcom_push(
                    context=context,
                    key=xcom_key,
                    value={
                        "value": wildcard_output_table_id,
                        "type": INPUT_TABLE_KEY,
                    },
                )

    def update_table_expiracy(
        self,
        bq_client: BqClient,
        table_id: str,
        table_expiration_days_offset: Optional[int] = None,
        partition_expiration_days_offset: Optional[int] = None,
    ):
        """
        Update table/partition expircy
        if table doesn't exist before this component executes
        and user specifies the new expiration values
        """

        if not table_expiration_days_offset and not partition_expiration_days_offset:
            return

        table = bq_client.client.get_table(table_id)

        _update_fields = []
        if table_expiration_days_offset:
            table_expires = datetime.now(timezone.utc) + timedelta(
                days=table_expiration_days_offset
            )
            table.expires = table_expires
            _update_fields.append("expires")

        if partition_expiration_days_offset:
            partition_expiration_ms_offset = (
                partition_expiration_days_offset * 24 * 60 * 60 * 1000
            )
            table.time_partitioning = TimePartitioning(
                type_=TimePartitioningType.DAY,
                expiration_ms=partition_expiration_ms_offset,
            )
            _update_fields.append("timePartitioning")

        bq_client.client.update_table(table, _update_fields)

    @staticmethod
    def table_exists(bq_client: BqClient, output_table_id: str):
        """
        Check if table or partition exists
        """
        try:
            _table, _partition = output_table_id.split("$")
            table_exists = bq_client.table_exists(_table)
        except NotFound:
            table_exists = False
        except ValueError:
            table_exists = bq_client.table_exists(output_table_id)
        return table_exists

    @staticmethod
    def create_table(
        bq_client: BqClient,
        output_table_id: str,
        schema: Any,
        time_partitioning: TimePartitioning,
        clustering_fields: List[str],
    ):
        """
        Create the table if it doesn't exist
        """
        is_partitioned = "$" in output_table_id

        table = Table(
            table_ref=(
                output_table_id.split("$")[0] if is_partitioned else output_table_id
            ),
            schema=schema,
        )
        if time_partitioning:
            table.time_partitioning = time_partitioning

        if clustering_fields:
            table.clustering_fields = clustering_fields

        bq_client.client.create_table(table, exists_ok=True)
