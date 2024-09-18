import collections
import logging
import subprocess

from collections import defaultdict
from copy import deepcopy
from typing import Any, Dict, Optional, Text, Union

from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud.bigquery import OperationType, Table
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import TimePartitioning, TimePartitioningType

from airflow_common.common.operators.bigquery.base import BaseBQInsertJobOperator
from airflow_common.constants import (
    BQ_PARAMS_KEY,
    CURRENT_SHARD_KEY,
    INPUT_AGG_ARTIFACTS_KEY,
    INPUT_TABLE_KEY,
    INPUT_XCOM_KEY,
    OUTPUT_ALIAS_KEY,
    RUN_ID_KEY,
    TOTAL_SHARD_KEY,
    UNION_SHARDS_FLAG,
)
from tapad_common.bigquery.job import DGXSqlJob
from tapad_common.dates import resolve_lookback_end_date
from tapad_common.gcp.resources.bigquery.client import BqClient
from tapad_common.utils import find_subclasses_of

logger = logging.getLogger(__name__)


class BigQueryComponent(BaseBQInsertJobOperator):
    def __init__(
        self,
        module_file: Text,
        virtualenv_path: Text,
        **kwargs,
    ):
        # Activate the virtual environment
        activate_script = f"{virtualenv_path}/bin/activate"
        subprocess.run(["source", activate_script], shell=True, check=True)

        super().__init__(module_file=module_file, **kwargs)

    def execute(self, context: Any):

        logger.info(f"DAG Runtime Context: {context}")
        # Logic for updating static custom config with runtime parameters
        job_params, custom_config, graph_id_info = self.airflow_prep(context)
        try:
            job: DGXSqlJob = find_subclasses_of(
                DGXSqlJob, self.module_file.split(".py")[0].replace("/", ".")
            )[0](job_params)
        except IndexError as exc:
            logger.error(
                f"No subclass of `DGXSqlJob` found in `module_file`: {self.module_file}"
            )
            raise exc

        bq_client: BqClient = BqClient(
            client=BigQueryHook().get_client(
                project_id=self.compute_project_id, location=self.compute_location
            ),
        )
        (
            query,
            output_table_id,
            wildcard_output_table_id,
            partition,
        ) = self.handle_bq_outputs(
            job=job,
            bq_client=bq_client,
            graph_id_info=graph_id_info,
            run_id=custom_config[BQ_PARAMS_KEY].get(RUN_ID_KEY, None),
            output_alias=custom_config[BQ_PARAMS_KEY].get(OUTPUT_ALIAS_KEY, None),
        )

        logger.info(
            f"Using Cloud resource: {self.compute_project_id}, {self.compute_location}"
        )
        self.handle_bq_schema(
            job=job, bq_client=bq_client, output_table_id=output_table_id
        )
        logger.info(f"Writing to: {output_table_id}")
        self.configuration = self.job_configuration(
            query=query, destination_table=output_table_id, partition=partition, job=job
        )
        logger.info(f"Configurations: {self.configuration}")
        super().execute(context=context)

        logger.info(f"Job Id: {self.job_id}")
        self.write_xcom_keys(
            job=job,
            custom_config=custom_config,
            context=context,
            output_table_id=output_table_id,
            wildcard_output_table_id=wildcard_output_table_id,
        )

    def handle_bq_schema(
        self, job: DGXSqlJob, bq_client: BqClient, output_table_id: str
    ):
        """
        Create table with schema, partition, clustering fields if provided.
        If table exists but not a partitoned table,
        deletes the existing one and creates a new one.
        Checks whether the output table is partitioned and
        create the table if it doesn't exist
        """
        if job.schema():
            table_exists = BaseBQInsertJobOperator.table_exists(
                bq_client, output_table_id
            )

            schema = BigQueryComponent._to_schema_fields(job.schema())

            is_partitioned = "$" in output_table_id
            if table_exists:
                if (job.is_prod or job.is_recurring) and is_partitioned:
                    return
                logger.info(f"Deleting: {output_table_id}")
                bq_client.delete_table(table_id=output_table_id, verbose=True)

            logger.info(f"Creating: {output_table_id}")

            default_time_partition = (
                TimePartitioning(type_=TimePartitioningType.DAY)
                if is_partitioned
                else None
            )
            BaseBQInsertJobOperator.create_table(
                bq_client=bq_client,
                output_table_id=output_table_id,
                schema=schema,
                time_partitioning=(
                    TimePartitioning(
                        type_=self.time_partition_type, field=self.time_partition_field
                    )
                    if self.time_partition_type
                    else default_time_partition
                ),
                clustering_fields=self.clustering_fields,
            )

            # set these to None avoids error in job_configuration
            self.time_partition_type = None
            self.clustering_fields = None

    @staticmethod
    def _to_schema_fields(schema):
        """Convert `schema` to a list of schema field instances.
        :param schema: Table schema to convert.
        :type project_id: (Sequence[Union[SchemaField, Mapping[str, Any]]])
        """
        for field in schema:
            if not isinstance(field, (SchemaField, collections.abc.Mapping)):
                raise ValueError(
                    "Schema items must either be fields or compatible "
                    "mapping representations."
                )

        return [
            (
                field
                if isinstance(field, SchemaField)
                else SchemaField.from_api_repr(field)
            )
            for field in schema
        ]

    def job_configuration(
        self, query: str, destination_table: str, partition: str, job: DGXSqlJob
    ):
        # For more info on more examples refer to here:
        # noqa: https://github.com/apache/airflow/blob/main/airflow/providers/google/cloud/example_dags/example_bigquery_queries.py
        # more info on big query config params:
        # noqa: https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery
        dest_table_obj = Table(table_ref=destination_table)

        _configuration = {
            "query": {
                "query": query,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": dest_table_obj.project,
                    "datasetId": dest_table_obj.dataset_id,
                    "tableId": dest_table_obj.table_id,
                },
            },
        }
        if self.write_disposition:
            _configuration["query"].update({"writeDisposition": self.write_disposition})
        if self.create_disposition:
            _configuration["query"].update(
                {"createDisposition": self.create_disposition}
            )

        if self.time_partition_type:
            _configuration["query"].update(
                {"timePartitioning": {"type": self.time_partition_type}}
            )
            if self.time_partition_field:
                _configuration["query"]["timePartitioning"].update(
                    {"field": self.time_partition_field}
                )

        if self.clustering_fields:
            _configuration["query"].update(
                {"clustering": {"fields": self.clustering_fields}}
            )
        if self.labels:
            _configuration["labels"] = self.labels

        return _configuration


class BaseShardedBigQueryComponent:
    def __init__(
        self,
        task_id: Text,
        num_shards: int,
        module_file: Text,
        custom_config: Optional[Dict] = {},
        wild_card_key: Optional[bool] = False,
        output_splitter: Optional[Text] = ".",
        labels: Optional[Dict[Text, Text]] = None,
    ):
        assert num_shards >= 2, "num_shards supports >= 2"
        self.num_shards = num_shards
        self.custom_config = custom_config
        self.task_id = task_id
        self.module_file = module_file
        self.output_splitter = output_splitter
        self.labels = labels

        self.sharded_cmps: Dict[str, BigQueryComponent] = None
        self.agg_cmp: BigQueryComponent = None
        self.wait_cmp: EmptyOperator = None

        self.wild_card_key: bool = wild_card_key

    def set_agg_downstream(self):
        assert self.agg_cmp, "Initialize `aggregate_output` to True."
        for cmp in self.sharded_cmps.values():
            cmp.set_downstream(self.agg_cmp)

    def set_wait_downstream(self):
        assert self.wait_cmp, "Initialize `wait_output` to True."
        for cmp in self.sharded_cmps.values():
            cmp.set_downstream(self.wait_cmp)

    def _spawn_aggregate_cmp(self, **kwargs):
        _custom_config = deepcopy(self.custom_config)
        _custom_config[BQ_PARAMS_KEY] = {
            **_custom_config[BQ_PARAMS_KEY],
            INPUT_XCOM_KEY: [
                f"{_custom_config[BQ_PARAMS_KEY][OUTPUT_ALIAS_KEY]}_shard_{idx}"
                for idx in range(self.num_shards)
            ],
            UNION_SHARDS_FLAG: True,
        }
        if (
            INPUT_XCOM_KEY in _custom_config[BQ_PARAMS_KEY]
            and INPUT_TABLE_KEY in _custom_config[BQ_PARAMS_KEY]
        ):
            _custom_config[BQ_PARAMS_KEY].pop(INPUT_TABLE_KEY)

        return BigQueryComponent(
            task_id=f"{self.task_id}_aggregator",
            custom_config=_custom_config,
            module_file=self.module_file,
            output_splitter=self.output_splitter,
            labels=self.labels,
            **kwargs,
        )

    def _spawn_shard_cmps(self, **kwargs):
        sharded_cmps = {}
        for shard_id in range(self.num_shards):
            # ingest shardings into static custom config
            _custom_config = deepcopy(self.custom_config)
            _custom_config[BQ_PARAMS_KEY] = {
                **_custom_config[BQ_PARAMS_KEY],
                **{TOTAL_SHARD_KEY: self.num_shards, CURRENT_SHARD_KEY: shard_id},
            }
            bq_cmp = BigQueryComponent(
                task_id=f"{self.task_id}_shard_{shard_id}",
                custom_config=_custom_config,
                module_file=self.module_file,
                output_splitter=self.output_splitter,
                wild_card_key=self.wild_card_key,
                **kwargs,
            )
            sharded_cmps[f"{self.task_id}_shard_{shard_id}"] = bq_cmp
        return sharded_cmps


class ShardedBigQueryComponent(BaseShardedBigQueryComponent):
    """Regular BQ Sharding; Split query into multiple shards
    and aggregate together (optionally)
    """

    def __init__(
        self,
        task_id: Text,
        num_shards: int,
        module_file: Text,
        custom_config: Optional[Dict] = {},
        output_splitter: Optional[Text] = ".",
        wait_output: Optional[bool] = False,
        aggregate_output: Optional[bool] = True,
        wild_card_key: Optional[bool] = False,
        labels: Optional[Dict[Text, Text]] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            task_id=task_id,
            num_shards=num_shards,
            module_file=module_file,
            custom_config=custom_config,
            output_splitter=output_splitter,
            wild_card_key=wild_card_key,
            labels=labels,
        )
        self.sharded_cmps = self._spawn_shard_cmps(**kwargs)
        if aggregate_output:
            self.agg_cmp = self._spawn_aggregate_cmp(**kwargs)
            self.set_agg_downstream()
        if wait_output:
            self.wait_cmp = EmptyOperator(
                task_id=f"{self.task_id}_wait_shards",
                dag=kwargs["dag"],
            )
            self.set_wait_downstream()


class SplitToSplitShardedBigQueryComponent(BaseShardedBigQueryComponent):
    def __init__(
        self,
        task_id: Text,
        num_shards: int,
        module_file: Text,
        custom_config: Optional[Dict] = {},
        output_splitter: Optional[Text] = ".",
        upstream_sharded_cmps: Optional[Dict[Text, BigQueryComponent]] = {},
        aggregate_output: Optional[bool] = False,
        wait_output: Optional[bool] = False,
        wild_card_key: Optional[bool] = False,
        connect_to_upstream: Optional[bool] = True,
        labels: Optional[Dict[Text, Text]] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            task_id=task_id,
            num_shards=num_shards,
            module_file=module_file,
            custom_config=custom_config,
            output_splitter=output_splitter,
            wild_card_key=wild_card_key,
            labels=labels,
        )
        self.upstream_sharded_cmps = upstream_sharded_cmps
        self.connect_to_upstream = connect_to_upstream
        self.sharded_cmps = self.upstream_sharded_cmps_query(**kwargs)
        if wait_output:
            self.wait_cmp = EmptyOperator(
                task_id=f"{self.task_id}_wait_shards",
                dag=kwargs["dag"],
            )
            self.set_wait_downstream()
        if aggregate_output:
            self.agg_cmp = self._spawn_aggregate_cmp(**kwargs)
            self.set_agg_downstream()

        self._check_valid_number_of_splits()

    def _check_valid_number_of_splits(self):
        unique_task_ids = defaultdict(int)
        for task_id in self.upstream_sharded_cmps.keys():
            shard_idx = int(task_id.split("_")[-1])
            id_name = "".join(task_id.split("_")[:-1])
            unique_task_ids[id_name] = max(shard_idx, unique_task_ids.get(id_name, 0))

        for task_id, splits in unique_task_ids.items():
            num_shard = splits + 1  # because index of split starts at 0
            assert (
                num_shard == self.num_shards
            ), f"{task_id} splits {num_shard} must equal to {self.num_shards}"

    def upstream_sharded_cmps_query(self, **kwargs):
        """
        Taking in a set of sharded components. For each sharded component, want to set
        it's downstream component to the newly spawned bq component.
        """
        sharded_cmps = {}

        for task_id, cmp in self.upstream_sharded_cmps.items():
            shard_idx = int(task_id.split("_")[-1])  # indicates which split we're on
            _custom_config = deepcopy(self.custom_config)

            # get unsharded xcom inputs
            _input_xcom_keys = _custom_config[BQ_PARAMS_KEY].get(INPUT_XCOM_KEY, [])
            # update with sharded xcom input from upstream compnent
            _input_xcom_keys.extend(
                [
                    comp_name
                    for comp_name in self.upstream_sharded_cmps.keys()
                    if int(comp_name.split("_")[-1]) == shard_idx
                ]
            )
            assert (
                _input_xcom_keys
            ), "Must have upstream shards for split to split sharding"

            _custom_config[BQ_PARAMS_KEY] = {
                **_custom_config[BQ_PARAMS_KEY],
                INPUT_XCOM_KEY: _input_xcom_keys,
                CURRENT_SHARD_KEY: shard_idx,
            }
            if f"{self.task_id}_shard_{shard_idx}" not in sharded_cmps:
                bq_cmp = BigQueryComponent(
                    task_id=f"{self.task_id}_shard_{shard_idx}",
                    custom_config=_custom_config,
                    module_file=self.module_file,
                    output_splitter=self.output_splitter,
                    wild_card_key=self.wild_card_key,
                    **kwargs,
                )
                sharded_cmps[f"{self.task_id}_shard_{shard_idx}"] = bq_cmp

        for cmp_id, cmp in self.upstream_sharded_cmps.items():
            shard_idx = int(cmp_id.split("_")[-1])

        if self.connect_to_upstream:
            for cmp_id, cmp in self.upstream_sharded_cmps.items():
                cmp.set_downstream(sharded_cmps[f"{self.task_id}_shard_{shard_idx}"])

        return sharded_cmps


class BigQueryDataTransferComponent(BaseBQInsertJobOperator):
    class _DummySqlJob(DGXSqlJob):
        def output_table_name(self) -> Optional[Union[str, Dict[str, str]]]:
            partition_date = resolve_lookback_end_date(
                self.graph_info.graph_date, self._PARTITION_DATE_FORMAT
            )
            return f"{self.params[OUTPUT_ALIAS_KEY]}${partition_date}"

        def query(self):
            pass

    def __init__(
        self,
        operation_type: Optional[str] = OperationType.COPY,
        table_expiration_days_offset: Optional[int] = None,
        partition_expiration_days_offset: Optional[int] = None,
        **kwargs,
    ) -> None:
        if table_expiration_days_offset:
            assert table_expiration_days_offset > 0 and isinstance(
                table_expiration_days_offset, int
            ), "table_expiration_days_offset must be a positive integer"

        if partition_expiration_days_offset:
            assert partition_expiration_days_offset > 0 and isinstance(
                partition_expiration_days_offset, int
            ), "partition_expiration_days_offset must be a positive integer"

        super().__init__(**kwargs)
        self.operation_type = operation_type

        self.table_expiration_days_offset = table_expiration_days_offset
        self.partition_expiration_days_offset = partition_expiration_days_offset

    def execute(self, context: Any):
        logger.info(f"DAG Runtime Context: {context}")
        # Logic for updating static custom config with runtime parameters
        job_params, custom_config, graph_id_info = self.airflow_prep(context)

        input_table = job_params[BQ_PARAMS_KEY].get(INPUT_TABLE_KEY, None)
        input_tables = job_params[BQ_PARAMS_KEY].get(INPUT_AGG_ARTIFACTS_KEY, None)
        source_table = []
        if input_table:
            source_table.append(input_table)
        if input_tables:
            for val in input_tables.values():
                if val and "value" in val:
                    source_table.append(val["value"])

        bq_client: BqClient = BqClient(
            client=BigQueryHook().get_client(
                project_id=self.compute_project_id, location=self.compute_location
            ),
        )
        job = self._DummySqlJob(params=job_params)
        (
            _,
            output_table_id,
            wildcard_output_table_id,
            partition,
        ) = self.handle_bq_outputs(
            job=job,
            bq_client=bq_client,
            graph_id_info=graph_id_info,
            run_id=custom_config[BQ_PARAMS_KEY].get(RUN_ID_KEY, None),
            output_alias=custom_config[BQ_PARAMS_KEY].get(OUTPUT_ALIAS_KEY, None),
        )

        logger.info(
            f"Using Cloud resource: {self.compute_project_id}, {self.compute_location}"
        )
        logger.info(f"Writing to: {output_table_id}")
        self.configuration = self.job_configuration(
            src_table=source_table,
            dest_table=output_table_id,
        )
        logger.info(f"Configurations: {self.configuration}")

        # check if table exists
        _is_table_first_creation = not BaseBQInsertJobOperator.table_exists(
            bq_client, output_table_id
        )

        if (job.is_prod or job.is_recurring) and partition:
            # create partition table if it's a prod or recurring run
            # set clustering fields if provided
            BaseBQInsertJobOperator.create_table(
                bq_client=bq_client,
                output_table_id=output_table_id,
                schema=bq_client.client.get_table(source_table[0]).schema,
                time_partitioning=TimePartitioning(
                    type_=TimePartitioningType.DAY,
                ),
                clustering_fields=self.clustering_fields,
            )

        super().execute(context=context)

        logger.info(f"Job Id: {self.job_id}")
        self.write_xcom_keys(
            job=job,
            custom_config=custom_config,
            context=context,
            output_table_id=output_table_id,
            wildcard_output_table_id=wildcard_output_table_id,
        )

        if _is_table_first_creation:
            self.update_table_expiracy(
                bq_client=bq_client,
                table_id=output_table_id.split("$")[0],
                table_expiration_days_offset=self.table_expiration_days_offset,
                partition_expiration_days_offset=self.partition_expiration_days_offset,
            )

    def job_configuration(self, src_table, dest_table):
        list_src_tbl = []
        for tbl in src_table:
            src = Table(tbl)
            table_id = src.table_id
            list_src_tbl.append(
                {
                    "projectId": src.project,
                    "datasetId": src.dataset_id,
                    "tableId": table_id,
                }
            )

        dest = Table(dest_table)
        dest_project_id, dest_dataset, dest_table_id = (
            dest.project,
            dest.dataset_id,
            dest.table_id,
        )
        _configuration = {
            "copy": {
                "sourceTables": list_src_tbl,
                "destinationTable": {
                    "projectId": dest_project_id,
                    "datasetId": dest_dataset,
                    "tableId": dest_table_id,
                },
                "operationType": self.operation_type,
            }
        }

        if self.write_disposition:
            _configuration["copy"].update({"writeDisposition": self.write_disposition})
        if self.create_disposition:
            _configuration["copy"].update(
                {"createDisposition": self.create_disposition}
            )

        if self.labels:
            _configuration["labels"] = self.labels

        return _configuration
