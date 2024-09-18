from tapad_common.config.gcp import Location

BQ_FORMAT = "bigquery"
AVRO_FORMAT = "avro"
DG5_AVRO_FORMAT = "dg5_avro"
JSON_FORMAT = "json"
CSV_FORMAT = "csv"
TEXT_FORMAT = "txt"
TF_RECORD_FORMAT = "tfrecords"
TF_RECORD_GZIP_FORMAT = "gz"

APPROX_ZERO = 1.11e-16

DATAFLOW_API = "dataflow"
DATAFLOW_VERSION = "v1b3"

ML_API = "ml"
ML_VERSION = "v1"

DEVICE_INFO_KEY = "device_info"
LOCATION_INFO_KEY = "location_info"

OUTPUT_TFRECORDS_KEY = "output_tfrecords"
OUTPUT_AVRO_KEY = "output_avro"
OUTPUT_CSV_KEY = "output_csv"
OUTPUT_TEXT_KEY = "output_text"

RELEVANT_DEVICE_HISTORY_KEY = "relevant_device_history"
INPUT_PATH_KEY = "input_path"
INPUT_TABLE_KEY = "input_table"
INPUT_XCOM_KEY = "input_xcom_task_id"
INPUT_AGG_ARTIFACTS_KEY = "input_aggregated_artifacts"
OP_NAME_KEY = "op_name"

RUN_ID_KEY = "run_id"
STATIC_CUSTOM_CONFIG_KEY = "static_custom_config"

GRAPH_ID_KEY = "graph_id"
GRAPH_ID_INFO_KEY = "graph_id_info"
GRAPH_ID_PREFIX_KEY = "graph_id_prefix"
BQ_DS_DEFAULT_EXPIRY_DAYS_KEY = "bq_dataset_default_expiry_days"

STABLE_TABLE_FLAG = "stable_table_flag"
RECURRING_RUN_FLAG = "recurring_run_flag"
OUTPUT_DATASET_KEY = "output_dataset"
OUTPUT_BUCKET_KEY = "output_bucket"

PUSHED_MODEL_URI_KEY = "pushed_model_uri"
REFERENCE_GRAPH_KEY = "reference_graph"

CURRENT_SHARD_KEY = "curr_shard"
TOTAL_SHARD_KEY = "num_shard"

LABELS_KEY = "labels"

LOCAL_METADATA_PATH = "/tmp/metadata.sqlite"

TRAINING_PIPELINE_NAME_KEY = "training_pipeline_name"
MODEL_TYPE_KEY = "model_type"
MODEL_TYPE_TF_ESTIMATOR = "estimator"
MODEL_TYPE_TF_KERAS = "keras"

MODULE_FILE_KEY = "module_file"

PROJECT_ID_KEY = "project_id"
ENVIRONMENT_KEY = "env"
LOCATION_KEY = "location"

COMPUTE_PROJECT_ID_KEY = "compute_pid"
COMPUTE_LOCATION_KEY = "compute_loc"
DATA_PROJECT_ID_KEY = "data_pid"
DATA_LOCATION_KEY = "data_loc"

REGION_KEY = "region"
GRAPH_DATE_KEY = "graph_date"
DG5_GRAPH_TYPE_KEY = "dg5_graph_type"
OUTPUT_TABLE_KEY = "output_table"
OUTPUT_PATH_KEY = "output_path"
AVRO_OUTPUT_PATH_KEY = "avro_output_path"
AGG_INPUT_KEY = "agg_input"
HH_INPUT_KEY = "hh_input"
INDV_INPUT_KEY = "indv_input"
BQ_DATASET_ID_KEY = "bq_dataset_id"
MODEL_PUSH_URI_KEY = "model_push_uri"
SERVING_ARGS_KEY = "bigquery_serving_args"
PARTITION_TIME_KEY = "partition_table"
ROW_CAP = "row_cap"
UNION_SHARDS_FLAG = "union_bq_shards_flag"
DEVICE_GRAPH_OUTPUT_TABLE = "device_graph_v001"
DEVICE_INFO_TABLE_KEY = "device_info_table_key"
LOCATION_INFO_TABLE_KEY = "location_info_table_key"
AGG_ALIAS_TABLE_KEY = "agg_alias_table_key"
AGG_CANONICAL_HISTORY_TABLE_KEY = "agg_canonical_history_table_key"
NON_PERSISTENT_HH_TABLE_KEY = "non_persistent_hh"
OUTPUT_ALIAS_KEY = "output_alias"
DEVICE_DATA_TABLE_KEY = "device_data_table"
TFIDF_CALCULATION_CONFIG_KEY = "tfidf_calculation_config"
ANONYMIZE_IDS_KEY = "anonymize_ids"

DG5_PYTHON_IMAGE = "us.gcr.io/tapad-registry/devicegraph-5/dg5-python"

DGX_INTEGRATION_TEST_LOCAL_PATH = "assets/mock_data/integration_testing"
DGX_SERVICE_ACCOUNT = "dgx-workflows@%(project_id)s.iam.gserviceaccount.com"
DGX_SA_KUBE_SECRET_KEY = "dgx-workflows"

# TODO: Depreciate dlr and replace to canonical
MAX_EVENTS_PER_DEVICE = "max_events_per_device"
MAX_IPS_PER_DEVICE = "max_ips_per_device"
MAX_VOCAB_ARRAY_SIZE = "max_vocab_array_size"

NUM_EASY_TRIPLETS = "num_easy_triplets"
NUM_HARD_TRIPLETS = "num_hard_triplets"
EVAL_SPLIT = "eval_split"
BATCH_SIZE = "batch_size"

DEVICE2VEC_DATASET_GENERATION_OUTPUT = "denormalized_triplets_v003"
DEVICE2VEC_DATASET_TRAIN_EVAL_SPLIT = "d2v_truth_data_split_v001"

# subfolder names for D2V GCS output path
D2V_DEVICE_DATASET_VOCAB = "d2v_device_dataset_vocab"
D2V_DEVICE_DATASET_HISTORY = "d2v_device_dataset_history"
D2V_EMBEDDINGS = "d2v_embeddings"
D2V_TRIPLETS_DATASET = "d2v_triplets_dataset"

IP_VECTOR_SIZE = 32
SUCCESS_FILE = "_SUCCESS"

DF_RUNNER_V2 = "use_runner_v2"
DF_ENABLE_PRIME = "enable_prime"
DF_FLEXRS_GOAL = "dataflow_flexrs_goal"
COST_OPTIMIZED = "COST_OPTIMIZED"
SPEED_OPTIMIZED = "SPEED_OPTIMIZED"

# Common Runtime Parameter Keys
IS_RECURRING_KEY = "is-recurring"
CUSTOM_CONFIG_RUNTIME_NAME = "custom-config"

# Runtime parameter keys
GRAPH_INFO_KEY = "graph_info"
BQ_PARAMS_KEY = "bq_custom_parameters"
BEAM_PARAMS_KEY = "beam_custom_parameters"
TRAINER_PARAMS_KEY = "trainer_custom_parameters"
TRAINER_MODEL_DIR_KEY = "trainer_model_dir"
TRAINER_MODEL_RUN_DIR_KEY = "trainer_model_run_dir"
TUNER_PARAMS_KEY = "tuner_custom_parameters"
PUSHER_PARAMS_KEY = "pusher_custom_parameters"
ZERO_INDEX_FUTURE_WEEK = "zero_index_future_week"

# Aggregation Standard Events
TIMESTAMP_LIMIT = "timestamp_limit"
NUM_DGX_HISTORY_SHARDS = "num_dgx_history_shards"
AGG_HISTORY_LIMIT = "agg_history_limit"
CANONICAL_ID_LIMIT = "canonical_id_limit"
MAX_UNIQUE_IPS = "max_unique_ips"
MAX_DISTINCT_IP_AGG = "max_distinct_ip_agg"
MAX_HISTORY = "max_history"
MAX_ALIAS_HASH = "max_alias_hash"
MAX_TIMESTAMP_HISTORY = "max_timestamp_history"
FIELDS_SELECTION = "fields_selection"
FILTER_IDS_WITH_MORE_TS_THAN = "filter_ids_with_more_ts_than"
MIN_HOUSEHOLD_SIZE = "min_household_size"
MAX_HOUSEHOLD_SIZE = "max_household_size"
AUTOTUNE_FLAG = "autotune_flag"
HH_HISTORY_LENGTH_LIMIT = "hh_history_length_limit"
HH_HISTORY_AND_SIZE_FILTER_TABLE = "dgx_hh_history_length_and_size_filtered_table_v001"
HH_HISTORY_AND_SIZE_FILTER_TABLE_ALIAS = "hh_history_length_and_size_filter_alias"
MIN_SIGHTING = "min_sighting"
MIN_LOSS = "min_loss"
DEFAULT_MIN_LOSS_THRESHOLD = "default_min_loss_threshold"
MIN_LOSS_BY_ID_TYPE = "min_loss_by_id_type"

# Aggregation Alias Hash
NUM_DG5_ALIAS_SHARDS = "num_dg5_alias_shards"
NUM_DG5_HISTORY_SHARDS = "num_dg5_history_shards"


# Aggregation Alias Hash
NUM_DG5_ALIAS_SHARDS = "num_dg5_alias_shards"
NUM_DG5_HISTORY_SHARDS = "num_dg5_history_shards"

# Device Clustering Metrics
DC_DEVICE_GRAPH_KEY = "device_graph_sensor"
DC_DEVICE_CLUSTER_KEY = "device_cluster_sensor"
DC_DEVICE_SIZES_KEY = "device_sizes"
DC_HH_SIZES_KEY = "hh_sizes"
DC_ALIAS_TYPE_MAPPING_KEY = "alias_type_mapping"
DC_DEVICE_COMPOSITION_KEY = "device_composition"
DC_ID_TYPE_CONNECTIONS_KEY = "id_type_connections"
DC_SINGLE_ALIAS_DEVICE_KEY = "single_alias_device"
DC_DEVICE_METRICS_AGG_KEY = "device_metrics_agg"


# Data Discrepancy Metrics
DEVICE_CLUSTER_NUM_DUPLICATE_ALIAS_KEY = "device_cluster_num_duplicate_alias"
AGG_ALIAS_NUM_CANONICALS_KEY = "agg_alias_num_canonicals"
AGG_CANONICAL_HISTORY_NUM_CANONICALS_KEY = "agg_canonical_history_num_canonicals"
NON_PERSISTENT_HH_NUM_CANONICALS_KEY = "non_persistent_hh_num_canonicals"
DATA_DISCREPANCY_AGG_KEY = "data_discrepancy_agg"

# Airflow context variables
EXECUTION_DATE_KEY = "execution_date"

# Auto Tuner config keys
USE_HH_FILTER = "use_hh_filter"
HH_FILTER_TABLE_KEY = "hh_filter_table_key"
COUNTRY_HHS_MAPPING = "country_hh_size_mapping"
HH_AUTOTUNER_JOB_PARAMS = "hh_autotuner_params"

# Auto Tuner Alias names
PROBLEM_TYPE = "problem_type"
LENGTH = "length"
MAXIMIZE = "maximize"
CURVE = "curve"
RANDINT_MAX_RANGE_MAX_HH_STATE = "randint_max_range_max_hh_state"
RANDINT_MAX_RANGE_MIN_HH_STATE = "randint_max_range_min_hh_state"

# Auto Tuner hyperpameter keys
TARGET_HH_KEY = "target_hh"
TARGET_PHONE_DEVICES_KEY = "target_phone_devices"
TARGET_ANDROID_DEVICES_KEY = "target_android_devices"
TARGET_IDFA_DEVICES_KEY = "target_idfa_devices"
TARGET_CTV_DEVICES_KEY = "target_ctv_devices"
PARAMS_KEY = "params"
INIT_STATE_KEY = "init_state"
STEP_SIZES_KEY = "step_sizes"
MAX_ATTEMPTS_KEY = "max_attempts"
MAX_ITERS_KEY = "max_iters"
RESTARTS_KEY = "restarts"
RANDOM_SEED_KEY = "random_seed"

HISTORY_TABLE_TIMESTAMPS_SUMMARY_UDF_TYPE = (
    "timestamps_summary ARRAY<STRUCT<timestamp TIMESTAMP, cnt INT64>>"
)


class SizeConversions(object):
    # converting bytes to different sizes
    MB = 1000000
    GB = MB * 1000
    TB = GB * 1000


class StandardGraphs(object):
    DG5 = "v5"
    NON_TTD_V5 = "non_ttd_v5"

    SAMPLE_IL = "smp_dg_usa_il_sample_ref_v004"

    QUIQ_US = "smp_dg_quiq_us_60d_1in1000_v001"
    QUIQ_EU = "smp_dg_quiq_eu_88d_1in1000_v001"
    QUIQ_APAC = "smp_dg_quiq_apac_60d_1in1000_v001"

    QUIQ_GRAPHS = [QUIQ_EU, QUIQ_US, QUIQ_APAC]
    SAMPLE_GRAPHS = QUIQ_GRAPHS + [SAMPLE_IL]

    V5_PROD_GRAPHS = [DG5, NON_TTD_V5]

    ALL = SAMPLE_GRAPHS + V5_PROD_GRAPHS

    QUIQ_MAP = {
        Location.US: QUIQ_US,
        Location.EU: QUIQ_EU,
        Location.APAC: QUIQ_APAC,
    }

    EXTERNAL_DATASET_MAP = {
        DG5: "dg_external_us_v001",
        NON_TTD_V5: "dg_external_us_non_ttd_v5_v001",
    }
