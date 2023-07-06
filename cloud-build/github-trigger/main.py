import json
import sys
import os
from datetime import datetime
import traceback
import logging
from processor import Task
from multiprocessing import cpu_count
from datetime import timedelta
import uuid

# Add CloudProcessor classes
from cloudprocessor.equalizer import Equalizer
from cloudprocessor.biq_query import BigQuery
from cloudprocessor.avro import AVRO
from cloudprocessor.cp_io import Data, MetaData, BusinessDate
from cloudprocessor.biq_query import BigQuery
from cloudprocessor.statistics import Statistics
from cloudprocessor.analytics import SourceFileDetection
from cloudprocessor.technical_column import TechnicalColumn
from cloudprocessor.pii_encryption import PIIEncryption
from cloudprocessor.utils import *
from cloudprocessor.derived_columns import DerivedColumns

# Add Common classes
from Common.logging_utils import get_logger
from config_utils import Config
from Common.fileset_completeness_checker import FileSetCompletenessChecker
from Common.gcs_bucket import GcsBucket

# Get Global Variables
TASK_INDEX = os.getenv("CLOUD_RUN_TASK_INDEX", 0)
TASK_ATTEMPT = os.getenv("CLOUD_RUN_TASK_ATTEMPT", 0)
LOG_TYPE = os.getenv("log_type")
BATCH_ID = os.getenv("batch_id")

# App config keys
FILE_SENTINEL_CONFIG = "file_sentinel_conf"
FILE_SET_PATTERNS_CONFIG = "file_set_patterns"
COMMON_CONFIG = "common_conf"
BUCKET_CONFIG = "bucket_conf"
BQ_CONFIG = "bq_conf"
DP_CONFIG = "data_processor_conf"
LOGGING_CONFIG = "logging_conf"
BUCKET_APP_CONFIG = "app_conf"
LOGGER_FORMAT = "common_log_message_pylogger_format"
DP_MESSAGE_EXTENSION = "data_processor_specific_message_extension"
DP_LOG_LEVEL = "data_processor_loglevel"
UNIQUE_ID = uuid.uuid4().hex
SOURCE_MODEL_CONFIG = "source_model_config"
SOURCE_MODEL_FILE_NAME = "rename_source_model_file_to"

stepkey = f"TaskStep_{TASK_INDEX}"
os.environ[stepkey] = "MAIN PROCESS"

# Get the configuration data
config = Config()
config_data, file_list = config.read_config()

# Common Application configuration
app_config = config_data["app_config"] if "app_config" in config_data else {}

# Start Time
main_process_start_time = datetime.now()

# Call Logger class
log = get_logger(
    p_format=app_config[COMMON_CONFIG][LOGGING_CONFIG][LOGGER_FORMAT],
    p_component_name="DataProcessor",
    p_message_extension=app_config[COMMON_CONFIG][LOGGING_CONFIG][DP_MESSAGE_EXTENSION],
    p_batch_id=BATCH_ID if BATCH_ID else "",
    p_batch_start_time=main_process_start_time,
    p_component_run_id=f"{UNIQUE_ID}",
    p_job_container_num=int(TASK_INDEX),
    p_log_level=logging.INFO
    if config
    and COMMON_CONFIG in app_config
    and LOGGING_CONFIG in app_config[COMMON_CONFIG]
    and DP_LOG_LEVEL in app_config[COMMON_CONFIG][LOGGING_CONFIG]
    and app_config[COMMON_CONFIG][LOGGING_CONFIG][DP_LOG_LEVEL] == "INFO"
    else logging.DEBUG,
)

# Main Process Start
log_extra = {"step": "MAIN PROCESS"}
log.info(f"Start", extra=log_extra)

if "app_config" in config_data:
    log.info(f'LOAD CONF : {", ".join(file_list)}', extra={"step": "LOAD CONF"})

# Get Hash Column Blacklist configuration
row_hash_column_blacklist = (
    config_data["row_hash_column_blacklist"]
    if "row_hash_column_blacklist" in config_data
    else {}
)
# Get Technical Column configuration
tech_config = (
    config_data["technical_config"] if "technical_config" in config_data else {}
)

# Get Delta Column map configuration
delta_column_map = (
    config_data["delta_column_map"] if "delta_column_map" in config_data else {}
)

# Retrieve File Set pattern for completeness_checker
file_set_patterns = app_config[COMMON_CONFIG][FILE_SET_PATTERNS_CONFIG]

# Retrieve Project, dataset name and Layer
project = app_config[COMMON_CONFIG][BQ_CONFIG]["raw_staging_project"]
dataset = app_config[COMMON_CONFIG][BQ_CONFIG]["raw_staging_dataset"]
location = app_config[COMMON_CONFIG][BQ_CONFIG]["raw_staging_location"]
partition_expiration_time = app_config[COMMON_CONFIG][BQ_CONFIG][
    "partition_expiration_time"
]
partition_field = app_config[COMMON_CONFIG][BQ_CONFIG]["partition_field"]

proj_dataset = project + "." + dataset
layer = app_config[DP_CONFIG]["dl_raw_layer"]
file_size_limit_for_avro = app_config[DP_CONFIG]["file_size_limit_for_avro"]
upload_by_avro = app_config[DP_CONFIG]["upload_by_avro"]
temp_bucket = app_config[DP_CONFIG]["temp_bucket"]

# Retrieve bucket and folder_name for Exec_conf.json
bucket_name = app_config[COMMON_CONFIG][BUCKET_CONFIG][BUCKET_APP_CONFIG]["conf_bucket"]
folder = (
    app_config[COMMON_CONFIG][BUCKET_CONFIG][BUCKET_APP_CONFIG]["conf_folder"]
    + "/exec_conf.json"
)

# Get Chunk Size
chunk_size = app_config[DP_CONFIG]["file_read_buffer_size_bytes"]

# Get Source model file from config
source_model_name = app_config[FILE_SENTINEL_CONFIG][SOURCE_MODEL_CONFIG][
    SOURCE_MODEL_FILE_NAME
]

# GCS Bucket class call
gb = GcsBucket(p_log=log)

exec_conf = json.loads(gb.read_file_from_gcs_bucket(bucket_name, folder))

# Get Batch Id from exec conf file if we are not getting batch id from environemnt variable.
BATCH_ID = (
    BATCH_ID
    if BATCH_ID is not None
    else exec_conf[COMMON_CONFIG][BUCKET_CONFIG]["batch_id"]
)
log.change_extra("batch_id", f"{BATCH_ID}")
DATA_BUCKET = exec_conf[COMMON_CONFIG][BUCKET_CONFIG]["landing_zone_conf"][
    "data_bucket"
]


def pipeline(
    p_process_no: str,
    p_process_count: int,
    meta_data: dict,
    file_name_list: list,
    avro_to_bq: bool,
):
    """Run pipeline tasks
    Args:
        p_process_no (str): Pipeline name
        p_process_count (int): Process count number
        meta_data (dict): meta_data json dictionary
        file_name_list (list): Filename
        avro_to_bq (bool): Set this true if you want to create Avro file and store in temp bucket
    """
    try:
        log.change_extra("dp_stream_num", f"{p_process_no}")
        if avro_to_bq:
            pipeline = (
                Task.StartTask(p_process_no, p_process_count)
                | Task.inject_data("meta data", meta_data)
                | Task(loader.load_data, DATA_BUCKET, file_name_list, int(chunk_size))
                | Task(detect.analyse_raw_data)
                | Task(eq.equalize, delta_column_map)
                | Task(dc.generate_columns)
                | Task(tc.transform_data)
                | Task(pii.encrypt)
                | Task(avro.write_avro)
                | Task(stats.calculate_stats)
            )
        else:
            pipeline = (
                Task.StartTask(p_process_no, p_process_count)
                | Task.inject_data("meta data", meta_data)
                | Task(loader.load_data, DATA_BUCKET, file_name_list, int(chunk_size))
                | Task(detect.analyse_raw_data)
                | Task(eq.equalize, delta_column_map)
                | Task(dc.generate_columns)
                | Task(tc.transform_data)
                | Task(pii.encrypt)
                | Task(bq.load_table)
                | Task(stats.calculate_stats)
            )
        # | Task(bq.load_table)
        # | Task(avro.write_avro)

        pipeline.execute()
    except Exception as err:
        log_err_msg(err, stepkey, log)


if __name__ == "__main__":
    try:
        # Get cpu_count
        n_cores = cpu_count()

        p_count = 1
        if n_cores > 1:
            p_count = n_cores

        # Set Parallel count as per 2 processor
        # p_count = 8

        task_conf = exec_conf["task_conf"]

        for task_no in task_conf:
            # Run loop according to the tasks
            process_start_time = datetime.now()

            if int(TASK_INDEX) == int(task_no):
                # PROCESS Folder Log
                log_extra = {"step": "PROCESS FOLDER"}
                log.info(f"Start", extra=log_extra)

                # Set Sub folder value
                DATA_FOLDER = task_conf[task_no]["data_folder"]
                prefix = "gs://" + DATA_BUCKET + "/" + DATA_FOLDER
                log.info(f"Folder - {str(prefix)}", extra=log_extra)

                task_dir = DATA_FOLDER.rstrip("/") + "/"

                # Get list of the subfolder inside staging subfolder for example 0/1/2 subfolder
                list_folders = gb.list_gcs_directories(DATA_BUCKET, prefix=task_dir)

                column_name = ""
                # Fetch subfolder files in a loop
                for internal_folder in list_folders:
                    # Set Stream start time
                    stream_start_time = datetime.now()

                    # STREAM Log
                    log_extra = {"step": "STREAM"}
                    log.info(f"Start", extra=log_extra)

                    # Fetch subfolder files like 0 subfolder have 3 files
                    prefix = "gs://" + DATA_BUCKET + "/" + internal_folder


                    object_names = gb.list_files_stored_under_gcs_bucket(prefix)

                    # Get dp_stream_number from the internal_folder name
                    dp_stream_number = internal_folder.split("/")[-2]

                    files_list = []
                    parse_business_date = []
                    data_feed = ""
                    source_system = ""
                    stage_table_name = ""
                    raw_table_name = ""
                    delta_table_name = ""
                    list_of_dates = None
                    src_feed_name = ""
                    avro_to_bq = False

                    # Read files from each internal subfolder
                    for files in object_names:
                        filename = os.path.basename(files)


                        # If filename is not empty/same as data folder/source_meta_data
                        if filename is not None and filename not in [
                            "",
                            DATA_FOLDER,
                            source_model_name,
                        ]:
                            
                            log.info(f"Sub Folder - {str(files)}", extra=log_extra)
                            
                            # Get File Size
                            file_size = gb.get_file_size(files, DATA_BUCKET, "mb")

                            # If a file is smaller than 400MB,
                            # load it using a local AVRO file to prevent too many update ops in BQ.
                            if (
                                upload_by_avro == True
                                and file_size < file_size_limit_for_avro
                            ):
                                avro_to_bq = True

                            src_feed_name = filename
                            technical_config = tech_config["default"]

                            # Add file name in a list
                            files_list.append(filename)
                            log.change_extra("file", f"{filename}")

                            # Fetch Market code
                            market = filename.split(".", 2)[1]

                            log.change_extra("dp_stream_num", f"{dp_stream_number}")
                            log.change_extra("market_code", f"{market}")
                            # Set meta data filepath
                            meta_data_file_name = (
                                DATA_FOLDER
                                + "/"
                                + dp_stream_number
                                + "/"
                                + source_model_name
                            )

                            # Get Meta data from source_meta_data.json
                            meta = MetaData(p_log=log)

                            meta_data = meta.load_meta_data(
                                DATA_BUCKET, meta_data_file_name
                            )

                            meta_data = check_interface_mode(meta_data)

                            # Call IO Class
                            loader = Data(p_log=log)

                            # Call Analytics Class
                            detect = SourceFileDetection(p_log=log)

                            # Call Equalizer class
                            eq = Equalizer(p_log=log)

                            dc = DerivedColumns(p_log=log)

                            # Call PI class
                            pii = PIIEncryption(p_log=log)

                            # Call Statstics class
                            stats = Statistics(p_log=log)

                            if meta_data["interface_mode"] in ["Full"]:
                                technical_config = tech_config["full"]

                            # Call Technical Column class
                            tc = TechnicalColumn(
                                p_batch_id=BATCH_ID,
                                p_technical_meta=technical_config,
                                p_row_hash_column_blacklist=row_hash_column_blacklist,
                                p_current_time=datetime.now(),
                                p_src_feed_type="File",
                                p_logging=log,
                            )

                            column_name = get_all_columns(meta_data, technical_config)

                            data_feed = meta_data["name"].lower()
                            source_system = meta_data["source system [source]"]

                            log.change_extra("data_feed", f"{data_feed}")
                            log.change_extra("source_system", f"{source_system}")

                            param = {
                                "source": source_system,
                                "name": data_feed,
                                "market": market,
                            }

                            # Get File Business date
                            business_date = BusinessDate.extract_date(filename)
                            b_date = str(
                                tc._convert_timestamp(business_date, "datetime")
                            )
                            # Parse businessDate in Timestamp
                            parse_business_date.append(b_date)

                            log.change_extra("business_date", f"{b_date}")

                            log.debug(
                                f"File {filename} Business Date - {b_date}",
                                extra={"step": "STREAM"},
                            )

                            table_name = set_table_name(layer["object_name"], param, "")
                            stage_table_name = f"{table_name}__stg"
                            raw_table_name = f"{table_name}__raw"
                            delta_table_name = f"{table_name}__stgdelta"

                            avro = AVRO(
                                p_table_name=str(proj_dataset + "." + stage_table_name),
                                p_bucket=None,
                                p_unique_name_per_block=True,
                                p_output_folder=meta_data["name"],
                                p_log=log,
                            )

                            # Call BigQuery class
                            bq = BigQuery(
                                p_data_set_name=proj_dataset,
                                p_project=project,
                                p_table_name=stage_table_name,
                                p_location=location,
                                p_partition_expiration_time=partition_expiration_time,
                                p_partition_field=partition_field,
                                p_log=log,
                            )

                            # Delete the staging table record according to SRC_FEED_NAME
                            del_query_stg = f"Delete FROM `{proj_dataset}.{stage_table_name}` where SRC_SYSTEM_NAME='{source_system}' and SRC_FEED_NAME='{filename}'"
                            # print_st(del_query_stg)
                            bq.run_sql_query(del_query_stg, data_feed)

                            Task.log = log

                            # Start Pipeline
                            results = Task.start_pipeline(
                                p_count, pipeline, meta_data, [files], avro_to_bq
                            )

                            if avro_to_bq:
                                """
                                This is used to Avro to BQ data movement.
                                """
                                gb.upload_local_directory_to_gcs(
                                    meta_data["name"], temp_bucket, meta_data["name"]
                                )

                                # Write Data in BQ
                                bq.write_avro_to_bq(meta_data["name"])

                                # Delete Folder from GCS bucket
                                gb.delete_folder(temp_bucket, meta_data["name"])

                            total_stream_time = datetime.now() - stream_start_time

                            log.info(
                                f"Total Straming Time - {total_stream_time}",
                                extra={"step": "STREAM"},
                            )
                            log.info(f"End", extra={"step": "STREAM"})

                            # Fetch all src_file_name for given date
                            list_of_dates = ", ".join(
                                ['"{}"'.format(value) for value in parse_business_date]
                            )

                    col = ",".join(column_name)
                    # TEMP to Stage Movement, Uncomment this line if you want temp table scenario
                    # stg_query = f"""
                    #             BEGIN
                    #                 BEGIN TRANSACTION;
                    #                 INSERT INTO `{proj_dataset}.{raw_table_name}` ({col}) SELECT {col} from `{proj_dataset}.{stage_table_name}_*` where src_extr_ts in ({list_of_dates});
                    #                 COMMIT TRANSACTION;
                    #             END;
                    #             """
                    # print(stg_query)
                    # bq.fetch_data_from_table(stg_query)

                    if list_of_dates is not None:
                        # Completeness Check Log
                        log_extra = {"step": "COMPLETENESS CHECK"}

                        os.environ[stepkey] = "COMPLETENESS CHECK"

                        log.info(f"Start", extra=log_extra)

                        # Completeness Check condition
                        query = f"SELECT DISTINCT src_feed_name FROM `{proj_dataset}.{stage_table_name}` where CAST(src_extr_ts as DATETIME) in ({list_of_dates})"
                        query_result = bq.run_sql_query(query, data_feed)
                        # print_st(query)
                        src_file_names = []

                        record = query_result.result()
                        for row in record:
                            src_file_names.append(row["src_feed_name"])

                        extra = {"source_system": source_system, "data_feed": data_feed}

                        completeness_checker = FileSetCompletenessChecker(
                            p_file_set_patterns=file_set_patterns,
                            p_file_names=src_file_names,
                            p_logger=log,
                        )
                        # Completeness check before moving data to Raw or calculate Delta
                        completeness_check = completeness_checker.is_file_set_complete()

                        log.info(
                            f"CHECK RESULT {completeness_check[1]}",
                            extra=log_extra | extra,
                        )
                        log.info(
                            f"END {completeness_check}",
                            extra=log_extra | extra,
                        )

                        if (
                            completeness_check is not None
                            and completeness_check[1] == True
                        ):
                            # Check If interface mode is Delta or Full?
                            if meta_data["interface_mode"] in ["Full"]:
                                parse_business_dt = tc._convert_timestamp(
                                    business_date, "datetime"
                                )
                                # Fetch yesterday date
                                y_query = f"SELECT MAX(src_extr_ts) as yesterday_date FROM `{proj_dataset}.{stage_table_name}` WHERE src_extr_ts < (CAST('{str(parse_business_dt)}' AS TIMESTAMP))"
                                # print_st(y_query)
                                query_result = bq.run_sql_query(y_query, data_feed)
                                # Default yesterday date is always same as current business date
                                yesterday = parse_business_dt
                                record = query_result.result()
                                for row in record:
                                    yesterday = row["yesterday_date"]

                                # Call Delta Count
                                delta_params = [
                                    project,
                                    dataset,
                                    stage_table_name,
                                    stage_table_name,
                                    delta_table_name,
                                    f'src_extr_ts in (CAST(\\"{str(yesterday)}\\" AS TIMESTAMP))',
                                    f'src_extr_ts in (CAST(\\"{str(parse_business_dt)}\\" AS TIMESTAMP))',
                                    project,
                                    dataset,
                                    raw_table_name,
                                    BATCH_ID,
                                    main_process_start_time,
                                    UNIQUE_ID,
                                    source_system,
                                    market,
                                    data_feed,
                                ]
                                sp_params = ", ".join(
                                    ['"{}"'.format(c) for c in delta_params]
                                )
                                sp_params = (
                                    sp_params
                                    + ","
                                    + f"CAST('{parse_business_dt}' AS DATETIME)"
                                )
                                sp_params = f"{sp_params}"

                                sp_query = f"DECLARE trigger_delta_result BOOL;DECLARE trigger_delta_return_msg STRING; CALL `{project}.admin_meta`.trigger_delta_calculation_sp({sp_params}); SELECT trigger_delta_return_msg;"
                                # Run Delta Calculator
                                bq.run_sql_query(sp_query, data_feed)
                            else:
                                # Insert data in Raw table
                                log_extra = {"step": "STAGE TO RAW MOVEMENT"}

                                os.environ[stepkey] = "STAGE TO RAW MOVEMENT"
                                log.info(f"Start", extra=log_extra)

                                src_filename = ", ".join(
                                    ['"{}"'.format(value) for value in src_file_names]
                                )
                                del_query_raw = f"Delete FROM `{proj_dataset}.{raw_table_name}` where SRC_SYSTEM_NAME='{source_system}' and SRC_FEED_NAME in ({src_filename})"
                                bq.run_sql_query(del_query_raw, data_feed)

                                # Stage to row movement sql query
                                raw_query = f"""
                                    BEGIN
                                        BEGIN TRANSACTION;                            
                                        INSERT INTO `{proj_dataset}.{raw_table_name}` ({col}) SELECT {col} from `{proj_dataset}.{stage_table_name}` where SRC_SYSTEM_NAME='{source_system}' and SRC_FEED_NAME in ({src_filename});
                                        COMMIT TRANSACTION;
                                        select count(*) as total from `{proj_dataset}.{raw_table_name}` where SRC_SYSTEM_NAME='{source_system}' and SRC_FEED_NAME in ({src_filename});
                                    END;
                                    """

                                query_result = bq.run_sql_query(raw_query, data_feed)
                                record = query_result.result()
                                for rec in record:
                                    total_rec = rec["total"]
                                    log.info(f"Record : {total_rec} ", extra=log_extra)

                                log_extra = {"step": "STAGE TO RAW MOVEMENT"}
                                log.info(f"End", extra=log_extra)

                    # Empty the stream log column
                    log.change_extra("source_system", "")
                    log.change_extra("data_feed", "")
                    log.change_extra("business_date", "")
                    log.change_extra("file", "")
                    log.change_extra("dp_stream_num", "")
                    log.change_extra("market_code", "")

            # Process End
            log_extra = {"step": "PROCESS FOLDER"}
            total_time = datetime.now() - process_start_time

            log.info(f"Total Processing Time - {total_time}", extra=log_extra)
            log.info(f"End", extra=log_extra)

        # MAIN PROCESS Folder Log
        log_extra = {"step": "MAIN PROCESS"}

        total_time = datetime.now() - main_process_start_time
        log.info(f"Total Processing Time - {total_time}", extra=log_extra)
        log.info(f"End", extra=log_extra)
    except Exception as err:
        message = (
            f"Task #{TASK_INDEX}, " + f"Attempt #{TASK_ATTEMPT} failed: {str(err)}"
        )
        print({str(traceback.format_exc())})
        log_err_msg(message,  os.getenv(stepkey, "Error"), log)
        # log.error(f"Error - {str(err)}", extra={"step": os.getenv(stepkey, "Error")})
        # print(json.dumps({"message": message, "severity": "ERROR"}))
        # sys.exit(1)  # Retry Job Task by exiting the process
