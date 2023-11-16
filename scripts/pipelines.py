from get_data import get_countries
from polars_delta import upload_to_s3
from dagster_aws.s3.resources import s3_resource
from resources import boto3_connection
from dagster_aws.s3 import s3_plus_default_intermediate_storage_defs
from dagster import (
    pipeline,
    solid,
    repository,
    file_relative_path,
    ModeDefinition,
    PresetDefinition,
    composite_solid,
    local_file_manager,
    execute_pipeline,
    InputDefinition,
    OutputDefinition,
    Output,
    FileHandle,
    Bool,
    Optional,
    List,
    Nothing,
)

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        # 'pyspark': pyspark_resource,
        's3': s3_resource,
        # 'druid': druid_db_info_resource,
        'boto3': boto3_connection,
        # 'tempfile': tempfile_resource,
        # "file_cache": fs_file_cache,
        # "db_info": postgres_db_info_resource,
    },
        intermediate_storage_defs=s3_plus_default_intermediate_storage_defs,
)

def get_and_upload_data(context):
    # Get data from get_data.py
    countries_data = get_countries()

    # Upload data to S3
    s3_coordinate = upload_to_s3(context, local_file=FileHandle("data/countries.json.gz"), s3_coordinate={"bucket": "geodata", "key": "data/countries.json.gz"})

@pipeline(
    mode_defs=[
        ModeDefinition(
            name="local",
            resource_defs={
                's3': s3_resource,
                'boto3': boto3_connection,
            },
        )
    ],
    preset_defs=[
        PresetDefinition.from_files(
            "local",
            config_files=[
                file_relative_path(__file__, "your_config_file.yaml"),
            ],
        ),
    ],
)
def upload_data_pipeline():
    get_and_upload_data()