from dagster_aws.s3 import S3Coordinate
from functools import reduce
from jinja2 import Template
import dagster_polars
import re
import os
from botocore.exceptions import NoCredentialsError

import pandas as pd
import pandasql as ps


from dagster import (
    make_python_type_usable_as_dagster_type,
    solid,
    file_relative_path,
    Field,
    String,
    Bool,
    Int,
    Output,
    InputDefinition,
    OutputDefinition,
    check,
    Permissive,
    FileHandle,
    List,
    composite_solid,
    EventMetadataEntry,
    AssetMaterialization,
    Optional,
)

from dagster_aws.s3 import dict_with_fields
from dagster import PythonObjectDagsterType, Field, String
PARQUET_SPECIAL_CHARACTERS = r'[ ,;{}()\n\t=]'




# Make pyspark.sql.DataFrame map to dagster_pyspark.DataFrame
make_python_type_usable_as_dagster_type(
    python_type=pd.DataFrame, dagster_type=dagster_polars.DataFrame
)

def _get_s3a_path(bucket, path):
    # TODO: remove unnessesary slashs if there
    return 's3a://' + bucket + '/' + path

def rename_polars_dataframe_columns(data_frame, fn):
    new_columns = [fn(c) for c in data_frame.columns]
    return data_frame.rename(new_columns)

def upload_to_s3(context, local_file: FileHandle, s3_coordinate: S3Coordinate) -> S3Coordinate:

    # add filename to key
    return_s3_coordinates: S3Coordinate = {
        'bucket': s3_coordinate['bucket'],
        'key': s3_coordinate['key'] + "/" + os.path.basename(local_file.path),
    }

    s3 = context.resources.boto3.get_client()
    context.log.info(
        "s3 upload location: {bucket}/{key}".format(
            bucket=return_s3_coordinates['bucket'], key=return_s3_coordinates['key']
        )
    )

    try:
        s3.upload_file(
            local_file.path, return_s3_coordinates['bucket'], return_s3_coordinates['key'],
        )
        context.log.info("Upload Successful")
        return return_s3_coordinates
    except FileNotFoundError:
        context.log.error("The file was not found")
    except NoCredentialsError:
        context.log.error("Credentials not available")      