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



# Make pyspark.sql.DataFrame map to dagster_pyspark.DataFrame
make_python_type_usable_as_dagster_type(
    python_type=pd.DataFrame, dagster_type=dagster_polars.DataFrame
)

def _get_s3a_path(bucket, path):
    # TODO: remove unnessesary slashs if there
    return 's3a://' + bucket + '/' + path

