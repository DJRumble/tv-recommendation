import os

import pandas as pd
import pytest
import s3fs
from aws_tools.project_config import get_aws_config
from aws_tools.s3_tools import (get_csv_from_s3_as_df, send_df_to_s3_as_csv,
                                sync_s3_filepath)
#from pyprojroot import here


@pytest.fixture
def config():
    config = get_aws_config('model_parameters.json')
    return config


def test_config(config):
    config_keys = set(config.keys())
    required_keys = set(['brand_request_query_params',
                     'brand_request_url',
                     'code_version',
                     'customer_type_list',
                     'discovery_feed_request_url',
                     'drop_music',
                     'season_to_brand_filename'])
    assert config_keys == required_keys


# def test_project_folder_in_s3(config):
#     s3 = s3fs.S3FileSystem()
#     s3_bucket = config['s3_bucket']
#     project_name = config['project_name']
#     path = f's3://{s3_bucket}/{project_name}'
#     assert s3.exists(path)


# def test_required_artifacts_present(config):
#     s3 = s3fs.S3FileSystem()
#     s3_bucket = config['s3_bucket']
#     s3_project = config['project_name']
#     for filepath in config['required_files']:
#         path = f's3://{s3_bucket}/{s3_project}/{filepath}'
#         assert s3.exists(path)