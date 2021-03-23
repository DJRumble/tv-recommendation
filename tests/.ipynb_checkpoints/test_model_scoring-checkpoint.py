import pytest
import pytest_check as check
import pandas as pd

import argparse

from commendaroo.models.bt_tv_recommendation_engine import  run_scoring_job

from aws_tools.cloudwatch_logging import logger
from aws_tools.project_config import get_aws_config
from aws_tools.s3_tools import send_scoring_output

# def test_get_data():
#     data = mdt.get_data()
#     assert isinstance(data, pd.DataFrame)
    
    
def test_model_scoring():
    data_foryou, data_morelikethis = run_scoring_job()
    
    assert isinstance(data_foryou, pd.DataFrame)
    assert isinstance(data_morelikethis, pd.DataFrame)
    