import argparse
import csv
import pandas as pd

from commendaroo.models.bt_tv_recommendation_engine import  run_scoring_job

from aws_tools.cloudwatch_logging import logger
from aws_tools.project_config import get_aws_config
from aws_tools.s3_tools import send_scoring_output
from aws_tools.dynamodb_tools import DynamoDBWriter

def main():
    
    # Create the parser
    parser = argparse.ArgumentParser()

    # Add the arguments
    parser.add_argument('--score',
                        action='store_true',
                        help='runs scoring if true')

    parser.set_defaults(score=False)

    # Execute the parse_args() method
    args = parser.parse_args()

    if args.score:
        
            config_foryou = get_aws_config('for_you.json')
            config_foryou['quoting'] = csv.QUOTE_NONE
            config_foryou['quotechar'] = ""
            config_foryou['escapechar'] = "\\"
        
            config_morelikethis = get_aws_config('more_like_this.json')
            config_morelikethis['quoting'] = csv.QUOTE_NONE
            config_morelikethis['quotechar'] = ""
            config_morelikethis['escapechar'] = "\\"
            
            config_generic = get_aws_config('generic.json')
            config_generic['quoting'] = csv.QUOTE_NONE
            config_generic['quotechar'] = ""
            config_generic['escapechar'] = "\\"
            
            data_foryou, data_morelikethis, data_generic = run_scoring_job()
            
            logger.info(f'Got {len(data_foryou)} recommendations for For You')
            logger.info(f'Got {len(data_morelikethis)} recommendations for More Like This')
            logger.info(f'Got {len(data_generic)} recommendations for the generic table')
            
            send_scoring_output(data_foryou, config_foryou)
            send_scoring_output(data_morelikethis, config_morelikethis)
            send_scoring_output(data_generic, config_generic)
            
            data_foryou.columns = ['ID', 'prediction']
            DynamoDBWriter(**config_foryou).write(data_foryou)
            logger.info(f'Upload to dynamoDB for For You completed. Total recommendations: {len(data_foryou)}')       
            
            data_morelikethis.columns = ['ID', 'prediction']
            DynamoDBWriter(**config_morelikethis).write(data_morelikethis)
            logger.info(f'Upload to dynamoDB for More Like This completed. Total recommendations: {len(data_morelikethis)}')
            
            data_generic.columns = ['ID', 'prediction']
            DynamoDBWriter(**config_generic).write(data_generic)
            logger.info(f'Upload to dynamoDB for generic table completed. Total recommendations: {len(data_generic)}')

if __name__ == '__main__':
    main()
