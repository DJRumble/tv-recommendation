import commendaroo.data_transformer.athena_queries as aq
import commendaroo.data_transformer.season_to_brand as stb
import commendaroo.data_transformer.get_metadata as gmd

from aws_tools.cloudwatch_logging import logger
from aws_tools.project_config import get_aws_config

import pandas as pd
from datetime import datetime, timedelta

def create_FY_hide_logic(df):
    
    today = datetime.today()
    
    if (df[0] in ['Film', 'TV', 'Sport']) and (df[1].strftime('%Y-%m-%d') > (today - timedelta(days=90)).strftime('%Y-%m-%d')):
        val = 1
    elif (df[0] in ['Kids', 'TV Replay']) and (df[1].strftime('%Y-%m-%d') > (today - timedelta(days=30)).strftime('%Y-%m-%d')):
        val = 1
    else:
        val = 0
    return val

def assign_eventStrength(x):
    '''
    based on type of entitlement (purchase/rental/ppv/view) give different strength [EVOD=purchase;TVOD=rental]
    '''
    if x == 'EVOD':
        val = 1
    elif x == 'TVOD':
        val = 1
    elif x == 'PPV':
        val = 1
    elif x == 'VIEW':
        val = 1
    else:
        val = 1
    return val

def rating_toNumeric(x):
    '''
    Turn duplicate the rating column in a form that is numeric so that they can be compared
    '''
    if x == 'u':
        return 0
    elif x == 'pg':
        return 1
    elif x == '12':
        return 2
    elif x == '15':
        return 3
    elif x == '18':
        return 4
    else:
        return 5

def type_production(x):
    if (x == 'season') or (x == 'episode'):
        return 'BRAND'
    elif x == 'collection':
        return 'COLLECTION'
    else:
        return 'PROGRAM'
    
def get_data():
    '''
    Create dataset ready to train implicit model and then calculate recommendations
    '''
    
    params = get_aws_config('model_parameters.json')
    
    # Load the data
    logger.info('Getting the data')
    data_implicit = aq.load_data()
    logger.info('Data implicit loaded')
    
    # Drop music, if wanted
    DROP_MUSIC = get_aws_config('model_parameters.json')['drop_music']
    if DROP_MUSIC:
        data_implicit = data_implicit[(data_implicit['type_asset'] != 'Music') & (data_implicit['type_asset'] != 'music')]
    else:
        pass
    
    # Updated season-to-brand mapping and aggregate data
    logger.info('Aggregate seasons into brands')
    data_implicit = stb.season_to_brand(data_implicit)
    
    
    logger.info('Prepping data for implicit model')
    
    # Assign strength to each event
    logger.info('Assigning event strength')
    data_implicit['eventStrength'] = data_implicit['type_entitlement'].apply(assign_eventStrength)
    
    # Create logic to hide For You elements based on time windows and scheduler channel
    logger.info('Create For You logic to hide already seen content based on different time windows')
    
    data_implicit['FY_logic'] = data_implicit[['scheduler_channel', 'event_date']].apply(create_FY_hide_logic, axis = 1)
    
    # Create PEGI rating as numeric
    logger.info('Turning PEGI rating into numeric')
    data_implicit['rating_n'] = data_implicit['rating'].apply(rating_toNumeric)
    
    # Translate production type
    logger.info('Assigning type PROGRAM/COLLECTION/BRAND')
    data_implicit['type_production'] = data_implicit['type'].apply(type_production)
    
    # get synopsis for each item
    logger.info('Getting synopsis')
    data_implicit = gmd.get_synopsis(data_implicit)
    
    # Add columns to be used by the model training
    data_implicit['title'] = data_implicit['title'].astype("category")
    data_implicit['id_user'] = data_implicit['id_user'].astype("category")
    data_implicit['id_editorial'] = data_implicit['id_editorial'].astype("category")
    data_implicit['id_user_simple'] = data_implicit['id_user'].cat.codes
    data_implicit['id_editorial_simple'] = data_implicit['id_editorial'].cat.codes
    
    return data_implicit


def get_data_nohist(data_implicit):
    
    logger.info('Loading all metadata including for items with no history')
    
    # get unique editorial ids
    guids_unique = data_implicit['id_editorial'].drop_duplicates().tolist()
    
    data_nohist = gmd.get_metadata_nohist(guids_unique)
    
    data_nohist['rating_n'] = data_nohist['rating'].apply(rating_toNumeric)
    
    return data_nohist