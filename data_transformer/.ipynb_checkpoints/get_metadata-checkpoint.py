from aws_tools.cloudwatch_logging import logger
from aws_tools.project_config import get_aws_config

import pandas as pd
import numpy as np
import boto3
import os

def return_description(table, ed_id, title):
    for d in table:
        if d['guid']['S'] == ed_id:
            try:
                return d['longDescription']['S'].split('<br><br>')[0]
            except:
                return title
    return title
    

def pull_synopsis(content, table_b, table_c, table_p):
    
    if content[1] == 'BRAND':
        return return_description(table_b, content[0], content[2])
        
    elif content[1] == 'COLLECTION':
        return return_description(table_c, content[0], content[2])
        
    elif content[1] == 'PROGRAM':
        return return_description(table_p, content[0], content[2])
        
    else:
        return ''
    
def get_full_table(TableName, dynamo_client):
    table = dynamo_client.scan(TableName = TableName)
    data = table['Items']
    while 'LastEvaluatedKey' in table:
        table = dynamo_client.scan(TableName = TableName, ExclusiveStartKey=table['LastEvaluatedKey'])
        data.extend(table['Items'])
    
    return data

def initiate_client():
    
    sts_client = boto3.client('sts')
    
    assumed_role_object=sts_client.assume_role(
        RoleArn="arn:aws:iam::881289283440:role/pipeline/bttv-dar-prod-role-tvrecommend-prd-consrecommend",
        RoleSessionName="recommendAssume"
    )
    
    credentials = assumed_role_object['Credentials']
    
    return boto3.client(
        'dynamodb',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
        region_name='eu-west-1'
    )

def get_synopsis(data_implicit):
    
    dynamo_client = initiate_client()
    
    #response = dynamo_client.list_tables(Limit=10)
    
    table_b = get_full_table('tvrecommend-brand-prd', dynamo_client)
    table_c = get_full_table('tvrecommend-collection-prd', dynamo_client)
    table_p = get_full_table('tvrecommend-programme-prd', dynamo_client)
    
    
    data_unique = data_implicit[['id_editorial', 'type_production', 'title']].drop_duplicates()
    
    data_unique['synopsis'] = data_unique.apply(pull_synopsis, args=(table_b, table_c, table_p), axis = 1)
    
    data_implicit = data_implicit.merge(data_unique[['id_editorial', 'synopsis']], left_on='id_editorial', right_on='id_editorial', how='left') 
    
    return data_implicit

def get_metadata_table(guids_unique, table, type_production, table_pd):
    output = []
    for i in table:
        try:
            if 'episodeNumber' not in list(i.keys()): # not episode or season AND not in data with history
                
                type_production = type_production
                guid = i['guid']['S']
                title = i['title']['S']
                
                 # HISTORY
                if (guid not in guids_unique):
                    history = 1
                else:
                    history = 0
                
                # SYNOPSIS
                try:
                    synopsis = i['longDescription']['S'].split('<br><br>')[0]
                except:
                    synopsis = title
                
                # GENRE
                try:
                    genre = i['genres']['L'][0]['S']
                except:
                    try:
                        if history == 1:
                            for j in table_pd:
                                if j['overarchingInfo']['M']['guid']['S'] == guid:
                                    genre = j['genres']['L'][0]['S']
                        else:
                            genre = np.nan
                    except:
                        genre = np.nan
                    
                # SUBGENRES
                try:
                    if len(i['subGenres']['L']) > 0:
                        subgenres = ' '.join([x['S'] for x in i['subGenres']['L']])
                    else:
                        if history == 1:
                            for j in table_pd:
                                if j['overarchingInfo']['M']['guid']['S'] == guid:
                                    subgenres = ' '.join([x['S'] for x in j['subGenres']['L']])
                        else:
                            subgenres = ''
                except:
                    subgenres = ''
                    
                # SCHEDULER CHANNEL
                try:
                    scheduler_channel = i['schedulerChannels']['L'][0]['S']
                except:
                    try:
                        if history == 1:
                            for j in table_pd:
                                if j['overarchingInfo']['M']['guid']['S'] == guid:
                                    scheduler_channel = j['schedulerChannels']['L'][0]['S']
                        else:
                            scheduler_channel = np.nan
                    except:
                        scheduler_channel = np.nan
                
                # RATING
                try:
                    rating = i['rating']['S']
                except:
                    try:
                        for j in table_p:
                            if j['guid']['S'] == i['anchors']['L'][0]['M']['programGuid']['S']:
                                rating = j['rating']['S']
                    except:
                        rating = np.nan
                
               
                
                #
                output.append(
                    [guid,
                    type_production,
                    title,
                    synopsis,
                    genre,
                    subgenres,
                    scheduler_channel,
                    rating,
                    history]
                )
        except:
            #print('Could not load: ' + i['guid']['S'])
            pass

    data = pd.DataFrame(
        output,
        columns=[
            'id_editorial',
            'type_production',
            'title',
            'synopsis',
            'genre',
            'subgenres',
            'scheduler_channel',
            'rating',
            'history'
        ]
    )
    
    return data


def get_metadata_nohist(guids_unique):
    
    dynamo_client = initiate_client()
    
    table_b = get_full_table('tvrecommend-brand-prd', dynamo_client)
    table_c = get_full_table('tvrecommend-collection-prd', dynamo_client)
    table_p = get_full_table('tvrecommend-programme-prd', dynamo_client)
    
    table_pd = get_full_table('tvrecommend-product-prd', dynamo_client)
    
    if "ENVIRON_PROD" in os.environ:
        if os.environ['ENVIRON_PROD'] == 'TESTING':
            data_b = get_metadata_table(guids_unique, table_b[:10], 'BRAND', table_pd)
            data_c = get_metadata_table(guids_unique, table_c[:10], 'COLLECTION', table_pd)
            data_p = get_metadata_table(guids_unique, table_p[:10], 'PROGRAM', table_pd)
            
        else:
            data_b = get_metadata_table(guids_unique, table_b, 'BRAND', table_pd)
            data_c = get_metadata_table(guids_unique, table_c, 'COLLECTION', table_pd)
            data_p = get_metadata_table(guids_unique, table_p, 'PROGRAM', table_pd)
    else:
        data_b = get_metadata_table(guids_unique, table_b, 'BRAND', table_pd)
        data_c = get_metadata_table(guids_unique, table_c, 'COLLECTION', table_pd)
        data_p = get_metadata_table(guids_unique, table_p, 'PROGRAM', table_pd)
    
    data_nohist = pd.concat([data_b, data_c, data_p], ignore_index=True)
    
    return data_nohist