from aws_tools.cloudwatch_logging import logger
from aws_tools.project_config import get_aws_config

import pandas as pd
import requests
import json
import csv
from io import StringIO # python3; python2: BytesIO 
import boto3

def callDiscoveryFeed(feedUrl, queryParams):
    request = requests.get(feedUrl, params=queryParams)    
    return request.json()["discoveryFeedResponse"]["items"]

def getSeasonsForBrand(brandGuid, brandRequestQueryParams, brandRequestUrl):
    brandRequestQueryParams["brandGuid"] = brandGuid
    request = requests.get(brandRequestUrl, params=brandRequestQueryParams)
    groups = request.json()["bdResponse"]["brandInfo"]["groups"]
    seasonGuids = []
    for season in groups:
        seasonGuids.append(season["guid"])
    return seasonGuids

def getBrandListForFeedUrl(feedUrl, queryParams, foundBrandGuids, existingBrandGuids):
    queryParamsForCall = queryParams
    gotContent = True
    while gotContent == True:
        itemList = callDiscoveryFeed(feedUrl, queryParamsForCall)   ## Get items for this page (because response is paged)
        if (len(itemList) == 0):                                    ## If page had no items, we've reached the bottom of the available content
            gotContent = False                                      ##If the call returns nothing then we've got no data to work with so break
            continue
                
        for content in itemList:                                    ## If we have content, loop through each one
            if (content['type']) == 'BRAND':                        ## Select only brand contents
                if(content["guid"] in foundBrandGuids):               ## Check if we've found this brandGuid before on a previous call
                    continue                                        
                foundBrandGuids.append(content["guid"])## If not pop it in the list

        queryParamsForCall['cursor'] = itemList[-1]["guid"]         ## Set us up for the next loop, telling it to use that last content as the cursor location
        queryParamsForCall['direction'] = 'FORWARD'
    return foundBrandGuids

        
# def getExistingSeasonToBrand(existingBrandGuids, existingSeasonToBrand):
#     existingSeasonToBrand_reformat = {}
#     for b in existingBrandGuids:
#         existingSeasonToBrand_reformat[b] = existingSeasonToBrand[existingSeasonToBrand['BRAND_GUID'] == b]['SEASON_GUID'].unique().tolist()
#     return existingSeasonToBrand_reformat

def findNewBrandGuids(customerTypesList, discoveryFeedRequestUrl, existingBrandGuids):
    foundBrandGuids = []
    tags = ['TV', 'TV Replay', 'Kids']
    for tag in tags:
        for customerType in customerTypesList:
            queryParams = {
                'byTags': '{schedulerChannel, ' + tag + '}',
                'context': 'commendaroo-testing',
            }
            queryParams['customerType'] = customerType
            foundBrandGuids = getBrandListForFeedUrl(discoveryFeedRequestUrl, queryParams, foundBrandGuids, existingBrandGuids)
    return foundBrandGuids

def findSeasonsForBrands(existingSeasonToBrand, newBrandGuidList, brandRequestQueryParams, brandRequestUrl):
    for brandGuid in newBrandGuidList:
        for seasonGuid in getSeasonsForBrand(brandGuid, brandRequestQueryParams, brandRequestUrl):
            if (seasonGuid not in existingSeasonToBrand['SEASON_GUID'].tolist()):
                ## Add these to our dictionary
                #output_df.append([brandGuid, seasonGuid], ignore_index=True)
                existingSeasonToBrand.loc[len(existingSeasonToBrand)] = [seasonGuid, brandGuid]
    return existingSeasonToBrand

def update_season_to_brand():
    '''
    '''
    params = get_aws_config('model_parameters.json')
    FILENAME = params['season_to_brand_filename']
    customerTypesList = params['customer_type_list']
    discoveryFeedRequestUrl = params['discovery_feed_request_url']

    brandRequestUrl = params['brand_request_url']
    brandRequestQueryParams = params['brand_request_query_params']
    
    existingSeasonToBrand = pd.read_csv('s3://bt-data-science-playground/bt-tv-recommendation-system/model_objects/' + FILENAME)
    existingBrandGuids = existingSeasonToBrand['BRAND_GUID'].unique().tolist()
    
    #existingSeasonToBrand = getExistingSeasonToBrand(existingBrandGuids, existingSeasonToBrand)
    
    newBrandGuidList = findNewBrandGuids(customerTypesList, discoveryFeedRequestUrl, existingBrandGuids)
    
    existingSeasonToBrand = findSeasonsForBrands(existingSeasonToBrand, newBrandGuidList, brandRequestQueryParams, brandRequestUrl)
    
    bucket = 'bt-data-science-playground' # already created on S3
    csv_buffer = StringIO()
    existingSeasonToBrand.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, 'bt-tv-recommendation-system/model_objects/' + FILENAME).put(Body=csv_buffer.getvalue())
    
    
def season_to_brand(data):
    '''
    '''
    
    #update_season_to_brand()
    
    # load season-to-brand mapping
    season_to_brand_map = pd.read_csv('s3://bt-data-science-playground/bt-tv-recommendation-system/model_objects/SeasonToBrandMapping.csv')
    
    data = data.merge(season_to_brand_map, how='left', left_on = 'id_editorial', right_on = 'SEASON_GUID')
    data['BRAND_GUID'].fillna(data['id_editorial'], inplace=True)
    data['id_editorial'] = data['BRAND_GUID']
    data.drop(['SEASON_GUID', 'BRAND_GUID'], axis=1, inplace=True)
    
    return data
    

