import pandas as pd 
from pyathena import connect
import os

from aws_tools.cloudwatch_logging import logger
from aws_tools.project_config import get_aws_config
from aws_tools import athena_tools

def create_connection():
    
    # create a connection to Athena
    return connect(s3_staging_dir = 's3://aws-athena-query-results-341377015103-eu-west-2/',
                       region_name='eu-west-2')

def create_views():
    '''
    Create dataset ready to train implicit model and then calculate recommendations
    '''
    
    suff = ''
    if "ENVIRON_PROD" in os.environ:
        if os.environ['ENVIRON_PROD'] == 'TESTING':
            suff = '''_dev'''
            logger.info('ENVIRON_PROD set to testing, creating/dropping dev tables')
    
    # create a connection to Athena
    conn = create_connection() 
    
    query = '''
    DROP TABLE IF EXISTS bt_home_ds.bt_tv_recommendation_engine_data_views''' + suff + ''';
    '''
    pd.read_sql(query, conn)

    query = '''
    CREATE TABLE IF NOT EXISTS bt_home_ds.bt_tv_recommendation_engine_data_views''' + suff + ''' AS
    SELECT
        vs.VISION_SERVICE_ID AS id_user -- account ID
        ,vs.CI_ASSET_TYPE AS TYPE_ASSET -- e.g. Film, Music, etc.
        ,vs.VIEW_TIME_ST AS EVENT_DATE -- time of event

        ,COALESCE(ps2.EDITORIAL_VERSION_ID, ps1.EDITORIAL_VERSION_ID) AS id_editorial -- identifies multiple instances of same film/season/episode, e.g. rent/purchase & SD/HD

        ,COALESCE(ps2.CI_TITLE, ps1.CI_TITLE) AS TITLE --  human readable title
        ,COALESCE(ps2.CI_TYPE, ps1.CI_TYPE) AS TYPE -- type, like film/music/episode/season/collection
        
        ,COALESCE(ps2.CI_AVAILABLE_END_DT, ps1.CI_AVAILABLE_END_DT) AS END_DATE -- date until availability of item
        ,COALESCE(ps2.CI_AVAILABLE_START_DT, ps1.CI_AVAILABLE_START_DT) AS START_DATE -- date from availability of item
        
        ,COALESCE(ps2.GENRE, ps1.GENRE) AS GENRE
        ,vs.SUB_GENRES AS SUB_GENRES
        
        ,COALESCE(ps2.rating, ps1.rating) AS rating
        ,COALESCE(ps2.SCHEDULER_CHANNEL, ps1.SCHEDULER_CHANNEL) AS SCHEDULER_CHANNEL
        
        ,COALESCE(ps2.CI_STATUS, ps1.CI_STATUS) AS STATUS

        ,'VIEW' AS type_entitlement

    FROM
        bt_home_datamart.l_edw_vod_views vs

    -- perform an inner join of the events with the catalogue to get the product id for each entry:
    -- this is needed to later match the product if with the parent id and group episodes into seasons
    INNER JOIN 
        bt_home_datamart.l_edw_vod_products ps1
    ON
         vs.CONTENT_ID = ps1.PRODUCT_GUID

    -- perform left join to match ID_PARENT with product id where available:
    -- this will give either the same element due to the coalesce OR the actual parent, i.e. the season rather than the episode
    LEFT JOIN 
       bt_home_datamart.l_edw_vod_products ps2
    ON
       ps1.CI_PARENTGUID = ps2.PRODUCT_GUID 

    WHERE vs.EVENT_SLOT_TYPE = 'Feature'
    AND vs.VISION_SERVICE_ID IS NOT NULL
    AND ps1.EDITORIAL_VERSION_ID IS NOT NULL;
    '''
    pd.read_sql(query, conn)
    
    
def create_views_maxexpiration():
    '''
    
    '''
    
    suff = ''
    if "ENVIRON_PROD" in os.environ:
        if os.environ['ENVIRON_PROD'] == 'TESTING':
            suff = '''_dev'''
            logger.info('ENVIRON_PROD set to testing, creating/dropping dev tables')
    
    # create a connection to Athena
    conn = create_connection()
    
    query = '''
    DROP TABLE IF EXISTS bt_home_ds.bt_tv_recommendation_engine_data_views_maxenddt''' + suff + ''';
    '''
    pd.read_sql(query, conn)

    query = '''
    CREATE TABLE IF NOT EXISTS bt_home_ds.bt_tv_recommendation_engine_data_views_maxenddt''' + suff + ''' AS
    SELECT 
        aaa.id_user -- account ID
        ,aaa.TYPE_ASSET -- e.g. Film, Music, etc.
        ,aaa.EVENT_DATE -- time of event

        ,aaa.id_editorial -- identifies multiple instances of same film/season/episode, e.g. rent/purchase & SD/HD

        ,aaa.TITLE --  human readable title
        ,aaa.TYPE -- type, like film/music/episode/season/collection

        ,COALESCE(ps.CI_AVAILABLE_END_DT, aaa.END_DATE) AS END_DATE -- date until availability of item
        ,aaa.START_DATE -- date from availability of item

        ,aaa.GENRE
        ,aaa.SUB_GENRES
        
        ,aaa.rating
        ,aaa.SCHEDULER_CHANNEL
        
        ,aaa.STATUS

        ,aaa.type_entitlement -- TVOD / EVOD / PPV

    FROM bt_home_ds.bt_tv_recommendation_engine_data_views''' + suff + ''' aaa

    -- left join with catalogue again where we keep only max availability date for each editorial id
    LEFT JOIN
        (SELECT EDITORIAL_VERSION_ID, MAX(DATE(CI_AVAILABLE_END_DT)) AS CI_AVAILABLE_END_DT
        FROM bt_home_datamart.l_edw_vod_products 
        GROUP BY EDITORIAL_VERSION_ID) ps
    ON 
        aaa.id_editorial = ps.EDITORIAL_VERSION_ID;
    '''
    pd.read_sql(query, conn)
    

def create_prp():
    '''
    Create dataset ready to train implicit model and then calculate recommendations
    '''
    
    suff = ''
    if "ENVIRON_PROD" in os.environ:
        if os.environ['ENVIRON_PROD'] == 'TESTING':
            suff = '''_dev'''
            logger.info('ENVIRON_PROD set to testing, creating/dropping dev tables')
    
    # create a connection to Athena
    conn = create_connection()
    
    query = '''
    DROP TABLE IF EXISTS bt_home_ds.bt_tv_recommendation_engine_data_prps''' + suff + ''';
    '''
    pd.read_sql(query, conn)

    query = '''
    CREATE TABLE IF NOT EXISTS bt_home_ds.bt_tv_recommendation_engine_data_prps''' + suff + ''' AS
    SELECT
        vs.VISION_SERVICE_ID AS id_user -- account ID
        ,vs.CI_ASSET_TYPE AS TYPE_ASSET -- e.g. Film, Music, etc.
        ,vs.PURCHASE_TIME_ST AS EVENT_DATE -- time of event

        ,COALESCE(ps2.EDITORIAL_VERSION_ID, ps1.EDITORIAL_VERSION_ID) AS id_editorial -- identifies multiple instances of same film/season/episode, e.g. rent/purchase & SD/HD

        ,COALESCE(ps2.CI_TITLE, ps1.CI_TITLE) AS TITLE --  human readable title
        ,COALESCE(ps2.CI_TYPE, ps1.CI_TYPE) AS TYPE -- type, like film/music/episode/season/collection

        ,COALESCE(ps2.CI_AVAILABLE_END_DT, ps1.CI_AVAILABLE_END_DT) AS END_DATE -- date until availability of item
        ,COALESCE(ps2.CI_AVAILABLE_START_DT, ps1.CI_AVAILABLE_START_DT) AS START_DATE -- date from availability of item

        ,COALESCE(ps2.GENRE, ps1.GENRE) AS GENRE
        ,'ZZZZZ' AS SUB_GENRES
        
        ,COALESCE(ps2.rating, ps1.rating) AS rating
        ,COALESCE(ps2.SCHEDULER_CHANNEL, ps1.SCHEDULER_CHANNEL) AS SCHEDULER_CHANNEL
        
        ,COALESCE(ps2.CI_STATUS, ps1.CI_STATUS) AS STATUS

        ,vs.ENTITLEMENT_TYPE AS type_entitlement -- TVOD / EVOD / PPV

    FROM
        bt_home_datamart.l_edw_vod_purchases vs

    -- perform an inner join of the events with the catalogue to get the product id for each entry:
    -- this is needed to later match the product if with the parent id and group episodes into seasons
    INNER JOIN 
        bt_home_datamart.l_edw_vod_products ps1
    ON
        vs.PRODUCT_ID = ps1.PRODUCT_GUID

    -- perform left join to match ID_PARENT with product id where available:
    -- this will give either the same element due to the coalesce OR the actual parent, i.e. the season rather than the episode
    LEFT JOIN 
       bt_home_datamart.l_edw_vod_products ps2
    ON
       ps1.CI_PARENTGUID = ps2.PRODUCT_GUID 

    WHERE vs.VISION_SERVICE_ID IS NOT NULL
    AND ps1.EDITORIAL_VERSION_ID IS NOT NULL;
    '''
    pd.read_sql(query, conn)
    
    
def create_prp_maxexpiration():
    '''
    
    '''
    
    suff = ''
    if "ENVIRON_PROD" in os.environ:
        if os.environ['ENVIRON_PROD'] == 'TESTING':
            suff = '''_dev'''
            logger.info('ENVIRON_PROD set to testing, creating/dropping dev tables')
    
    # create a connection to Athena
    conn = create_connection()
    
    query = '''
    DROP TABLE IF EXISTS bt_home_ds.bt_tv_recommendation_engine_data_prps_maxenddt''' + suff + ''';
    '''
    pd.read_sql(query, conn)

    query = '''
    CREATE TABLE IF NOT EXISTS bt_home_ds.bt_tv_recommendation_engine_data_prps_maxenddt''' + suff + ''' AS
    SELECT
        aaa.id_user -- account ID
        ,aaa.TYPE_ASSET -- e.g. Film, Music, etc.
        ,aaa.EVENT_DATE -- time of event

        ,aaa.id_editorial -- identifies multiple instances of same film/season/episode, e.g. rent/purchase & SD/HD

        ,aaa.TITLE --  human readable title
        ,aaa.TYPE -- type, like film/music/episode/season/collection

        ,COALESCE(ps.CI_AVAILABLE_END_DT, aaa.END_DATE) AS END_DATE -- date until availability of item
        ,aaa.START_DATE -- date from availability of item

        ,aaa.GENRE
        ,aaa.SUB_GENRES
        
        ,aaa.rating
        ,aaa.SCHEDULER_CHANNEL
        
        ,aaa.STATUS

        ,aaa.type_entitlement -- TVOD / EVOD / PPV

    FROM
        bt_home_ds.bt_tv_recommendation_engine_data_prps''' + suff + ''' aaa

    -- left join with catalogue again where we keep only max availability date for each editorial id
    LEFT JOIN
        (SELECT EDITORIAL_VERSION_ID, MAX(DATE(CI_AVAILABLE_END_DT)) AS CI_AVAILABLE_END_DT
        FROM bt_home_datamart.l_edw_vod_products 
        GROUP BY EDITORIAL_VERSION_ID) ps
    ON 
        aaa.id_editorial = ps.EDITORIAL_VERSION_ID;
    '''
    pd.read_sql(query, conn)

    
def get_users_notoptout():
    
    # load the full list
    opt_outs_df = pd.read_csv('s3://bt-data-science-playground/bt-tv-recommendation-system/model_objects/optouts.csv')
    # get column with VSIDs
    opt_outs = opt_outs_df['VISION_SERVICE_ID'].tolist()
    # concatenate into a single string
    return ','.join(["'" + str(x) + "'" for x in opt_outs])   
    
    
def load_views():
    '''
    
    '''
    
    suff = ''
    #
    optouts = get_users_notoptout()
    limit = '''
    WHERE id_user NOT IN ('''  + optouts + ''')'''
    #
    if "ENVIRON_PROD" in os.environ:
        if os.environ['ENVIRON_PROD'] == 'TESTING':
            suff = '''_dev'''
            logger.info('ENVIRON_PROD set to testing, creating/dropping dev tables')
            
            limit = '''WHERE id_user in ('V3006126692',
                'V3007158774',
                'V3009295704',
                'V3000606445',
                'V2283739102',
                'V2200004307',
                'V3009378451',
                'V3009436446',
                'V1000019727',
                'V3008138884',
                'V3003052251',
                'V3008613163',
                'V3578624855',
                'V3000143118',
                'V2200007285')'''
            logger.info('ENVIRON_PROD set to testing, limiting scoring data to only some users')
    
    logger.info('Loading views data')
    
    query = f"""
    SELECT *
    FROM bt_home_ds.bt_tv_recommendation_engine_data_views_maxenddt{suff}
    {limit};
    """

    return athena_tools.AthenaQuerier().execute_query(query=query)
    
    
def load_prp():
    '''
    
    '''
    suff = ''
    #
    optouts = get_users_notoptout()
    limit = '''
    WHERE id_user NOT IN ('''  + optouts + ''')'''
    #
    if "ENVIRON_PROD" in os.environ:
        if os.environ['ENVIRON_PROD'] == 'TESTING':
            suff = '''_dev'''
            logger.info('ENVIRON_PROD set to testing, creating/dropping dev tables')
            
            limit = '''WHERE id_user in ('V3006126692',
                'V3007158774',
                'V3009295704',
                'V3000606445',
                'V2283739102',
                'V2200004307',
                'V3009378451',
                'V3009436446',
                'V1000019727',
                'V3008138884',
                'V3003052251',
                'V3008613163',
                'V3578624855',
                'V3000143118',
                'V2200007285')'''
            logger.info('ENVIRON_PROD set to testing, limiting scoring data to only some users')
    
    logger.info('Loading purchases/rentals/PPVs data')
    
    query = f"""
    SELECT *
    FROM bt_home_ds.bt_tv_recommendation_engine_data_prps_maxenddt{suff}
    {limit};
    """

    return athena_tools.AthenaQuerier().execute_query(query=query)


def load_data():
    '''
    '''
    # Create first table for views data
    logger.info('Creating views tables')
    create_views()
    # Join first again with catalogue to get maximum expiration date
    create_views_maxexpiration()
    
    # Create first table for purchases/rentals/PPVs data
    logger.info('Creating purchases/rentals/PPVs tables')
    create_prp()
    # Join first again with catalogue to get maximum expiration date
    create_prp_maxexpiration()
    
    vws = load_views()
    prps = load_prp()
    
    # Join the two datasets together
    data_implicit = vws.append(prps, ignore_index=True)
    
    return data_implicit