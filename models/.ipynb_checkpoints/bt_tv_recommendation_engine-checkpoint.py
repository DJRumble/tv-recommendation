import commendaroo.data_transformer.model_data as mdt

from scipy import sparse
import implicit
from datetime import datetime
from sklearn.preprocessing import MinMaxScaler
from sklearn.feature_extraction.text import CountVectorizer
import numpy as np
import pandas as pd
import json
import os
import boto3
import tensorflow_hub as hub

from aws_tools.cloudwatch_logging import logger
from aws_tools.project_config import get_aws_config

np.seterr(divide='ignore', invalid='ignore') # to avoid divide by nan warning (genre in music is nan)

def create_model(data_implicit):
    '''
    '''
    
    logger.info('Training the model...')
    
    sparse_content_person = sparse.csr_matrix(
        (data_implicit['eventStrength'].astype(float), (data_implicit['id_editorial_simple'], data_implicit['id_user_simple']))
    )
    
    model = implicit.als.AlternatingLeastSquares(factors=30, regularization=0.1, iterations=50, use_gpu = False)
    alpha = 15
    data_tofit = (sparse_content_person * alpha).astype('double')
    model.fit(data_tofit)
    
    return model

def downloadDirectoryFroms3(bucket, remoteDirectoryName, project_folder):
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket) 
    for object in bucket.objects.filter(Prefix = remoteDirectoryName):
        if not os.path.exists(os.path.dirname(object.key.split(project_folder)[-1])):
            os.makedirs(os.path.dirname(object.key.split(project_folder)[-1]))
        if object.key.split(project_folder)[-1][-1] != '/': #avoids trying to copy files from an empty folder
            bucket.download_file(object.key,object.key.split(project_folder)[-1])

def load_use():
    logger.info('Loading Universal Sentence Encoder')
    
    bucket = 'bt-data-science-playground'
    project_folder = 'nps-score-verbatim-text-analysis/'
    model_filename = 'model_objects_UniversalSentenceEncoders/4'
    
    # Download the relevant model objects
    downloadDirectoryFroms3(bucket, project_folder+model_filename, project_folder)
    
    return hub.load(model_filename)


def more_like_this_nohist(content_id, syn_vecs, syn_norms, othermd_vecs, othermd_norms, id_editorial_list_nohist, id_editorial_simple_list_nohist, type_production_list_nohist, rating_nohist, genre_nohist, scheduler_channel_nohist):
    
    ## synopsis
    scores_syn = syn_vecs.dot(syn_vecs[content_id,:])  / (syn_norms * syn_vecs[content_id,:].sum()) # i.e. calculating cosine similarity, (A.B) / (|A| x |B|) --> |B| just a constant so effectively won't need it
    scores_syn =  MinMaxScaler().fit_transform(scores_syn.reshape(-1,1))[:,0]
    
    ## other metadata
    scores_othermd = othermd_vecs.dot(othermd_vecs[content_id,:])  / (othermd_norms * othermd_vecs[content_id,:].sum()) # i.e. calculating cosine similarity, (A.B) / (|A| x |B|) --> |B| just a constant so effectively won't need it
    scores_othermd =  MinMaxScaler().fit_transform(scores_syn.reshape(-1,1))[:,0]
    
    ## sum scores
    a_syn = get_aws_config('model_parameters.json')['MLT_strength_synopsis']
    a_othermd = get_aws_config('model_parameters.json')['MLT_strength_othermetadata']
    #
    scores = ((a_syn * scores_syn) + (a_othermd * scores_othermd)) / (a_syn + a_othermd)
    
    # make zero the content not matching the same scheduler_channel
    if scheduler_channel_nohist[content_id] in ['Kids', 'Film', 'TV', 'TV Replay', 'Sport', 'Music']:
        scheduler_channel_logical = scheduler_channel_nohist == scheduler_channel_nohist[content_id]
        scheduler_channel_logical = np.array(scheduler_channel_logical.astype(int))
        #
        scores = scores * scheduler_channel_logical
    
    # make zero the content not matching the same genre
    # do it only if genre is not nan or empty string
    if genre_nohist[content_id] in [x for x in list(genre_nohist) if ((x == x) and (x != ''))]:
        genre_logical = genre_nohist == genre_nohist[content_id]
        genre_logical = np.array(genre_logical.astype(int))
        #
        scores = scores * genre_logical
    
    # make zero the content with 2 or more degrees higher rating except for kids (same rating only)
    if scheduler_channel_nohist[content_id] == 'Kids':
        rating_logical = rating_nohist == rating_nohist[content_id]
        rating_logical = np.array(rating_logical.astype(int))
        #
        scores = scores * rating_logical
    else:
        rating_logical = rating_nohist <= (rating_nohist[content_id] + 1)
        rating_logical = np.array(rating_logical.astype(int))
        #
        scores = scores * rating_logical
    
    similar = sorted(zip(id_editorial_list_nohist, scores[id_editorial_simple_list_nohist], type_production_list_nohist), key=lambda x: -x[1])
    
    return similar[:21] # return 21, so later can drop itself and will have at least 20

def more_like_this(content_id, content_vecs, content_norms, syn_vecs, syn_norms, othermd_vecs, othermd_norms, availability, id_editorial_list, id_editorial_simple_list, type_production_list, rating, genre, scheduler_channel):
    
    ## historical data
    scores_cont = content_vecs.dot(content_vecs[content_id,:])  / (content_norms * content_vecs[content_id,:].sum()) # i.e. calculating cosine similarity, (A.B) / (|A| x |B|) --> |B| just a constant so effectively won't need it
    scores_cont =  MinMaxScaler().fit_transform(scores_cont.reshape(-1,1))[:,0]
    
    ## synopsis
    scores_syn = syn_vecs.dot(syn_vecs[content_id,:])  / (syn_norms * syn_vecs[content_id,:].sum()) # i.e. calculating cosine similarity, (A.B) / (|A| x |B|) --> |B| just a constant so effectively won't need it
    scores_syn =  MinMaxScaler().fit_transform(scores_syn.reshape(-1,1))[:,0]
    
    ## other metadata
    scores_othermd = othermd_vecs.dot(othermd_vecs[content_id,:])  / (othermd_norms * othermd_vecs[content_id,:].sum()) # i.e. calculating cosine similarity, (A.B) / (|A| x |B|) --> |B| just a constant so effectively won't need it
    scores_othermd =  MinMaxScaler().fit_transform(scores_syn.reshape(-1,1))[:,0]
    
    ## sum scores
    a_cont = get_aws_config('model_parameters.json')['MLT_strength_historical']
    a_syn = get_aws_config('model_parameters.json')['MLT_strength_synopsis']
    a_othermd = get_aws_config('model_parameters.json')['MLT_strength_othermetadata']
    #
    scores = ((a_cont * scores_cont) + (a_syn * scores_syn) + (a_othermd * scores_othermd)) / (a_cont + a_syn + a_othermd)
    
    # make zero the content no longer avilable
    scores = scores * availability
    
    # make zero the content not matching the same scheduler_channel
    if scheduler_channel[content_id] in ['Kids', 'Film', 'TV', 'TV Replay', 'Sport', 'Music']:
        scheduler_channel_logical = scheduler_channel == scheduler_channel[content_id]
        scheduler_channel_logical = np.array(scheduler_channel_logical.astype(int))
        #
        scores = scores * scheduler_channel_logical
    
    # make zero the content not matching the same genre
    # do it only if genre is not nan or empty string
    if genre[content_id] in [x for x in list(genre) if ((x == x) and (x != ''))]:
        genre_logical = genre == genre[content_id]
        genre_logical = np.array(genre_logical.astype(int))
        #
        scores = scores * genre_logical
    
    # make zero the content with 2 or more degrees higher rating except for kids (same rating only)
    if scheduler_channel[content_id] == 'Kids':
        rating_logical = rating == rating[content_id]
        rating_logical = np.array(rating_logical.astype(int))
        #
        scores = scores * rating_logical
    else:
        rating_logical = rating <= (rating[content_id] + 1)
        rating_logical = np.array(rating_logical.astype(int))
        #
        scores = scores * rating_logical
    
    similar = sorted(zip(id_editorial_list, scores[id_editorial_simple_list], type_production_list), key=lambda x: -x[1])
    
    return similar[:21] # return 21, so later can drop itself and will have at least 20
    
def for_you(person_id, sparse_person_content_hidelogic, content_vecs_T, person_vecs, availability, id_editorial_list, id_editorial_simple_list, type_production_list, id_user_list, id_user_simple_list):

    # Get the interactions scores from the sparse person content matrix
    person_interactions = sparse_person_content_hidelogic[person_id,:].toarray()

    # Add 1 to everything, so that articles with no interaction yet become equal to 1
    person_interactions = person_interactions.reshape(-1) + 1

    # Make articles already interacted zero
    person_interactions[person_interactions > 1] = 0

    # Get dot product of person vector and all content vectors
    rec_vector = person_vecs[person_id,:].dot(content_vecs_T)

    rec_vector = MinMaxScaler().fit_transform(rec_vector.reshape(-1,1))[:,0]

    # Multiply by zero the scores of items that need to be hidden according to the logic
    recommend_vector = person_interactions * rec_vector
    
    # make zero the content no longer avilable
    recommend_vector = recommend_vector * availability
    
    order = np.argsort(-recommend_vector)[:50] # get only top 50 items
    b = recommend_vector[id_editorial_simple_list][order]
    a = np.array(id_editorial_list)[order]
    c = np.array(type_production_list)[order]
    recs = list(zip(list(a),list(b),list(c)))
    
    user_id = id_user_list[id_user_simple_list.index(person_id)]
    
    return recs, user_id

def get_generic(data_implicit, condition, n_days, date_today, data_update_date, code_version):
    
    popular = data_implicit[condition]['id_editorial_simple'].value_counts()
    
    rec_guids = []
    scores_fake = sorted(list(range(len(popular.index.tolist()[:20]))), reverse=True)
    for count, i in enumerate(popular.index.tolist()[:20]):
        
        rec_guids.append({
                'guid' : data_implicit[data_implicit['id_editorial_simple'] == i]['id_editorial'].iloc[0],
                'score' : scores_fake[count],
                'type' : data_implicit[data_implicit['id_editorial_simple'] == i]['type_production'].iloc[0]
            })
        
    output_rec = {'score_date' : date_today, 'data_update_date' : data_update_date, 'code_version' : code_version, 'recommendations' : rec_guids}
    
    return output_rec


def get_recommendations(data_implicit, model, data_nohist):
    '''
    '''
    
    logger.info('Creating recommendations...')
    
    # (sorting first alphabetically by status so ACTIVE versions of same items appears first over INACTIVE versions)
    id_editorial_legend = data_implicit[['id_editorial', 'id_editorial_simple', 'type_production', 'end_date', 'start_date', 'status', 'rating_n', 'genre', 'scheduler_channel', 'synopsis', 'sub_genres']].sort_values(by=['status','sub_genres']).drop_duplicates(subset=['id_editorial', 'id_editorial_simple']).sort_values(by='id_editorial_simple')

    id_editorial_simple_list = id_editorial_legend['id_editorial_simple'].tolist()
    id_editorial_list = id_editorial_legend['id_editorial'].tolist()
    type_production_list = id_editorial_legend['type_production'].tolist()

    # get array with 1 or 0 based on availability of content today + status
    availability = (id_editorial_legend['status'] == 'ACTIVE') & (id_editorial_legend['end_date'] > datetime.today().strftime('%Y-%m-%d')) & (id_editorial_legend['start_date'] <= datetime.today().strftime('%Y-%m-%d'))
    availability = np.array(availability.astype(int))
    
    scheduler_channel = np.array(id_editorial_legend['scheduler_channel'].tolist())
    
    genre = np.array(id_editorial_legend['genre'].tolist())
    
    sub_genres = [str(x).lower().replace(' ', '').replace(',', ' ') for x in id_editorial_legend['sub_genres'].tolist()]
    
    rating = np.array(id_editorial_legend['rating_n'].tolist())
    
    synopsis = id_editorial_legend['synopsis'].tolist()


    id_user_legend = data_implicit[['id_user', 'id_user_simple']].drop_duplicates(subset=['id_user', 'id_user_simple']).sort_values(by='id_user_simple')
    id_user_simple_list = id_user_legend['id_user_simple'].tolist()
    id_user_list = id_user_legend['id_user'].tolist()

    date_today = datetime.today().strftime('%Y-%m-%d')
    code_version = get_aws_config('model_parameters.json')['code_version']
    
    data_update_date =  {
        'view' : str(data_implicit[data_implicit['type_entitlement'] == 'VIEW']['event_date'].max()),
        'purchase' : str(data_implicit[data_implicit['type_entitlement'] == 'EVOD']['event_date'].max()),
        'rental' : str(data_implicit[data_implicit['type_entitlement'] == 'TVOD']['event_date'].max())
    }
    
    sparse_content_person = sparse.csr_matrix(
        (data_implicit['eventStrength'].astype(float), (data_implicit['id_editorial_simple'], data_implicit['id_user_simple']))
    )
#     sparse_person_content = sparse.csr_matrix(
#         (data_implicit['eventStrength'].astype(float), (data_implicit['id_user_simple'], data_implicit['id_editorial_simple']))
#     )
    sparse_person_content_hidelogic = sparse.csr_matrix(
        (data_implicit['FY_logic'].astype(float), (data_implicit['id_user_simple'], data_implicit['id_editorial_simple']))
    )
    
    ### More Like This   
    logger.info('More Like This')
    
    content_vecs = model.item_factors
    content_norms = np.sqrt((content_vecs * content_vecs).sum(axis=1)) # i.e. calculating abs. value of the vector of each item -->  |A|
    
    embed = load_use()
    syn_vecs = np.array(embed(synopsis))
    syn_norms = np.sqrt((syn_vecs * syn_vecs).sum(axis=1)) # i.e. calculating abs. value of the vector of each item -->  |A|
    
    vectorizer = CountVectorizer()
    othermd_vecs = vectorizer.fit_transform(sub_genres).toarray()
    othermd_norms = np.sqrt((othermd_vecs * othermd_vecs).sum(axis=1)) # i.e. calculating abs. value of the vector of each item -->  |A|

    # create the output for dynamoDB table
    output = []

    for index, i in enumerate(id_editorial_simple_list): 

        if availability[index] == 1: # check if content is available otherwise those recommendations will be wrong due to having multiplied scores by zero for unavailable content

            con_id = id_editorial_list[index]
            recs = more_like_this(i, content_vecs, content_norms, syn_vecs, syn_norms, othermd_vecs, othermd_norms, availability, id_editorial_list, id_editorial_simple_list, type_production_list, rating, genre, scheduler_channel)

            rec_guids = []
            for r in recs:
                
                if r[0] != con_id: # skip itself

                    rec_guids.append({
                        'guid' : r[0],
                        'score' : round(float(r[1]), 5),
                        'type' : r[2]
                    })

            output_rec = {'score_date' : date_today, 'data_update_date' : data_update_date, 'code_version' : code_version, 'recommendations' : rec_guids[:20]}

            #output.append([con_id, output_rec])
            
            #also append same recommendations with <type>|<guid> lookup
            con_id_type = type_production_list[index]
            output.append([con_id_type + '|' + con_id, output_rec])

    output_morelikethis = pd.DataFrame(output,columns=['content','recommendations'])

    output_morelikethis['recommendations'] = output_morelikethis['recommendations'].apply(lambda x: json.dumps(x)) # if already a dict

#     # remove any duplicates; these can still occur if some content has exactly the same watch history as other and gets most similar item itself and other stuff too
#     output_morelikethis = output_morelikethis.drop_duplicates(subset=['content']).reset_index(drop=True)

    logger.info('More Like This with history: done - found {} items'.format(len(output_morelikethis)))
   
    
    ####### More Like This for items without history 
    logger.info('More Like This for items without history')
    data_nohist['id_editorial_simple'] = data_nohist['id_editorial'].astype("category").cat.codes.tolist()
    data_nohist = data_nohist.drop_duplicates(subset=['id_editorial', 'id_editorial_simple']).sort_values(by='id_editorial_simple')
    
    synopsis_nohist = data_nohist['synopsis'].tolist()
    sub_genres_nohist = data_nohist['subgenres'].tolist()
    
    # this here is 1 if missingbhistory and needs MLT or 0 if already has history and can be skipped
    history = data_nohist['history'].tolist() 
    
    id_editorial_list_nohist = data_nohist['id_editorial'].tolist()
    id_editorial_simple_list_nohist = data_nohist['id_editorial_simple'].tolist()
    
    type_production_list_nohist = data_nohist['type_production'].tolist()
    
    rating_nohist = np.array(data_nohist['rating_n'].tolist())
    
    genre_nohist = np.array(data_nohist['genre'].tolist())
    
    scheduler_channel_nohist = np.array(data_nohist['scheduler_channel'].tolist())
    
    syn_vecs = np.array(embed(synopsis_nohist))
    syn_norms = np.sqrt((syn_vecs * syn_vecs).sum(axis=1)) # i.e. calculating abs. value of the vector of each item -->  |A|
    
    vectorizer = CountVectorizer()
    othermd_vecs = vectorizer.fit_transform(sub_genres_nohist).toarray()
    othermd_norms = np.sqrt((othermd_vecs * othermd_vecs).sum(axis=1)) # i.e. calculating abs. value of the vector of each item -->  |A|

    # create the output for dynamoDB table
    output = []

    for index, i in enumerate(id_editorial_simple_list_nohist): 

        if history[index] == 1: # check if content is available otherwise those recommendations will be wrong due to having multiplied scores by zero for unavailable content

            con_id = data_nohist['id_editorial'].tolist()[index]
            recs = more_like_this_nohist(i, syn_vecs, syn_norms, othermd_vecs, othermd_norms, id_editorial_list_nohist, id_editorial_simple_list_nohist, type_production_list_nohist, rating_nohist, genre_nohist, scheduler_channel_nohist)

            rec_guids = []
            for r in recs:
                
                if r[0] != con_id: # skip itself

                    rec_guids.append({
                        'guid' : r[0],
                        'score' : round(float(r[1]), 5),
                        'type' : r[2]
                    })

            output_rec = {'score_date' : date_today, 'data_update_date' : data_update_date, 'code_version' : code_version, 'recommendations' : rec_guids[:20]}

            #output.append([con_id, output_rec])
            
            #also append same recommendations with <type>|<guid> lookup
            con_id_type = type_production_list_nohist[index]
            output.append([con_id_type + '|' + con_id, output_rec])

    output_morelikethis_nohist = pd.DataFrame(output,columns=['content','recommendations'])

    output_morelikethis_nohist['recommendations'] = output_morelikethis_nohist['recommendations'].apply(lambda x: json.dumps(x)) # if already a dict
    
    logger.info('More Like This without history: done - found {} items'.format(len(output_morelikethis_nohist)))
    
    
    output_morelikethis_full = pd.concat([output_morelikethis, output_morelikethis_nohist], ignore_index=True)
    logger.info('More Like This full: done - found {} items'.format(len(output_morelikethis_full)))
    
    #######
    
    
    ### For You
    logger.info('For You')
    content_vecs_T = model.item_factors.T
    person_vecs = model.user_factors

    # create the output for dynamoDB table
    output = []
    
    # Do For You for all users loaded in, unless testing
    users_list_foryou = id_user_simple_list.copy()
    #
    if "ENVIRON_PROD" in os.environ:
        if os.environ['ENVIRON_PROD'] == 'TESTING':
            users_tech_trial = [
                'V3006126692',
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
                'V2200007285',
                'V3000163582',
                'V3000537931'
            ]
            # turn the VSID of the tech trail list to simple user ids
            users_list_foryou = [int(data_implicit[data_implicit['id_user'] == x]['id_user_simple'].iloc[0]) for x in users_tech_trial if x in id_user_list]
        
        
    for i in users_list_foryou: 

        recs, user_id = for_you(i, sparse_person_content_hidelogic, content_vecs_T, person_vecs, availability, id_editorial_list, id_editorial_simple_list, type_production_list, id_user_list, id_user_simple_list)

        rec_guids = []
        for r in recs:

            rec_guids.append({
                'guid' : r[0],
                'score' : round(float(r[1]), 5),
                'type' : r[2]
            })

        output_rec = {'score_date' : date_today, 'data_update_date' : data_update_date, 'code_version' : code_version, 'recommendations' : rec_guids}

        output.append([user_id, output_rec])
        
        if int(i+1) % 50_000 == 0:
            logger.info('For You calculated for {} users'.format(int(i)+1))
    logger.info('For You calculated for all users ({}).'.format(int(i)+1)) 
    
    ##### add 'anonymous' entry for most popular content
    logger.info('Adding anonymous in For You table')
    n_days = 9
    
    condition = (data_implicit['event_date'] + pd.DateOffset(n_days) >= date_today)
    output.append(['anonymous', get_generic(data_implicit, condition, n_days, date_today, data_update_date, code_version)])
    #####

    output_foryou = pd.DataFrame(output,columns=['user','recommendations'])

    output_foryou['recommendations'] = output_foryou['recommendations'].apply(lambda x: json.dumps(x)) # if already a dict

    logger.info('For You: done')
    
    
    ### Generic table
    logger.info('Creating generic recommendations...')
    n_days = 9
    output = []
    
    aval = (data_implicit['status'] == 'ACTIVE') & (data_implicit['end_date'] > datetime.today().strftime('%Y-%m-%d')) & (data_implicit['start_date'] <= datetime.today().strftime('%Y-%m-%d'))
    
    # mostPopularAll
    condition = (data_implicit['event_date'] + pd.DateOffset(n_days) >= date_today) & aval
    output.append(['mostPopularAll', get_generic(data_implicit, condition, n_days, date_today, data_update_date, code_version)])
    logger.info('mostPopularAll done')
    
    # mostPopularCatchUp
    condition = (data_implicit['event_date'] + pd.DateOffset(n_days) >= date_today) & (data_implicit['scheduler_channel'] == 'TV Replay') & aval
    output.append(['mostPopularCatchUp', get_generic(data_implicit, condition, n_days, date_today, data_update_date, code_version)])
    logger.info('mostPopularCatchUp done')
    
    # mostPopularKids
    condition = (data_implicit['event_date'] + pd.DateOffset(n_days) >= date_today) & (data_implicit['scheduler_channel'] == 'Kids') & aval
    output.append(['mostPopularKids', get_generic(data_implicit, condition, n_days, date_today, data_update_date, code_version)])
    logger.info('mostPopularKids done')
    
    # mostPopularTV
    condition = (data_implicit['event_date'] + pd.DateOffset(n_days) >= date_today) & (data_implicit['scheduler_channel'] == 'TV') & aval
    output.append(['mostPopularTV', get_generic(data_implicit, condition, n_days, date_today, data_update_date, code_version)])
    logger.info('mostPopularTV done')
    
    # mostPopularSport
    condition = (data_implicit['event_date'] + pd.DateOffset(n_days) >= date_today) & (data_implicit['scheduler_channel'] == 'Sport') & aval
    output.append(['mostPopularSport', get_generic(data_implicit, condition, n_days, date_today, data_update_date, code_version)])
    logger.info('mostPopularSport done')
    
    # mostPopularFilm
    condition = (data_implicit['event_date'] + pd.DateOffset(n_days) >= date_today) & (data_implicit['scheduler_channel'] == 'Film') & aval
    output.append(['mostPopularFilm', get_generic(data_implicit, condition, n_days, date_today, data_update_date, code_version)])
    logger.info('mostPopularFilm done')
    
    # mostPopularFilmPurchases
    condition = (data_implicit['event_date'] + pd.DateOffset(n_days) >= date_today) & (data_implicit['scheduler_channel'] == 'Film') & (data_implicit['type_entitlement'] == 'EVOD') & aval
    output.append(['mostPopularFilmPurchases', get_generic(data_implicit, condition, n_days, date_today, data_update_date, code_version)])
    logger.info('mostPopularFilmPurchases done')
    
    # mostPopularFilmRentals
    condition = (data_implicit['event_date'] + pd.DateOffset(n_days) >= date_today) & (data_implicit['scheduler_channel'] == 'Film') & (data_implicit['type_entitlement'] == 'TVOD') & aval
    output.append(['mostPopularFilmRentals', get_generic(data_implicit, condition, n_days, date_today, data_update_date, code_version)])
    logger.info('mostPopularFilmRentals done')
    
    output_generic = pd.DataFrame(output,columns=['id','recommendations'])

    output_generic['recommendations'] = output_generic['recommendations'].apply(lambda x: json.dumps(x)) # if already a dict
    
    logger.info('Generic recommendations: done')
    
    
    return output_foryou, output_morelikethis_full, output_generic
    
def run_scoring_job():
    '''
    Run all scoring operations
    '''
    
    logger.info('Starting up BT TV Recommendation Engine...')
    
#     config = get_aws_config()
#     check_required_files_present(config)
    
    data = mdt.get_data()
    logger.info('Data is ready for model training')
    
    data_nohist = mdt.get_data_nohist(data)
    logger.info('Data with no history is also loaded')
    
    model = create_model(data)
    logger.info('Model trained with data with history')
    
    recs_foryou, recs_morelikethis, recs_generic = get_recommendations(data, model, data_nohist)
    
    return recs_foryou, recs_morelikethis, recs_generic
