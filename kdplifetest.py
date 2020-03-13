import requests
import time, os, re, datetime, json, csv
import pandas as pd
from datetime import datetime, timedelta

from pprint import pprint
from requests_oauthlib import OAuth2Session


def main():

# Declaring global variables

    refresh_token = 'eyJraWQiOiJyZWZyZXNoLXRva2VuLWExMjhnY20uMCIsInR5cCI6IkpXVCIsImVuYyI6IkExMjhHQ00iLCJhbGciOiJkaXIifQ..iWNLgezQSiFHbP12.-i_D01OxODwVXM7lHVkMFiTsCOuYN4ZYhNwVxkCnHLwRzb5J77noQG1lJ0wmeqNE98oIba2nmnKUcKS5CrPN0zB5zFPr39oNVcotqzedQk6apcrSXAdOp4MJDy5h5CmvlMIAkE1v91TGm-8a-aZQQE67OS05wFZkFXe1lIt6lbdQOlMTDFv0YdpqwLyCzH2IxxR9HCiG6kCOva4wJWpcF-tAmn5UeOxgM0zA5mJZlfft90aAnts4rBeKHFutos1vTVj3kk0Fi5ZhKSQ.rFw86K89WLpfPGiI-bJakA'
    client_id = 'f1304535-70d6-4a4d-a4ea-96ea7c7e51a3'
    client_secret = '7a221d4220bebea2f06b'
    grant_type = 'refresh_token'

    granularity = 'LIFETIME'
    
# snapchat ad accounts for KDP client

#    ad_account_dict = {'a1d31626-7eae-419d-961b-dcc048b66bf3':'2019-2020_3dpe_sna_snapple-base_mixed-ios_sc_social'}

    ad_account_dict = {'a1d31626-7eae-419d-961b-dcc048b66bf3':'2019-2020_3dpe_sna_snapple-base_mixed-ios_sc_social',
                       'b2a6b0ce-18cb-40f2-b3d7-c8181e939ef1':'2019_3dpe_core water_2019-core_o-1mlzw_sc_social',
                       'c98c3d58-934c-47f0-a49a-b80b50f6a08f':'2019_3dpe_dr pepper_2019-cfb_o-1rdhb_sc_social',
                       '4c0e15e4-4f3e-420d-b7dd-8e2f4803a20d':'DPSG',
                       '117b4b5b-3f83-4dd6-8023-68c87caaf123':'DPSG_DPTM_2019 Dr Pepper - Base_O-1JC8C_3DPE_DRP__72275_CPPNS6',
                       'c234985a-6715-4480-b44f-3425871b6c2f':'DPSG_DPTM_2019 Dr Pepper - Cherry_O-1KQ54_3DPE_DPC__72276_CPPNSH',
                       '88fd8064-01b7-4730-ae68-ed9a5ffeb4f3':'DPSG_DPTM_2019 Dr Pepper - Hispanic_O-1J0G2_3DPE_DPH__72274_CPPN6G'}
                        
    cwd = os.getcwd()
    if not os.path.exists(cwd + '/snaptemp'):
        os.makedirs(cwd + '/snaptemp')
    csv_file = cwd + '/snaptemp/KDPLifeTest.csv'    # <<<<<=== Change to a desirable output file location and/or name
     
# Get an authorization token

    token = refreshtoken(client_id,client_secret,grant_type,refresh_token)

# Get all campaigns for ad accounts as a dataframe 

    campaigns_df = getcampaigns(token,ad_account_dict)
    
# Get all ad_squads for ad accounts as a dataframe 

    ad_squad_df = getad_squad(token,ad_account_dict)
     
# Get all ads for ad accounts as a dataframe 

    ad_df = getad(token,ad_account_dict)
     
# Get ad stats for each ad as a dataframe 

    adstats_df_lifetime = adstats(token,granularity,ad_df)

# Get GEO stats for each ad as a dataframe 

    adsgeo_df_lifetime = adsgeo(token,granularity,ad_df)

# Merge all dataframes to a result frame

    resultframe = joinframes(campaigns_df,ad_squad_df,ad_df,adstats_df_lifetime,adsgeo_df_lifetime)

# Write output file as csv

    try:
        resultframe.to_csv(csv_file, encoding='utf-8', index=False)
        print ('Process completed successfuly')   
    except IOError:
       print("File I/O error")  

def refreshtoken(client_id,client_secret,grant_type,refresh_token):
    token = ''
    tokenargs = {'client_id':client_id,'client_secret':client_secret,'grant_type':grant_type,'refresh_token':refresh_token}
    
    r = requests.post('https://accounts.snapchat.com/login/oauth2/access_token',data = tokenargs)

    if r.status_code != 200:
        print ('Bad Return from getting auth token - {} {}'.format(r.status_code,r.text))
    else:
        print ('>>>> Get token - OK <<<<< {}'.format(r.status_code) )
    outdict = r.json()
    token = outdict['access_token']
    
    return token

def reformatdate(date):

    temp = date.split('T')
    temp = temp[0].split('-')

    refdate = temp[1] + '/' + temp[2] + '/' + temp[0] + ' 0:00'
    return(refdate)

def getcampaigns(token,ad_account_dict):
    campaigns_frame_list = []
    bearer_token = 'Bearer ' + token
    headerargs = {'Authorization':bearer_token}

    for ad_account in ad_account_dict:
        url = 'https://adsapi.snapchat.com/v1/adaccounts/'+ ad_account + '/campaigns'
        r = requests.get(url,headers=headerargs)

        if r.status_code != 200:
            print ('Bad Return from get campaigns api - {} {}'.format(r.status_code,r.text))
        else:
            print ('>>>> Get capmaigns - OK <<<<< {}'.format(r.status_code) )

        apiresult = r.json()

        campaigns = apiresult["campaigns"]
       
        for campaign in campaigns:
          
            campaign['campaign']['created_at'] = reformatdate(campaign['campaign']['created_at'])

            if 'updated_at' in campaign['campaign'].keys():
           
                campaign['campaign']['updated_at'] = reformatdate(campaign['campaign']['updated_at'])
             
            campaign['campaign']['start_time'] = reformatdate(campaign['campaign']['start_time'])

            if 'end_time' in campaign['campaign'].keys():
            
                campaign['campaign']['end_time'] = reformatdate(campaign['campaign']['end_time'])

            df = pd.DataFrame([campaign["campaign"]],columns=campaign["campaign"].keys())
      
            campaigns_frame_list.append(df)

    resultframe = pd.concat(campaigns_frame_list)    
    resultframe.rename(columns = {'id':'campaign_id', 'updated_at':'campaign_updated_at', 
                              'name':'campaign_name', 'start_time':'campaign_start_time','end_time':'campaign_end_time',  
                              'status':'campaign_status', 'daily_budget_micro':'campaign_daily_budget_micro',   
                              'created_at':'campaign_created_at'}, inplace = True)  
     
    return (resultframe)

def getad_squad(token,ad_account_dict):

    ad_squad_frame_list = []
    bearer_token = 'Bearer ' + token
    headerargs = {'Authorization':bearer_token}

    for ad_account in ad_account_dict:
        url = 'https://adsapi.snapchat.com/v1/adaccounts/'+ ad_account + '/adsquads'
        r = requests.get(url,headers=headerargs)

        if r.status_code != 200:
            print ('Bad Return from get ad_squads api - {} {}'.format(r.status_code,r.text))
        else:
            print ('>>>> Get ad squads - OK <<<<< {}'.format(r.status_code) )

        apiresult = r.json()
        
        ad_squads = apiresult["adsquads"]
         
        for ad_squad in ad_squads:

            ad_squad['adsquad']['created_at'] = reformatdate(ad_squad['adsquad']['created_at'])

            if 'updated_at' in ad_squad['adsquad'].keys():
           
                ad_squad['adsquad']['updated_at'] = reformatdate(ad_squad['adsquad']['updated_at'])

            if 'start_time' in ad_squad['adsquad'].keys():
             
                ad_squad['adsquad']['start_time'] = reformatdate(ad_squad['adsquad']['start_time'])

            if 'end_time' in ad_squad['adsquad'].keys():
            
                ad_squad['adsquad']['end_time'] = reformatdate(ad_squad['adsquad']['end_time'])
             
            df = pd.DataFrame([ad_squad["adsquad"]],columns=ad_squad["adsquad"].keys())
            
            ad_squad_frame_list.append(df)

    resultframe = pd.concat(ad_squad_frame_list)     
    resultframe.rename(columns = {'id':'ad_squad_id', 'updated_at':'ad_squad_updated_at', 
                              'name':'ad_squad_name', 'start_time':'ad_squad_start_time','end_time':'ad_squad_end_time','type':'ad_squad_type',  
                              'status':'ad_squad_status', 'daily_budget_micro':'ad_squad_daily_budget_micro',   
                              'created_at':'ad_squad_created_at'}, inplace = True)  
     
    return (resultframe)

def getad(token,ad_account_dict):
    
    ad_frame_list = []
    bearer_token = 'Bearer ' + token
    headerargs = {'Authorization':bearer_token}

    for ad_account in ad_account_dict:
        url = 'https://adsapi.snapchat.com/v1/adaccounts/'+ ad_account + '/ads'
        r = requests.get(url,headers=headerargs)

        if r.status_code != 200:
            print ('Bad Return from get ads api - {} {}'.format(r.status_code,r.text))
        else:
            print ('>>>> Get ads - OK <<<<< {}'.format(r.status_code) )

        apiresult = r.json()

        ads = apiresult["ads"]
        
        for ad in ads:

            ad['ad']['created_at'] = reformatdate(ad['ad']['created_at'])

            if 'updated_at' in ad['ad'].keys():
           
                ad['ad']['updated_at'] = reformatdate(ad['ad']['updated_at'])
             
            df = pd.DataFrame([ad["ad"]],columns=ad["ad"].keys())
            
            ad_frame_list.append(df)

    resultframe = pd.concat(ad_frame_list)     
    resultframe.rename(columns = {'id':'ad_id', 'updated_at':'ad_updated_at', 
                              'name':'ad_name', 'type':'ad_type',  
                              'status':'ad_status',   
                              'created_at':'ad_created_at'}, inplace = True)  
     
    return (resultframe)

def adstats(token,granul,ad_df):

    adstats_frame_list_lifetime = []

    bearer_token = 'Bearer ' + token
    headerargs = {'Authorization':bearer_token}

    source_types = 'total,web,app'
     
    returnfields1 = 'impressions,swipes,view_time_millis,screen_time_millis,quartile_1,quartile_2,quartile_3,view_completion,spend,video_views,shares,saves,'
    returnfields3 = 'conversion_purchases,conversion_save,conversion_start_checkout,conversion_add_cart,conversion_view_content,conversion_add_billing,' 
    returnfields2 = 'conversion_sign_ups,conversion_searches,conversion_level_completes,conversion_app_opens,conversion_page_views,'
    
    returnfields  = returnfields1 + returnfields2 + returnfields3

    apiargs = {'granularity':granul,'conversion_source_types':source_types,'fields':returnfields}
                
    for column in ad_df[['ad_id']]:
        columnSeriesObj = ad_df[column]
        for ad_id in columnSeriesObj:
            adstats_dict = {'ad_id':ad_id}
            url = 'https://adsapi.snapchat.com/v1/ads/'+ ad_id + '/stats'
             
            r = requests.get(url,params=apiargs,headers=headerargs)

            if r.status_code != 200:
                print ('Bad Return from ad stats api - {} {}'.format(r.status_code,r.text))
            else:
                print ('>>>> Get LIFETIME ad stats - OK <<<<< ad id - {} '.format(ad_id) )
            apiresult = r.json()
                         
            adstats_dict_stat = apiresult["lifetime_stats"][0]["lifetime_stat"]["stats"]
             
            if len(adstats_dict_stat) > 0:
                
                adstats_dict.update(adstats_dict_stat)
            
            df = pd.DataFrame([adstats_dict],columns=adstats_dict.keys())
            
            adstats_frame_list_lifetime.append(df)

    resultframe = pd.concat(adstats_frame_list_lifetime)  

    return resultframe

def adsgeo(token,granul,ad_df):

    adsgeo_frame_list_lifetime = []

    bearer_token = 'Bearer ' + token
    headerargs = {'Authorization':bearer_token}

    returnfields = 'impressions,impression_composition,uniques,unique_composition,spend,swipes'
     
    apiargs = {'granularity':granul,'dimension':'GEO','pivots':'country,region,dma','fields':returnfields}
                
    for column in ad_df[['ad_id']]:
        columnSeriesObj = ad_df[column]
        for ad_id in columnSeriesObj:
            adsgeo_dict = {'ad_id':ad_id}
            url = 'https://adsapi.snapchat.com/v1/ads/'+ ad_id + '/stats'
             
            r = requests.get(url,params=apiargs,headers=headerargs)

            if r.status_code != 200:
                print ('Bad Return from GEO stats api - {} {}'.format(r.status_code,r.text))
            else:
                print ('>>>> Get LIFETIME GEO stats - OK <<<<< ad id - {} '.format(ad_id) )
            apiresult = r.json()
                    
            adsgeo_dict_stat = apiresult["lifetime_stats"][0]["lifetime_stat"]["dimension_stats"]
            
            if len(adsgeo_dict_stat) > 0:
                for item in adsgeo_dict_stat:
                    adsgeo_dict.update(item)
                    df = pd.DataFrame([adsgeo_dict],columns=adsgeo_dict.keys())
                    adsgeo_frame_list_lifetime.append(df)
                    
    resultframe = pd.concat(adsgeo_frame_list_lifetime)  
     
    resultframe.rename(columns = {'impressions':'geo_impressions', 'impression_composition':'geo_impression_composition', 
                              'uniques':'geo_uniques', 'unique_composition':'geo_unique_composition',  
                              'spend':'geo_spend',   
                              'swipes':'geo_swipes'}, inplace = True)  

    return resultframe  
    

 
def joinframes(campaigns_df,ad_squad_df,ad_df,adstats_df_total,adsgeo_df_total):
    
    joindf0 = adsgeo_df_total.merge(adstats_df_total,left_on = 'ad_id',right_on = 'ad_id',how='outer')
    
    joindf1 = ad_df.merge(joindf0,left_on = 'ad_id',right_on = 'ad_id',how='outer')
    joindf2 = ad_squad_df.merge(joindf1,left_on = 'ad_squad_id',right_on = 'ad_squad_id',how='outer')
    joindf = campaigns_df.merge(joindf2,left_on = 'campaign_id',right_on = 'campaign_id',how = 'outer')
     
    return joindf

if __name__ == '__main__':
    main()
