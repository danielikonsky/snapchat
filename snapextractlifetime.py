import requests
import time, os, re, datetime, json, csv
import pandas as pd
from datetime import datetime, timedelta

from pprint import pprint

def main():

# Declaring global variables

    refresh_token = 'eyJraWQiOiJyZWZyZXNoLXRva2VuLWExMjhnY20uMCIsInR5cCI6IkpXVCIsImVuYyI6IkExMjhHQ00iLCJhbGciOiJkaXIifQ..iWNLgezQSiFHbP12.-i_D01OxODwVXM7lHVkMFiTsCOuYN4ZYhNwVxkCnHLwRzb5J77noQG1lJ0wmeqNE98oIba2nmnKUcKS5CrPN0zB5zFPr39oNVcotqzedQk6apcrSXAdOp4MJDy5h5CmvlMIAkE1v91TGm-8a-aZQQE67OS05wFZkFXe1lIt6lbdQOlMTDFv0YdpqwLyCzH2IxxR9HCiG6kCOva4wJWpcF-tAmn5UeOxgM0zA5mJZlfft90aAnts4rBeKHFutos1vTVj3kk0Fi5ZhKSQ.rFw86K89WLpfPGiI-bJakA'
    client_id = 'f1304535-70d6-4a4d-a4ea-96ea7c7e51a3'
    client_secret = '7a221d4220bebea2f06b'
    grant_type = 'refresh_token'

    granularity = 'LIFETIME'
    
# snapchat ad accounts for KDP client

#    ad_account_dict = {'a1d31626-7eae-419d-961b-dcc048b66bf3':['2019-2020_3dpe_sna_snapple-base_mixed-ios_sc_social','kdp','snapple']}

    ad_account_dict = {'a1d31626-7eae-419d-961b-dcc048b66bf3':['2019-2020_3dpe_sna_snapple-base_mixed-ios_sc_social','kdp','snapple'],
                       'b2a6b0ce-18cb-40f2-b3d7-c8181e939ef1':['2019_3dpe_core water_2019-core_o-1mlzw_sc_social','kdp','core water'],
                       'c98c3d58-934c-47f0-a49a-b80b50f6a08f':['2019_3dpe_dr pepper_2019-cfb_o-1rdhb_sc_social','kdp','dr pepper'],
                       '4c0e15e4-4f3e-420d-b7dd-8e2f4803a20d':['DPSG','',''],
                       '117b4b5b-3f83-4dd6-8023-68c87caaf123':['DPSG_DPTM_2019 Dr Pepper - Base_O-1JC8C_3DPE_DRP__72275_CPPNS6','kdp','Dr Pepper Base'],
                       'c234985a-6715-4480-b44f-3425871b6c2f':['DPSG_DPTM_2019 Dr Pepper - Cherry_O-1KQ54_3DPE_DPC__72276_CPPNSH','kdp','Dr Pepper Cherry'],
                       '88fd8064-01b7-4730-ae68-ed9a5ffeb4f3':['DPSG_DPTM_2019 Dr Pepper - Hispanic_O-1J0G2_3DPE_DPH__72274_CPPN6G','kdp','Dr Pepper Hispanic'],
                       '6804e233-6abb-42bf-97d0-91396182535b':['Applebee"s_Initiative_NDM','Applebee"s',''],
                       '89f6d110-24a1-4c4f-9993-d7054afb7f62':['Gardasil YA_Merck','Merck',''],
                       '52852a1e-a51f-4c75-91e5-235ddf8597aa':['SpinMaster_TechDeck_Reprise_SnapAds','SpinMaster',''],
                       '7e98138f-4c15-4c7a-9e34-3f2a6af2de54':['NEXPLANON || Merck Credit Line','Merck',''],
                       '9c26e57c-8326-4642-96d4-0a7ddb0a2930':['Spin Master - Headbanz','Spin Master',''],
                       '1f8019d3-d797-4cf4-87f1-6ebc2613554d':['SpinMaster_Fugglers_Reprise','SpinMaster',''],
                       '78100553-58ac-4d2d-88fb-c4c1ed3e259a':['SpinMaster_Moonlite_Reprise','SpinMaster',''],
                       '3eb7ced7-5cbe-4785-9f73-4447f6ca34fe':['SpinMaster_TechDeck_Reprise_ODG','SpinMaster','']}
                       
                        
    cwd = os.getcwd()
    if not os.path.exists(cwd + '/snaptemp'):
        os.makedirs(cwd + '/snaptemp')
    csv_file = cwd + '/snaptemp/SnapExtractLifetime.csv'    # <<<<<=== Change to a desirable output file location and/or name
     
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

# Merge all dataframes to a result frame

    resultframe = joinframes(campaigns_df,ad_squad_df,ad_df,adstats_df_lifetime)

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
             
            if 'start_time' in campaign['campaign'].keys():

                campaign['campaign']['start_time'] = reformatdate(campaign['campaign']['start_time'])

            if 'end_time' in campaign['campaign'].keys():
            
                campaign['campaign']['end_time'] = reformatdate(campaign['campaign']['end_time'])

            df = pd.DataFrame([campaign["campaign"]],columns=campaign["campaign"].keys())
      
            campaigns_frame_list.append(df)

    resultframe = pd.concat(campaigns_frame_list,sort=True)    
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

    resultframe = pd.concat(ad_squad_frame_list,sort=True)     
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

    resultframe = pd.concat(ad_frame_list,sort=True)     
    resultframe.rename(columns = {'id':'ad_id', 'updated_at':'ad_updated_at', 
                              'name':'ad_name', 'type':'ad_type',  
                              'status':'ad_status',   
                              'created_at':'ad_created_at'}, inplace = True)  
     
    return (resultframe)

def adstats(token,granul,ad_df):

    adstats_frame_list_lifetime = []

    bearer_token = 'Bearer ' + token
    headerargs = {'Authorization':bearer_token}

    returnfields = 'impressions,swipes,swipe_up_percent,view_time_millis,screen_time_millis,quartile_1,quartile_2,quartile_3,view_completion,spend,video_views,shares,saves,frequency,total_installs,uniques'

    apiargs = {'granularity':granul,'fields':returnfields}
                
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

    resultframe = pd.concat(adstats_frame_list_lifetime,sort=True)  

    return resultframe

def joinframes(campaigns_df,ad_squad_df,ad_df,adstats_df_total):
    
    joindf1 = ad_df.merge(adstats_df_total,left_on = 'ad_id',right_on = 'ad_id',how='outer')
    joindf2 = ad_squad_df.merge(joindf1,left_on = 'ad_squad_id',right_on = 'ad_squad_id',how='outer')
    joindf = campaigns_df.merge(joindf2,left_on = 'campaign_id',right_on = 'campaign_id',how = 'outer')
     
    return joindf

if __name__ == '__main__':
    main()
