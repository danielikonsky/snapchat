import requests
import pyodbc
import pytz
import time, os, re, datetime, json, csv
import pandas as pd
from datetime import datetime, timedelta

# Declaring global variables

refresh_token = 'eyJraWQiOiJyZWZyZXNoLXRva2VuLWExMjhnY20uMCIsInR5cCI6IkpXVCIsImVuYyI6IkExMjhHQ00iLCJhbGciOiJkaXIifQ..iWNLgezQSiFHbP12.-i_D01OxODwVXM7lHVkMFiTsCOuYN4ZYhNwVxkCnHLwRzb5J77noQG1lJ0wmeqNE98oIba2nmnKUcKS5CrPN0zB5zFPr39oNVcotqzedQk6apcrSXAdOp4MJDy5h5CmvlMIAkE1v91TGm-8a-aZQQE67OS05wFZkFXe1lIt6lbdQOlMTDFv0YdpqwLyCzH2IxxR9HCiG6kCOva4wJWpcF-tAmn5UeOxgM0zA5mJZlfft90aAnts4rBeKHFutos1vTVj3kk0Fi5ZhKSQ.rFw86K89WLpfPGiI-bJakA'
client_id = 'f1304535-70d6-4a4d-a4ea-96ea7c7e51a3'
client_secret = '7a221d4220bebea2f06b'
grant_type = 'refresh_token'
period = 30  
granularity = 'DAY'

cwd = os.getcwd()
if not os.path.exists(cwd + '/snaptemp'):
    os.makedirs(cwd + '/snaptemp')

def main():

# Open SnapDB connection

    cnxn = db_connection()

# Get Snap ad accounts 

    ad_account_df = getadaccount(cnxn)     

# Get an authorization token

    token = refreshtoken(client_id,client_secret,grant_type,refresh_token)

# Get all campaigns for ad accounts as a dataframe 

    campaigns_df = getcampaigns(token,ad_account_df)
    
# Get all ad_squads for ad accounts as a dataframe 

    ad_squad_df = getad_squad(token,ad_account_df)
     
# Get all ads for ad accounts as a dataframe 

    ad_df = getad(token,ad_account_df)
     
# Get ad stats for each ad as a dataframe 

    adstats(token,granularity,ad_df,campaigns_df,ad_squad_df,period,refresh_token)

    print ('Extract process completed successfuly')

def db_connection():
    
    cnxn = pyodbc.connect('Trusted_Connection=yes',driver='{SQL Server Native Client 11.0}', server='p1-initde-01.ext.ipgnetwork.com' , database='SnapchatDB')
    print ('DB connection open')
    return cnxn

def getadaccount(cnxn):

    cursor = cnxn.cursor()
    cursor.execute("SELECT dbo.kdp_ad_account.ad_account_id \
                    ,dbo.kdp_ad_account.ad_account_name     \
                    ,dbo.kdp_ad_account.ad_account_client   \
                    ,dbo.kdp_ad_account.ad_account_brand    \
                    ,dbo.kdp_ad_account.ad_account_timezone \
                        FROM dbo.kdp_ad_account             \
                        WHERE ad_account_include_ind = 'YES'")

    rows = cursor.fetchall() 
    resultframe = pd.DataFrame([tuple(t) for t in rows],columns=['ad_account_id','ad_account_name','ad_account_client','ad_account_brand','ad_account_timezone']) 
    print ("Accounts loaded")
     
    return (resultframe) 

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

def joinframes(campaigns_df,ad_squad_df,ad_df,adstats_df):
    
    joindf1 = ad_df.merge(adstats_df,left_on = 'ad_id',right_on = 'ad_id',how='outer')
    joindf2 = ad_squad_df.merge(joindf1,left_on = 'ad_squad_id',right_on = 'ad_squad_id',how='outer')
    joindf = campaigns_df.merge(joindf2,left_on = 'campaign_id',right_on = 'campaign_id',how = 'outer')
     
    return joindf

def getcampaigns(token,ad_account_df):
    campaigns_frame_list = []
    bearer_token = 'Bearer ' + token
    headerargs = {'Authorization':bearer_token}

    for i in range(0, len(ad_account_df)):

        ad_account = ad_account_df.iloc[i]['ad_account_id']

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

def getad_squad(token,ad_account_df):

    ad_squad_frame_list = []
    bearer_token = 'Bearer ' + token
    headerargs = {'Authorization':bearer_token}

    for i in range(0, len(ad_account_df)):
    
        ad_account = ad_account_df.iloc[i]['ad_account_id']
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

def getad(token,ad_account_df):
    
    ad_frame_list = []
    bearer_token = 'Bearer ' + token
    headerargs = {'Authorization':bearer_token}

    for i in range(0, len(ad_account_df)):
    
        ad_account = ad_account_df.iloc[i]['ad_account_id']
        timezone = ad_account_df.iloc[i]['ad_account_timezone']

        url = 'https://adsapi.snapchat.com/v1/adaccounts/'+ ad_account + '/ads'
        r = requests.get(url,headers=headerargs)

        if r.status_code != 200:
            print ('Bad Return from get ads api - {} {}'.format(r.status_code,r.text))
        else:
            print ('>>>> Get ads - OK <<<<< {}'.format(r.status_code) )

        apiresult = r.json()

        ads = apiresult["ads"]
        
        for ad in ads:
            ad['ad']['timezone'] = timezone
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

def adstats(token,granul,ad_df,campaigns_df,ad_squad_df,period,refresh_token):

    #refresh_token = 'eyJraWQiOiJyZWZyZXNoLXRva2VuLWExMjhnY20uMCIsInR5cCI6IkpXVCIsImVuYyI6IkExMjhHQ00iLCJhbGciOiJkaXIifQ..iWNLgezQSiFHbP12.-i_D01OxODwVXM7lHVkMFiTsCOuYN4ZYhNwVxkCnHLwRzb5J77noQG1lJ0wmeqNE98oIba2nmnKUcKS5CrPN0zB5zFPr39oNVcotqzedQk6apcrSXAdOp4MJDy5h5CmvlMIAkE1v91TGm-8a-aZQQE67OS05wFZkFXe1lIt6lbdQOlMTDFv0YdpqwLyCzH2IxxR9HCiG6kCOva4wJWpcF-tAmn5UeOxgM0zA5mJZlfft90aAnts4rBeKHFutos1vTVj3kk0Fi5ZhKSQ.rFw86K89WLpfPGiI-bJakA'
    #client_id = 'f1304535-70d6-4a4d-a4ea-96ea7c7e51a3'
    #client_secret = '7a221d4220bebea2f06b'
    #grant_type = 'refresh_token'
 
    adstats_frame_list_day = []

    bearer_token = 'Bearer ' + token
    headerargs = {'Authorization':bearer_token}
    
    returnfields = 'impressions,swipes,swipe_up_percent,view_time_millis,screen_time_millis,quartile_1,quartile_2,quartile_3,view_completion,spend,video_views,shares,saves,frequency,total_installs,uniques'

    #start_date = datetime.now().date() - timedelta(period)
    start_date = datetime.strptime('01/01/20', '%m/%d/%y')
    end_date = start_date + timedelta(30) 
    keep_running = True
    while end_date <=  datetime.now():
        for i in range(0, len(ad_df)):
        
                timezone = pytz.timezone(ad_df.iloc[i]['timezone'])
                
                tmend = str(timezone.utcoffset(end_date))
                
                endutcoffset = ((tmend.split(' '))[2]).split(':')[0]

                if tmend[0:2] == '-1':
                    endutcoffset = 24-int(endutcoffset)
                
                start_tme = [int(i) for i in start_date.strftime('%Y-%m-%d').split('-')]
                tmstart = str(timezone.utcoffset(datetime(start_tme[0],start_tme[1],start_tme[2])))
                
                startutcoffset = ((tmstart.split(' '))[2]).split(':')[0]
                
                if tmstart[0:2] == '-1':
                    startutcoffset = 24-int(startutcoffset)
                
                end_time = end_date.strftime('%Y-%m-%d')+"T" + str(endutcoffset) + ":00:00.000-00:00"
                
                start_time = start_date.strftime('%Y-%m-%d')+"T" + str(startutcoffset) + ":00:00.000-00:00"
                
                adstats_dict = {'ad_id':ad_df.iloc[i]['ad_id'],'adstats_date':start_time}
                url = 'https://adsapi.snapchat.com/v1/ads/'+ ad_df.iloc[i]['ad_id'] + '/stats'
                apiargs = {'granularity':granul,'fields':returnfields,'start_time':start_time,'end_time':end_time}
                
                r = requests.get(url,params=apiargs,headers=headerargs)

                if r.status_code != 200:
                    if r.status_code == 401:
                        print ('API call failed on Autorization,Attempting token refresh')
                        token = refreshtoken(client_id,client_secret,grant_type,refresh_token)
                        bearer_token = 'Bearer ' + token
                        headerargs = {'Authorization':bearer_token}
                        r = requests.get(url,params=apiargs,headers=headerargs)
                        if r.status_code != 200:
                            print ('API call failed on Autorization, Refresh token attempt failed')
                            resultframe = pd.concat(adstats_frame_list_day,sort=True)  
                            return resultframe
                        else:
                            print ('Refresh token attempt successful')
                            apiresult = r.json()
                        
                            adstats_list_stats = apiresult["timeseries_stats"][0]["timeseries_stat"]["timeseries"]
                            for i in range (len(adstats_list_stats)):
                                adstats_dict['adstats_date'] = adstats_list_stats[i]['start_time']
                                adstats_dict_stat = adstats_list_stats[i]['stats']
                                adstats_dict.update(adstats_dict_stat)
                                
                                df = pd.DataFrame([adstats_dict],columns=adstats_dict.keys())
                                adstats_frame_list_day.append(df)
                    else:
                        print ('Bad Return from get ads api - {} {}'.format(r.status_code,r.text))
                        resultframe = pd.concat(adstats_frame_list_day,sort=True)  
                        return resultframe
                else:
                    print ('>>>> Get DAY ad stats - OK <<<<< ad id - {} '.format(ad_df.iloc[i]['ad_id']) )
                    apiresult = r.json()
                        
                    adstats_list_stats = apiresult["timeseries_stats"][0]["timeseries_stat"]["timeseries"]
                    for i in range (len(adstats_list_stats)):
                        adstats_dict['adstats_date'] = adstats_list_stats[i]['start_time']
                        adstats_dict_stat = adstats_list_stats[i]['stats']
                        adstats_dict.update(adstats_dict_stat)
                        
                        df = pd.DataFrame([adstats_dict],columns=adstats_dict.keys())
                        adstats_frame_list_day.append(df)
        print ('Dates batch processed from - ',start_date,' to - ', end_date)

        adstats_df = pd.concat(adstats_frame_list_day,sort=True) 
        resultframe = joinframes(campaigns_df,ad_squad_df,ad_df,adstats_df)

        csv_file = cwd + '/snaptemp/SnapExtractDaily'+start_date.strftime('%Y-%m-%d')+'.csv'   
        try:
            resultframe.to_csv(csv_file, encoding='utf-8', index=False)
            print ('Batch file ' + 'SnapExtractDaily' + start_date.strftime('%Y-%m-%d') + '.csv' + ' created sucessfuly')
        except IOError:
            print("File I/O error")  

        end_date = end_date + timedelta(30) 
        if end_date >  datetime.now() and keep_running:
            end_date = datetime.now()
            keep_running = False
        start_date = start_date + timedelta(30) 

        adstats_frame_list_day = []

    return(1) 

if __name__ == '__main__':
    main()
