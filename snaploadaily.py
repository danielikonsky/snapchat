import pyodbc
import time, os, re, datetime, json, csv
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def main():

    cnxn = db_connection()

    cwd = os.getcwd()
    inputpath = cwd + '/snaptemp'
    if not os.path.exists(inputpath):
        print ("Daily Extra File folder does not exist - End of Process")
    else:
        listfiles = getdailyextract(inputpath)
        if len(listfiles) == 0:
            print ("Daily Extra File does not exist - End of Process")
        else:
            for f in listfiles:
                csv_file = inputpath + '/' + f

                raw_df = create_df_from_csv(csv_file)
                print('Extract file ' + f + ' loaded')

                load_campaign(cnxn,raw_df)

                load_ad_squad(cnxn,raw_df)

                load_ad(cnxn,raw_df)

                load_stats(cnxn,raw_df)

                archive_source(csv_file,f)

                print ('Ad core stats DAILY load process ended successfully for input - ',f)

    cnxn.close()

def getdailyextract(inputpath):

    listfiles = [f for f in (os.listdir(inputpath)) if 'SnapExtractDaily' in f]
    
    return listfiles

def db_connection():
    
    cnxn = pyodbc.connect('Trusted_Connection=yes',driver='{SQL Server Native Client 11.0}', server='p1-initde-01.ext.ipgnetwork.com' , database='SnapchatDB')
    print ('DB connection open')
    return cnxn

def create_df_from_csv(csv_file):

    raw_df = pd.read_csv(csv_file,keep_default_na=False,low_memory=False)  
    return (raw_df)

def load_campaign(cnxn,raw_df):

    load_campaign_list = []

    cursor = cnxn.cursor()

    cursor.execute("select distinct [campaign_id]    \
            ,[campaign_name]                \
              from [dbo].[kdp_campaign]")

    for row in cursor.fetchall():
        load_campaign_list.append(row.campaign_id)  
     
    campaign_id_df = raw_df.loc[:,['campaign_id','campaign_name']]
    
    campaign_id_df.sort_values("campaign_id", inplace = True) 
   
    campaign_id_df.drop_duplicates(subset = "campaign_id" ,inplace = True) 
    
    for i in range(0, len(campaign_id_df)):
    
        campaign_id_check = campaign_id_df.iloc[i]['campaign_id']
        
        if campaign_id_check in load_campaign_list:
            
            cursor.execute("update dbo.kdp_campaign set campaign_name = ?         \
                            where campaign_id = ? "   
                                    ,
                                    campaign_id_df.iloc[i]['campaign_name'], 
                                    campaign_id_df.iloc[i]['campaign_id']
                                    )
        else:   
            if campaign_id_check > ' ':                                          
                campaign_df = raw_df.loc[:, ["campaign_id"  
                            ,"campaign_updated_at"
                            ,"campaign_created_at"
                            ,"campaign_name"
                            ,"ad_account_id"
                            ,"campaign_status"
                            ,"objective"
                            ,"campaign_start_time"
                            ,"campaign_end_time"
                            ,"lifetime_spend_cap_micro"
                            ,"buy_model"
                            ,"campaign_daily_budget_micro"]]
                campaign_df.set_index(["campaign_id"], inplace=True)
            
                campaign_df = campaign_df.loc[campaign_id_check,:] 
                
                campaign_df.drop_duplicates(inplace = True) 
                
                val = np.float32(campaign_df.iloc[0]['campaign_daily_budget_micro'])
                campaign_daily_budget_micro = val.item()
                
                val = np.float32(campaign_df.iloc[0]['lifetime_spend_cap_micro'])
                lifetime_spend_cap_micro = val.item()
                
                cursor.execute("insert into dbo.kdp_campaign                        \
                                                ([campaign_id]                      \
                                                ,[campaign_updated_at]              \
                                                ,[campaign_created_at]              \
                                                ,[campaign_name]                    \
                                                ,[ad_account_id]                    \
                                                ,[campaign_status]                  \
                                                ,[objective]                        \
                                                ,[campaign_start_time]              \
                                                ,[campaign_end_time]                \
                                                ,[lifetime_spend_cap_micro]         \
                                                ,[buy_model]                        \
                                                ,[campaign_initiative_created_on]   \
                                                ,[campaign_initiative_updated_on]   \
                                                ,[campaign_daily_budget_micro])     \
                                                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                                campaign_id_check
                                                ,campaign_df.iloc[0]['campaign_updated_at']
                                                ,campaign_df.iloc[0]['campaign_created_at']
                                                ,campaign_df.iloc[0]['campaign_name']
                                                ,campaign_df.iloc[0]['ad_account_id']
                                                ,campaign_df.iloc[0]['campaign_status']
                                                ,campaign_df.iloc[0]['objective']
                                                ,campaign_df.iloc[0]['campaign_start_time']
                                                ,campaign_df.iloc[0]['campaign_end_time']
                                                ,lifetime_spend_cap_micro 
                                                ,campaign_df.iloc[0]['buy_model']
                                                ,datetime.now().date()
                                                ,datetime.now().date()
                                                ,campaign_daily_budget_micro
                                            )

                if campaign_df.iloc[0]['campaign_end_time'] == '' or campaign_df.iloc[0]['campaign_end_time'] == ' ':
                    cursor.execute("update dbo.kdp_campaign set campaign_end_time = NULL where campaign_id = ?" , campaign_df.iloc[0]['campaign_id'] )
    
    cnxn.commit()    
    print ('Campaign load process ended successfully')

def load_ad_squad(cnxn,raw_df):

    load_ad_squad_list = []

    cursor = cnxn.cursor()

    cursor.execute("select [ad_squad_id]    \
            ,[ad_squad_name]                \
              from [dbo].[kdp_ad_squad]")
     
    for row in cursor.fetchall():
        load_ad_squad_list.append(row.ad_squad_id) 

    ad_squad_id_df = raw_df.loc[:,['ad_squad_id','ad_squad_name']]
    
    ad_squad_id_df.sort_values("ad_squad_id", inplace = True) 
   
    ad_squad_id_df.drop_duplicates(subset = "ad_squad_id" ,inplace = True) 

    for i in range(0, len(ad_squad_id_df)):
        
        ad_squad_id_check = ad_squad_id_df.iloc[i]['ad_squad_id']
        
        if ad_squad_id_check in load_ad_squad_list:
            
            cursor.execute("update dbo.kdp_ad_squad set ad_squad_name = ?         \
                            where ad_squad_id = ? "   
                                    ,
                                    ad_squad_id_df.iloc[i]['ad_squad_name'], 
                                    ad_squad_id_df.iloc[i]['ad_squad_id']
                                    )
        else:          
            if ad_squad_id_check > ' ':                                         
                ad_squad_df = raw_df.loc[:, ["ad_squad_id"  
                                    ,"ad_squad_updated_at"
                                    ,"ad_squad_created_at"
                                    ,"ad_squad_name"
                                    ,"ad_squad_status"
                                    ,"ad_squad_type"
                                    ,"campaign_id"
                                    ,"targeting_reach_status"
                                    ,"placement"
                                    ,"billing_event"
                                    ,"bid_micro"
                                    ,"auto_bid"
                                    ,"target_bid"
                                    ,"ad_squad_start_time"
                                    ,"ad_squad_end_time"
                                    ,"optimization_goal"
                                    ,"delivery_constraint"
                                    ,"pacing_type"
                                    ,"lifetime_budget_micro"
                                    ]]
                ad_squad_df.set_index(["ad_squad_id"], inplace=True)
                    
                ad_squad_df = ad_squad_df.loc[ad_squad_id_check,:] 
                        
                ad_squad_df.drop_duplicates(inplace = True) 
                
                if ad_squad_df.iloc[0]['auto_bid']:
                    auto_bid = "TRUE"
                else:
                    auto_bid = "FALSE"

                if ad_squad_df.iloc[0]['target_bid']:
                    target_bid = "TRUE"
                else:
                    target_bid = "FALSE"

                val = np.float32(ad_squad_df.iloc[0]['lifetime_budget_micro'])
                lifetime_budget_micro = val.item()

                val = np.float32(ad_squad_df.iloc[0]['bid_micro'])
                bid_micro = val.item()

                cursor.execute("insert into dbo.kdp_ad_squad ([ad_squad_id]         \
                                    ,[ad_squad_updated_at]                          \
                                    ,[ad_squad_created_at]                          \
                                    ,[ad_squad_name]                                \
                                    ,[ad_squad_status]                              \
                                    ,[ad_squad_type]                                \
                                    ,[campaign_id]                                  \
                                    ,[targeting_reach_status]                       \
                                    ,[placement]                                    \
                                    ,[billing_event]                                \
                                    ,[bid_micro]                                    \
                                    ,[auto_bid]                                     \
                                    ,[target_bid]                                   \
                                    ,[ad_squad_start_time]                          \
                                    ,[ad_squad_end_time]                            \
                                    ,[optimization_goal]                            \
                                    ,[delivery_constraint]                          \
                                    ,[pacing_type]                                  \
                                    ,[lifetime_budget_micro]                        \
                                    ,[ad_squad_initiative_created_on]               \
                                    ,[ad_squad_initiative_updated_on])              \
                                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                    ad_squad_id_check
                                    ,ad_squad_df.iloc[0]["ad_squad_updated_at"]                          
                                    ,ad_squad_df.iloc[0]["ad_squad_created_at"]                          
                                    ,ad_squad_df.iloc[0]["ad_squad_name"]                                
                                    ,ad_squad_df.iloc[0]["ad_squad_status"]                              
                                    ,ad_squad_df.iloc[0]["ad_squad_type"]                                
                                    ,ad_squad_df.iloc[0]["campaign_id"]                                  
                                    ,ad_squad_df.iloc[0]["targeting_reach_status"]                       
                                    ,ad_squad_df.iloc[0]["placement"]                                    
                                    ,ad_squad_df.iloc[0]["billing_event"]                                
                                    ,bid_micro                                
                                    ,auto_bid                                   
                                    ,target_bid                            
                                    ,ad_squad_df.iloc[0]["ad_squad_start_time"]                          
                                    ,ad_squad_df.iloc[0]["ad_squad_end_time"]                            
                                    ,ad_squad_df.iloc[0]["optimization_goal"]                            
                                    ,ad_squad_df.iloc[0]["delivery_constraint"]                          
                                    ,ad_squad_df.iloc[0]["pacing_type"]                                  
                                    ,lifetime_budget_micro                    
                                    ,datetime.now().date()
                                    ,datetime.now().date())
                             
                if ad_squad_df.iloc[0]['ad_squad_end_time'] == '' or ad_squad_df.iloc[0]['ad_squad_end_time'] == ' ':
                    cursor.execute("update dbo.kdp_ad_squad set ad_squad_end_time = NULL where ad_squad_id = ?" , ad_squad_df.iloc[0]['ad_squad_id'] )
                     
    cnxn.commit()    
    print ('Ad Squad load process ended successfully')

def load_ad(cnxn,raw_df):

    load_ad_list = []

    cursor = cnxn.cursor()

    cursor.execute("select [ad_id]    \
            ,[ad_name]                \
              from [dbo].[kdp_ad]")

    for row in cursor.fetchall():
        load_ad_list.append(row.ad_id) 

    ad_id_df = raw_df.loc[:,['ad_id','ad_name']]
    
    ad_id_df.sort_values("ad_id", inplace = True) 
   
    ad_id_df.drop_duplicates(subset = "ad_id" ,inplace = True) 

    for i in range(0, len(ad_id_df)):
        
        ad_id_check = ad_id_df.iloc[i]['ad_id']
     
        if ad_id_check in load_ad_list:
          
            cursor.execute("update dbo.kdp_ad set ad_name = ?         \
                            where ad_id = ? "   
                                    ,
                                    ad_id_df.iloc[i]['ad_name'], 
                                    ad_id_df.iloc[i]['ad_id']
                                    )
        else:             
            if  ad_id_check > ' ':                                      
                ad_df = raw_df.loc[:, ['ad_id',  
                                    'ad_squad_id',
                                    'ad_updated_at',
                                    'ad_created_at',
                                    'ad_name',
                                    'creative_id',
                                    'ad_status',
                                    'ad_type',
                                    'render_type',
                                    'review_status']]
                                    
                ad_df.set_index(["ad_id"], inplace=True)
                    
                ad_df = ad_df.loc[ad_id_check,:] 
                        
                ad_df.drop_duplicates(inplace = True) 
         
                cursor.execute("insert into dbo.kdp_ad ([ad_id] \
                                    ,[ad_squad_id]              \
                                    ,[ad_updated_at]            \
                                    ,[ad_created_at]            \
                                    ,[creative_id]              \
                                    ,[ad_name]                  \
                                    ,[ad_status]                \
                                    ,[ad_type]                  \
                                    ,[render_type]              \
                                    ,[ad_initiative_created_on] \
                                    ,[ad_initiative_updated_on] \
                                    ,[review_status])           \
                                            values (?,?,?,?,?,?,?,?,?,?,?,?)",
                                            ad_id_check 
                                            ,ad_df.iloc[0]['ad_squad_id']    
                                            ,ad_df.iloc[0]['ad_updated_at']   
                                            ,ad_df.iloc[0]['ad_created_at'] 
                                            ,ad_df.iloc[0]['creative_id']  
                                            ,ad_df.iloc[0]['ad_name']  
                                            ,ad_df.iloc[0]['ad_status']    
                                            ,ad_df.iloc[0]['ad_type']          
                                            ,ad_df.iloc[0]['render_type']   
                                            ,datetime.now().date()
                                            ,datetime.now().date()  
                                            ,ad_df.iloc[0]['review_status']  
                                            )   
    cnxn.commit()    
    print ('Ad load process ended successfully')

def load_stats(cnxn,raw_df):

    ad_stats_df = raw_df.loc[:, ['ad_id',
                        'adstats_date',
                        'impressions',
                        'swipes',
                        'quartile_1',
                        'quartile_2',
                        'quartile_3',
                        'view_completion',
                        'spend',
                        'view_time_millis',
                        'video_views',
                        'screen_time_millis',
                        'shares',
                        'saves',
                        'swipe_up_percent',
                        'frequency',
                        'total_installs',
                        'uniques'
                        ]]
                       
    ad_stats_df.sort_values(["ad_id","adstats_date"], inplace = True) 
     
    ad_stats_df.drop_duplicates(subset = ["ad_id","adstats_date"] ,inplace = True) 

    min_date = min(ad_stats_df['adstats_date'])[0:10]
    max_date = max(ad_stats_df['adstats_date'])[0:10]
       
    load_ad_stats_list = []

    cursor = cnxn.cursor()

    cursor.execute("SELECT [ad_id_date_granul]      \
                            ,[ad_id]                \
                            ,[stats_date]           \
                            ,[granularity]          \
                            ,[impressions]          \
                            ,[swipes]               \
                            ,[quartile_1]           \
                            ,[quartile_2]           \
                            ,[quartile_3]           \
                            ,[view_completion]      \
                            ,[spend]                \
                            ,[view_time_millis]     \
                            ,[video_views]          \
                            ,[screen_time_millis]   \
                            ,[shares]               \
                            ,[saves]                \
                            ,[swipe_up_percent]     \
                            ,[frequency]            \
                            ,[total_installs]       \
                            ,[uniques]              \
                        FROM [dbo].[kdp_stats] WHERE [dbo].[kdp_stats].[granularity]='DAY' AND ([dbo].[kdp_stats].[stats_date] BETWEEN ? AND ?)"
                            ,
                                min_date, 
                                max_date
                                )

    for row in cursor.fetchall():
        load_ad_stats_list.append(row.ad_id + str(row.stats_date))  
    
    for i in range(0, len(ad_stats_df)):
        if (ad_stats_df.iloc[i]['ad_id'] > ' ') and (ad_stats_df.iloc[i]['adstats_date'] > ' '):
            if ad_stats_df.iloc[i]["impressions"] == '':
                impressions = ''
            else:
                impressions             = int(float(ad_stats_df.iloc[i]["impressions"]))

            if ad_stats_df.iloc[i]["swipes"] == '':
                swipes = ''
            else:
                swipes                  = int(float(ad_stats_df.iloc[i]["swipes"]))

            if ad_stats_df.iloc[i]["quartile_1"] == '':
                quartile_1 = ''
            else:
                quartile_1              = int(float(ad_stats_df.iloc[i]["quartile_1"]))

            if ad_stats_df.iloc[i]["quartile_2"] == '':
                quartile_2 = ''
            else:
                quartile_2              = int(float(ad_stats_df.iloc[i]["quartile_2"]))

            if ad_stats_df.iloc[i]["quartile_3"] == '':
                quartile_3 = ''
            else:
                quartile_3              = int(float(ad_stats_df.iloc[i]["quartile_3"]))

            if ad_stats_df.iloc[i]["view_completion"] == '':
                view_completion = ''
            else:
                view_completion         = int(float(ad_stats_df.iloc[i]["view_completion"]))

            if ad_stats_df.iloc[i]["spend"] == '':
                spend = ''
            else:
                spend                   = int(float(ad_stats_df.iloc[i]["spend"]))

            if ad_stats_df.iloc[i]["view_time_millis"] == '':
                view_time_millis = ''
            else:
                view_time_millis        = int(float(ad_stats_df.iloc[i]["view_time_millis"]))

            if ad_stats_df.iloc[i]["video_views"] == '':
                video_views = ''
            else:
                video_views             = int(float(ad_stats_df.iloc[i]["video_views"]))

            if ad_stats_df.iloc[i]["screen_time_millis"] == '':
                screen_time_millis = ''
            else:
                screen_time_millis      = int(float(ad_stats_df.iloc[i]["screen_time_millis"]))

            if ad_stats_df.iloc[i]["shares"] == '':
                shares = ''
            else:
                shares                  = int(float(ad_stats_df.iloc[i]["shares"]))

            if ad_stats_df.iloc[i]["saves"] == '':
                saves  = ''
            else:
                saves                   = int(float(ad_stats_df.iloc[i]["saves"]))

            if ad_stats_df.iloc[i]["total_installs"] == '':
                total_installs = ''
            else:
                total_installs          = int(float(ad_stats_df.iloc[i]["total_installs"]))

            if ad_stats_df.iloc[i]["uniques"] == '':
                uniques = ''
            else:
                uniques                 = int(float(ad_stats_df.iloc[i]["uniques"]))

            if ad_stats_df.iloc[i]["frequency"] == '':
                frequency = ''
            else:
                val = np.float32(ad_stats_df.iloc[i]['frequency'])
                frequency = val.item()

            if ad_stats_df.iloc[i]["swipe_up_percent"] == '':
                swipe_up_percent = ''
            else:
                val = np.float32(ad_stats_df.iloc[i]['swipe_up_percent'])
                swipe_up_percent = val.item()

            if (impressions == 0 or impressions == '') and (spend == 0 or spend == '') and (video_views == 0 or video_views == ''):
                pass
            else:
                tempkey = ad_stats_df.iloc[i]['ad_id'] + str(ad_stats_df.iloc[i]['adstats_date'])[0:10]
            
                if tempkey in load_ad_stats_list:
                    
                    cursor.execute("update dbo.kdp_stats set [impressions] = ?  \
                                                ,[swipes] = ?                   \
                                                ,[quartile_1] = ?               \
                                                ,[quartile_2] = ?               \
                                                ,[quartile_3] = ?               \
                                                ,[view_completion] = ?          \
                                                ,[spend] = ?                    \
                                                ,[view_time_millis] = ?         \
                                                ,[video_views] = ?              \
                                                ,[screen_time_millis] = ?       \
                                                ,[shares] = ?                   \
                                                ,[saves] = ?                    \
                                                ,[swipe_up_percent] = ?         \
                                                ,[frequency] = ?                \
                                                ,[total_installs]  = ?          \
                                                ,[uniques]  = ?                 \
                                                where ad_id = ? AND stats_date = ? AND granularity = 'DAY'  " ,  
                                                impressions
                                                ,swipes
                                                ,quartile_1
                                                ,quartile_2
                                                ,quartile_3
                                                ,view_completion
                                                ,spend
                                                ,view_time_millis
                                                ,video_views
                                                ,screen_time_millis
                                                ,shares
                                                ,saves
                                                ,swipe_up_percent
                                                ,frequency
                                                ,total_installs 
                                                ,uniques                  
                                                ,ad_stats_df.iloc[i]["ad_id"]
                                                ,ad_stats_df.iloc[i]["adstats_date"]
                                                )

                else:
                    granularity = 'DAY'
                    stats_date = ad_stats_df.iloc[i]["adstats_date"]
                    ad_id_date_granul = str(ad_stats_df.iloc[i]['ad_id']) + str(stats_date)[0:10] + str(granularity)
                    
                    cursor.execute("insert into dbo.kdp_stats ([ad_id_date_granul]      \
                                                            ,[ad_id]                    \
                                                            ,[stats_date]               \
                                                            ,[granularity]              \
                                                            ,[impressions]              \
                                                            ,[swipes]                   \
                                                            ,[quartile_1]               \
                                                            ,[quartile_2]               \
                                                            ,[quartile_3]               \
                                                            ,[view_completion]          \
                                                            ,[spend]                    \
                                                            ,[view_time_millis]         \
                                                            ,[video_views]              \
                                                            ,[screen_time_millis]       \
                                                            ,[shares]                   \
                                                            ,[saves]                    \
                                                            ,[swipe_up_percent]         \
                                                            ,[frequency]                \
                                                            ,[total_installs]           \
                                                            ,[uniques])                 \
                                                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                                            ad_id_date_granul     
                                                            ,ad_stats_df.iloc[i]['ad_id']                    
                                                            ,stats_date             
                                                            ,granularity 
                                                            ,impressions
                                                            ,swipes
                                                            ,quartile_1
                                                            ,quartile_2
                                                            ,quartile_3
                                                            ,view_completion
                                                            ,spend
                                                            ,view_time_millis
                                                            ,video_views
                                                            ,screen_time_millis
                                                            ,shares
                                                            ,saves
                                                            ,swipe_up_percent
                                                            ,frequency
                                                            ,total_installs 
                                                            ,uniques  
                                                            )

    cnxn.commit()    

def archive_source(csv_file,f):

    source_path = csv_file
    if f == 'SnapExtractDaily.csv':
        target_file = 'SnapExtractDaily'+datetime.now().date().strftime('%Y-%m-%d') +'.csv'
    else:
        target_file = f

    #target_directory = 'C:/Users/daniel.ikonsky/Python/Test/KDP/snaparchive/'    # <=== Local machine
    target_directory = 'C:/Util/SnapChatExtracts/2020/'                           # <=== Server

    target_path = target_directory + target_file
    os.replace(source_path, target_path)
    
if __name__ == '__main__':
    main()
