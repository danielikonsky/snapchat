import pyodbc
import time, os, re, datetime, json, csv
import pandas as pd
from datetime import datetime, timedelta

def main():

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

    cnxn = db_connection()
    load_ad_account(cnxn,ad_account_dict)     
    raw_df = create_df_from_csv(csv_file)
   
    load_campaign(cnxn,raw_df)

    load_ad_squad(cnxn,raw_df)

    load_ad(cnxn,raw_df)

    load_stats(cnxn,raw_df)

    #load_geo(cnxn,raw_df)                    # <===== Currently Snapchat only supports LIFETIME

    #load_conversion(df_from_csv)                  
    cnxn.close()
def db_connection():

    cnxn = pyodbc.connect('Trusted_Connection=yes',driver='{SQL Server Native Client 11.0}', server='p1-initde-01.ext.ipgnetwork.com' , database='SnapchatDB')
    return cnxn

def load_ad_account(cnxn,ad_account_dict):

    load_account_dict = {}

    cursor = cnxn.cursor()

    cursor.execute("SELECT dbo.kdp_ad_account.ad_account_id,dbo.kdp_ad_account.ad_account_name FROM dbo.kdp_ad_account")

    for row in cursor.fetchall():
        load_account_dict[row.ad_account_id] = row.ad_account_name

    for id in ad_account_dict:
        if id not in load_account_dict:
            cursor.execute("insert into dbo.kdp_ad_account(ad_account_id, ad_account_name) values (?, ?)", id, ad_account_dict[id])

    cnxn.commit()      
    print ('Ad Account load process ended successfully')

def create_df_from_csv(csv_file):

    raw_df = pd.read_csv(csv_file,keep_default_na=False,low_memory=False)  
    return (raw_df)

def load_campaign(cnxn,raw_df):

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

    
    campaign_df.sort_values("campaign_id", inplace = True) 
     
    campaign_df.drop_duplicates(subset = "campaign_id" ,inplace = True) 
     
    load_campaign_list = []

    cursor = cnxn.cursor()

    cursor.execute("select [campaign_id]    \
            ,[campaign_updated_at]          \
            ,[campaign_created_at]          \
            ,[campaign_name]                \
            ,[ad_account_id]                \
            ,[campaign_status]              \
            ,[objective]                    \
            ,[campaign_start_time]          \
            ,[campaign_end_time]            \
            ,[lifetime_spend_cap_micro]     \
            ,[buy_model]                    \
            ,[campaign_daily_budget_micro]  \
            from [dbo].[kdp_campaign]")

    for row in cursor.fetchall():
        load_campaign_list.append(row.campaign_id)  
     
    for i in range(0, len(campaign_df)):

        if campaign_df.iloc[i]['campaign_id'] in load_campaign_list:
              
            cursor.execute("update dbo.kdp_campaign set campaign_updated_at = ?         \
                                                    ,campaign_end_time = ?              \
                                                    ,campaign_status = ?                \
                                                    ,ad_account_id = ?                  \
                                                    ,lifetime_spend_cap_micro = ?       \
                                                    ,campaign_daily_budget_micro  = ?   \
                                                    ,objective = ?                      \
                                                    ,buy_model = ?                      \
                                                    ,campaign_name = ?                  \
                                                    where campaign_id = ? "   
                                                    ,
                                                    campaign_df.iloc[i]['campaign_updated_at'],  
                                                    campaign_df.iloc[i]['campaign_end_time'], 
                                                    campaign_df.iloc[i]['campaign_status'],    
                                                    campaign_df.iloc[i]['ad_account_id'],   
                                                    campaign_df.iloc[i]['lifetime_spend_cap_micro'], 
                                                    campaign_df.iloc[i]['campaign_daily_budget_micro'],
                                                    campaign_df.iloc[i]['objective'],    
                                                    campaign_df.iloc[i]['buy_model'],          
                                                    campaign_df.iloc[i]['campaign_name'],                
                                                    campaign_df.iloc[i]['campaign_id']
                                                    )

        else:
            
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
                                            ,[campaign_daily_budget_micro])     \
                            values (?,?,?,?,?,?,?,?,?,?,?,?)",
                                             campaign_df.iloc[i]['campaign_id']
                                            ,campaign_df.iloc[i]['campaign_updated_at']
                                            ,campaign_df.iloc[i]['campaign_created_at']
                                            ,campaign_df.iloc[i]['campaign_name']
                                            ,campaign_df.iloc[i]['ad_account_id']
                                            ,campaign_df.iloc[i]['campaign_status']
                                            ,campaign_df.iloc[i]['objective']
                                            ,campaign_df.iloc[i]['campaign_start_time']
                                            ,campaign_df.iloc[i]['campaign_end_time']
                                            ,campaign_df.iloc[i]['lifetime_spend_cap_micro']  
                                            ,campaign_df.iloc[i]['buy_model']
                                            ,campaign_df.iloc[i]['campaign_daily_budget_micro'])

        if campaign_df.iloc[i]['campaign_end_time'] == '' or campaign_df.iloc[i]['campaign_end_time'] == ' ':
            cursor.execute("update dbo.kdp_campaign set campaign_end_time = NULL where campaign_id = ?" , campaign_df.iloc[i]['campaign_id'] )
 
    cnxn.commit()    
    print ('Campaign load process ended successfully')

def load_ad_squad(cnxn,raw_df):
    
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
                ,"ad_squad_daily_budget_micro"
                ,"ad_squad_start_time"
                ,"ad_squad_end_time"
                ,"optimization_goal"
                ,"delivery_constraint"
                ,"pacing_type"
                ,"lifetime_budget_micro"
                ,"impression_goal"
                ,"reach_goal"
                ,"reach_and_frequency_status"
                ,"reach_and_frequency_rate_micro"
                ,"reach_and_frequency_debug_message"
                ,"reserved_status"
                ,"reserved_rate_micro"
                ,"reserved_debug_message"
                ]]

    ad_squad_df.sort_values("ad_squad_id", inplace = True) 
     
    ad_squad_df.drop_duplicates(subset = "ad_squad_id" ,inplace = True) 
     
    load_ad_squad_list = []

    cursor = cnxn.cursor()

    cursor.execute("SELECT [ad_squad_id]                    \
                    ,[ad_squad_updated_at]                  \
                    ,[ad_squad_created_at]                  \
                    ,[ad_squad_name]                        \
                    ,[campaign_id]                          \
                    ,[ad_squad_status]                      \
                    ,[ad_squad_type]                        \
                    ,[targeting_reach_status]               \
                    ,[placement]                            \
                    ,[billing_event]                        \
                    ,[bid_micro]                            \
                    ,[auto_bid]                             \
                    ,[target_bid]                           \
                    ,[ad_squad_daily_budget_micro]          \
                    ,[ad_squad_start_time]                  \
                    ,[ad_squad_end_time]                    \
                    ,[optimization_goal]                    \
                    ,[delivery_constraint]                  \
                    ,[pacing_type]                          \
                    ,[lifetime_budget_micro]                \
                    ,[impression_goal]                      \
                    ,[reach_goal]                           \
                    ,[reach_and_frequency_status]           \
                    ,[reach_and_frequency_rate_micro]       \
                    ,[reach_and_frequency_debug_message]    \
                    ,[reserved_status]                      \
                    ,[reserved_rate_micro]                  \
                    ,[reserved_debug_message]               \
                FROM [dbo].[kdp_ad_squad]")

    for row in cursor.fetchall():
        load_ad_squad_list.append(row.ad_squad_id)  
    
    for i in range(0, len(ad_squad_df)):

        if ad_squad_df.iloc[i]['auto_bid']:
            auto_bid = "TRUE"
        else:
            auto_bid = "FALSE"

        if ad_squad_df.iloc[i]['target_bid']:
            target_bid = "TRUE"
        else:
            target_bid = "FALSE"

        if ad_squad_df.iloc[i]['ad_squad_id'] in load_ad_squad_list:
            
            cursor.execute("update dbo.kdp_ad_squad set [ad_squad_updated_at] = ?   \
                            ,[ad_squad_created_at] = ?                              \
                            ,[ad_squad_name] = ?                                    \
                            ,[ad_squad_status] = ?                                  \
                            ,[ad_squad_type] = ?                                    \
                            ,[campaign_id] = ?                                      \
                            ,[targeting_reach_status] = ?                           \
                            ,[placement] = ?                                        \
                            ,[billing_event] = ?                                    \
                            ,[bid_micro] = ?                                        \
                            ,[auto_bid] = ?                                         \
                            ,[target_bid] = ?                                       \
                            ,[ad_squad_daily_budget_micro] = ?                      \
                            ,[ad_squad_start_time] = ?                              \
                            ,[ad_squad_end_time] = ?                                \
                            ,[optimization_goal] = ?                                \
                            ,[delivery_constraint] = ?                              \
                            ,[pacing_type] = ?                                      \
                            ,[lifetime_budget_micro] = ?                            \
                            ,[impression_goal] = ?                                  \
                            ,[reach_goal] = ?                                       \
                            ,[reach_and_frequency_status] = ?                       \
                            ,[reach_and_frequency_rate_micro] = ?                   \
                            ,[reach_and_frequency_debug_message] = ?                \
                            ,[reserved_status] = ?                                  \
                            ,[reserved_rate_micro] = ?                              \
                            ,[reserved_debug_message] = ?                           \
                            where ad_squad_id = ? " ,  
                            ad_squad_df.iloc[i]["ad_squad_updated_at"]
                            ,ad_squad_df.iloc[i]["ad_squad_created_at"]
                            ,ad_squad_df.iloc[i]["ad_squad_name"]
                            ,ad_squad_df.iloc[i]["ad_squad_status"]
                            ,ad_squad_df.iloc[i]["ad_squad_type"]
                            ,ad_squad_df.iloc[i]["campaign_id"]
                            ,ad_squad_df.iloc[i]["targeting_reach_status"]
                            ,ad_squad_df.iloc[i]["placement"]
                            ,ad_squad_df.iloc[i]["billing_event"]
                            ,ad_squad_df.iloc[i]["bid_micro"]
                            ,auto_bid
                            ,target_bid
                            ,ad_squad_df.iloc[i]["ad_squad_daily_budget_micro"]
                            ,ad_squad_df.iloc[i]["ad_squad_start_time"]
                            ,ad_squad_df.iloc[i]["ad_squad_end_time"]
                            ,ad_squad_df.iloc[i]["optimization_goal"]
                            ,ad_squad_df.iloc[i]["delivery_constraint"]
                            ,ad_squad_df.iloc[i]["pacing_type"]
                            ,ad_squad_df.iloc[i]["lifetime_budget_micro"]
                            ,ad_squad_df.iloc[i]["impression_goal"]
                            ,ad_squad_df.iloc[i]["reach_goal"]
                            ,ad_squad_df.iloc[i]["reach_and_frequency_status"]
                            ,ad_squad_df.iloc[i]["reach_and_frequency_rate_micro"]
                            ,ad_squad_df.iloc[i]["reach_and_frequency_debug_message"]
                            ,ad_squad_df.iloc[i]["reserved_status"]
                            ,ad_squad_df.iloc[i]["reserved_rate_micro"]
                            ,ad_squad_df.iloc[i]["reserved_debug_message"] 
                            ,ad_squad_df.iloc[i]["ad_squad_id"]
                            )

        else:
            cursor.execute("insert into dbo.kdp_ad_squad ([ad_squad_id]         \
                            ,[ad_squad_created_at]                              \
                            ,[ad_squad_start_time]                              \
                            ,[ad_squad_name]                                    \
                            ,[campaign_id]                                      \
                            ,[ad_squad_updated_at]                              \
                            ,[bid_micro]                                        \
                            ,[ad_squad_daily_budget_micro]                      \
                            ,[lifetime_budget_micro]                            \
                            ,[reserved_rate_micro]                              \
                            ,[reserved_debug_message]                           \
                            ,[impression_goal]                                  \
                            ,[reach_goal]                                       \
                            ,[reach_and_frequency_status]                       \
                            ,[reach_and_frequency_rate_micro]                   \
                            ,[reach_and_frequency_debug_message]                \
                            ,[reserved_status]                                  \
                            ,[ad_squad_end_time]                                \
                            ,[optimization_goal]                                \
                            ,[delivery_constraint]                              \
                            ,[pacing_type]                                      \
                            ,[auto_bid]                                         \
                            ,[target_bid]                                       \
                            ,[ad_squad_status]                                  \
                            ,[ad_squad_type]                                    \
                            ,[targeting_reach_status]                           \
                            ,[placement]                                        \
                            ,[billing_event])                                   \
                            values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                             ad_squad_df.iloc[i]["ad_squad_id"]
                            ,ad_squad_df.iloc[i]["ad_squad_created_at"]
                            ,ad_squad_df.iloc[i]["ad_squad_start_time"]
                            ,ad_squad_df.iloc[i]["ad_squad_name"]
                            ,ad_squad_df.iloc[i]["campaign_id"]
                            ,ad_squad_df.iloc[i]["ad_squad_updated_at"]
                            ,ad_squad_df.iloc[i]["bid_micro"]
                            ,ad_squad_df.iloc[i]["ad_squad_daily_budget_micro"]
                            ,ad_squad_df.iloc[i]["lifetime_budget_micro"]
                            ,ad_squad_df.iloc[i]["reserved_rate_micro"]
                            ,ad_squad_df.iloc[i]["reserved_debug_message"]
                            ,ad_squad_df.iloc[i]["impression_goal"]
                            ,ad_squad_df.iloc[i]["reach_goal"]
                            ,ad_squad_df.iloc[i]["reach_and_frequency_status"]
                            ,ad_squad_df.iloc[i]["reach_and_frequency_rate_micro"]
                            ,ad_squad_df.iloc[i]["reach_and_frequency_debug_message"]
                            ,ad_squad_df.iloc[i]["reserved_status"]
                            ,ad_squad_df.iloc[i]["ad_squad_end_time"]
                            ,ad_squad_df.iloc[i]["optimization_goal"]
                            ,ad_squad_df.iloc[i]["delivery_constraint"]
                            ,ad_squad_df.iloc[i]["pacing_type"]
                            ,auto_bid
                            ,target_bid
                            ,ad_squad_df.iloc[i]["ad_squad_status"]
                            ,ad_squad_df.iloc[i]["ad_squad_type"]
                            ,ad_squad_df.iloc[i]["targeting_reach_status"]
                            ,ad_squad_df.iloc[i]["placement"]
                            ,ad_squad_df.iloc[i]["billing_event"]
                            )
        if ad_squad_df.iloc[i]['ad_squad_end_time'] == '' or ad_squad_df.iloc[i]['ad_squad_end_time'] == ' ':
            cursor.execute("update dbo.kdp_ad_squad set ad_squad_end_time = NULL where ad_squad_id = ?" , ad_squad_df.iloc[i]['ad_squad_id'] )
                     

    cnxn.commit()    
    print ('Ad Squad load process ended successfully')

def load_ad(cnxn,raw_df):
    
    ad_df = raw_df.loc[:, ['ad_id',
            'ad_squad_id',
            'ad_updated_at',
            'ad_created_at',
            'ad_name',
            'creative_id',
            'ad_status',
            'ad_type',
            'render_type',
            'review_status',
            'container_chain_ids',
            'unlockable_id',
            'review_status_reasons']]

    ad_df.sort_values("ad_id", inplace = True) 
     
    ad_df.drop_duplicates(subset = "ad_id" ,inplace = True) 
    
    load_ad_list = []

    cursor = cnxn.cursor()

    cursor.execute("SELECT [ad_id]      \
                    ,[ad_squad_id]      \
                    ,[ad_updated_at]    \
                    ,[ad_created_at]    \
                    ,[ad_name]          \
                    ,[creative_id]      \
                    ,[ad_status]        \
                    ,[ad_type]          \
                    ,[render_type]      \
                    ,[review_status]    \
                    FROM [dbo].[kdp_ad]")

    for row in cursor.fetchall():
        load_ad_list.append(row.ad_id)  
    
    for i in range(0, len(ad_df)):

        if ad_df.iloc[i]['ad_id'] in load_ad_list:
            
            cursor.execute("update dbo.kdp_ad set [ad_updated_at] = ?   \
                                        ,[ad_created_at] = ?            \
                                        ,[ad_name]  = ?                 \
                                        ,[ad_squad_id]  = ?             \
                                        ,[creative_id] = ?              \
                                        ,[ad_status]  = ?               \
                                        ,[ad_type] = ?                  \
                                        ,[render_type] = ?              \
                                        ,[review_status]  = ?           \
                                        where ad_id = ? " ,  
                                         ad_df.iloc[i]["ad_updated_at"]
                                        ,ad_df.iloc[i]["ad_created_at"]
                                        ,ad_df.iloc[i]["ad_name"]
                                        ,ad_df.iloc[i]["ad_squad_id"]
                                        ,ad_df.iloc[i]["creative_id"]
                                        ,ad_df.iloc[i]["ad_status"]
                                        ,ad_df.iloc[i]["ad_type"]
                                        ,ad_df.iloc[i]["render_type"]
                                        ,ad_df.iloc[i]["review_status"]
                                        ,ad_df.iloc[i]["ad_id"]
                                        )

        else:
            
            cursor.execute("insert into dbo.kdp_ad ([ad_id] \
                                ,[ad_squad_id]              \
                                ,[ad_updated_at]            \
                                ,[ad_created_at]            \
                                ,[creative_id]              \
                                ,[ad_name]                  \
                                ,[ad_status]                \
                                ,[ad_type]                  \
                                ,[render_type]              \
                                ,[review_status])           \
                                        values (?,?,?,?,?,?,?,?,?,?)",
                                        ad_df.iloc[i]['ad_id'] 
                                        ,ad_df.iloc[i]['ad_squad_id']    
                                        ,ad_df.iloc[i]['ad_updated_at']   
                                        ,ad_df.iloc[i]['ad_created_at'] 
                                        ,ad_df.iloc[i]['creative_id']  
                                        ,ad_df.iloc[i]['ad_name']  
                                        ,ad_df.iloc[i]['ad_status']    
                                        ,ad_df.iloc[i]['ad_type']          
                                        ,ad_df.iloc[i]['render_type']     
                                        ,ad_df.iloc[i]['review_status']  
                                        )   
        
    cnxn.commit()    
    print ('Ad load process ended successfully')

def load_stats(cnxn,raw_df):

    ad_stats_df = raw_df.loc[:, ['ad_id',
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
                        'saves']]

    ad_stats_df.sort_values("ad_id", inplace = True) 
     
    ad_stats_df.drop_duplicates(subset = "ad_id" ,inplace = True) 
     
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
                        FROM [dbo].[kdp_stats] WHERE [dbo].[kdp_stats].[granularity]='LIFETIME' ")

    for row in cursor.fetchall():
        load_ad_stats_list.append(row.ad_id)  
    
    for i in range(0, len(ad_stats_df)):

        impressions             = int(ad_stats_df.iloc[i]["impressions"])
        swipes                  = int(ad_stats_df.iloc[i]["swipes"])
        quartile_1              = int(ad_stats_df.iloc[i]["quartile_1"])
        quartile_2              = int(ad_stats_df.iloc[i]["quartile_2"])
        quartile_3              = int(ad_stats_df.iloc[i]["quartile_3"])
        view_completion         = int(ad_stats_df.iloc[i]["view_completion"])
        spend                   = int(ad_stats_df.iloc[i]["spend"])
        view_time_millis        = int(ad_stats_df.iloc[i]["view_time_millis"])
        video_views             = int(ad_stats_df.iloc[i]["video_views"])
        screen_time_millis      = int(ad_stats_df.iloc[i]["screen_time_millis"])
        shares                  = int(ad_stats_df.iloc[i]["shares"])
        saves                   = int(ad_stats_df.iloc[i]["saves"])
     
        if ad_stats_df.iloc[i]['ad_id'] in load_ad_stats_list:
             
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
                                        where ad_id = ? AND granularity = 'LIFETIME'" ,  
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
                                        ,ad_stats_df.iloc[i]["ad_id"]
                                        )

        else:
            granularity = 'LIFETIME'
            stats_date = datetime.now().date()
            ad_id_date_granul = str(ad_stats_df.iloc[i]['ad_id']) + str(stats_date) + str(granularity)

            
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
                                                    ,[saves])                   \
                                        values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
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
                                                    )

    cnxn.commit()    
    print ('Ad core stats load process ended successfully')

def load_geo(cnxn,raw_df):

    ad_geo_df = raw_df.loc[:, ['ad_id',
                        'geo_impressions',
                        'geo_swipes',
                        'geo_uniques',
                        'geo_spend',
                        'country',
                        'region',
                        'geo_impression_composition',
                        'geo_unique_composition',
                        'dma'
                        ]]

    ad_geo_df.sort_values(by=['ad_id', 'dma'], inplace = True) 
     
    ad_geo_df.drop_duplicates(subset = ['ad_id', 'dma'] ,inplace = True) 
     
    load_ad_geo_list = []

    cursor = cnxn.cursor()

    cursor.execute("SELECT [ad_id_dma_granul_date]              \
                            ,[ad_id]                            \
                            ,[dma]                              \
                            ,[granularity]                      \
                            ,[geo_date]                         \
                            ,[geo_impressions]                  \
                            ,[geo_swipes]                       \
                            ,[geo_uniques]                      \
                            ,[geo_spend]                        \
                            ,[country]                          \
                            ,[region]                           \
                            ,[geo_impression_composition]       \
                            ,[geo_unique_composition]           \
                        FROM [dbo].[kdp_geo] WHERE [dbo].[kdp_geo].[granularity]='LIFETIME' ")

    for row in cursor.fetchall():
        load_ad_geo_list.append(row.ad_id + row.dma)  
    
    for i in range(0, len(ad_geo_df)):

        if ad_geo_df.iloc[i]['ad_id']  + ad_geo_df.iloc[i]['dma'] in load_ad_geo_list:
            print ('update dma - ',ad_geo_df.iloc[i]['ad_id'] , ad_geo_df.iloc[i]['dma']) 
            cursor.execute("update dbo.kdp_geo set [geo_impressions] = ?                \
                                                ,[geo_swipes] = ?                       \
                                                ,[geo_uniques] = ?                      \
                                                ,[geo_spend] = ?                        \
                                                ,[country] = ?                          \
                                                ,[region] = ?                           \
                                                ,[geo_impression_composition] = ?       \
                                                ,[geo_unique_composition] = ?           \
                                        where ad_id = ? AND dma = ? AND granularity = 'LIFETIME'" ,  
                                         ad_geo_df.iloc[i]['geo_impressions']
                                        ,ad_geo_df.iloc[i]['geo_swipes']
                                        ,ad_geo_df.iloc[i]['geo_uniques']
                                        ,ad_geo_df.iloc[i]['geo_spend']
                                        ,ad_geo_df.iloc[i]['country']
                                        ,ad_geo_df.iloc[i]['region']
                                        ,ad_geo_df.iloc[i]['geo_impression_composition']
                                        ,ad_geo_df.iloc[i]['geo_unique_composition']
                                        ,ad_geo_df.iloc[i]['ad_id']
                                        ,ad_geo_df.iloc[i]['dma']
                                        )

        else:
            granularity = 'LIFETIME'
            geo_date = datetime.now().date()
            ad_id_dma_granul_date = str(ad_geo_df.iloc[i]['ad_id']) + str(ad_geo_df.iloc[i]['dma']) + str(geo_date) + str(granularity)
            print ('insert dma - ', ad_id_dma_granul_date )
            cursor.execute("insert into dbo.kdp_geo ([ad_id_dma_granul_date]                    \
                                                        ,[ad_id]                                \
                                                        ,[dma]                                  \
                                                        ,[granularity]                          \
                                                        ,[geo_date]                             \
                                                        ,[geo_impressions]                      \
                                                        ,[geo_swipes]                           \
                                                        ,[geo_uniques]                          \
                                                        ,[geo_spend]                            \
                                                        ,[country]                              \
                                                        ,[region]                               \
                                                        ,[geo_impression_composition]           \
                                                        ,[geo_unique_composition])              \
                                        values (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                                     ad_id_dma_granul_date   
                                                    ,ad_geo_df.iloc[i]['ad_id']                    
                                                    ,ad_geo_df.iloc[i]['dma']            
                                                    ,granularity 
                                                    ,geo_date   
                                                    ,ad_geo_df.iloc[i]['geo_impressions']
                                                    ,ad_geo_df.iloc[i]['geo_swipes']
                                                    ,ad_geo_df.iloc[i]['geo_uniques']
                                                    ,ad_geo_df.iloc[i]['geo_spend']
                                                    ,ad_geo_df.iloc[i]['country']
                                                    ,ad_geo_df.iloc[i]['region']
                                                    ,ad_geo_df.iloc[i]['geo_impression_composition']
                                                    ,ad_geo_df.iloc[i]['geo_unique_composition']
                                                    )

    cnxn.commit()    
    print ('DMA stats load process ended successfully')

if __name__ == '__main__':
    main()
