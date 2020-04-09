import pyodbc
import time, os, re, datetime, json, csv
import pandas as pd
import numpy as np
import calendar
from datetime import datetime, timedelta

def main():

    cnxn = db_connection()

    load_stats(cnxn)

    print ('Ad core stats MONTHLY load process ended successfully')

    cnxn.close()

def db_connection():
    
    cnxn = pyodbc.connect('Trusted_Connection=yes',driver='{SQL Server Native Client 11.0}', server='p1-initde-01.ext.ipgnetwork.com' , database='SnapchatDB')
    print ('DB connection open')
    return cnxn

def current_month():

    current_date = datetime.now().strftime('%Y-%m-%d').split('-')
    year = current_date[0]
    month = current_date[1]
    last_day = calendar.monthrange(int(year), int(month))[1]
    
    date_range = (year + '-' + month + '-' + '01',year + '-' + month + '-' + str(last_day))
     
    return (date_range)

def isnewmonth(cnxn,min_date,max_date):

    cursor = cnxn.cursor()
    
    cursor.execute("SELECT    COUNT ([ad_id])  as count_ad_id                                      \
                            FROM [dbo].[kdp_stats] WHERE [dbo].[kdp_stats].[granularity]='MONTH' AND ([dbo].[kdp_stats].[stats_date] BETWEEN ? AND ?)"
                                ,
                                    min_date, 
                                    max_date
                                    )

    for row in cursor.fetchall():
                 
                if row.count_ad_id == 0:
                    new_month = True
                else:
                    new_month = False
    return (new_month)

def insertnewmonth(cnxn,min_date,max_date):

    cursor = cnxn.cursor()

    cursor.execute("SELECT       [ad_id]                                        \
                                ,SUM ([impressions])     as impressions         \
                                ,SUM ([swipes])          as swipes              \
                                ,SUM ([quartile_1])      as quartile_1          \
                                ,SUM ([quartile_2])      as quartile_2          \
                                ,SUM ([quartile_3])      as quartile_3          \
                                ,SUM ([view_completion]) as view_completion     \
                                ,SUM ([spend])           as spend               \
                                ,SUM ([video_views])     as video_views         \
                                ,SUM ([shares])          as shares              \
                                ,SUM ([saves])           as saves               \
                                ,SUM ([frequency])       as frequency           \
                                ,SUM ([uniques])         as uniques             \
                            FROM [dbo].[kdp_stats] WHERE [dbo].[kdp_stats].[granularity]='DAY' AND ([dbo].[kdp_stats].[stats_date] BETWEEN ? AND ?) GROUP BY [dbo].[kdp_stats].[ad_id]"
                                ,
                                    min_date, 
                                    max_date
                                    )

    for row in cursor.fetchall():
                 
                if (row.impressions == 0 or row.impressions == '') and (row.spend == 0 or row.spend == '') and (row.video_views == 0 or row.video_views == ''):
                    pass
                else:
                    granularity = 'MONTH'
                    stats_date = min_date[0:10] 
                    tempkey = row.ad_id + str(min_date[0:10]) + str(granularity)
                
                    cursor.execute("insert into dbo.kdp_stats ([ad_id_date_granul]          \
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
                                                                ,[video_views]              \
                                                                ,[shares]                   \
                                                                ,[saves]                    \
                                                                ,[frequency]                \
                                                                ,[uniques])                 \
                                                    values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                                                tempkey    
                                                                ,row.ad_id             
                                                                ,stats_date             
                                                                ,granularity 
                                                                ,row.impressions
                                                                ,row.swipes
                                                                ,row.quartile_1
                                                                ,row.quartile_2
                                                                ,row.quartile_3
                                                                ,row.view_completion
                                                                ,row.spend
                                                                ,row.video_views
                                                                ,row.shares
                                                                ,row.saves
                                                                ,row.frequency
                                                                ,row.uniques  
                                                                )

    cnxn.commit()    

def updatenewmonth(cnxn,min_date,max_date):

    load_ad_list = []

    cursor = cnxn.cursor()

    cursor.execute("SELECT DISTINCT [ad_id]    \
            FROM [dbo].[kdp_stats] WHERE [dbo].[kdp_stats].[granularity]='MONTH' AND ([dbo].[kdp_stats].[stats_date] BETWEEN ? AND ?)"
            ,
                min_date, 
                max_date
                )

    for row in cursor.fetchall():
        load_ad_list.append(row.ad_id) 
    
    cursor.execute("SELECT       [ad_id]                                        \
                                ,SUM ([impressions])     as impressions         \
                                ,SUM ([swipes])          as swipes              \
                                ,SUM ([quartile_1])      as quartile_1          \
                                ,SUM ([quartile_2])      as quartile_2          \
                                ,SUM ([quartile_3])      as quartile_3          \
                                ,SUM ([view_completion]) as view_completion     \
                                ,SUM ([spend])           as spend               \
                                ,SUM ([video_views])     as video_views         \
                                ,SUM ([shares])          as shares              \
                                ,SUM ([saves])           as saves               \
                                ,SUM ([frequency])       as frequency           \
                                ,SUM ([uniques])         as uniques             \
                            FROM [dbo].[kdp_stats] WHERE [dbo].[kdp_stats].[granularity]='DAY' AND ([dbo].[kdp_stats].[stats_date] BETWEEN ? AND ?) GROUP BY [dbo].[kdp_stats].[ad_id]"
                                ,
                                    min_date, 
                                    max_date
                                    )

    for row in cursor.fetchall():
        
        if row.ad_id in load_ad_list:
                    stats_date = datetime.now().strftime('%Y-%m-%d')  
                    cursor.execute("update dbo.kdp_stats set                    \
                                                 [stats_date] = ?               \
                                                ,[impressions] = ?              \
                                                ,[swipes] = ?                   \
                                                ,[quartile_1] = ?               \
                                                ,[quartile_2] = ?               \
                                                ,[quartile_3] = ?               \
                                                ,[view_completion] = ?          \
                                                ,[spend] = ?                    \
                                                ,[video_views] = ?              \
                                                ,[shares] = ?                   \
                                                ,[saves] = ?                    \
                                                ,[frequency] = ?                \
                                                ,[uniques]  = ?                 \
                                                where ad_id = ? AND (stats_date BETWEEN ? AND ?) AND granularity = 'MONTH'  " , 
                                                 stats_date 
                                                ,row.impressions
                                                ,row.swipes
                                                ,row.quartile_1
                                                ,row.quartile_2
                                                ,row.quartile_3
                                                ,row.view_completion
                                                ,row.spend
                                                ,row.video_views
                                                ,row.shares
                                                ,row.saves
                                                ,row.frequency
                                                ,row.uniques                  
                                                ,row.ad_id
                                                ,min_date
                                                ,max_date
                                                )
        else:
                    granularity = 'MONTH'
                    stats_date = min_date[0:10] 
                    tempkey = row.ad_id + str(min_date[0:10]) + str(granularity)
                
                    cursor.execute("insert into dbo.kdp_stats ([ad_id_date_granul]          \
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
                                                                ,[video_views]              \
                                                                ,[shares]                   \
                                                                ,[saves]                    \
                                                                ,[frequency]                \
                                                                ,[uniques])                 \
                                                    values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                                                tempkey    
                                                                ,row.ad_id             
                                                                ,stats_date             
                                                                ,granularity 
                                                                ,row.impressions
                                                                ,row.swipes
                                                                ,row.quartile_1
                                                                ,row.quartile_2
                                                                ,row.quartile_3
                                                                ,row.view_completion
                                                                ,row.spend
                                                                ,row.video_views
                                                                ,row.shares
                                                                ,row.saves
                                                                ,row.frequency
                                                                ,row.uniques  
                                                                )
        cnxn.commit()    

def load_stats(cnxn):

    dates = current_month()
       
    min_date = dates[0]
    max_date = dates[1]
    
    new_month = isnewmonth(cnxn,min_date,max_date)

    if new_month:
        insertnewmonth(cnxn,min_date,max_date)
    else:
        updatenewmonth(cnxn,min_date,max_date)
        
if __name__ == '__main__':
    main()
