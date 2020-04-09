import pyodbc
import time, os, re, datetime, json, csv
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def main():

    datelist = [('2019-01-01','2019-01-31'),
                ('2019-02-01','2019-02-28'),
                ('2019-03-01','2019-03-31'),
                ('2019-04-01','2019-04-30'),
                ('2019-05-01','2019-05-31'),
                ('2019-06-01','2019-06-30'),
                ('2019-07-01','2019-07-31'),
                ('2019-08-01','2019-08-31'),
                ('2019-09-01','2019-09-30'),
                ('2019-10-01','2019-10-31'),
                ('2019-11-01','2019-11-30'),
                ('2019-12-01','2019-12-31'),
                ('2020-01-01','2020-01-31'),
                ('2020-02-01','2020-02-29'),
                ('2020-03-01','2020-03-31'),
                ('2020-04-01','2020-04-30')]

    cnxn = db_connection()

    load_stats(cnxn,datelist)

    print ('Ad core stats INITIAL MONTHLY load process ended successfully')

    cnxn.close()

def db_connection():
    
    cnxn = pyodbc.connect('Trusted_Connection=yes',driver='{SQL Server Native Client 11.0}', server='p1-initde-01.ext.ipgnetwork.com' , database='SnapchatDB')
    print ('DB connection open')
    return cnxn

def load_stats(cnxn,datelist):

    cursor = cnxn.cursor()

    for dates in datelist:
       
        min_date = dates[0]
        max_date = dates[1]
        print (min_date,max_date)

        cursor.execute("SELECT   [ad_id]                                        \
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
                print (row)
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
    
if __name__ == '__main__':
    main()
