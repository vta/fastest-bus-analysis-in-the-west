import requests
import pandas as pd
import glob
import json
import time
import os

def pull_ridership_by_stop(line_number):

    # allFiles = glob.glob('.' + "/gps_*.csv")
    frame = pd.DataFrame()
    frames = []
    for file_ in ['Ridership/WEEKDAY.XLSX','Ridership/LRTWEEKDAY.XLSX']:
        df = pd.read_excel(file_, header=0)
        df['DAY']='RS_WKDY'
        #     df['DAY']='Weekday'
        frames.append(df)
    # for file_ in ['Ridership/SATURDAY.XLSX','Ridership/LRTSATURDAY.XLSX']:
    #     df = pd.read_excel(file_, header=0)
    #     df['DAY']='RS_SAT'
    #     frames.append(df)
    # for file_ in ['Ridership/SUNDAY.XLSX','Ridership/LRTSUNDAY.XLSX']:
    #     df = pd.read_excel(file_, header=0)
    #     df['DAY']='RS_SUN'
    #     frames.append(df)

    df = pd.concat(frames)
    df = df[~df['ROUTE_NUMBER'].isin([911,912,913,914])]

    df = df.query("DAY=='RS_WKDY'&ROUTE_NUMBER=='%d'" % line_number)

    #     pd.pivot_table(df.reset_index(), index=["STOP_ID"],values=["BOARD_ALL",'ALIGHT_ALL']).head()

    rid_line = pd.pivot_table(df.reset_index(), index=["STOP_ID","DIRECTION_NAME","TIME_PERIOD"],values=["SORT_ORDER","BOARD_ALL",'ALIGHT_ALL','LOAD_ALL','AVG_SERVICED','TIME_PERIOD_SORT']).reset_index().sort_values(by=['DIRECTION_NAME','TIME_PERIOD_SORT','SORT_ORDER'])
    rid_line['ALIGHT_ALL']= rid_line['ALIGHT_ALL'].round(2)
    rid_line['AVG_SERVICED'] = rid_line['AVG_SERVICED'].round(2)
    rid_line['BOARD_ALL'] = rid_line['BOARD_ALL'].round(2)
    rid_line['LOAD_ALL'] = rid_line['LOAD_ALL'].round(2)
    return rid_line

def pull_early_late_by_stop(line_number,SWIFTLY_API_KEY, dateRange, timeRange):
    '''http://dashboard.goswift.ly/vta/api-guide/docs/otp'''
#     line_number = '22'
#     dateRange = '10012017-10302017'
#     timeRange = '0500-0900'
    line_table = pd.read_csv('line_table.csv')
    line_table.rename(columns={"DirNum":"direction_id","DirectionName":"DIRECTION_NAME"},inplace=True)
    line_table['direction_id'] = line_table['direction_id'].astype(str)
    headers = {'Authorization': SWIFTLY_API_KEY}
    payload = {'agency': 'vta', 'route': line_number, 'dateRange': dateRange,'timeRange': timeRange, 'onlyScheduleAdherenceStops':'True'}
    # payload = {'agency': 'vta', 'dateRange': '10292017-10302017'}
    url = 'https://api.goswift.ly/otp/by-stop'
    r = requests.get(url, headers=headers, params=payload)
#     print(r.text)
    try:
        swiftly_df = pd.DataFrame(r.json()['data'])
        swiftly_df.rename(columns={"stop_id":"STOP_ID"},inplace=True)
        swiftly_df = pd.merge(swiftly_df,line_table.query('lineabbr==%s'%line_number)[['direction_id','DIRECTION_NAME']])
        swiftly_df['STOP_ID'] = swiftly_df['STOP_ID'].astype(int)
        return swiftly_df
    except KeyError:
        print(r.json())

def stop_frequency_percent(connection, line_number, days_to_consider, date_range):
    sql = '''SELECT DATEPART(month, apc_date_time) as month_of_year,
    DATEPART ( day , apc_date_time ) as day_of_month,
    datepart(dy, [apc_date_time]) as 'day_of_year',
    apc_date_time, current_route_id, K.direction_code_id,
    dc.[direction_description], bs_id, ext_trip_id
          FROM [ACS_13].[dbo].[apc_correlated] K
              LEFT join [ACS_13].[dbo].[direction_codes] dc
        on k.direction_code_id = dc.[direction_code_id]
          where 
      (apc_date_time between %s) and 
      current_route_id = %d 
      and bs_id != 0
      order by direction_code_id, bs_id, apc_date_time''' % (date_range, line_number)

    trips_sampled =  pd.read_sql(sql,connection).rename(columns={'bs_id':'STOP_ID','direction_description':'DIRECTION_NAME'})
    #Only consider certain days of the month
    trips_sampled = trips_sampled.loc[trips_sampled['day_of_month'].isin(days_to_consider),]
    
    #Add a time grouping
    trips_sampled['TIME_PERIOD'] = trips_sampled['apc_date_time'].apply(TIME_PERIOD)
    
    stops_visited_counts = trips_sampled.groupby(['current_route_id','DIRECTION_NAME','TIME_PERIOD','STOP_ID'])['ext_trip_id'].count().reset_index()
    stops_visited_counts.rename(columns={'ext_trip_id':'number_of_times_stopped'},inplace=True)
    
    trips_sampled_unique = trips_sampled.groupby(['current_route_id','DIRECTION_NAME','day_of_year','TIME_PERIOD'])['ext_trip_id'].nunique().reset_index()
    trips_sampled_count = trips_sampled_unique.groupby(['current_route_id','DIRECTION_NAME','TIME_PERIOD'])['ext_trip_id'].sum().reset_index()
    # stops_visited_counts
    trips_sampled_count.rename(columns={'ext_trip_id':'total_trips_sampled'},inplace=True)

#     stopped_frequency = pd.merge(stops_visited_counts,trips_sampled_count, how='outer')
    return stops_visited_counts, trips_sampled_count

def minutes_of_day(hour, minute):
    return hour*60 + minute
    # 1430-1829 = pm peak
    #Where is AM Late?
def TIME_PERIOD(x):
    #X is a timestamp
#     if df['time'].dt.hour >= 5 and df['time'].dt.hour <= 8:
    x_min = x.hour * 60 + x.minute

    if (x_min >= minutes_of_day(5,30) and  x_min < minutes_of_day(9,0)):    
        return 'AM Peak'
    elif (x_min >= minutes_of_day(14,30) and x_min < minutes_of_day(18,30)):
        return 'PM Peak'
    elif (x_min >= minutes_of_day(22,0) or x_min < minutes_of_day(3,0)):
        return 'PM Nite'
    elif (x_min >= minutes_of_day(3,0) and x_min < minutes_of_day(5,30)):
        return 'AM Early'
    elif (x_min >= minutes_of_day(9,0) and x_min < minutes_of_day(14,30)):
        return 'Midday'
    elif (x_min >= minutes_of_day(18,30) and x_min < minutes_of_day(22,0)):
        return 'PM Late'
    else:
        return 'Neither time zone'

def read_in_dwell_runtime(month):
    """This function takes a long time to run so it's broken out.  It reads in the swiftly data"""
    frame = pd.DataFrame()
    frames = []

    for dir_name in ['00-06','06-12','12_18','18_24']:
        allFiles = glob.glob('.' + "/swiftly_data/" + dir_name + "/*_" + str(month) + "-*.csv")
#         allFiles = glob.glob('.' + "/swiftly_data/" + dir_name + "/*.csv")
        for file_ in allFiles:
            df = pd.read_csv(file_)
            frames.append(df)
    df = pd.concat(frames, ignore_index=True)
    df['time'] = pd.to_datetime(df['actual_date'] + ' ' + df['actual_time'], format='%m-%d-%y %H:%M:%S')

    #Clean out spare vehicle.
    try:
        df = df[df.vehicle_id != 'spare']
        df['vehicle_id'] = df['vehicle_id'].astype(int)
    except TypeError:
        pass

    return df


def dwell_runtime(swiftly_source_data_df, line_number, days_to_consider, debug=True):
    df = swiftly_source_data_df

    df['day_of_month'] = df['time'].dt.day
    df = df.loc[df['day_of_month'].isin(days_to_consider),]
    
    df = df.query("route_id=='%d'" % line_number)
    df_stop_path_length = df.groupby(['route_id','direction_id','stop_id'])['stop_path_length_meters'].max().reset_index().rename(columns={'stop_id':'STOP_ID'})
    
    df['TIME_PERIOD'] = df['time'].apply(TIME_PERIOD)
    
    #Pull out the bottom percentile of results.
    df['dwell_rank'] = df.groupby(['route_id','direction_id','stop_id'])['dwell_time_secs'].rank(pct=True)
    df['travel_time_rank'] = df.groupby(['route_id','direction_id','stop_id'])['travel_time_secs'].rank(pct=True)

    if(debug):
        df.to_csv('rankings.csv', index=False)
    
    df = df.query("travel_time_rank>.05&travel_time_rank<.95|dwell_rank>.05&dwell_rank<.95")
    if(debug):
        df.to_csv('cut_data_rankings.csv', index=False)

    f = {'dwell_time_secs':['mean','std','size']}
    df_dwell = df.query("route_id=='%d'&is_departure==True" % line_number).groupby(['route_id','direction_id','TIME_PERIOD','stop_id']).agg(f)

    f = {'travel_time_secs':['mean','std','size']}
    df_runtime = df.query("route_id=='%d'&is_departure==False" % line_number).groupby(['route_id','direction_id','TIME_PERIOD','stop_id']).agg(f)
    
    df_min_travel_time = df.groupby(['route_id','direction_id','stop_id'])['travel_time_secs'].min().reset_index().rename(columns={'travel_time_secs':'travel_time_min_secs', 'stop_id':'STOP_ID'})

    df_results = pd.merge(df_dwell.reset_index(), df_runtime.reset_index())

    line_table = pd.read_csv('line_table.csv')
    line_table.rename(columns={"DirNum":"direction_id","DirectionName":"DIRECTION_NAME", "lineabbr":"route_id"},inplace=True)
    line_table['direction_id'] = line_table['direction_id'].astype(int)
    # line_table.direction_id = line_table.direction_id.astype(int)
    df_results = pd.merge(df_dwell.reset_index(), df_runtime.reset_index(), how='outer')
    df_results.rename(columns={'dwell_time_secs':'dwell_time_secs_','travel_time_secs':'travel_time_secs_'},inplace=True)
    df_results.columns = [''.join(t) for t in df_results.columns]
    #     df_results = pd.merge(df_results,df.groupby(['route_id','direction_id','stop_id'])['stop_path_length_meters'].max().reset_index(),how='left')
#     df_results = pd.merge(df_stop_path_length, df_results, how='outer')
    df_results.rename(columns={'stop_id':'STOP_ID'},inplace=True)
#     df_results['avg_travel_speed_meters_second'] = (df_results['stop_path_length_meters']/df_results['travel_time_secs_mean']).round(1)
    df_results = df_results.round({'dwell_time_secs_mean': 1, 'dwell_time_secs_std': 1, 'travel_time_secs_mean': 1, 'travel_time_secs_std': 1})
    df_results = pd.merge(df_results,line_table, how='left', left_on = ['route_id','direction_id'], right_on = ['route_id','direction_id'])
    return df_results, df_stop_path_length, df_min_travel_time