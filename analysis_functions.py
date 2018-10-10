import requests
import pandas as pd
import glob
import json
import time
import os


def pull_ridership_by_stop(line_number):
    """
    Pull the ridership data from the local files - only get the fields we care
    about, sorted, and return the Dataframe.
    """
    # allFiles = glob.glob('.' + "/gps_*.csv")
    frame = pd.DataFrame()
    frames = []
    for file_ in ['Ridership/WEEKDAY.XLSX','Ridership/LRTWEEKDAY.XLSX']:
        df = pd.read_excel(file_, header=0)
        df['DAY']='RS_WKDY'
        frames.append(df)

    #When adding Saturday and Sunday numbers, use this to help
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

    rid_line = df[
            [
                'STOP_ID','DIRECTION_NAME','TIME_PERIOD','SORT_ORDER','BOARD_ALL',
                'ALIGHT_ALL','LOAD_ALL','AVG_SERVICED','TIME_PERIOD_SORT','TRIPS_ALL','TRIPS_GROSS'
            ]
        ].sort_values(by=['DIRECTION_NAME','TIME_PERIOD_SORT','SORT_ORDER'])
    rid_line['ALIGHT_ALL']= rid_line['ALIGHT_ALL'].round(2)
    rid_line['AVG_SERVICED'] = rid_line['AVG_SERVICED'].round(2)
    rid_line['BOARD_ALL'] = rid_line['BOARD_ALL'].round(2)
    rid_line['LOAD_ALL'] = rid_line['LOAD_ALL'].round(2)
    return rid_line

def pull_early_late_by_stop(line_number,SWIFTLY_API_KEY, dateRange, timeRange):
    """
    Pulls from the Swiftly APIS to get OTP.
    Follow the docs: http://dashboard.goswift.ly/vta/api-guide/docs/otp
    """
    line_table = pd.read_csv('line_table.csv')
    line_table.rename(columns={"DirNum":"direction_id","DirectionName":"DIRECTION_NAME"},inplace=True)
    line_table['direction_id'] = line_table['direction_id'].astype(str)
    headers = {'Authorization': SWIFTLY_API_KEY}
    payload = {'agency': 'vta', 'route': line_number, 'dateRange': dateRange,'timeRange': timeRange, 'onlyScheduleAdherenceStops':'True'}
    url = 'https://api.goswift.ly/otp/by-stop'
    r = requests.get(url, headers=headers, params=payload)
    try:
        swiftly_df = pd.DataFrame(r.json()['data'])
        swiftly_df.rename(columns={"stop_id":"STOP_ID"},inplace=True)
        swiftly_df = pd.merge(swiftly_df,line_table.query('lineabbr==%s'%line_number)[['direction_id','DIRECTION_NAME']])
        swiftly_df['STOP_ID'] = swiftly_df['STOP_ID'].astype(int)
        return swiftly_df
    except KeyError:
        print(r.json())

def stop_frequency_percent(connection, line_number, days_to_consider, date_range):
    """
    From parameters, query the MSSQL database to get how often vehicles on a route
    and direction get apc data, thus how often they stop and open the doors, then
    generate a percentage.
    """
    sql = '''
    SELECT
    DATEPART(month, apc_date_time) as month_of_year,
    DATEPART ( day , apc_date_time ) as day_of_month,
    datepart(dy, [apc_date_time]) as 'day_of_year',
    apc_date_time,
    current_route_id,
    K.direction_code_id,
    dc.[direction_description],
    bs_id,
    ext_trip_id
    FROM
    [ACS_13].[dbo].[apc_correlated] K
    LEFT JOIN
    [ACS_13].[dbo].[direction_codes] dc
    on k.direction_code_id = dc.[direction_code_id]
    WHERE
    (apc_date_time between %s) and
    current_route_id = %d and
    bs_id != 0
    ORDER BY
    direction_code_id, bs_id, apc_date_time
    ''' % (date_range, line_number)

    trips_sampled =  pd.read_sql(sql,connection).rename(columns={'bs_id':'STOP_ID','direction_description':'DIRECTION_NAME'})

    #Only consider certain days of the month
    trips_sampled = trips_sampled.loc[trips_sampled['day_of_month'].isin(days_to_consider),]
    
    #Add a time grouping
    trips_sampled['TIME_PERIOD'] = trips_sampled['apc_date_time'].apply(TIME_PERIOD)
    
    stops_visited_counts = trips_sampled.groupby([
        'current_route_id','DIRECTION_NAME','TIME_PERIOD','STOP_ID'
        ])['ext_trip_id'].count().reset_index()
    stops_visited_counts.rename(columns={'ext_trip_id':'number_of_times_stopped'},inplace=True)
    
    trips_sampled_unique = trips_sampled.groupby([
        'current_route_id','DIRECTION_NAME','day_of_year','TIME_PERIOD'
        ])['ext_trip_id'].nunique().reset_index()
    trips_sampled_count = trips_sampled_unique.groupby([
        'current_route_id','DIRECTION_NAME','TIME_PERIOD'
        ])['ext_trip_id'].sum().reset_index()
    # stops_visited_counts
    trips_sampled_count.rename(columns={'ext_trip_id':'total_trips_sampled'},inplace=True)

    return stops_visited_counts, trips_sampled_count

def minutes_of_day(hour, minute):
    return hour*60 + minute
def TIME_PERIOD(x):
    """
    X is a timestamp.  Breack up minutes and return a zone.  This gets used by Pandas's apply.
    """
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

def read_in_dwell_runtime(month = 10, year = 2018):
    """
    This function reads in the scraped swiftly runtinme data from disk and returns a Dataframe.
    """

    # Match the 01, 02, data format for months.
    if month < 10:
        month = '0' + str(month)
    else:
        month = str(month)

    frame = pd.DataFrame()
    frames = []

    for dir_name in ['00-06','06-12','12_18','18_24']:
        allFiles = glob.glob('.' + "/swiftly_data/" + dir_name + "/*_" + month + "-*" + str(year) + ".csv")
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
    """
    Generate 3 dfs: travel_time/dwell, stop path lengths, and minimum travel time.
    Take the swiftly data, filter down to the route in consideration, and then filter and rename, 
    clean out bottom and top 5% of dwell and travel time data, create summary statistics and return.
    """
    df = swiftly_source_data_df

    df['day_of_month'] = df['time'].dt.day
    df = df.loc[df['day_of_month'].isin(days_to_consider),]
    
    df = df.query("route_id=='%d'" % line_number)

    df_stop_path_length = df.groupby(['route_id','direction_id','stop_id'])['stop_path_length_meters'
        ].max().reset_index().rename(columns={'stop_id':'STOP_ID'})
    
    df.loc[df.index, 'TIME_PERIOD'] = df['time'].apply(TIME_PERIOD)
    
    # Generate percentiles of results for a given route, direction and stop.
    df['dwell_rank'] = df.groupby(['route_id','direction_id','stop_id'])['dwell_time_secs'].rank(pct=True)
    df['travel_time_rank'] = df.groupby(['route_id','direction_id','stop_id'])['travel_time_secs'].rank(pct=True)

    if(debug):
        df.to_csv('debug/full_rankings.csv', index=False)
    
    # Only keep .05 to .95 percentile of the data.
    df = df.query("travel_time_rank>.05&travel_time_rank<.95|dwell_rank>.05&dwell_rank<.95")
    if(debug):
        df.to_csv('debug/cut_data_rankings.csv', index=False)

    f = {'dwell_time_secs':['mean','std','size']}
    df_dwell = df.query("route_id=='%d'&is_departure==True" % line_number).groupby(['route_id','direction_id','TIME_PERIOD','stop_id']).agg(f)

    f = {'travel_time_secs':['mean','std','size']}
    df_runtime = df.query("route_id=='%d'&is_departure==False" % line_number).groupby(['route_id','direction_id','TIME_PERIOD','stop_id']).agg(f)
    
    df_min_travel_time = df.groupby(['route_id','direction_id','stop_id'
        ])['travel_time_secs'].min().reset_index().rename(columns={'travel_time_secs':'travel_time_min_secs', 'stop_id':'STOP_ID'})

    df_results = pd.merge(df_dwell.reset_index(), df_runtime.reset_index())

    # Use the line_table to generate east/west/south/north directions to corresponding 0/1
    line_table = pd.read_csv('line_table.csv')
    line_table.rename(columns={"DirNum":"direction_id","DirectionName":"DIRECTION_NAME", "lineabbr":"route_id"},inplace=True)
    line_table['direction_id'] = line_table['direction_id'].astype(int)

    df_results = pd.merge(df_dwell.reset_index(), df_runtime.reset_index(), how='outer')
    df_results.rename(columns={'dwell_time_secs':'dwell_time_secs_','travel_time_secs':'travel_time_secs_'},inplace=True)
    df_results.columns = [''.join(t) for t in df_results.columns]


    df_results.rename(columns={'stop_id':'STOP_ID'},inplace=True)
    df_results = df_results.round({'dwell_time_secs_mean': 1, 'dwell_time_secs_std': 1, 'travel_time_secs_mean': 1, 'travel_time_secs_std': 1})
    df_results = pd.merge(df_results,line_table, how='left', left_on = ['route_id','direction_id'], right_on = ['route_id','direction_id'])
    return df_results, df_stop_path_length, df_min_travel_time

def timepoint_finder(transitfeeds_url = 'http://transitfeeds.com/p/vta/45/20170929/download'):
    """
    Given a certain gtfs on transitfeeds, (if the vta posted a time in the feed, we marked the stop as a timepoint), get the timepoints and returns the df.
    """
    def gtfs_downloader(url):
        import urllib.request
        import zipfile
        file_name = 'gtfs.zip'
        urllib.request.urlretrieve(url, file_name)
        
        with zipfile.ZipFile(file_name,"r") as zip_ref:
            zip_ref.extractall("gtfs/")

    gtfs_downloader(transitfeeds_url)

    routes = pd.read_csv('gtfs/routes.txt')
    trips = pd.read_csv('gtfs/trips.txt')
    st = pd.read_csv('gtfs/stop_times.txt')

    count_df = trips[trips.service_id=='Weekdays'].groupby(['route_id','direction_id','shape_id']).count().reset_index()
    top_shapes = count_df.sort_values('service_id',ascending=False).drop_duplicates(['route_id','direction_id']).sort_values(by=['route_id','direction_id'],ascending=True)

    trip_set = []
    for i,r in top_shapes.iterrows():
        shape_id = r['shape_id']
        trip_set.extend(trips.query("shape_id=='%s'" %(shape_id))['trip_id'].head(1).values)

    trip_subset = trips.loc[trips['trip_id'].isin(trip_set)]
    timepoints = pd.merge(trip_subset,st.loc[st['arrival_time'].dropna(axis='index').index,], how='left')
    timepoints = timepoints[['route_id','direction_id','stop_id']]
    timepoints['timepoint'] = 1
    timepoints.rename(columns={'stop_id':'STOP_ID'},inplace=True)
    return timepoints