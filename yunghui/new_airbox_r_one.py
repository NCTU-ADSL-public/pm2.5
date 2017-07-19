
# coding: utf-8



import pandas as pd
import numpy as np
import json


import math
from sklearn.metrics import mean_squared_error


## pass data================================================
def del_sensor(df, device_id):
    pre = pd.DataFrame()
    length = len(df.loc[df['device_id'] == device_id])
    for ID in df['device_id'].unique():
        temp_df = df.loc[df['device_id'] == ID]
        if len(temp_df) == length:
            pre = pd.concat([pre, temp_df])
    return pre



def get_sensor_info(df):
    latest_time = max(df['datetime'])
    latest_df = df.loc[(df['datetime'] == latest_time)]
    fetch_info_df = latest_df[['device_id','lon','lat']] 
    return fetch_info_df.drop_duplicates(keep=False)



def create_new_df(df, info_df):
    left = df[['datetime','device_id','s_d0','time_fix','time']]
    right = info_df
    new_df = pd.merge(left, right, on=['device_id'])
    return new_df



def pass_data(csv, device_id):
    df = pd.read_csv(csv)
    
    temp_datetime = df['datetime'].str.split(" ")
    df['date'] = temp_datetime.apply(lambda x: x[0])
    df['time'] = temp_datetime.apply(lambda x: x[1])
    df['time_fix'] = pd.to_datetime(df['time'], format=('%H:%M')).dt.time
    df['datetime'] = pd.to_datetime(df['datetime'], format=('%Y-%m-%d %H:%M:%S'))
    
    df = df.sort_values(['device_id','date','time'], ascending=[1,1,1])
    
    filter_data = del_sensor(df, device_id)
    info_df = get_sensor_info(filter_data)
    new_df = create_new_df(filter_data, info_df)
    
    return new_df, info_df
## ====================================================pass data END============


def get_time(df, time_start, time_end):   
    time_df = df.loc[(df['time_fix'] >= time_start) & (df['time_fix'] < time_end)]
    return time_df



def x_y_split(input_SiteEngName,df):

    df_x = df.loc[df['device_id'] != input_SiteEngName]
    df_y = df.loc[df['device_id'] == input_SiteEngName]

    return df_x, df_y



def flter_distance(df_y, df_x, r):
    
    lon_center = df_y['lon'].unique()[0]
    lat_center = df_y['lat'].unique()[0]

    new_df_x = df_x.loc[(df_x['lon'] < lon_center + r) & (df_x['lat'] < lat_center + r) &                         (df_x['lon'] > lon_center - r) & (df_x['lat'] > lat_center - r)]
    return new_df_x



def data_to_column(df):
    
    mat_PM25 = []
    for i in df['device_id'].unique().tolist():
        #print 'i:  {}'.format(i)
        new_df = df.loc[(df['device_id'] == i)]
        PM25 = new_df['s_d0'].values
        mat_PM25.append(PM25)
            
    return mat_PM25



def my_model(mat_PM25_x, mat_PM25_y):
    
    y_pred = []
    y_true = []
    
    for j in range(len(mat_PM25_y[0])):
        x_list = []
        #print '## round:{}'.format(j)
        for i in range(len(mat_PM25_x)):
            if len(mat_PM25_x[i]) == len(mat_PM25_y[0]):
                #print 'i: {}, len(mat_PM25_x[i]):  {}'.format(i, len(mat_PM25_x[i]))
                x_list.append(mat_PM25_x[i][j])
                
        #print 'x_list:  {}'.format(x_list)
        near_give_value = np.mean(np.array(x_list))
        real_value = mat_PM25_y[0][j]
        y_pred.append(near_give_value)
        y_true.append(real_value)
        #print 'estimated_value:  {}'.format(near_give_value)
        #print 'real_value:  {}'.format(real_value)
        #print 'diff:  {} '.format(abs(near_give_value-real_value))
        
        #print '----------------------------------------------------'
        
    return y_pred, y_true



def train_r_model(df_x, df_y, r_list):
    
    MSE_list = []
    for r in r_list:
        new_df_x = flter_distance(df_y, df_x, r)
        #print 'r  {}'.format(r) 
        #print 'new_df_x:  {}'.format(new_df_x) 
    
        # transfer data to matrix
        mat_PM25_x = data_to_column(new_df_x)
        mat_PM25_y = data_to_column(df_y)
        #print 'len(mat_PM25_x):  {}'.format(len(mat_PM25_x))
    
   
        #apply to model
        y_pred, y_true = my_model(mat_PM25_x, mat_PM25_y)
        MSE = mean_squared_error(y_true, y_pred)
    
        #print 'radius: {}'.format(r)
        #print 'MSE:  {}'.format(MSE)

        #print '-----------------------------------------------'
        MSE_list.append(MSE)

        
    #print 'best radius(MSE): {}, train_MSE: {}'.format(r_list[MSE_list.index(min(MSE_list))], min(MSE_list))     
    return r_list[MSE_list.index(min(MSE_list))] 



def get_distance(data1, data2):
    points = zip(data1, data2)
    diffs_squared_distance = [pow(a - b, 2) for (a, b) in points]
    return math.sqrt(sum(diffs_squared_distance))



def count_R_bytime(device_id, new_df):
    
    time_range = pd.date_range(min(new_df['time']), max(new_df['time']), freq='20min').time
    time_list = zip(time_range[:-1],time_range[1:])

    MSE_r_list = []
    for time in time_list:
        #print '### time range: {} ~ {}'.format(time[0],time[1])
        filter_df = get_time(new_df,time[0],time[1]) # input_time 
        
        # split x and y
        input_SiteEngName = device_id
        df_x, df_y = x_y_split(input_SiteEngName,filter_df)
        #print df_x, df_y
    
    
        # filter df_x by setting r
        lon_lat_x = df_x[['lon','lat']].drop_duplicates().as_matrix()
        lon_lat_y = df_y[['lon','lat']].drop_duplicates().as_matrix()
        distances = [get_distance(x, lon_lat_y[0]) for x in lon_lat_x] # calculate the min distance
        
        
        r_list = np.arange(min(distances)+0.0001, min(distances)+0.1, 0.002) #can be change
        min_MSE_r = train_r_model(df_x, df_y, r_list)
        MSE_r_list.append(min_MSE_r)
        
    #print type(range[:-1])
    #print MSE_r_list
    #print len(MSE_r_list)
    time_and_r_dict = dict(zip(time_range[:-1], MSE_r_list))
    return time_and_r_dict
    


def save_file(time_and_r_dict, sensor_info):
    result = {}
    result['Radius'] = {}
    for i in time_and_r_dict:
        result['Radius'][str(i)]=time_and_r_dict[i]
        
    result['device_id'] = sensor_info['device_id'].item()
    result['lon'] = sensor_info['lon'].item()
    result['lat'] = sensor_info['lat'].item()
        
    #print result  
    with open('airbox_R_one_sensor.json', 'w') as outfile:
        json.dump(result, outfile)


def radius(new_df, device_id, info_df):
    
    
    time_and_r_dict = count_R_bytime(device_id, new_df)
    
    # save result
    sensor_info = info_df.loc[info_df['device_id'] == device_id]
    save_file(time_and_r_dict, sensor_info)
    
    


def main(): # input device_id

    device_id = '28C2DDDD400A' ## user input
    print 'input_device_id: {}'.format(device_id)
    ## load the data
    new_df, info_df = pass_data('passdata_airbox.csv', device_id)

    radius(new_df, device_id, info_df)
    

    
 
if __name__ == "__main__":
    main()





