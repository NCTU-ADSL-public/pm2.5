# -*- coding: utf-8 -*-

## airbox estimate
import json
import requests
import datetime



def main(): ## input device_id

    device_id = '28C2DDDD400A' ## user input
    #print 'input_device_id: {}'.format(device_id)
    
    ## get real time data
    real_time_airbox_url = requests.get("https://pm25.lass-net.org/data/last-all-airbox.json")
    real_time_airbox = real_time_airbox_url.json()['feeds']
    
    ## open R file 
    with open('airbox_R_one_sensor.json') as json_data:
        airbox_R = json.load(json_data)
    R = airbox_R['Radius'][u'23:20:00']
    
    
    ## split y
    for i in range(len(real_time_airbox)):
        if real_time_airbox[i]['device_id'] == device_id:
            test_y_info = {}
            test_y_info['device_id'] =  device_id
            test_y_info['lon'] = real_time_airbox[i]['gps_lon']
            test_y_info['lat']  = real_time_airbox[i]['gps_lat']
            test_y_info['s_d0']  = real_time_airbox[i]['s_d0']
            test_y_info['time']  = real_time_airbox[i]['time']
            break
            
    #print test_y_info
    
    
    ## get R
    time_pre = (datetime.datetime.strptime(test_y_info['time'], '%H:%M:%S') - datetime.timedelta(minutes=20)).time()
    time_now = test_y_info['time']
    #print time_pre, time_now
    
    for key, value in airbox_R['Radius'].iteritems():
        if (key > str(time_pre)) & (key < str(time_now)):
            R = airbox_R['Radius'][key]
            break
        
    ## filter lon & lat    
    U_lon = test_y_info['lon'] + R
    L_lon = test_y_info['lon'] - R
    U_lat = test_y_info['lat'] + R
    L_lat = test_y_info['lat'] - R
    
    
    ## split x
    test_x_pm25 = []
    for j in range(len(real_time_airbox)):
        if (real_time_airbox[j]['gps_lon'] < U_lon) & (real_time_airbox[j]['gps_lon'] > L_lon) & \
         (real_time_airbox[j]['gps_lat'] < U_lat) & (real_time_airbox[j]['gps_lat'] > L_lat):
             pm25_nearby = real_time_airbox[j]['s_d0']
             test_x_pm25.append(pm25_nearby)
             
    #print test_x_pm25
    
    
    pred = sum(test_x_pm25) / float(len(test_x_pm25))
    real = test_y_info['s_d0']
     
    estimate = {}
    estimate['pred'] = pred
    estimate['real'] = real
    estimate['device_id'] = device_id
    estimate['lon'] = test_y_info['lon']
    estimate['lat'] = test_y_info['lat']
    estimate['diff'] = abs(pred-real)
    print estimate
    
    

 
if __name__ == "__main__":
    main()