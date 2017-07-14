"""
@author: Ching Chang

api_description:
    get recent data for one device without unified sample_rate

for python api:
    function name:
        get_recent_data_for_one_device_without_unified_sample_rate
    input:
        device_id: str
    output:
        data: list
            data[i][0] = timestamp
            data[i][0] = pm25
            data[i][0] = temperature
            data[i][0] = humidity

for web api:   
    function name:
        get_recent_data_for_one_device_without_unified_sample_rate_web
    input:
        device_id: str
    output:
        output_json_file: str
            keys = timestamp, pm25, temperature, humidity
"""



import json
import csv
import datetime as dt
from operator import itemgetter
import numpy as np
import random
from pprint import pprint
import time
import requests



def get_recent_data_for_one_device_without_unified_sample_rate(device_id):
    
    # web crawler
    
    res = requests.get("https://pm25.lass-net.org/data/history.php?device_id=" + device_id)

    input_json_file = {}
    if not res.json()['feeds']:
        # no data
        #print(date + " : no data")
        return "no data"
    
    if 'AirBox' in res.json()['feeds'][0]:
        input_json_file = res.json()['feeds'][0]["AirBox"]
    elif 'LASS' in res.json()['feeds'][0]:
        input_json_file = res.json()['feeds'][0]["LASS"]
    
    
    # get timestamp, pm25, temperature, humidity
    
    data = []
    
    i = 0
    for record in input_json_file:
        data.append([])
        data[i].append(dt.datetime.strptime(record, "%Y-%m-%dT%H:%M:%SZ"))
        data[i].append(int(input_json_file[record]["s_d0"]))
        data[i].append(int(input_json_file[record]["s_t0"]))
        data[i].append(int(input_json_file[record]["s_h0"]))
        i = i + 1
    
    
    # sort with timestamp
    
    data.sort(key = itemgetter(0), reverse = False)
    
        
    return data
        
        
    '''
    # epa
    if 'EPA' in res.json()['feeds'][0]:
        
        input_json_file = res.json()['feeds']
        output_json_file = []
            
        
        for i in range(len(input_json_file)):
            data = {}
            data['lat'] = input_json_file[i]['gps_lat']
            data['lon'] = input_json_file[i]['gps_lon']
            data['device_id'] = ""
            
            if 'PM2_5' not in input_json_file[i]:
                continue
            else:
                data['s_d0'] = input_json_file[i]['PM2_5']
                
            data['s_h0'] = ""
            data['s_t0'] = ""
            
            data['name'] = input_json_file[i]['SiteName']
            data['timestamp'] = input_json_file[i]['timestamp'].replace("T", " ").replace("Z", "")
            
            
            output_json_file.append(data)
                
                
        return json.dumps({"epa" : output_json_file})
    '''


def get_recent_data_for_one_device_without_unified_sample_rate_web(device_id):
    
    # call function
    
    data = get_recent_data_for_one_device_without_unified_sample_rate(device_id)

    
    # return data as json file
    
    output_json_file = []
    
    for record in data:
        record_as_dict = {}
        record_as_dict['timestamp'] = str(record[0])
        record_as_dict['pm25'] = record[1]
        record_as_dict['temperature'] = record[2]
        record_as_dict['humidity'] = record[3]
        output_json_file.append(record_as_dict)
    
    output_json_file = json.dumps(output_json_file)            
    
    
    return output_json_file



if __name__ == "__main__":
    
    # initial start_time for calculating spending time
    
    start_time = time.time()
    
    
    # call function
    
    #device_id = "74DA3895C2F0"
    device_id = "FT3_022_1"
    
    data = get_recent_data_for_one_device_without_unified_sample_rate(device_id)
    
    
    # save as csv file
    
    with open('test/01-full_data.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["timesatmp", "pm25", "temperature", "humidity"])
        for record in data:
            writer.writerow(record)
    
    with open('test/02-timestamp.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for record in data:
            writer.writerow([record[0]])   
            
    with open('test/03-pm25.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for record in data:
            writer.writerow([record[1]])
        
    with open('test/04-temperature.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for record in data:
            writer.writerow([record[2]])
            
    with open('test/05-humidity.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for record in data:
            writer.writerow([record[3]])
    
    
    # show spending time
    
    print("spending time: %s seconds" % (time.time() - start_time))