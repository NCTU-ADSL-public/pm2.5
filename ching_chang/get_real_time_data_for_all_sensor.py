"""
@author: Ching Chang

api_description:
    get real time data for all sensor

for python api:
    function name:
        get_real_time_data_for_all_sensor
    input:
        
    output:
        data: list
            output_json_file_1: str (json file for airbox)
            output_json_file_2: str (json file for lass)
            output_json_file_3: str (json file for epa)

for web api:   
    function name:
        get_real_time_data_for_all_sensor_web
    input:
        
    output:
        output_json_file: str
            keys = airbox, lass, temperature, epa
            subkeys = lat, lon, device_id, pm25, humidity, temperature, name, timestamp
for script:
    run command:
        nohup python3 get_real_time_data_for_all_sensor.py &
    kill command:
        ps aux | grep python3
        # check for the relevant PID
        kill <relevantPID>
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
from pathlib import Path
import logging
import os



def get_real_time_data_for_all_sensor():
        
    # airbox
    
    res = requests.get("https://pm25.lass-net.org/data/last-all-airbox.json")
    
    if (res.json()):
        input_json_file = res.json()['feeds']
        output_json_file_1 = []
            
        
        for i in range(len(input_json_file)):
            data = {}
            data['lat'] = input_json_file[i]['gps_lat']
            data['lon'] = input_json_file[i]['gps_lon']
            data['device_id'] = input_json_file[i]['device_id']
            data['pm25'] = input_json_file[i]['s_d0']
            
            if 's_h0' not in input_json_file[i]:
                continue
            else:
                data['humidity'] = input_json_file[i]['s_h0']
            
            if 's_t0' not in input_json_file[i]:
                continue
            else:
                data['temperature'] = input_json_file[i]['s_t0']
            
            data['name'] = input_json_file[i]['device']
            data['timestamp'] = input_json_file[i]['timestamp'].replace("T", " ").replace("Z", "")
            
            
            output_json_file_1.append(data)
    
    
    # lass
    
    res = requests.get("https://pm25.lass-net.org/data/last-all-lass.json")
    
    if (res.json()):
        input_json_file = res.json()['feeds']
        output_json_file_2 = []
            
        
        for i in range(len(input_json_file)):
            data = {}
            data['lat'] = input_json_file[i]['gps_lat']
            data['lon'] = input_json_file[i]['gps_lon']
            data['device_id'] = input_json_file[i]['device_id']
            data['pm25'] = input_json_file[i]['s_d0']
            
            if 's_h0' not in input_json_file[i]:
                continue
            else:
                data['humidity'] = input_json_file[i]['s_h0']
            
            if 's_t0' not in input_json_file[i]:
                continue
            else:
                data['temperature'] = input_json_file[i]['s_t0']
            
            if 'device' not in input_json_file[i]:
                data['name'] = input_json_file[i]['device_id']
            else:
                data['name'] = input_json_file[i]['device']
            
            data['timestamp'] = input_json_file[i]['timestamp'].replace("T", " ").replace("Z", "")
            
            
            output_json_file_2.append(data)
    
    
    # epa
    
    res = requests.get("https://pm25.lass-net.org/data/last-all-epa.json")
    
    if (res.json()):
        input_json_file = res.json()['feeds']
        output_json_file_3 = []
            
        
        for i in range(len(input_json_file)):
            data = {}
            data['lat'] = input_json_file[i]['gps_lat']
            data['lon'] = input_json_file[i]['gps_lon']
            data['device_id'] = input_json_file[i]['SiteEngName']
            
            if 'PM2_5' not in input_json_file[i]:
                continue
            else:
                data['pm25'] = input_json_file[i]['PM2_5']
                
            data['humidity'] = ""
            data['temperature'] = ""
            
            data['name'] = input_json_file[i]['SiteName']
            data['timestamp'] = input_json_file[i]['timestamp'].replace("T", " ").replace("Z", "")
            
            
            output_json_file_3.append(data)
    
    
    return output_json_file_1, output_json_file_2, output_json_file_3



def get_real_time_data_for_all_sensor_web():
    
    # call function
  
    output_json_file_1, output_json_file_2, output_json_file_3 = get_real_time_data_for_all_sensor()
    
    output_json_file = json.dumps({"airbox" : output_json_file_1, "lass" : output_json_file_2, "epa" : output_json_file_3})
    
    
    return output_json_file


      
if __name__ == "__main__":
    
    # set up logging
    
    log_file = Path("get_real_time_data_for_all_sensor.log")
    
    if not log_file.is_file():
        with open("get_real_time_data_for_all_sensor.log", "w"):
            pass
    else:
        os.remove("get_real_time_data_for_all_sensor.log")
        with open("get_real_time_data_for_all_sensor.log", "w"):
            pass
    
    logging.basicConfig(level=logging.DEBUG, filename='get_real_time_data_for_all_sensor.log')
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    
    
    # call function infinitely time (every 5 min)
    
    while True:
        try:
            output_json_file_1, output_json_file_2, output_json_file_3 = get_real_time_data_for_all_sensor()
            
            
            with open('../html/data/real_time_data/airbox.json','w') as data_file:
                data_file.write(json.dumps({"airbox" : output_json_file_1}))
            
            
            with open('../html/data/real_time_data/lass.json','w') as data_file:
                data_file.write(json.dumps({"lass" : output_json_file_2}))
            
            
            if output_json_file_3 != []:
                with open('../html/data/real_time_data/epa.json','w') as data_file:
                    data_file.write(json.dumps({"epa" : output_json_file_3}))
            
            
            with open('../html/data/real_time_data/all.json','w') as data_file:
                
                epa_file = Path("../html/data/real_time_data/epa.json")
                
                if epa_file.is_file():
                    with open('../html/data/real_time_data/epa.json','r') as epa:
                        epa = json.load(epa)['epa']
                        data_file.write(json.dumps({"airbox" : output_json_file_1, "lass" : output_json_file_2, "epa" : epa}))
                else:
                    data_file.write(json.dumps({"airbox" : output_json_file_1, "lass" : output_json_file_2, "epa" : []}))
                
        except Exception as e:
            logging.error(e)
        
        time.sleep(5 * 60)
        