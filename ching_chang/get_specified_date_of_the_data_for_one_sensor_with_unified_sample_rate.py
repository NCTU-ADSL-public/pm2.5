"""
@author: Ching Chang

api_description:
    get specified date of the data for one sensor with unified sample rate

for python api:
    function name:
        get_specified_date_of_the_data_for_one_sensor_with_unified_sample_rate
    input:
        device_id: str
        start_date: str
        end_date: str
        sample_rate: int
        days: list
            0 = Sunday, 1 = Monday, ...
        hours: list
            0 = 00:00 ~ 00:59, 1 = 01:00 ~ 01:59, ...
    output:
        data: list
            data[i][0] = timestamp
            data[i][0] = pm25
            data[i][0] = temperature
            data[i][0] = humidity

for web api:   
    function name:
        get_specified_date_of_the_data_for_one_sensor_with_unified_sample_rate_web
    input:
        device_id: str
        start_date: str
        end_date: str
        sample_rate: int
        days: list
            0 = Sunday, 1 = Monday, ...
        hours: list
            0 = 00:00 ~ 00:59, 1 = 01:00 ~ 01:59, ...
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



def daily__get_specified_date_of_the_data_for_one_sensor_with_unified_sample_rate(device_id, date, sample_rate, hours):
    
    # web crawler
    
    res = requests.get("https://pm25.lass-net.org/data/history-date.php?device_id=" + device_id + "&date=" + date)
    
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
    
    
    # align head of timestamp (start at 00:00:00) and fill the missing items (copy the first value)
    
    final_data = []
    buf = []    # store the data between two new timestamps
    start = dt.datetime.strptime(date + " 00:00:00", "%Y-%m-%d %H:%M:%S")
    
    delta_int = sample_rate
    delta = dt.timedelta(minutes = delta_int)
    slidewindow = start
    
    
    i = 0   # data index
    j = 0   # final_data index
    b = 0   # buffer index
    
    while slidewindow < data[0][0]:
        final_data.append([])
        final_data[j].append(slidewindow)
        final_data[j].append(data[0][1])
        final_data[j].append(data[0][2])
        final_data[j].append(data[0][3])
        j = j + 1
        slidewindow = slidewindow + delta
    
    
    # unify the sample rate (have data => get average, no data => store None)
    
    while 1:
        if i == len(data):
            final_data.append([])
            final_data[j].append(slidewindow)
            final_data[j].append(int(np.mean(buf, axis = 0)[0]))
            final_data[j].append(int(np.mean(buf, axis = 0)[1]))
            final_data[j].append(int(np.mean(buf, axis = 0)[2]))
            del buf[:]
            break
        
        if data[i][0] - slidewindow < delta:
            buf.append([])
            buf[b].append(data[i][1])
            buf[b].append(data[i][2])
            buf[b].append(data[i][3])
            i = i + 1
            b = b + 1
        else:
            if buf:
                final_data.append([])
                final_data[j].append(slidewindow)
                final_data[j].append(int(np.mean(buf, axis = 0)[0]))
                final_data[j].append(int(np.mean(buf, axis = 0)[1]))
                final_data[j].append(int(np.mean(buf, axis = 0)[2]))
                j = j + 1
                del buf[:]
                b = 0
                slidewindow = slidewindow + delta
            else:
                final_data.append([])
                final_data[j].append(slidewindow)
                final_data[j].append(None)
                final_data[j].append(None)
                final_data[j].append(None)
                j = j + 1
                slidewindow = slidewindow + delta
    
    
    # align tail of timestamp (ex: end at 23:55:00) and fill the missing items (copy the last value)
    
    last_time_in_minutes = 0
    while (1440 - last_time_in_minutes) > sample_rate:
        last_time_in_minutes += sample_rate
        
    final_data_end_timestamp = dt.datetime.strptime(date + str(last_time_in_minutes // 60) + ":" + str(last_time_in_minutes % 60) + ":00", "%Y-%m-%d%H:%M:%S")
    
    while final_data[len(final_data) - 1][0] < final_data_end_timestamp:
        slidewindow = slidewindow + delta
        final_data.append([])
        final_data[len(final_data) - 1].append(slidewindow)
        final_data[len(final_data) - 1].append(final_data[len(final_data) - 2][1])
        final_data[len(final_data) - 1].append(final_data[len(final_data) - 2][2])
        final_data[len(final_data) - 1].append(final_data[len(final_data) - 2][3])
    
    
    # use interpolation to update the "None"
    
    buf = []    # store None information, buf[i][0] = start index of None, buf[i][1] = end index of None
    b = 0       # buffer index
    
    for i in range(len(final_data)):
        if final_data[i][1] == None and final_data[i-1][1] != None:
            buf.append([])
            buf[b].append(i)
        elif final_data[i][1] != None and final_data[i-1][1] == None:
            buf[b].append(i-1)
            b = b + 1
    
    for element in buf:
        
        number_of_filled_value = element[1] - element[0] + 1
        before_none = []
        after_none = []
        for i in range(3):
            before_none.append(final_data[element[0] - 1][i + 1])
        for i in range(3):
            after_none.append(final_data[element[1] + 1][i + 1])
        
        for record_index in range(element[0], element[1] + 1):
            for i in range(3):
                final_data[record_index][i + 1] = int( before_none[i] + (record_index - element[0] + 1) * (after_none[i] - before_none[i]) /  (number_of_filled_value + 1) )
    
    
    # remove the data that time isn't in hours
    
    hours_need_to_remove = [element for element in list(range(0, 24)) if element not in hours]
    
    for element in hours_need_to_remove:
        hours_need_to_remove_start = dt.time(hour = element)
        hours_need_to_remove_end = dt.time(hour = element, minute = 59)
        
        final_data = [record for record in final_data if not (hours_need_to_remove_start <= record[0].time() and record[0].time() <= hours_need_to_remove_end)]
    
    
    return final_data



def get_specified_date_of_the_data_for_one_sensor_with_unified_sample_rate(device_id, start_date, end_date, sample_rate, days, hours):
    
    # change the type of start_date, end_date to datetime
    
    start_date = dt.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = dt.datetime.strptime(end_date, "%Y-%m-%d")
    
    
    # initial current_date
    
    current_date = start_date
    
    
    # process the data from start_date to end_date
    
    data = []
    while current_date <= end_date:
        current_date_weekday = current_date.isoweekday() if current_date.isoweekday() != 7 else 0
        if current_date_weekday not in days:
            pass
        else:
            #print("processing " + str(current_date).split(" ")[0])
            daily_data = daily__get_specified_date_of_the_data_for_one_sensor_with_unified_sample_rate(device_id, str(current_date).split(" ")[0], sample_rate, hours)
            for record in daily_data:
                data.append(record)
        current_date = current_date + dt.timedelta(days = 1)
    
    
    return data



def get_specified_date_of_the_data_for_one_sensor_with_unified_sample_rate_web(device_id, start_date, end_date, sample_rate, days, hours):
    
    # call function
    
    data = get_specified_date_of_the_data_for_one_sensor_with_unified_sample_rate(device_id, start_date, end_date, sample_rate, days, hours)
    
    
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
    
    

if __name__ == '__main__':
    
    # initial start_time for calculating spending time
    
    start_time = time.time()
    
    
    # call python api
    
    device_id = "74DA3895C2F0"
    start_date = "2017-06-29"
    end_date = "2017-07-03"
    sample_rate = 5
    days = list(range(0, 7))
    #days = [0, 6]
    hours = list(range(0, 24))
    #hours = [21, 22, 23]
    
    data = get_specified_date_of_the_data_for_one_sensor_with_unified_sample_rate(device_id, start_date, end_date, sample_rate, days, hours)
    
    
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
    