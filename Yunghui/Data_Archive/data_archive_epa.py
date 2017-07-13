#!/usr/bin/env python
# Objctive: This program will do the following:
#     * retrieve the data from Cassandra

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement

import json
import csv
import sys
import ast
import pandas as pd

data =[]

# set the default program encoding to 'utf-8'
reload( sys )
sys.setdefaultencoding( 'utf-8' )
from datetime import datetime
from datetime import timedelta
import time

# Program Configuration ----------------------------
import config
Cassandra_SERVER = config.Cassandra_SERVER
Cassandra_USER = config.Cassandra_USER
Cassandra_PASS = config.Cassandra_PASS
Cassandra_CONSISTLEVEL = config.Cassandra_CONSISTLEVEL
Cassandra_FETCH_SIZE = config.Cassandra_FETCH_SIZE
Cassandra_TIMEOUT = config.Cassandra_TIMEOUT
Cassandra_ARCHIVE = config.Cassandra_ARCHIVE
LIST_SOURCES = config.LIST_SOURCES
# ---------------------------- Program Configuration

# Database Connection ------------------------------
# connect to Cassandra
auth_provider = PlainTextAuthProvider( username = Cassandra_USER, password = Cassandra_PASS )
# bypass the ssl check hostname procedure, which enables the program to connect to cassandra through local IP
ssl_options = {'check_hostname': False}
casscluster = Cluster( [ Cassandra_SERVER ], auth_provider = auth_provider, ssl_options = ssl_options )
casssession = casscluster.connect( Cassandra_ARCHIVE )
casssession.default_timeout = 60
# ------------------------------ Database Connection

##################################################
# the following parameters should be modified:
YEAR = 2017
MONTH = 05
DAY = 12
filename = "pm25_epa_05.txt"
csv_filename = "pm25_epa_05.csv"
##################################################


# start to archive the data
for SOURCE in LIST_SOURCES:
	# the Cassandra query command
	# ------------------------------
	# notes that:
	#  1. it is require to provide the source name!
	#  2. DO NOT REMOVE THE "ALLOW FILTERING" OPTION!
	# ------------------------------
	# query for 1 month's data:
	#Command = "SELECT data FROM " + Cassandra_ARCHIVE +  ".all WHERE source='" + SOURCE + "' and year=" + str( YEAR ) + " and month=" + str( int( MONTH ) ) + " ALLOW FILTERING;"
	# query for a date (ex. 2017-02-01):
	Command = "SELECT data FROM " + Cassandra_ARCHIVE +  ".all WHERE source='" + SOURCE + "' and year=" + str( YEAR ) + " and month=" + str( int( MONTH ) ) + " and day=" + str( int( DAY ) ) + " ALLOW FILTERING;"
	statement = SimpleStatement( Command, consistency_level = Cassandra_CONSISTLEVEL, fetch_size = Cassandra_FETCH_SIZE )

	try:
		# start archiving the data to the local database here!
		# ############################################################
		for result in casssession.execute( statement, timeout = Cassandra_TIMEOUT ):
			record = str(result.data).replace( '\n', '' ).replace( '\r', '' ).encode( 'utf-8' )
			#print(record)
			
			##### your code here... #####
			data.append(record)
		

		with open(filename, "w") as output:
			output.write(str(data))
		 

		# ############################################################
	except Exception as ex:
		print( "[ERROR] (" + str( ex ) + ") Cassandra Query Error for the command: " + str( Command ) )
		print( "[ERROR] Probably no data available for this source!" )
		continue

# close the database connections
casscluster.shutdown()



#---------------------------------------------------------------------------------------------
# python code here

with open(filename , 'r') as files:
    data = files.read()

data = data.replace("'", "")
data_dic= ast.literal_eval(data) # str to dic



# fill keyerror with NA
def get_node(name, node):
    try:
        val = node[name]
    except KeyError:
        val = 'NA'
    return val


##----------------------epa data--------------------------------------
f = csv.writer(open(csv_filename, "wb+"))
f.writerow(["ver_format","fmt_opt","app","date","time","Status","PSI","CO","SiteEngName","PM10","NO","SiteName","FPMI","WindDirec","PM2_5","PublishTime","County","WindSpeed","SO2","NOx","SiteType",            "O3","NO2","type","coordinates"])

for x in data_dic:
    f.writerow([get_node("ver_format", x),get_node("fmt_opt", x),get_node("app", x),get_node("date", x),get_node("time", x),get_node("Status", x), get_node('PSI', x), get_node("CO", x),                 get_node("SiteEngName", x),get_node("PM10", x),get_node("NO", x),get_node("SiteName", x),                 get_node("FPMI", x),get_node("WindDirec", x),get_node("PM2_5", x),get_node("PublishTime", x),                 get_node("County", x),get_node("WindSpeed", x),get_node("SO2", x),get_node("NOx", x),                 get_node("SiteType", x),get_node("O3", x),get_node("NO2", x),                 x["loc"]["type"],x["loc"]["coordinates"]])
    ##-----------------------------------------------------------------epa
