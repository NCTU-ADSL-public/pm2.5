
# DO NOT CHANGE THE FOLLOWING SETTINGS!
# ##################################################
# Cassandra Connection Configuration
# ----------------------------------------
# the server address
Cassandra_SERVER = '140.113.86.130'
# the user account and passwd for the participating party
Cassandra_USER = 'lassdb'
Cassandra_PASS = 'lassdb@dataarchive0222'
# the request consistency level setting
# doc: https://datastax.github.io/python-driver/api/cassandra.html#cassandra.ConsistencyLevel
Cassandra_CONSISTLEVEL = 1
# for large request, the result will be automatically paged
# set the size of each page
# doc: https://datastax.github.io/python-driver/api/cassandra/query.html#cassandra.query.Statement
Cassandra_FETCH_SIZE = 100
# set the timeout for the command execution in seconds
Cassandra_TIMEOUT = 60

# the name of the Cassandra Archive Table
Cassandra_ARCHIVE = 'archive'

# Source List Configuration
# ----------------------------------------

# this array contains all the data sources
#LIST_SOURCES = [ 'LASS', 'LASS4U', 'Indie', 'ProbeCube', 'WEBDUINO', 'AirBox', 'AsusAirBox', 'MAPS', 'EPA', 'CWB' ]
LIST_SOURCES = ['EPA']
#LIST_SOURCES = [ 'AirBox']

# ##################################################

# Local Database Configuration
# --------------------------------------------------

