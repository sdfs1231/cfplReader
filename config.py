import logging
from timer import timer
basicurl = 'http://10.254.137.222/fdbs.beta/ajax.cod.php'
dbuser = 'CFPL'
dbpass = 'cfplreader'
dbname = 'szxsoc'
#dbtable='cfpl'
cfpltable = 'cfpl'
CFPLpath='CFPLList/'
today=''
now=timer()[2]
for t in range(len(now)-4):
    today=today+t
loggerpath='Log/'
loggername=today+'.log'
#logger
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)

# Formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')

# FileHandler
file_handler = logging.FileHandler(loggerpath+loggername)#path+name
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# StreamHandler
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

def cfplfile(CFPLname,data):
    with open(CFPLpath + CFPLname, 'w') as f:
        f.write(data)

