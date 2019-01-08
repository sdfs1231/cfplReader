import logging
import os
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

# data fetch url
#basicurl = 'http://10.254.137.222/fdbs/ajax.cod.php'
basicurl = 'http://127.0.0.1/fdbs/ajax.cod.php'

# mysql db setting
dbuser = 'CFPL'
dbpass = 'cfplreader'
dbname = 'szxsoc'
cfpltable = 'cfpl'

# cfpl save path
CFPLdir = '/../wamp64/www/fdbs/OFP/'

# sleep Interval per round
interval = 300

# getFlightList setting
timeDeltaBefore = 8
timeDeltaAfter = 8

# network retry setting
networkretry_pedding = 3
networkretry_max = 20

# aio setting
aio_max_connection = 100
aio_timeout = 60

# logger
loggerpath = 'Log/cfplreader'

# formatter
formatter = logging.Formatter('%(asctime)s - %(module)s - %(levelname)s - %(lineno)d - %(message)s')

# full log split by day
rotatehandler = TimedRotatingFileHandler(loggerpath + '.log', when="midnight", interval=1)
rotatehandler.suffix = "%Y-%m-%d"
rotatehandler.setFormatter(formatter)
rotatehandler.setLevel(logging.DEBUG)

# warning+ log
debugHandler = logging.FileHandler(loggerpath + '_debug.log')
debugHandler.setFormatter(formatter)
debugHandler.setLevel(logging.WARNING)

# StreamHandler
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.DEBUG)

# logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(stream_handler)
logger.addHandler(rotatehandler)
logger.addHandler(debugHandler)


def saveOpf2File(CFPLname, data, date):
    CFPLpath = CFPLdir + date + '/'
    os.makedirs(os.path.dirname(CFPLpath + CFPLname + ".txt"), exist_ok=True)
    with open(CFPLpath + CFPLname + ".txt", 'a') as f:
        f.write(data)
