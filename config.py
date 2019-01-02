import logging
import os
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

#data fetch url
basicurl = 'http://10.254.137.222/fdbs/ajax.cod.php'

#mysql db setting
dbuser = 'CFPL'
dbpass = 'cfplreader'
dbname = 'szxsoc'
cfpltable = 'cfpl'

#cfpl save path
CFPLdir='/../wamp64/www/fdbs/OFP/'

#sleep Interval per round
interval = 300

#getFlightList setting
timeDeltaBefore = 8
timeDeltaAfter = 8

#network retry setting
networkretry_pedding = 10
networkretry_max = 100

#aio setting
aio_max_connection = 100
aio_timeout = 30




todaydire=datetime.now().__format__("%Y%m%d%H%M")



loggerpath='Log/'
#logger

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)

# Formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')

# FileHandler
file_handler = logging.FileHandler(loggerpath+datetime.now().__format__("%Y%m%d")+'.log')#path+name
file_handler.setFormatter(formatter)
rotatehandler= TimedRotatingFileHandler(loggerpath+datetime.now().__format__("%Y-%m-%d")+'.log', when="midnight", interval=1)
rotatehandler.suffix="%Y-%m-%d"
rotatehandler.setFormatter(formatter)
#logger.addHandler(file_handler)
logger.addHandler(rotatehandler)
# StreamHandler
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

def saveOpf2File(CFPLname,data,date):
    CFPLpath=CFPLdir+date+'/'
    os.makedirs(os.path.dirname(CFPLpath + CFPLname + ".txt"), exist_ok=True)
    with open(CFPLpath + CFPLname+".txt", 'a') as f:
        f.write(data)

