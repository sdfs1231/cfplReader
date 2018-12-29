import logging
import os
from datetime import datetime

#from timer import timer
todaydire=datetime.now().__format__("%Y%m%d%H%M")
basicurl = 'http://10.254.137.222/fdbs.beta/ajax.cod.php'
dbuser = 'CFPL'
dbpass = 'cfplreader'
dbname = 'szxsoc'
#dbtable='cfpl'
cfpltable = 'cfpl'
CFPLdire='/../wamp64/www/fdbs.beta/OFP/'
loggerpath='Log/'
loggername = datetime.now().__format__("%Y%m%d")+'.log'
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

def saveOpf2File(CFPLname,data,date):
    CFPLpath=CFPLdire+date+'/'
    os.makedirs(os.path.dirname(CFPLpath + CFPLname + ".txt"), exist_ok=True)
    with open(CFPLpath + CFPLname+".txt", 'w') as f:
        f.write(data)

