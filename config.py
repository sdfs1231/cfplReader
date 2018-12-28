import logging
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)

# Formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')

# FileHandler
file_handler = logging.FileHandler('/OFP/Log/result.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# StreamHandler
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

basicurl = 'http://10.254.137.222/fdbs.beta/ajax.cod.php'
dbuser = 'CFPL'
dbpass = 'cfplreader'
dbname = 'szxsoc'
#dbtable='cfpl'
cfpltable = 'cfpl'

