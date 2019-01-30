import os
import logging
import configparser
from ofptextprocess import ofptextprocess
from DB import Database

def updateData(cnx,ofpNr, data , logger=None):

    cursor = cnx.cursor()
    updateSQL = 'UPDATE `szxsoc`.`cfpl` SET `FL` = \'%s\', ' \
                '`eqpCd` = \'%s\',' \
                '`targetFuel` = %d ,'\
                '`routeDef` = \'%s\', ' \
                '`Rmk` = \'%s\', ' \
                '`Max_turb` = \'%s\', ' \
                '`turbPoint` = \'%s\', ' \
                '`Min_temp` = \'%s\', ' \
                '`tempPoint` = \'%s\' ' \
                'WHERE `ofpNr` like \'%s\' limit 1; ' \
                %(data['FL'],data['eqpCd'],data['targetFuel'],data['routeDef'],data['Rmk'],data['Max_turb'],data['turbPoint'],
                  data['Min_temp'],data['tempPoint'],ofpNr)
    try:
        cursor.execute(updateSQL)
        cnx.commit()
        return cursor.rowcount
    except Exception:
        logger.warning('query error:%s' % ofpNr, exc_info=True)
        logger.warning("sql statement : %s " % updateSQL)
        cursor.close()
        cnx.close()
        return -1

def get_file_list(root_path):
    result = []
    for file in os.listdir(root_path):
        fullpath = os.path.join(root_path, file)
        if os.path.isfile(fullpath):
            (filename, extension) = os.path.splitext(file)
            # print('processing: %s'% filename)
            result.append({"fullpath":fullpath,"filename":filename,"extension":extension})
        else:
            result += get_file_list(fullpath)
    return result

def logging_initial():
    # formatter
    formatter = logging.Formatter(logformat)

    # ofp update log
    debugHandler = logging.FileHandler(loggerpath + '_update.log')
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
    logger.addHandler(debugHandler)
    return logger


config = configparser.ConfigParser()
config.read('config.ini')
baseURL = config['API']['baseURL']
# mysql db setting
dbhost = config['MYSQL']['host']
dbport = config['MYSQL']['port']
dbuser = config['MYSQL']['user']
dbpassword = config['MYSQL']['pass']
dbname = config['MYSQL']['dbname']
tablename = config['MYSQL']['tablename']
# cfpl save path
save_path = config['CFPL']['save_path']
# sleep Interval per round
interval = int(config['INTERVAL']['interval'])
# getFlightList setting
timeDeltaBefore = int(config['FLIGHTLIST']['timeDeltaBefore'])
timeDeltaAfter = int(config['FLIGHTLIST']['timeDeltaAfter'])
# network retry setting
retry_waiting = int(config['NETWORK']['retry_waiting'])
retry_max = int(config['NETWORK']['retry_max'])

# aiohttp
max_connection = int(config['AIOHTTP']['max_connection'])
timeout = int(config['AIOHTTP']['timeout'])

# logger
loggerpath = config['LOGGING']['loggerpath']
logformat = config['LOGGING']['logformat']
loggersuffix = config['LOGGING']['loggersuffix']
logger = logging_initial()
logger.info('CFPL Reader Program Start')
logger.info('config read finfished')

db = Database(dbhost,
              dbport,
              dbuser,
              dbpassword,
              dbname,
              tablename,
              logger)

result = get_file_list("d:\\wamp64\\www\\fdbs\\ofp")
count = 0
for file in result:
    print("processing %s" % file['fullpath'])
    with open(file['fullpath']) as f:
        rowcount = updateData(db.getCnx(),file['filename'],ofptextprocess(f.read()),logger)
        print('affected row count :%s' % rowcount)
        count += rowcount

print(count)