import os
import config
import mysql.connector
from ofptextprocess import ofptextprocess

def updateData(ofpNr, data , logger=None):
    while True:
        try:
            cnx = mysql.connector.connect(user=config.dbuser, password=config.dbpass, database=config.dbname)
        except mysql.connector.Error:
            logger.warning('connecting DB failed', exc_info=True)
            continue
        break
    cursor = cnx.cursor()
    updateSQL = 'UPDATE `szxsoc`.`cfpl` SET `FL` = "%s", ' \
                '`routeDef` = "%s", ' \
                '`Rmk` = "%s", ' \
                '`Max_turb` = "%s", ' \
                '`turbPoint` = "%s", ' \
                '`Min_temp` = "%s", ' \
                '`tempPoint` = "%s" ' \
                'WHERE `ofpNr` = "%s"; ' \
                %(data['FL'],data['routeDef'],data['Rmk'],data['Max_turb'],data['turbPoint'],
                  data['Min_temp'],data['tempPoint'],ofpNr)
    try:
        cursor.execute(updateSQL)
    except Exception:
        logger.warning('query error:%s' % ofpNr, exc_info=True)
        logger.warning("sql statement : %s " % updateSQL)
        cursor.close()
        cnx.close()
        return -1
    return 1

def processfile(root_path,process_func):
    result = []
    for file in os.listdir(root_path):
        target_file = os.path.join(root_path, file)
        if os.path.isfile(target_file):
            (filename, extension) = os.path.splitext(file)
            with open(target_file) as f:
                updateData(filename,process_func(f.read()),config.logger)
        else:
            result += processfile(target_file,process_func)
    return result


processfile("d:\\wamp64\\www\\fdbs\\ofp\\2019-01-07",ofptextprocess)