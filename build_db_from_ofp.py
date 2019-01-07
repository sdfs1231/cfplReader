import os
import config
import mysql.connector
import re
from ofptextprocess import ofptextprocess

#text = "P01 CHG TO P333 AFTER 0 O'CLOCK(BEIJING TIME) 3RD JAN'"
#print(re.sub(r'([\'\"])',"\\\\\g<1>",text))
#exit()

def updateData(cnx,ofpNr, data , logger=None):

    cursor = cnx.cursor()
    updateSQL = 'UPDATE `szxsoc`.`cfpl` SET `FL` = \'%s\', ' \
                '`routeDef` = \'%s\', ' \
                '`Rmk` = \'%s\', ' \
                '`Max_turb` = \'%s\', ' \
                '`turbPoint` = \'%s\', ' \
                '`Min_temp` = \'%s\', ' \
                '`tempPoint` = \'%s\' ' \
                'WHERE `ofpNr` like \'%s\' limit 1; ' \
                %(data['FL'],data['routeDef'],data['Rmk'],data['Max_turb'],data['turbPoint'],
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


while True:
    try:
        cnx = mysql.connector.connect(user=config.dbuser, password=config.dbpass, database=config.dbname)
    except mysql.connector.Error:
        config.logger.warning('connecting DB failed', exc_info=True)
        continue
    break

result = get_file_list("d:\\wamp64\\www\\fdbs\\ofp")
count = 0
for file in result:
    print("processing %s" % file['fullpath'])
    with open(file['fullpath']) as f:
        rowcount = updateData(cnx,file['filename'],ofptextprocess(f.read()),config.logger)
        print('affected row count :%s' % rowcount)
        count += rowcount

print(count)