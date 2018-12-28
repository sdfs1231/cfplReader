import mysql.connector
import config

#DB insert function
def insertData(dict,detail,logger=None):
    while True:
        try:
            cnx = mysql.connector.connect(user=config.dbuser, password=config.dbpass, database=config.dbname)
        except mysql.connector.Error as err:
            logger.warning('connecting DB failed!')
            continue
        break
    cursor = cnx.cursor()
    dict['FL'] = detail[0]
    dict['routeDef'] = detail[1]
    dict['Rmk'] = detail[2]
    dict['Max_turb'] = detail[3]
    dict['turbPoint'] = detail[4]
    dict['Min_temp'] = detail[5]
    dict['tempPoint'] = detail[6]
    dict['mel'] = detail[7]
    if 8<len(detail):
        dict['altn1'] = detail[8]
    if 9 < len(detail):
        dict['altn2'] = detail[9]
    else:
        dict['altn2'] = ''
    if 10 < len(detail):
        dict['altn3'] = detail[10]
    else:
        dict['altn3'] = ''
    del (dict['ofpText'])
    del (dict['rlsText'])
    # 插入数据
    for key in dict:
        if dict[key] == {}:
            dict[key] = None
    sql = ("INSERT INTO cfpl"
           "(fltDt,fltNr,opSuffix,depCd,alnCd,ofpNr,tailNr,eqpCd,engineNr,engineDesc,aircraftBias,arvCd,depDt,arvDt,airDist,gndDist,altnCd,tkofAltnCd,driftAltnCd,targetFuel,tripFuel,destFuel,rsvrFuel,altnFuel,holdFuel,extrFuel,tkofFuel,taxiOutFuel,loadFuel,etow,mtow,eldw,mldw,ezfw,mzfw,epld,reqdFuel,reqdTm,ci1,ci2,mel,routeNr,wxprogTm,obsDt,ofpDt,dispName,dispPhone,togw,burn,lgw,desfl,fod,time,comp,avtas,gs,routeDis,secondLevelEte,secondLevelFuel,secondLevelZfw,secondLevelDesc,thirdLevelEte,thirdLevelFuel,thirdLevelZfw,thirdLevelDesc,descFl,descFlTemper,uploadTm,dbUpdateTm,dispTime,dispSeat,cruiseMode,airDistDescription,secondLevelCruiseMode,thirdLevelCruiseMode,airMiles,gnd,ete,cruiseFlDescription,FL,routeDef,Rmk,Max_turb,turbPoint,Min_temp,tempPoint,altn1,altn2,altn3) "
           "VALUES (%(fltDt)s,%(fltNr)s,%(opSuffix)s,%(depCd)s,%(alnCd)s,%(ofpNr)s,%(tailNr)s,%(eqpCd)s,%(engineNr)s,%(engineDesc)s,%(aircraftBias)s,%(arvCd)s,%(depDt)s,%(arvDt)s,%(airDist)s,%(gndDist)s,%(altnCd)s,%(tkofAltnCd)s,%(driftAltnCd)s,%(targetFuel)s,%(tripFuel)s,%(destFuel)s,%(rsvrFuel)s,%(altnFuel)s,%(holdFuel)s,%(extrFuel)s,%(tkofFuel)s,%(taxiOutFuel)s,%(loadFuel)s,%(etow)s,%(mtow)s,%(eldw)s,%(mldw)s,%(ezfw)s,%(mzfw)s,%(epld)s,%(reqdFuel)s,%(reqdTm)s,%(ci1)s,%(ci2)s,%(mel)s,%(routeNr)s,%(wxprogTm)s,%(obsDt)s,%(ofpDt)s,%(dispName)s,%(dispPhone)s,%(togw)s,%(burn)s,%(lgw)s,%(desfl)s,%(fod)s,%(time)s,%(comp)s,%(avtas)s,%(gs)s,%(routeDis)s,%(secondLevelEte)s,%(secondLevelFuel)s,%(secondLevelZfw)s,%(secondLevelDesc)s,%(thirdLevelEte)s,%(thirdLevelFuel)s,%(thirdLevelZfw)s,%(thirdLevelDesc)s,%(descFl)s,%(descFlTemper)s,%(uploadTm)s,%(dbUpdateTm)s,%(dispTime)s,%(dispSeat)s,%(cruiseMode)s,%(airDistDescription)s,%(secondLevelCruiseMode)s,%(thirdLevelCruiseMode)s,%(airMiles)s,%(gnd)s,%(ete)s,%(cruiseFlDescription)s,%(FL)s,%(routeDef)s,%(Rmk)s,%(Max_turb)s,%(turbPoint)s,%(Min_temp)s,%(tempPoint)s,%(altn1)s,%(altn2)s,%(altn3)s)")
    try:
        cursor.execute(sql, dict)
    except Exception:
        logger.warning('insert error!',exc_info=True)
        return 0
    cnx.commit()
    cursor.close()
    cnx.close()
    print('成功插入：航班号%s！'%dict['ofpNr'])

#DB query function
def queryoData(dict,logger=None):
    while True:
        try:
            cnx = mysql.connector.connect(user=config.dbuser, password=config.dbpass, database=config.dbname)
        except mysql.connector.Error :
            logger.warning('connecting DB failed!')
            continue
        break
    cursor = cnx.cursor()
    query = "SELECT count(*) FROM cfpl where ofpNr = '" + dict['ofpNr'] + "'"
    try:
        cursor.execute(query)
    except Exception:
        logger.warning('query ',exc_info=True)
        cursor.close()
        cnx.close()
        return 0
    return cursor.fetchone()[0]