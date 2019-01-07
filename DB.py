import mysql.connector
import config


# DB insert function
# noinspection PyPep8Naming
def insertData(ofpDict, logger=None):
    while True:
        try:
            cnx = mysql.connector.connect(user=config.dbuser, password=config.dbpass, database=config.dbname)
        except mysql.connector.Error:
            logger.warning('connecting DB failed', exc_info=True)
            continue
        break
    cursor = cnx.cursor()
    # 插入数据
    for key in ofpDict:
        if ofpDict[key] == {}:
            ofpDict[key] = None
    sql = ("INSERT INTO " + config.dbname + "." + config.cfpltable +
           "(fltDt,fltNr,opSuffix,depCd,alnCd,ofpNr,tailNr,eqpCd,engineNr,engineDesc,aircraftBias,arvCd,depDt,arvDt"
           ",airDist,gndDist,altnCd,tkofAltnCd,driftAltnCd,targetFuel,tripFuel,destFuel,rsvrFuel,altnFuel,holdFuel"
           ",extrFuel,tkofFuel,taxiOutFuel,loadFuel,etow,mtow,eldw,mldw,ezfw,mzfw,epld,reqdFuel,reqdTm,ci1,ci2,mel"
           ",routeNr,wxprogTm,obsDt,ofpDt,dispName,dispPhone,togw,burn,lgw,desfl,fod,time,comp,avtas,gs,routeDis"
           ",secondLevelEte,secondLevelFuel,secondLevelZfw,secondLevelDesc,thirdLevelEte,thirdLevelFuel,thirdLevelZfw"
           ",thirdLevelDesc,descFl,descFlTemper,uploadTm,dbUpdateTm,dispTime,dispSeat,cruiseMode,airDistDescription"
           ",secondLevelCruiseMode,thirdLevelCruiseMode,airMiles,gnd,ete,cruiseFlDescription,FL,routeDef,Rmk,Max_turb"
           ",turbPoint,Min_temp,tempPoint,altn1,altn2,altn3) "
           "VALUES (%(fltDt)s,%(fltNr)s,%(opSuffix)s,%(depCd)s,%(alnCd)s,%(ofpNr)s,%(tailNr)s,%(eqpCd)s,%(engineNr)s"
           ",%(engineDesc)s,%(aircraftBias)s,%(arvCd)s,%(depDt)s,%(arvDt)s,%(airDist)s,%(gndDist)s,%(altnCd)s"
           ",%(tkofAltnCd)s,%(driftAltnCd)s,%(targetFuel)s,%(tripFuel)s,%(destFuel)s,%(rsvrFuel)s,%(altnFuel)s"
           ",%(holdFuel)s,%(extrFuel)s,%(tkofFuel)s,%(taxiOutFuel)s,%(loadFuel)s,%(etow)s,%(mtow)s,%(eldw)s,%(mldw)s"
           ",%(ezfw)s,%(mzfw)s,%(epld)s,%(reqdFuel)s,%(reqdTm)s,%(ci1)s,%(ci2)s,%(mel)s,%(routeNr)s,%(wxprogTm)s"
           ",%(obsDt)s,%(ofpDt)s,%(dispName)s,%(dispPhone)s,%(togw)s,%(burn)s,%(lgw)s,%(desfl)s,%(fod)s,%(time)s"
           ",%(comp)s,%(avtas)s,%(gs)s,%(routeDis)s,%(secondLevelEte)s,%(secondLevelFuel)s,%(secondLevelZfw)s"
           ",%(secondLevelDesc)s,%(thirdLevelEte)s,%(thirdLevelFuel)s,%(thirdLevelZfw)s,%(thirdLevelDesc)s,%(descFl)s"
           ",%(descFlTemper)s,%(uploadTm)s,%(dbUpdateTm)s,%(dispTime)s,%(dispSeat)s,%(cruiseMode)s"
           ",%(airDistDescription)s,%(secondLevelCruiseMode)s,%(thirdLevelCruiseMode)s,%(airMiles)s,%(gnd)s,%(ete)s"
           ",%(cruiseFlDescription)s,%(FL)s,%(routeDef)s,%(Rmk)s,%(Max_turb)s,%(turbPoint)s,%(Min_temp)s,%(tempPoint)s"
           ",%(altn1)s,%(altn2)s,%(altn3)s)")
    try:
        cursor.execute(sql, ofpDict)
    except Exception:
        logger.warning('DB INSERT error : %s' % ofpDict['ofpNr'], exc_info=True)
        logger.warning('d:%s' % ofpDict)
        logger.warning("SQL statement : " + sql)
        return 0
    cnx.commit()
    cursor.close()
    cnx.close()
    config.logger.info('Insert Succeed：%s' % ofpDict['ofpNr'])


# DB query function
def queryoData(ofpNr, logger=None):
    while True:
        try:
            cnx = mysql.connector.connect(user=config.dbuser, password=config.dbpass, database=config.dbname)
        except mysql.connector.Error:
            logger.warning('connecting DB failed', exc_info=True)
            continue
        break
    cursor = cnx.cursor()
    query = "SELECT count(*) FROM " + config.dbname + "." + config.cfpltable + " where ofpNr = '" + ofpNr + "'"
    try:
        cursor.execute(query)
    except Exception:
        logger.warning('query error:%s' % ofpNr, exc_info=True)
        logger.warning("sql statement : %s " % query)
        cursor.close()
        cnx.close()
        return -1
    return cursor.fetchone()[0]
