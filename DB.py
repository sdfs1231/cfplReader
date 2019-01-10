import mysql.connector
import logging
import time


class Database:
    def __init__(self, host, port, username, password, db_name, table_name, logger=logging.getLogger()):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.db_name = db_name
        self.table_name = table_name
        self.logger = logger
        while True:
            try:
                self.cnx = mysql.connector.connect(host=self.host,
                                                   port=self.port,
                                                   user=self.username,
                                                   password=self.password,
                                                   database=self.db_name)
                self.cursor = self.cnx.cursor()
                self.logger.info('connected to DB')
            except mysql.connector.Error:
                self.logger.warning('connecting DB failed', exc_info=True)
                time.sleep(10)
                continue
            break

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cnx.commit()
        self.cnx.close()
        self.logger.info('connection closed')

    def insertData(self,ofpDict):
        # 插入数据
        for key in ofpDict:
            if ofpDict[key] == {}:
                ofpDict[key] = None
        sql = ("INSERT INTO " + self.db_name + "." + self.table_name +
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

            self.cursor.execute(sql, ofpDict)
            self.cnx.commit()
            #cursor.close()
            #self.cnx.close()
            self.logger.info('Insert Succeed：%s' % ofpDict['ofpNr'])
        except Exception:
            self.logger.warning('DB INSERT error : %s' % ofpDict['ofpNr'], exc_info=True)
            self.logger.warning('d:%s' % ofpDict)
            self.logger.warning("SQL statement : " + sql)
            return 0

    # DB query function
    def check_ofpNr(self, ofpNr):

        query = "SELECT count(*) FROM " + self.db_name + "." + self.table_name + " where ofpNr = '" + ofpNr + "'"
        try:
            #cursor = self.cnx.cursor()
            self.cursor.execute(query)
            count = self.cursor.fetchone()[0]
            #cursor.close()
            return count
        except Exception:
            self.logger.warning('query error:%s' % ofpNr, exc_info=True)
            self.logger.warning("sql statement : %s " % query)
            return -1
