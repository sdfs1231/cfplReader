import mysql.connector
import logging
import time
import json


class Database:
    def __init__(self, host, port, username, password, db_name, table_name, alarmtable, aircrafttable, logger=logging.getLogger()):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.db_name = db_name
        self.table_name = table_name
        self.logger = logger
        self.alarmtable=alarmtable
        self.aircrafttable = aircrafttable
        while True:
            try:
                self.cnx = mysql.connector.connect(host=self.host,
                                                   port=self.port,
                                                   user=self.username,
                                                   password=self.password,
                                                   database=self.db_name)
                #self.cursor = self.cnx.cursor()
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

    def getCnx(self):
        return self.cnx

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
            cursor = self.cnx.cursor()
            cursor.execute(sql, ofpDict)
            self.cnx.commit()
            cursor.close()
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
            cursor = self.cnx.cursor()
            cursor.execute(query)
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception:
            self.logger.warning('query error:%s' % ofpNr, exc_info=True)
            self.logger.warning("sql statement : %s " % query)
            return -1

    def fetch_latest_ofp(self, fltDt, fltNr, opSuffix, depCd, arvCd, depDt):
        if opSuffix is None:
            opSuffix = ''
            query = "SELECT * FROM "+self.db_name+"."+ self.table_name+ \
                " WHERE fltDt = %s AND fltNr = %s AND opSuffix IS NULL AND depCd = %s " \
                "AND arvCd = %s AND ABS(TIMESTAMPDIFF(HOUR,depDt,%s)) < 16 ORDER BY uploadTm DESC LIMIT 1"
        else:
            query = "SELECT * FROM "+self.db_name+"."+ self.table_name+ \
                " WHERE fltDt = %s AND fltNr = %s AND opSuffix = '"+opSuffix+"' AND depCd = %s " \
                "AND arvCd = %s AND ABS(TIMESTAMPDIFF(HOUR,depDt,%s)) < 16 ORDER BY uploadTm DESC LIMIT 1"

        try:
            cursor = self.cnx.cursor(dictionary=True, buffered=True)
            cursor.execute(query, (fltDt, fltNr,  depCd, arvCd, depDt))
            previous_cfpl = cursor.fetchone()
            cursor.close()
            return previous_cfpl
        except Exception:
            self.logger.warning('query error:%s CSN%s%s %s-%s', (fltDt, fltNr, opSuffix, depCd, arvCd, depDt), exc_info=True)
            #self.logger.warning("sql statement : %s " % (query,(fltDt, fltNr, opSuffix, depCd, arvCd, depDt)))
            return -1

    def insert_alarm(self,alarmDict):
        self.logger.warning(json.dumps(alarmDict))
        # alarm_text=alarmDict['route_alarm']+' '+alarmDict['tailNr_alarm']+' '+\
        #            alarmDict['altn_alarm']+' '+alarmDict['fuel_alarm']+' '+alarmDict['others']
        sql=("INSERT INTO " + self.db_name + "."+ self.alarmtable+
             " (fltDt,fltNr,opSuffix,tailNr,depDt,depCd,arvCd,alarmOfpNr,oldOfpNr,alarmType,alarmContent) "
               "VALUES (%(fltDt)s,%(fltNr)s,%(opSuffix)s,%(tailNr)s,%(depDt)s,%(depCd)s,%(arvCd)s,%(alarmOfpNr)s,%(oldOfpNr)s,%(alarmType)s,%(alarmContent)s)")

        try:
            cursor = self.cnx.cursor()
            cursor.execute(sql,alarmDict)
            self.cnx.commit()
            cursor.close()
            #self.cnx.close()
            self.logger.info('!!!!Find alarm!!!!: %s' % alarmDict['fltNr'])
            return 0
        except Exception:
            self.logger.warning('DB INSERT error : %s' % alarmDict['fltNr'], exc_info=True)
            self.logger.warning('d:%s' % json.dumps(alarmDict))
            self.logger.warning("SQL statement : " + sql)
            return 0

    def load_aircraft(self):
        sql="""SELECT `tailNr` from `"""+self.db_name+"`.`" + self.aircrafttable + "`"
        try:
            cursor = self.cnx.cursor(dictionary=True, buffered=True)
            cursor.execute(sql)
            aircraft = cursor.fetchall()
            cursor.close()
            return list(map(lambda x:x['tailNr'],aircraft))

        except Exception:
            self.logger.warning("SQL statement : " + sql)
            return 0