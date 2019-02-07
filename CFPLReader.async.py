#! python3
# -*- coding: utf-8 -*-

import os
import asyncio
import json
from json import JSONDecodeError
import aiohttp
from datetime import datetime, timedelta, timezone
import time
import base64
import logging
import configparser
from logging.handlers import TimedRotatingFileHandler

# user defined package
from DB import Database
from ofptextprocess import ofptextprocess


async def session_initial():
    logger.info('AIOHTTP session Initial start')
    conn = aiohttp.TCPConnector(limit=max_connection, force_close=True)
    session_timeout = aiohttp.ClientTimeout(total=timeout)
    session = aiohttp.ClientSession(timeout=session_timeout, connector=conn)
    logger.info('AIOHTTP session Initial done')
    return session


async def session_close():
    logger.info('AIOHTTP session close start')
    await session.close()
    logger.info('AIOHTTP session close done')


async def getflightlist(session, baseurl):
    for i in range(retry_max):
        try:
            now = datetime.now()
            starttime = now - timedelta(hours=timeDeltaBefore)
            endtime = now + timedelta(hours=timeDeltaAfter)
            datetimefomartter = "%Y%m%d%H%M"
            params = {'method': 'getFlightInfo', 'latestDepDtFrom': starttime.__format__(datetimefomartter),
                      'latestDepDtTo': endtime.__format__(datetimefomartter)}
            logger.info('get FlightList start: ' + json.dumps(params))
            async with session.get(baseurl, params=params) as resp:
                logger.info('get FlightList done: ' + json.dumps(params))
                flightlistJSON = await resp.json()
                if flightlistJSON == {} or type(flightlistJSON) is not dict:
                    logger.warning('get FlightList retrun null!')
                    time.sleep(retry_waiting)
                    continue
                else:
                    if flightlistJSON.get('FlightInfo') is None:
                        logger.warning('get FlightList return no flightInfo!')
                        time.sleep(retry_waiting)
                        continue
                    else:
                        return await resp.json()
        except asyncio.TimeoutError as TimeException:
            logger.info('get FlightList timeout,retry NO%d' % (i + 1))
            time.sleep(retry_waiting * (i + 1))
        except JSONDecodeError as jsonError:
            logger.warning(jsonError.doc)
            logger.warning('get FlightList JSONDecodeError,retry NO%d' % (i + 1))
            time.sleep(retry_waiting * (i + 1))
        except Exception as e:
            logger.warning(str(e))
            logger.warning('get FlightList exception,retry NO%d' % (i + 1), exc_info=True)
            time.sleep(retry_waiting * (i + 1))
    logger.error('after retry %s times , still can not get flightlist info, program wait 300s!'
                 % retry_max)
    for timer_count in range(interval + 1):
        print('\r',
              (repeat_to_length('-=', 60) + 'wait for next round : %ds' + repeat_to_length('-=', 60)) % (
                      interval - timer_count), sep='', end='', flush=True)
        time.sleep(1)
    #await session_close()
    #exit()


async def getCFPL(session, db, url, fltNr, alnCd, fltDt, opSuffix, depCd, arvCd, tailNr):
    logger.info('get CFPL start:fltNr=%s&alnCd=%s&fltDt=%s&opSuffix=%s&depCd=%s&arvCd=%s&tailNr=%s' % (
        fltNr, alnCd, fltDt, opSuffix, depCd, arvCd, tailNr))
    params = {'method': 'getCFPL', 'fltNr': fltNr, 'alnCd': alnCd, 'fltDt': fltDt, 'opSuffix': opSuffix,
              'depCd': depCd, 'arvCd': arvCd, 'tailNr': tailNr, 'type': "0"}
    for cfplretry in range(retry_max):
        try:
            async with session.get(url, params=params) as CFPLResp:
                ofp = await CFPLResp.json()
                # logger.info('get CFPL end:fltNr=%s&alnCd=%s&fltDt=%s&opSuffix=%s&depCd=%s&arvCd=%s&tailNr=%s' % (
                #     fltNr, alnCd, fltDt, opSuffix, depCd, arvCd, tailNr))
                return processofp(db, ofp, params)
        except asyncio.TimeoutError as TimeException:
            logger.info('get cfpl timeout,retry NO%d :'
                        'fltNr=%s&alnCd=%s&fltDt=%s&opSuffix=%s&depCd=%s&arvCd=%s&tailNr=%s'
                        % ((cfplretry + 1), fltNr, alnCd, fltDt, opSuffix, depCd, arvCd, tailNr))
            time.sleep(retry_waiting)
        except JSONDecodeError as jsonError:
            logger.warning('get cfpl jsonDecodeError,retry NO%d' % (cfplretry + 1))
            logger.warning(jsonError.doc)
            logger.warning('parameters:fltNr=%s&alnCd=%s&fltDt=%s&opSuffix=%s&depCd=%s&arvCd=%s&tailNr=%s'
                           % (fltNr, alnCd, fltDt, opSuffix, depCd, arvCd, tailNr))
            time.sleep(retry_waiting)
        except Exception as e:
            logger.warning('get cfpl exception,retry NO%d' % (cfplretry + 1))
            logger.warning(str(e))
            logger.warning('parameters:fltNr=%s&alnCd=%s&fltDt=%s&opSuffix=%s&depCd=%s&arvCd=%s&tailNr=%s'
                           % (fltNr, alnCd, fltDt, opSuffix, depCd, arvCd, tailNr), exc_info=True)
            time.sleep(retry_waiting)
    logger.error(
        'after retry %s times , still can not get CFPL info, ignore this cfpl!' % retry_max)
    return {}


def processofp(db, ofp, params):
    global cfplexistCount, nocfplCount, insertCount, queryofpCount
    if ofp == {} or type(ofp) is not dict:
        logger.info(
            'get CFPL done, NO CFPL:fltNr=%s&alnCd=%s&fltDt=%s&opSuffix=%s&depCd=%s&arvCd=%s&tailNr=%s' % (
                params['fltNr'], params['alnCd'], params['fltDt'], params['opSuffix'], params['depCd'], params['arvCd'],
                params['tailNr']))
        nocfplCount += 1
    else:
        if not 'ofpNr' in ofp:
            logger.warning('ofp has no key ofpNr:%s' % json.dumps(ofp))
            nocfplCount += 1
            return -1
        if db.check_ofpNr(ofp['ofpNr']) == 0:
            cfplDecode = base64.b64decode(ofp['ofpText']).decode('utf-8')
            saveOpf2File(ofp['ofpNr'], cfplDecode, ofp['fltDt'])
            logger.info('Saved the %s' % ofp['ofpNr'])
            try:
                detail = ofptextprocess(cfplDecode, logger)
            except Exception:
                logger.warning('ofpprocess error!ofpNr=%s,fltNr=%s,depCd=%s,arvCd=%s' % (
                    ofp['opfNr'], ofp['fltNr'], ofp['depCd'], ofp['arvCd']), exc_info=True)
                return -1
            db.insertData({**ofp, **detail})
            insertCount += 1
        else:
            cfplexistCount += 1
            logger.info("CFPL existed : %s" % ofp['ofpNr'])
    processedCount = nocfplCount + cfplexistCount + insertCount
    logger.info("processing %.2f%%, done %d/%d"
                % (processedCount / queryofpCount * 100, processedCount, queryofpCount))


def saveOpf2File(CFPLname, data, date):
    CFPLpath = save_path + date + '/'
    os.makedirs(os.path.dirname(CFPLpath + CFPLname + ".txt"), exist_ok=True)
    with open(CFPLpath + CFPLname + ".txt", 'a') as f:
        f.write(data)


def repeat_to_length(string_to_expand, length):
    return (string_to_expand * (int(length / len(string_to_expand)) + 1))[:length]


def logging_initial():
    # formatter
    formatter = logging.Formatter(logformat)

    # full log split by day
    rotatehandler = TimedRotatingFileHandler(loggerpath + '.log', when="midnight", interval=1,
                                             backupCount=10)
    rotatehandler.suffix = loggersuffix
    rotatehandler.setFormatter(formatter)
    rotatehandler.setLevel(logging.DEBUG)

    # warning+ log
    debugHandler = logging.FileHandler(loggerpath + '_debug.log')
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
    logger.addHandler(rotatehandler)
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
logger.warning('CFPL Reader Program Start')
logger.info('config read finfished')
loop = asyncio.get_event_loop()

while True:
    session = loop.run_until_complete(session_initial())
    starttime = datetime.now()
    flightlist = loop.run_until_complete(getflightlist(session, baseURL))
    flightinfo = flightlist['FlightInfo']
    flightlistCount = len(flightinfo)
    airborneCount = 0
    queryofpCount = 0
    nocfplCount = 0
    cfplexistCount = 0
    insertCount = 0
    cfpltasks = []
    db = Database(dbhost,
                  dbport,
                  dbuser,
                  dbpassword,
                  dbname,
                  tablename,
                  logger)
    for index, i in enumerate(flightinfo, start=0):
        # logger.info('processing : %s/%s' % (str(index + 1), flightlistCount))
        if i['depStsCd'] == 'AIR' or i['latestTailNr'] == {} or i['alnCd'] != 'CZ':
            if i['depStsCd'] == 'AIR':
                airborneCount += 1
            logger.info('fltDt:%s, alnCd:%s, FltNr:%s,%s, TailNr:%s, DepStaCd:%s' % (
                i['fltDt'], i['alnCd'], i['fltNr'], i['opSuffix'], i['latestTailNr'], i['depStsCd']))
            continue
        else:
            fltDt = datetime.strptime(i['schDepDt'] + " +0800", "%Y-%m-%d %H:%M %z").astimezone(timezone.utc)
            fltDt = fltDt.__format__('%Y%m%d')

            if i['opSuffix'] == {'0': ' '}:
                i['opSuffix'] = ''
            queryofpCount += 1
            cfpltasks.append(
                getCFPL(session, db, baseURL, i['fltNr'], i['alnCd'], fltDt,
                        i['opSuffix'], i['latestDepArpCd'], i['latestArvArpCd'], i['latestTailNr']))
    loop.run_until_complete(asyncio.gather(*cfpltasks))
    loop.run_until_complete(session_close())
    del db
    os.system("cls")
    logger.info('SUMMARY for previous round , used time : %ds' % ((datetime.now() - starttime).total_seconds()))
    logger.info("total flight info count :%d" % flightlistCount)
    logger.info("take-off flight count :%d" % airborneCount)
    logger.info("query CFPL count :%d" % queryofpCount)
    logger.info("no CFPL count :%d" % nocfplCount)
    logger.info("CFPL existed count :%d" % cfplexistCount)
    logger.info("insert CFPL count :%d" % insertCount)
    for timer_count in range(interval + 1):
        print('\r',
              (repeat_to_length('-=', 60) + 'wait for next round : %ds' + repeat_to_length('-=', 60)) % (
                      interval - timer_count), sep='', end='', flush=True)
        time.sleep(1)
    os.system("cls")
# loop.close()
