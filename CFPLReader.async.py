#! python3
# -*- coding: utf-8 -*-

import os
import asyncio
import json
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
    conn = aiohttp.TCPConnector(limit=int(config['AIOHTTP']['max_connection']), force_close=True)
    session_timeout = aiohttp.ClientTimeout(total=int(config['AIOHTTP']['timeout']))
    session = aiohttp.ClientSession(timeout=session_timeout, connector=conn)
    logger.info('AIOHTTP session Initial done')
    return session


async def session_close():
    logger.info('AIOHTTP session close start')
    await session.close()
    logger.info('AIOHTTP session close done')


async def getflightlist(session, baseurl):
    for i in range(int(config['NETWORK']['retry_max'])):
        try:
            now = datetime.now()
            starttime = now - timedelta(hours=int(config['FLIGHTLIST']['timeDeltaBefore']))
            endtime = now + timedelta(hours=int(config['FLIGHTLIST']['timeDeltaAfter']))
            datetimefomartter = "%Y%m%d%H%M"
            params = {'method': 'getFlightInfo', 'latestDepDtFrom': starttime.__format__(datetimefomartter),
                      'latestDepDtTo': endtime.__format__(datetimefomartter)}
            logger.info('get FlightList start: ' + json.dumps(params))
            async with session.get(baseurl, params=params) as resp:
                logger.info('get FlightList done: ' + json.dumps(params))
                flightlistJSON = await resp.json()
                if flightlistJSON == {} or flightlistJSON == b'':
                    logger.warning('get FlightList retrun null!')
                    time.sleep(config['NETWORK']['retry_waitting'])
                    continue
                else:
                    if flightlistJSON['FlightInfo'] == {} or flightlistJSON['FlightInfo'] == b'':
                        logger.warning('get FlightList return no flightInfo!')
                        time.sleep(config['NETWORK']['retry_waitting'])
                        continue
                    else:
                        return await resp.json()
        except Exception:
            logger.warning('get FlightList warning,retry NO%s' % str(i + 1), exc_info=True)
            time.sleep(config['NETWORK']['retry_waitting'] * (i + 1))
    logger.error('after retry %s times , still can not get flightlist info, program exit!'
                 % config['NETWORK']['retry_max'])
    await session_close()
    exit()


async def getCFPL(session, db, url, fltNr, alnCd, fltDt, opSuffix, depCd, arvCd, tailNr):
    logger.info('get CFPL start:fltNr=%s&alnCd=%s&fltDt=%s&opSuffix=%s&depCd=%s&arvCd=%s&tailNr=%s' % (
        fltNr, alnCd, fltDt, opSuffix, depCd, arvCd, tailNr))
    params = {'method': 'getCFPL', 'fltNr': fltNr, 'alnCd': alnCd, 'fltDt': fltDt, 'opSuffix': opSuffix,
              'depCd': depCd, 'arvCd': arvCd, 'tailNr': tailNr, 'type': "0"}
    for cfplretry in range(int(config['NETWORK']['retry_max'])):
        try:
            async with session.get(url, params=params) as CFPLResp:
                ofp = await CFPLResp.json()
                logger.info('get CFPL end:fltNr=%s&alnCd=%s&fltDt=%s&opSuffix=%s&depCd=%s&arvCd=%s&tailNr=%s' % (
                    fltNr, alnCd, fltDt, opSuffix, depCd, arvCd, tailNr))
                return processofp(db, ofp, params)
        except Exception:
            logger.warning('get cfpl warning,retry NO%d' % (cfplretry + 1))
            logger.warning('parameters:fltNr=%s&alnCd=%s&fltDt=%s&opSuffix=%s&depCd=%s&arvCd=%s&tailNr=%s'
                           % (fltNr, alnCd, fltDt, opSuffix, depCd, arvCd, tailNr), exc_info=True)
            time.sleep(config['NETWORK']['retry_waitting'])
    logger.error(
        'after retry %s times , still can not get CFPL info, ignore this cfpl!' % config['NETWORK']['retry_max'])
    return {}


def processofp(db, ofp, params):
    global cfplexistCount, nocfplCount, insertCount, queryofpCount
    if ofp == {} or ofp is False:
        logger.info(
            'get CFPL done, NO CFPL:fltNr=%s&alnCd=%s&fltDt=%s&opSuffix=%s&depCd=%s&arvCd=%s&tailNr=%s' % (
                params['fltNr'], params['alnCd'], params['fltDt'], params['opSuffix'], params['depCd'], params['arvCd'],
                params['tailNr']))
        nocfplCount += 1
    else:
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
    CFPLpath = config['CFPL']['save_path'] + date + '/'
    os.makedirs(os.path.dirname(CFPLpath + CFPLname + ".txt"), exist_ok=True)
    with open(CFPLpath + CFPLname + ".txt", 'a') as f:
        f.write(data)


def repeat_to_length(string_to_expand, length):
    return (string_to_expand * (int(length / len(string_to_expand)) + 1))[:length]


def logging_initial():
    # formatter
    formatter = logging.Formatter(config['LOGGING']['logformat'])

    # full log split by day
    rotatehandler = TimedRotatingFileHandler(config['LOGGING']['loggerpath'] + '.log', when="midnight", interval=1)
    rotatehandler.suffix = config['LOGGING']['loggersuffix']
    rotatehandler.setFormatter(formatter)
    rotatehandler.setLevel(logging.DEBUG)

    # warning+ log
    debugHandler = logging.FileHandler(config['LOGGING']['loggerpath'] + '_debug.log')
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
logger = logging_initial()
logger.info('CFPL Reader Program Start')
logger.info('config read finfished')
loop = asyncio.get_event_loop()

while True:
    session = loop.run_until_complete(session_initial())
    starttime = datetime.now()
    flightlist = loop.run_until_complete(getflightlist(session, config['API']['baseURL']))
    flightinfo = flightlist['FlightInfo']
    flightlistCount = len(flightinfo)
    airborneCount = 0
    queryofpCount = 0
    nocfplCount = 0
    cfplexistCount = 0
    insertCount = 0
    cfpltasks = []
    db = Database(config['MYSQL']['host'],
                  config['MYSQL']['port'],
                  config['MYSQL']['user'],
                  config['MYSQL']['pass'],
                  config['MYSQL']['dbname'],
                  config['MYSQL']['tablename'],
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
            fltDt = datetime.strptime(i['fltDt'] + " +0800", "%Y-%m-%d %H:%M %z").astimezone(timezone.utc)
            fltDt = fltDt.__format__('%Y%m%d')

            if i['opSuffix'] == {'0': ' '}:
                i['opSuffix'] = ''
            queryofpCount += 1
            cfpltasks.append(
                getCFPL(session, db, config['API']['baseURL'], i['fltNr'], i['alnCd'], fltDt,
                        i['opSuffix'], i['latestDepArpCd'], i['latestArvArpCd'], i['latestTailNr']))
    loop.run_until_complete(asyncio.gather(*cfpltasks))
    loop.run_until_complete(session_close())
    # logger.info('wait for next time : %ds' % config.interval)
    del db
    os.system("cls")
    logger.info('SUMMARY : time used in this round: %ds' % ((datetime.now() - starttime).total_seconds()))
    logger.info("SUMMARY : total flight info count :%d" % flightlistCount)
    logger.info("SUMMARY : take-off flight count :%d" % airborneCount)
    logger.info("SUMMARY : query CFPL count :%d" % queryofpCount)
    logger.info("SUMMARY : no CFPL count :%d" % nocfplCount)
    logger.info("SUMMARY : CFPL existed count :%d" % cfplexistCount)
    logger.info("SUMMARY : insert CFPL count :%d" % insertCount)
    interval = int(config['INTERVAL']['interval'])
    # timer_count = 0
    for timer_count in range(interval + 1):
        print('\r',
              (repeat_to_length('-=', 60) + 'wait for next time : %ds' + repeat_to_length('-=', 60)) % (
                      interval - timer_count), sep='', end='',flush=True)
        time.sleep(1)
    os.system("cls")
# loop.close()
