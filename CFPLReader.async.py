#! python3
# -*- coding: utf-8 -*-

import os
import asyncio
import json
import aiohttp
from datetime import datetime, timedelta, timezone
import time
import base64
# user defined package
import config
import DB
from ofptextprocess import ofptextprocess


async def session_initial():
    config.logger.info('AIOHTTP session Initial start')
    conn = aiohttp.TCPConnector(limit=config.aio_max_connection, force_close=True)
    session_timeout = aiohttp.ClientTimeout(total=config.aio_timeout)
    session = aiohttp.ClientSession(timeout=session_timeout, connector=conn)
    config.logger.info('AIOHTTP session Initial done')
    return session


async def session_close():
    config.logger.info('AIOHTTP session close start')
    await session.close()
    config.logger.info('AIOHTTP session close done')


async def getflightlist(session, baseurl):
    for i in range(config.networkretry_max):
        try:
            now = datetime.now()
            starttime = now - timedelta(hours=config.timeDeltaBefore)
            endtime = now + timedelta(hours=config.timeDeltaAfter)
            datetimefomartter = "%Y%m%d%H%M"
            params = {'method': 'getFlightInfo', 'latestDepDtFrom': starttime.__format__(datetimefomartter),
                      'latestDepDtTo': endtime.__format__(datetimefomartter)}
            config.logger.info('get FlightList start: ' + json.dumps(params))
            async with session.get(baseurl, params=params) as resp:
                config.logger.info('get FlightList done: ' + json.dumps(params))
                flightlistJSON = await resp.json()
                if flightlistJSON == {} or flightlistJSON == b'':
                    config.logger.warning('get FlightList retrun null!')
                    time.sleep(config.networkretry_pedding)
                    continue
                else:
                    if flightlistJSON['FlightInfo'] == {} or flightlistJSON['FlightInfo'] == b'':
                        config.logger.warning('get FlightList return no flightInfo!')
                        time.sleep(config.networkretry_pedding)
                        continue
                    else:
                        return await resp.json()
        except Exception:
            config.logger.warning('get FlightList warning,retry NO%s' % str(i + 1), exc_info=True)
            time.sleep(config.networkretry_pedding * (i + 1))
    config.logger.error('after retry %s times , still can not get flightlist info, program exit!'
                        % config.networkretry_max)
    await session_close()
    exit()


async def getCFPL(session, url, fltNr, alnCd, fltDt, opSuffix, depCd, arvCd, tailNr):
    config.logger.info('get CFPL start:fltNr=%s&alnCd=%s&fltDt=%s&opSuffix=%s&depCd=%s&arvCd=%s&tailNr=%s' % (
        fltNr, alnCd, fltDt, opSuffix, depCd, arvCd, tailNr))
    params = {'method': 'getCFPL', 'fltNr': fltNr, 'alnCd': alnCd, 'fltDt': fltDt, 'opSuffix': opSuffix,
              'depCd': depCd, 'arvCd': arvCd, 'tailNr': tailNr, 'type': "0"}
    for cfplretry in range(config.networkretry_max):
        try:
            async with session.get(url, params=params) as CFPLResp:
                ofp = await CFPLResp.json()
                config.logger.info('get CFPL end:fltNr=%s&alnCd=%s&fltDt=%s&opSuffix=%s&depCd=%s&arvCd=%s&tailNr=%s' % (
                    fltNr, alnCd, fltDt, opSuffix, depCd, arvCd, tailNr))
                return processofp(ofp, params)
        except Exception:
            config.logger.warning('get cfpl warning,retry NO%d' % (cfplretry + 1))
            config.logger.warning('parameters:fltNr=%s&alnCd=%s&fltDt=%s&opSuffix=%s&depCd=%s&arvCd=%s&tailNr=%s'
                                  % (fltNr, alnCd, fltDt, opSuffix, depCd, arvCd, tailNr), exc_info=True)
            time.sleep(config.networkretry_pedding)
    config.logger.error(
        'after retry %s times , still can not get CFPL info, ignore this cfpl!' % config.networkretry_max)
    return {}


def processofp(ofp, params):
    global cfplexistCount, nocfplCount, insertCount, queryofpCount
    if ofp == {} or ofp is False:
        config.logger.info(
            'get CFPL done, NO CFPL:fltNr=%s&alnCd=%s&fltDt=%s&opSuffix=%s&depCd=%s&arvCd=%s&tailNr=%s' % (
                params['fltNr'], params['alnCd'], params['fltDt'], params['opSuffix'], params['depCd'], params['arvCd'],
                params['tailNr']))
        nocfplCount += 1
    else:
        if DB.queryoData(ofp['ofpNr'], config.logger) == 0:
            cfplDecode = base64.b64decode(ofp['ofpText']).decode('utf-8')
            config.saveOpf2File(ofp['ofpNr'], cfplDecode, ofp['fltDt'])
            config.logger.info('Saved the %s' % ofp['ofpNr'])
            try:
                detail = ofptextprocess(cfplDecode, config.logger)
            except Exception:
                config.logger.warning('ofpprocess error!ofpNr=%s,fltNr=%s,depCd=%s,arvCd=%s' % (
                    ofp['opfNr'], ofp['fltNr'], ofp['depCd'], ofp['arvCd']), exc_info=True)
                return -1
            DB.insertData({**ofp, **detail}, config.logger)
            insertCount += 1
        else:
            cfplexistCount += 1
            config.logger.info("CFPL existed : %s" % ofp['ofpNr'])
    processedCount = nocfplCount + cfplexistCount + insertCount
    config.logger.info("processing %.2f%%, done %d/%d"
                       % (processedCount / queryofpCount * 100, processedCount, queryofpCount))


config.logger.info('CFPL Reader Program Start')
loop = asyncio.get_event_loop()

while True:
    session = loop.run_until_complete(session_initial())
    starttime = datetime.now()
    flightlist = loop.run_until_complete(getflightlist(session, config.basicurl))
    flightinfo = flightlist['FlightInfo']
    flightlistCount = len(flightinfo)
    airborneCount = 0
    queryofpCount = 0
    nocfplCount = 0
    cfplexistCount = 0
    insertCount = 0
    cfpltasks = []
    for index, i in enumerate(flightinfo, start=0):
        # config.logger.info('processing : %s/%s' % (str(index + 1), flightlistCount))
        if i['depStsCd'] == 'AIR' or i['latestTailNr'] == {} or i['alnCd'] != 'CZ':
            if i['depStsCd'] == 'AIR':
                airborneCount += 1
            config.logger.info('fltDt:%s, alnCd:%s, FltNr:%s,%s, TailNr:%s, DepStaCd:%s' % (
                i['fltDt'], i['alnCd'], i['fltNr'], i['opSuffix'], i['latestTailNr'], i['depStsCd']))
            continue
        else:
            fltDt = datetime.strptime(i['fltDt'] + " +0800", "%Y-%m-%d %H:%M %z").astimezone(timezone.utc)
            fltDt = fltDt.__format__('%Y%m%d')

            if i['opSuffix'] == {'0': ' '}:
                i['opSuffix'] = ''
            queryofpCount += 1
            cfpltasks.append(getCFPL(session, config.basicurl, i['fltNr'], i['alnCd'], fltDt, i['opSuffix'],
                                     i['latestDepArpCd'], i['latestArvArpCd'], i['latestTailNr']))
    loop.run_until_complete(asyncio.gather(*cfpltasks))
    loop.run_until_complete(session_close())
    #config.logger.info('wait for next time : %ds' % config.interval)
    os.system("cls")
    config.logger.info('SUMMARY : time used in this round: %ds' % ((datetime.now() - starttime).total_seconds()))
    config.logger.info("SUMMARY : total flight info count :%d" % flightlistCount)
    config.logger.info("SUMMARY : take-off flight count :%d" % airborneCount)
    config.logger.info("SUMMARY : query CFPL count :%d" % queryofpCount)
    config.logger.info("SUMMARY : no CFPL count :%d" % nocfplCount)
    config.logger.info("SUMMARY : CFPL existed count :%d" % cfplexistCount)
    config.logger.info("SUMMARY : insert CFPL count :%d" % insertCount)
    #timer_count = 2
    for timer_count in range(config.interval+1):
        print('\r',
              (config.repeat_to_length('-=', 60) + 'wait for next time : %ds' + config.repeat_to_length('-=', 60)) % (
                      config.interval - timer_count), sep='', end='')
        time.sleep(1)
    os.system("cls")
# loop.close()

