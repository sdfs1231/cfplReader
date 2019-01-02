import asyncio
import config
import json
import urllib.request
import aiohttp
from datetime import datetime, timedelta
import time
import base64
import DB
import re
from ofptextprocess import ofptextprocess


print(json.loads("{}")['flightInfo'])


InsertCount = 0
QueryCount= 0

async def getCFPL(fltNr, alnCd, fltDt, opSuffix, latestDepArpCd, latestArvArpCd, latestTailNr):
    params = {'method': 'getCFPL', 'fltNr': fltNr, 'alnCd': alnCd, 'fltDt': fltDt, 'opSuffix': opSuffix,
              'latestDepArpCd': latestDepArpCd,'latestArvArpCd': latestArvArpCd, 'latestTailNr': latestTailNr}
    conn = aiohttp.TCPConnector(limit=100)
    session_timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=session_timeout, connector=conn) as session:
        async with session.get(config.basicurl, params=params) as CFPLResp:
            ofp = await CFPLResp.json()
            config.logger.info('get ofp end')
            if ofp == {}:
                config.logger.info('NO CFPL:fltNr=%s&alnCd=%s&fltDt=%s&opSuffix=%s&depCd=%s&arvCd=%s&tailNr=%s' % (
                    i['fltNr'], i['alnCd'], i['fltDt'], i['opSuffix'], i['latestDepArpCd'], i['latestArvArpCd'],
                    i['latestTailNr']))
                return -1
            global QueryCount
            QueryCount += 1
            if DB.queryoData(ofp['ofpNr'], config.logger) == 0:
                cfplDecode = base64.b64decode(ofp['ofpText']).decode('utf-8')
                config.saveOpf2File(ofp['ofpNr'], cfplDecode, ofp['fltDt'])
                config.logger.info('Saved the %s!' % ofp['ofpNr'])

                try:
                    detail = ofptextprocess(cfplDecode, config.logger)
                except Exception:
                    config.logger.debug('ofpprocess error!ofpNr=%s,fltNr=%s,depCd=%s,arvCd=%s' % (
                        i['opfNr'], i['fltNr'], i['latestDepArpCd'], i['latestArvArpCd']), exc_info=True)
                    return -1
                DB.insertData({**ofp, **detail}, config.logger)
                global InsertCount
                InsertCount += 1
            else:
                config.logger.info("CFPL existed : %s." % ofp['ofpNr'])
                return -1
            await session.close()



async def getFlightList():
    now = datetime.now()
    startTime = now - timedelta(hours=config.timeDeltaBefore)
    endTime = now + timedelta(hours=config.timeDeltaAfter)
    dateTimefomartter = "%Y%m%d%H%M"
    params = {'method': 'getFlightInfo', 'latestDepDtFrom': startTime.__format__(dateTimefomartter),
              'latestDepDtTo': endTime.__format__(dateTimefomartter)}
    config.logger.info('get FlightList start')
    async with aiohttp.ClientSession(timeout=session_timeout, connector=conn) as session :
        async with session.get(config.basicurl, params=params) as flightListResp:
            flightlist = await flightListResp.json()
            config.logger.info('get FlightList end')
            flightlist = flightlist['FlightInfo']
            if (flightlist == {}):
                config.logger.info("FlightListEmpty")
                return -1
            await session.close()
            return flightlist


loop = asyncio.get_event_loop()
flightinfo = loop.run_until_complete(getFlightList())
tasks = []
airborneCount = 0
flightinfoCount = len(flightinfo)
for index, i in enumerate(flightinfo, start=0):
    config.logger.info('processing : %s/%s' % (str(index + 1), flightinfoCount))
    if i['depStsCd'] == 'AIR' or i['latestTailNr'] == {} or i['alnCd'] != 'CZ':
        if i['depStsCd'] == 'AIR':
            airborneCount += 1
        config.logger.info('fltDt:%s, alnCd:%s, FltNr:%s,%s, TailNr:%s, DepStaCd:%s' % (
        i['fltDt'], i['alnCd'], i['fltNr'], i['opSuffix'], i['latestTailNr'], i['depStsCd']))
        continue
    else:
        i['fltDt'] = re.sub(r'[-: ]', "", i['fltDt'])[0:8]
        if i['opSuffix'] == {'0': ' '}:
            i['opSuffix'] = ''
        tasks.append(getCFPL(i['fltNr'], i['alnCd'], i['fltDt'], i['opSuffix'], i['latestDepArpCd'], i['latestArvArpCd'],
            i['latestTailNr']))

config.logger.info("total flight numbers :%s" % FltlistCount)
config.logger.info("Take-off flight numbers :%s" % TofltCount)
config.logger.info("Query CFPL numbers :%s" % QueryCount)
config.logger.info("insert CFPL numbers :%s" % InsertCount)
config.logger.info('Wait next round!')
time.sleep(config.interval)

#loop.close()

#loop.run_until_complete(processFlightList())
