import config
import json
import urllib.request
from datetime import datetime, timedelta
import time
import base64
import DB
import re
from ofptextprocess import ofptextprocess

# Log
config.logger.info('Start')
while True:
    now = datetime.now()
    startTime = now - timedelta(hours=8)
    endTime = now + timedelta(hours=8)
    dateTimefomartter = "%Y%m%d%H%M"
    try:
        response = urllib.request.urlopen(config.basicurl+'?method=getFlightInfo&latestDepDtFrom=%s&latestDepDtTo=%s' % (startTime.__format__(dateTimefomartter), endTime.__format__(dateTimefomartter)), timeout=30)
    except Exception:
        config.logger.debug('network warning')
        time.sleep(30)
        continue
    responseJSON=response.read()
    if responseJSON=={}  or responseJSON==b'':
        config.logger.warning('there is no FlightList!')
        continue
   # print(response.read())
    try:
        flightinfo = json.loads(responseJSON)['FlightInfo']
    except Exception:
        config.logger.debug('something wrong with flightlist!',exc_info=True)
        continue

    for i in flightinfo:
        if i['depStsCd'] == 'AIR' or i['latestTailNr'] == {} or i['alnCd'] != 'CZ':
            config.logger.info('fltDt:%s, alnCd:%s, FltNr:%s,%s, TailNr:%s, DepStaCd:%s'%(i['fltDt'],i['alnCd'],i['fltNr'],i['opSuffix'],i['latestTailNr'],i['depStsCd']))
            continue
        else:
            i['fltDt'] = re.sub(r'[-: ]',"",i['fltDt'])[0:8]
            if i['opSuffix'] == {'0': ' '}:
                i['opSuffix'] = ''
            net =config.basicurl+'?method=getCFPL&fltNr=%s&alnCd=%s&fltDt=%s&opSuffix=%s&depCd=%s&arvCd=%s&tailNr=%s&type=0' % (
                i['fltNr'], i['alnCd'], i['fltDt'], i['opSuffix'], i['latestDepArpCd'], i['latestArvArpCd'],i['latestTailNr'])
            #print(net)
            try:
                response = urllib.request.urlopen(net, timeout=30)
            except Exception:
                config.logger.warning('network for cfpl failed',exc_info=True)
                continue
            try:
                fplJSON = json.loads(response.read())
            except Exception:
                config.logger.info('wrong with the fpl:%s'%i['fltNr'])
                config.logger.debug('wrong with fpl!',exc_info=True)
                continue
            if fplJSON=={}:
                config.logger.info('there is no flightplan:fltNr=%s&alnCd=%s&fltDt=%s&opSuffix=%s&depCd=%s&arvCd=%s&tailNr=%s&type=0' % (
                i['fltNr'], i['alnCd'], i['fltDt'], i['opSuffix'], i['latestDepArpCd'], i['latestArvArpCd'],i['latestTailNr']))
                continue
            if DB.queryoData(fplJSON['ofpNr']) == 0:
                cfplDecode = base64.b64decode(fplJSON['ofpText']).decode('utf-8')
                config.saveOpf2File(fplJSON['ofpNr'],cfplDecode,fplJSON['fltDt'])
                config.logger.info('Saved the %s!'%fplJSON['ofpNr'])
                try:
                    detail = ofptextprocess(cfplDecode)
                except Exception:
                    config.logger.debug('ofpprocess goes wrong!fltNr=%s,depCd=%s,arvCd=%s' % (
                        i['fltNr'], i['latestDepArpCd'], i['latestArvArpCd']), exc_info=True)
                    continue
                DB.insertData(fplJSON, detail, config.logger)
            else:
                config.logger.info("cfpl:%s existed."%fplJSON['ofpNr'])
                continue
    config.logger.info('Wait next round!')
    time.sleep(300)









