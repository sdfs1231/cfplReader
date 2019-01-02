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
config.logger.info('CFPL Reader Program Start')
while True:
    FltlistCount = 0
    TofltCount = 0
    InsertCount = 0
    QueryCount = 0
    now = datetime.now()
    startTime = now - timedelta(hours=config.timeDeltaBefore)
    endTime = now + timedelta(hours=config.timeDeltaAfter)
    dateTimefomartter = "%Y%m%d%H%M"
    config.logger.info('get FlightList processing')
    for Fltlistretry in range(3):
        try:
            response = urllib.request.urlopen(
                config.basicurl + '?method=getFlightInfo&latestDepDtFrom=%s&latestDepDtTo=%s' % (
                startTime.__format__(dateTimefomartter), endTime.__format__(dateTimefomartter)), timeout=30)
        except Exception:
            config.logger.warning('get FlightList network warning,retry NO%s'%str(Fltlistretry+1))
            time.sleep(10)
            continue
        break
    responseJSON=response.read()
    if responseJSON=={}  or responseJSON==b'':
        config.logger.warning('get FlightList retrun null!')
        continue
   # print(response.read())
    try:
        flightinfo = json.loads(responseJSON)['FlightInfo']
    except Exception:
        config.logger.debug('flightlist JSON format error! ',exc_info=True)
        continue
    FltlistCount = len(flightinfo)
    for index,i in enumerate(flightinfo, start=0):
        config.logger.info('processing : %s/%s'%(str(index+1),FltlistCount))
        if i['depStsCd'] == 'AIR' or i['latestTailNr'] == {} or i['alnCd'] != 'CZ':
            if i['depStsCd']=='AIR':
                TofltCount+=1
            config.logger.info('fltDt:%s, alnCd:%s, FltNr:%s,%s, TailNr:%s, DepStaCd:%s'%(i['fltDt'],i['alnCd'],i['fltNr'],i['opSuffix'],i['latestTailNr'],i['depStsCd']))
            continue
        else:
            i['fltDt'] = re.sub(r'[-: ]',"",i['fltDt'])[0:8]
            if i['opSuffix'] == {'0': ' '}:
                i['opSuffix'] = ''
            net =config.basicurl+'?method=getCFPL&fltNr=%s&alnCd=%s&fltDt=%s&opSuffix=%s&depCd=%s&arvCd=%s&tailNr=%s&type=0' % (
                i['fltNr'], i['alnCd'], i['fltDt'], i['opSuffix'], i['latestDepArpCd'], i['latestArvArpCd'],i['latestTailNr'])
            #print(net)
            for netconnect in range(3):
                try:
                    response = urllib.request.urlopen(net, timeout=30)
                except Exception:
                    config.logger.warning('get CFPL network error,retry NO%s'%str(netconnect+1), exc_info=True)
                    time.sleep(10)
                    continue
                break
            try:
                fplJSON = json.loads(response.read())
            except Exception:
                config.logger.debug('CFPL JSON format error:%s'%i['fltNr'])
                #config.logger.debug('wrong with fpl!',exc_info=True)
                continue
            if fplJSON=={}:
                config.logger.info('NO CFPL:fltNr=%s&alnCd=%s&fltDt=%s&opSuffix=%s&depCd=%s&arvCd=%s&tailNr=%s' % (
                i['fltNr'], i['alnCd'], i['fltDt'], i['opSuffix'], i['latestDepArpCd'], i['latestArvArpCd'],i['latestTailNr']))
                continue
            QueryCount+=1
            if DB.queryoData(fplJSON['ofpNr'],config.logger) == 0:
                cfplDecode = base64.b64decode(fplJSON['ofpText']).decode('utf-8')
                config.saveOpf2File(fplJSON['ofpNr'],cfplDecode,fplJSON['fltDt'])
                config.logger.info('Saved the %s!'%fplJSON['ofpNr'])

                try:
                    detail = ofptextprocess(cfplDecode,config.logger)
                except Exception:
                    config.logger.debug('ofpprocess error!ofpNr=%s,fltNr=%s,depCd=%s,arvCd=%s' % (
                        i['opfNr'],i['fltNr'], i['latestDepArpCd'], i['latestArvArpCd']), exc_info=True)
                    continue
                DB.insertData({**fplJSON, **detail}, config.logger)
                InsertCount+=1
            else:
                config.logger.info("CFPL existed : %s."%fplJSON['ofpNr'])
                continue
    config.logger.info("total flight numbers :%s" % FltlistCount)
    config.logger.info("Take-off flight numbers :%s" % TofltCount)
    config.logger.info("Query CFPL numbers :%s" % QueryCount)
    config.logger.info("insert CFPL numbers :%s" % InsertCount)
    config.logger.info('Wait next round!')
    time.sleep(config.interval)









