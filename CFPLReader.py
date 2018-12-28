import config
import json
import urllib.request
import time
import base64
import DB
from timer import timer
from ofptextprocess import ofptextprocess

def base64ToString(b):
    return base64.b64decode(b).decode('utf-8')

# Log
config.logger.info('Start')
while True:
    timestramp = timer()
    today=timer()[2]
    #print(timestramp[0],timestramp[1])
    try:
        response = urllib.request.urlopen(config.basicurl+'?method=getFlightInfo&latestDepDtFrom=%s&latestDepDtTo=%s' % (timestramp[0], timestramp[1]), timeout=30)
    except Exception:
        config.logger.debug('network warning')
        time.sleep(10)
        continue
    temporesponse=response.read()
    if temporesponse=={}  or temporesponse==b'':
        config.logger.warning('there is no FlightList!')
        continue
   # print(response.read())
    try:
        tempofl = temporesponse
        flightlist = json.loads(tempofl)
        flightinfo = flightlist['FlightInfo']
    except Exception:
        config.logger.debug('something wrong with flightlist!',exc_info=True)
        continue

    for i in flightinfo:
        if i['depStsCd'] == 'AIR' or i['latestTailNr'] == {} or i['alnCd'] != 'CZ':
            continue
        else:
            date = ''
            dates = i['fltDt'].split(' ')
            datestramp = dates[0].split('-')
            for j in datestramp:
                date = date + j
            i['fltDt'] = date
            if i['opSuffix'] == {'0': ' '}:
                i['opSuffix'] = ''
            net =config.basicurl+'?method=getCFPL&fltNr=%s&alnCd=%s&fltDt=%s&opSuffix=%s&depCd=%s&arvCd=%s&tailNr=%s&type=0' % (
                i['fltNr'], i['alnCd'], i['fltDt'], i['opSuffix'], i['latestDepArpCd'], i['latestArvArpCd'],i['latestTailNr'])
            print(i['fltNr'], i['alnCd'], i['fltDt'], i['opSuffix'], i['latestDepArpCd'], i['latestArvArpCd'],i['latestTailNr'])
            try:
                response = urllib.request.urlopen(net, timeout=30)
            except Exception:
                config.logger.warning('network for cfpl failed',exc_info=True)
                continue
            try:
                tempo = response.read()
                tempofpl = json.loads(tempo)
            except Exception:
                config.logger.debug('wrong with fpl!',exc_info=True)
                continue
            if DB.queryoData(tempofpl) == 0:
                tempofplfilename=timestramp[2]+'-'+i['fltNr']+'-'+i['latestDepArpCd']
                tempo = base64ToString(tempofpl['ofpText'])
                config.cfplfile(tempofplfilename,tempo)
                # TODO add write to file with config
                try:
                    detail = ofptextprocess(tempo)
                except Exception:
                    config.logger.debug('ofpprocess goes wrong!fltNr=%s,depCd=%s,arvCd=%s' % (
                        i['fltNr'], i['latestDepArpCd'], i['latestArvArpCd']), exc_info=True)
                    continue
                DB.insertData(tempofpl, detail, config.logger)
            else:
                config.logger.info("cfpl existed.")
                continue
    time.sleep(300)









