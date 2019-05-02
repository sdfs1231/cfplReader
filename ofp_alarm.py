import configparser
import re
import json
config=configparser.ConfigParser()
config.read('config.ini')

def route_compare(alarmdict,new_dict,old_dict):
    if old_dict is None:
        return None
    if old_dict['routeNr']!=new_dict['routeNr']:
        alarmdict['alarmType'] = 'route_compare'
        alarmdict['alarmContent'] = json.dumps({"alarmOfp":{
            "routeNr" : new_dict['routeNr']
        },"oldOfp":{
            "routeNr" : old_dict['routeNr']
        }})
        alarmdict['oldOfpNr']=old_dict['ofpNr']
        return alarmdict
    else:
        return None

def route_check(alarmdict,new_dict):
    if not re.match('\w{6}01.*',new_dict['routeNr']):
        alarmdict['alarmType']='route_check'
        alarmdict['alarmContent']=json.dumps({"alarmOfp":{
            "routeNr" : new_dict['routeNr']
        },"oldOfp":{
            "routeNr" : ''
        }})
        return alarmdict
    else:
        return None

def turb_alarm(alarmdict,new_dict):
    if int(new_dict['Max_turb'])>=int(config['ALARM']['Max_turb']):
        alarmdict['alarmType']='turb_alarm'
        alarmdict['alarmContent']=json.dumps({"alarmOfp":{
            "Max_turb" : new_dict['Max_turb']
        },"oldOfp":{
            "Max_turb" : ''
        }})
        return alarmdict
    else:
        return None

def temp_alarm(alarmdict,new_dict):
    if new_dict['Min_temp']>=config['ALARM']['Min_temp']:
        alarmdict['alarmType']='min_temp_check'
        alarmdict['alarmContent'] = json.dumps({"alarmOfp": {
            "Min_temp": new_dict['Min_temp']
        }, "oldOfp": {
            "Min_temp": ''
        }})
        return alarmdict
    else:
        return None

def ofp_alarm(logger,old_dict,new_dict):
    alarm_dict={'fltDt':new_dict['fltDt'],'fltNr':new_dict['fltNr'],'opSuffix':new_dict['opSuffix'],'tailNr':new_dict['tailNr'],'depCd':new_dict['depCd'],
                'arvCd':new_dict['arvCd'],'depDt':new_dict['depDt'],'alarmOfpNr':new_dict['ofpNr'],
                'oldOfpNr':None,'alarmType':'','alarmContent':json.dumps({})}
    alarm_list=list(filter(lambda x:x!=None,[route_compare(alarm_dict.copy(),new_dict,old_dict),
                                             route_check(alarm_dict.copy(),new_dict),
                                             turb_alarm(alarm_dict.copy(),new_dict),
                                             temp_alarm(alarm_dict.copy(),new_dict)]))
    return alarm_list

