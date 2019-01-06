import re

# 正则表达式
reRoute = r'\b(ROUTE NO\.\s[0-9A-Z]+) +.+\b'
rePoint = r'\b\d{3} +\d{3}\/\d{4} (\w+) +.*\b'
reFL = r'\b(FL\s\d).+\b'
reFLend = r'.+ALL WEIGHTS IN KILOS'
reTurbTemp = r'\bM(\d{2})\.+.* {7}(\d{2})? .*\b'
reAltn = r'\b(\w{4})\/\w{3}\/.*\b'
endFpl = r'----------------------\s{3}END OF FLIGHT PLAN\s{3}----------------------[\s\S]+$'
rmkEnd = '                         ALTERNATE SUMMARY'
rmkBgn = 'DISP RMKS'
melBgnSign = '-------------      -----------'


def ofptextprocess(data, logger=None):
    ALTN = []
    Min_temp = 0
    Max_turb = 0
    point = ''
    tempPoint = []
    turbPoint = []
    FL = []
    FL_start_flag = 0
    FL_done_flag = 0
    Rmk = []
    MEL = []
    RouteDef = []
    Rmksign = 0
    MELsign = 0
    routeDef_start_flag = 0
    routedef_end_flag = 0
    lines = re.sub(endFpl, '', data).split('\n')
    for line in lines:
        # FL
        if re.match(reFL, line):
            FL_start_flag = 1
            routedef_end_flag = 1
        if re.match(reFLend,line):
            FL_done_flag = 1
        if FL_start_flag == 1 and FL_done_flag == 0:
            if not re.match(r'^\s*$',line):
                FL.append(line)
        # 航路详情
        if routeDef_start_flag == 1 and routedef_end_flag == 0:
            RouteDef.append(line)
        # 航路代号
        if re.match(reRoute, line):
            # tempo = re.match(reRoute, line)
            # ROUTE = tempo.group(1)
            routeDef_start_flag = 1
        # RMK
        if line == rmkEnd:
            Rmksign = 0
            MELsign = 0
        if Rmksign != 0:
            Rmk.append(line)
        if line == rmkBgn:
            Rmksign = 1
            MELsign = 0
        # 备降场列表
        if re.match(reAltn, line):
            tempo = re.match(reAltn, line)
            ALTN.append(tempo.group(1))

        # 航路点和颠簸
        if re.match(rePoint, line):
            tempo = re.match(rePoint, line)
            point = tempo.group(1)
            continue
        if re.match(reTurbTemp, line):
            tempo = re.match(reTurbTemp, line)
            turb = tempo.group(2)
            if tempo.group(2) is None:
                turb = 0
            temp = tempo.group(1)
            if int(temp) > Min_temp:
                Min_temp = int(temp)
                tempPoint.clear()
                tempPoint.append(point)
            else:
                if int(temp) == Min_temp:
                    tempPoint.append(point)

            if int(turb) > Max_turb:
                Max_turb = int(turb)
                turbPoint.clear()
                turbPoint.append(point)
            else:
                if int(turb) == Max_turb:
                    turbPoint.append(point)

        # MEL
        if MELsign != 0:
            MEL.append(line)
        if line == melBgnSign:
            MELsign = 1

    if Max_turb < 10:
        Max_turb = '0'+str(Max_turb)
    else:
        Max_turb = str(Max_turb)
    Min_temp = 'M'+str(Min_temp)

    detail = {}
    if len(FL) == 0:
        logger.warning('ofpProcess Warning : FL empty!')
    detail['FL'] = "\n".join(FL)
    if RouteDef == '':
        logger.warning('ofpProcess Warning : routedefinition empty!')
    detail['routeDef'] = "\n".join(RouteDef)
    detail['Rmk'] = "\n".join(Rmk)
    detail['Max_turb'] = Max_turb
    # 颠簸点
    if not turbPoint:
        detail['turbPoint'] = ''
        logger.warning('ofpProcess Warning : turbpoints empty')
    else:
        detail['turbPoint'] = ",".join(turbPoint)
    detail['Min_temp'] = Min_temp
    if not tempPoint:
        logger.warning('ofpProcess Warning : temppoints empty!')
        detail['tempPoint'] = ''
    else:
        detail['tempPoint'] = ",".join(tempPoint)
    detail['MEL'] = "\n".join(MEL)
    detail['altn0'] = ""
    detail['altn1'] = ""
    detail['altn2'] = ""
    detail['altn3'] = ""
    i = 0
    for altn in ALTN:
        detail['altn'+str(i)] = altn
        i += 1
    return detail
