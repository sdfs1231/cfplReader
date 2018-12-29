import re

# 正则表达式
reRoute = r'\b(ROUTE NO\.\s\w{6}\d{2}..).+\b'
rePoint = r'\b\d{3} +\d{3}\/\d{4} (\w+) +.*\b'
reFL = r'\b(FL\s\d).+\b'
reTurbTemp = r'\bM(\d{2})\.+.* (\d{2}) .*\b'
reAltn = r'\b(\w{4})\/\w{3}\/.*\b'
endFpl=r'----------------------\s{3}END OF FLIGHT PLAN\s{3}----------------------[\s\S]+$'
rmkEnd='                         ALTERNATE SUMMARY'
rmkBgn='DISP RMKS'
melBgnSign='-------------      -----------'
def ofptextprocess(data,logger=None):
    detail=[]
    # begin
    lines = data.strip()
    ALTN = []
    Min_temp = 0
    Max_turb = 0
    tempPoint =[]
    tempPoints=''
    turbPoint = []
    turbPoints=''
    ROUTE = ''
    FL = ''
    line = ''
    Rmk = ''
    MEL = ''
    RouteDef = ''
    Rmksign = 0
    MELsign = 0
    RouteDefSign = 0
    altnnum = 0
    lines = re.sub(endFpl, '', data)
    lines = data.split('\n')
    for line in lines:
        # FL
        if re.match(reFL, line):
            tempo = re.match(reFL, line)
            FL = tempo.group(0)
            RouteDefSign = 0
        # 航路详情
        if RouteDefSign != 0:
            RouteDef = RouteDef + line
        # 航路代号
        if re.match(reRoute, line):
            tempo = re.match(reRoute, line)
            ROUTE = tempo.group(1)
            RouteDefSign = 1
        # RMK
        if line == rmkEnd:
            Rmksign = 0
            MELsign = 0
        if Rmksign != 0:
            Rmk = Rmk + line
        if line == rmkBgn:
            Rmksign = 1
            MELsign = 0
        # 备降场列表
        if re.match(reAltn, line):
            tempo = re.match(reAltn, line)
            ALTN.append(tempo.group(1))
            altnnum+=1

        # 航路点和颠簸
        if re.match(rePoint, line):
            tempo = re.match(rePoint, line)
            point = tempo.group(1)
            continue
        if re.match(reTurbTemp, line):
            tempo = re.match(reTurbTemp, line)
            temp = tempo.group(1)
            turb = tempo.group(2)
            if int(temp) > Min_temp:
                Min_temp = int(temp)
                tempPoint.clear()
            if int(temp) == Min_temp:
                tempPoint.append(point)
            if int(turb) > Max_turb:
                Max_turb = int(turb)
                turbPoint.clear()
            if int(turb) == Max_turb:
                turbPoint.append(point)
        # MEL
        if MELsign != 0:
            MEL += line
        if line == melBgnSign:
            MELsign = 1

    if Max_turb<10:
        Max_turb='0'+str(Max_turb)
    else:
        Max_turb=str(Max_turb)
    Min_temp='M'+str(Min_temp)
    if FL=='':
        logger.warning('FL is empty!')
    detail.append(FL)
    if FL=='':
        logger.warning('routedefinition is empty!')
    #TODO
    detail.append(RouteDef)
    detail.append(Rmk)
    detail.append(Max_turb)
    detail.append(",".join(turbPoint))
    detail.append(Min_temp)
    detail.append(",".join(tempPoint))
    detail.append(MEL)
    detail += ALTN
    return detail