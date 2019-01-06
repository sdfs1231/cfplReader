from ofptextprocess import ofptextprocess
import config

with open(config.CFPLdir+'2019-01-07/CSN 3340  B3206 ZGMX ZGGG ZSAM 039257 20190106133816332.txt') as f:
    print(ofptextprocess(f.read()))