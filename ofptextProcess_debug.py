from ofptextprocess import ofptextprocess
import config

with open(config.CFPLdir+'2019-01-06/CSN 0308  B1063 EHAM ZGGG ZSAM 165000 20190106084223319.txt') as f:
    print(ofptextprocess(f.read()))