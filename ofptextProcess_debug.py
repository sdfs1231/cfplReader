from ofptextprocess import ofptextprocess

with open('/../wamp64/www/fdbs/OFP/'+'2019-01-06/CSN 0308  B1063 EHAM ZGGG ZSAM 165000 20190106084223319.txt') as f:
    print(ofptextprocess(f.read()))