from ipwhois import IPWhois
from pprint import pprint

obj = IPWhois('65.55.44.109')

res=obj.lookup_whois()
#pprint(res["nets"][0]["description"])
pprint(res)