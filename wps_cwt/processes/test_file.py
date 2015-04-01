import json

request_args = '{"url":"file://Users/tpmaxwel/Data/AConaty/comp-ECMWF/geos5.xml","id":"uwnd"}'
request_object =  json.loads(request_args)

print str( request_object )