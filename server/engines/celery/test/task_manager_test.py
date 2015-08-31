from request.manager import taskManager
request1 = {'version': [u'1.0.0'],
           'service': [u'WPS'],
           'embedded': [u'true'],
           'rawDataOutput': [u'result'],
           'identifier': [u'cdas'],
           'request': [u'Execute'],
           'datainputs': [u'[region={"level":"100000"};data={"collection":"MERRA/mon/atmos","id":"hur"};operation=[{"kernel":"time","type":"departures","bounds":"np"},{"kernel":"time","type":"climatology","bounds":"annualcycle"}]' ] };




data = {"collection":"MERRA/mon/atmos","id":"hur"};
region = {"level":"100000"}
operation = [{"kernel":"time","type":"departures","bounds":"np"},{"kernel":"time","type":"climatology","bounds":"annualcycle"}]

request = { 'datainputs': { 'data':data, 'region':region, 'operation':operation } }
response = taskManager.processRequest( request )
print response
