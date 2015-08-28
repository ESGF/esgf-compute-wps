from request.manager import taskManager
request = {'version': [u'1.0.0'], 'service': [u'WPS'], 'embedded': [u'true'], 'rawDataOutput': [u'result'], 'identifier': [u'cdas'], 'request': [u'Execute'], 'datainputs': [u'[region={"level":"100000"};data={"collection":"MERRA/mon/atmos","id":"hur"};]']}
response = taskManager.processRequest( request )
print response
