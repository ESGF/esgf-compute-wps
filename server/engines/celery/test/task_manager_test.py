from request.manager import taskManager
request = '[region={"level":"100000"};data={"collection":"MERRA/mon/atmos","id":"hur"};'
[('version', u'1.0.0'), ('embedded', u'true'), ('service', u'WPS'), ('rawDataOutput', u'result'), ('identifier', u'cdas'), ('request', u'Execute'), ('datainputs', u'[region={"level":"100000"};data={"collection":"MERRA/mon/atmos","id":"hur"};')]
response = taskManager.processRequest( request )
print response
