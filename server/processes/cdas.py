from pywps.Process import WPSProcess
import json, types, traceback, os
from pywps.Process import WPSProcess
from cdasProcess import  wpsLog, loadValue
from server.engines import getComputeEngine

# Test arguments for run configuration:
# version=1.0.0&service=wps&request=Execute&RawDataOutput=result&identifier=timeseries&datainputs=[domain={\"longitude\":10.0,\"latitude\":10.0,\"level\":1000.0};variable={\"url\":\"file://Users/tpmaxwel/Data/AConaty/comp-ECMWF/geos5.xml\",\"id\":\"uwnd\"}]

class Process(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(self, identifier=os.path.split(__file__)[-1].split('.')[0], title='timeseries', version=0.1, abstract='Extract a timeseries at a spatial location', storeSupported='true', statusSupported='true')
        self.domain = self.addComplexInput(identifier='domain', title='spatial location of timeseries', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}])
#        self.download = self.addLiteralInput(identifier='download', type=bool, title='download output', default=False)
        self.data = self.addComplexInput(identifier='variables', title='variable to process', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}], minOccurs=1, maxOccurs=1)
        self.operation = self.addComplexInput(identifier='operation', title='analysis operation', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}], minOccurs=0, maxOccurs=1)
        self.result = self.addLiteralOutput( identifier='result', title='timeseries data', type=types.StringType )
        self.cacheVariableData = False

    def execute(self):
        try:
            data = loadValue( self.data )
            domain = loadValue( self.domain )
            operation = loadValue( self.operation )
            wpsLog.debug( " $$$ CDAS Process: DataIn='%s', Domain='%s', Operation='%s' ", str( data ), str( domain ), str( operation ) )

            engine = getComputeEngine()
            result =  engine.execute( data, domain, operation )
            result_json = json.dumps( result )
            self.result.setValue( result_json )
        except Exception, err:
             wpsLog.debug( "Exception executing timeseries process:\n " + traceback.format_exc() )
             self.result.setValue( '' )

