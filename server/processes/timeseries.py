from pywps.Process import WPSProcess
import json, types
from cdasProcess import CDASProcess, wpsLog
from analysis.timeseries_analysis import *

# Test arguments for run configuration:
# version=1.0.0&service=wps&request=Execute&RawDataOutput=result&identifier=timeseries&datainputs=[domain={\"longitude\":10.0,\"latitude\":10.0,\"level\":1000.0};variable={\"url\":\"file://Users/tpmaxwel/Data/AConaty/comp-ECMWF/geos5.xml\",\"id\":\"uwnd\"}]

class Process(CDASProcess):
    def __init__(self):
        WPSProcess.__init__(self, identifier=os.path.split(__file__)[-1].split('.')[0], title='timeseries', version=0.1, abstract='Extract a timeseries at a spatial location', storeSupported='true', statusSupported='true')
        self.domain = self.addComplexInput(identifier='domain', title='spatial location of timeseries', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}])
#        self.download = self.addLiteralInput(identifier='download', type=bool, title='download output', default=False)
        self.dataIn = self.addComplexInput(identifier='variable', title='variable to process', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}], minOccurs=1, maxOccurs=1)
        self.operation = self.addComplexInput(identifier='operation', title='analysis operation', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}], minOccurs=0, maxOccurs=1)
        self.result = self.addLiteralOutput( identifier='result', title='timeseries data', type=types.StringType )
        self.cacheVariableData = False

    def execute(self):
        dataIn=self.loadData()
        domain = self.loadDomain()
        operation = self.loadOperation()

        wpsLog.debug( " $$$ Timeseries Process: DataIn='%s', Domain='%s', Operation='%s' ", str(dataIn), str( domain ), str( operation ) )

        processor = TimeseriesAnalytics( dataIn[0])
        result = processor.execute( operation, domain )
        result_json = json.dumps( result )
        self.result.setValue( result_json )

