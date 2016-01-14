from pywps.Process import WPSProcess
import os
import json
import cdms2
import cdutil
cdms2.setNetcdfShuffleFlag(0)
cdms2.setNetcdfDeflateFlag(0)
cdms2.setNetcdfDeflateLevelFlag(0)
import random
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'output'))
from esgfcwtProcess import esgfcwtProcess

# Test arguments for run configuration:
# version=1.0.0&service=wps&request=Execute&identifier=averager&datainputs=[domain={\"longitude\":{\"start\":-180.0,\"end\":180.0}};variable={\"url\":\"file://Users/tpmaxwel/Data/AConaty/comp-ECMWF/geos5.xml\",\"id\":\"uwnd\"}]

class Process(esgfcwtProcess):
    def __init__(self):
        """Process initialization"""
        WPSProcess.__init__(self, identifier=os.path.split(__file__)[-1].split('.')[0], title='averager', version=0.1, abstract='Average a variable over a (many) dimension', storeSupported='true', statusSupported='true')
        self.dataIn = self.addComplexInput(identifier='variable', title='variable to average', formats=[{'mimeType': 'text/json'}], minOccurs=1, maxOccurs=1)
        self.domain = self.addComplexInput(identifier='domain', title='domain over which to average', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}])
        self.axes = self.addLiteralInput(identifier = "axes", type=str,title = "Axes to average over", default = 't')
        self.outputformat = self.addLiteralInput(identifier = "outputformat", type=str,title = "Output Format", default = 'opendap')
        # TODO application/netcdf ???
        self.output = self.addComplexOutput(identifier='output', title='averaged variable', formats=[{'mimeType': 'text/json'},{'mimeType': "application/netcdf"}, {"mimeType":"image/png"}])

    def execute(self):
        # What data did the user send us?
        # We are only taking care of the first one
        # TODO: Add check there is only ONE dataset sent over.
        dataIn=self.loadData()[0]
        # Let's read this in
        data,cdms2keyargs = self.loadVariable(dataIn)
        # Which axes do we average over?
        axes = self.axes.getValue()
        # Ok actual average happens here
        print "AXES TO AVG OVER:",axes
        data = cdutil.averager(data,axis=axes)
        # Save
        data.id=self.getVariableName(dataIn)
        # TODO json vs netcdf embeded?
        self.saveVariable(data,self.output)
        return
