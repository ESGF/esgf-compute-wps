from pywps.Process import WPSProcess
import os
import logging
import json, types
import cdms2
import numpy
import cdutil
cdms2.setNetcdfShuffleFlag(0)
cdms2.setNetcdfDeflateFlag(0)
cdms2.setNetcdfDeflateLevelFlag(0)
import random
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'output'))
from tools import ESGFCWTProcess

def record_attributes( var, attr_name_list, additional_attributes = {} ):
    mdata = {}
    for attr_name in attr_name_list:
        if attr_name == '_data_' and hasattr(var,"getValue"):
            attr_val =  var.getValue()
        else:
            attr_val = var.__dict__.get(attr_name,None)
        if attr_val is None:
            attr_val = var.attributes.get(attr_name,None)
        if attr_val is not None:
            if isinstance( attr_val, numpy.ndarray ):
               attr_val = attr_val.tolist()
            mdata[attr_name] = attr_val
    for attr_name in additional_attributes:
        mdata[attr_name] = additional_attributes[attr_name]
    return mdata

# Test arguments for run configuration:
# version=1.0.0&service=wps&request=Execute&RawDataOutput=result&identifier=timeseries&datainputs=[domain={\"longitude\":10.0,\"latitude\":10.0,\"level\":1000.0};variable={\"url\":\"file://Users/tpmaxwel/Data/AConaty/comp-ECMWF/geos5.xml\",\"id\":\"uwnd\"}]

class Process(ESGFCWTProcess):
    def __init__(self):
        """Process initialization"""
        WPSProcess.__init__(self, identifier=os.path.split(__file__)[-1].split('.')[0], title='timeseries', version=0.1, abstract='Extract a timeseries at a spatial location', storeSupported='true', statusSupported='true')
        self.domain = self.addComplexInput(identifier='domain', title='spatial location of timeseries', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}])
#        self.download = self.addLiteralInput(identifier='download', type=bool, title='download output', default=False)
        self.dataIn = self.addComplexInput(identifier='variable', title='variable to average', formats=[{'mimeType': 'text/json'}], minOccurs=1, maxOccurs=1)
        self.result = self.addLiteralOutput( identifier='result', title='timeseries data', type=types.StringType )

    def execute(self):
        dataIn=self.loadData()[0]
        location = self.loadDomain()
        cdms2keyargs = self.domain2cdms(location)
        dataset=self.loadFileFromURL(dataIn["url"])
        logging.debug( " $$$ Data Request: '%s', '%s', '%s' ", dataIn["url"], dataIn["id"], str( cdms2keyargs ) )
        variable = dataset[dataIn["id"]]
        result_variable = variable(**cdms2keyargs)
        result_data = result_variable.squeeze().tolist( numpy.nan )
        time_axis = result_variable.getTime()
        result_obj = {}
        result_obj['data'] = result_data
        result_obj['variable'] = record_attributes( result_variable, [ 'long_name', 'name', 'units' ], { 'id': dataIn["id"] } )
        result_obj['dataset'] = record_attributes( dataset, [ 'id', 'uri' ])
        if time_axis is not None:
            result_obj['time'] = record_attributes( time_axis, [ 'units', 'calendar', '_data_' ] )
        result_json = json.dumps( result_obj )
        self.result.setValue( result_json )
        return

if __name__ == "__main__":
    dataset = cdms2.open( '/Developer/Web/data/MERRA/Temp2D/MERRA_3Hr_Temp.xml' )
    variable = dataset['t']
    result_variable = variable( latitude = ( 59.0,59.0,"cob"), longitude= (103.0,103.0,"cob") )
    result_data = result_variable.squeeze().tolist( numpy.nan )
    result_json = json.dumps( result_data )
    print " "