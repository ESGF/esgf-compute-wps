from pywps.Process import WPSProcess
import os, time
import logging
import json, types, traceback
from cdasProcess import CDASProcess, loadValue
from engines.registry import engineRegistry
from django.conf import settings
wpsLog = logging.getLogger( 'wps' )

class Process(CDASProcess):
    def __init__(self):
        """Process initialization"""
        WPSProcess.__init__(self, identifier=os.path.split(__file__)[-1].split('.')[0], title='timeseries', version=0.1, abstract='Extract a timeseries at a spatial location', storeSupported='true', statusSupported='true')
        self.region = self.addComplexInput(identifier='region', title='spatial location of timeseries', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}])
#        self.download = self.addLiteralInput(identifier='download', type=bool, title='download output', default=False)
        self.data = self.addComplexInput(identifier='variables', title='variable to process', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}], minOccurs=1, maxOccurs=1)
        self.operation = self.addComplexInput(identifier='operation', title='analysis operation', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}], minOccurs=0, maxOccurs=1)
        self.result = self.addLiteralOutput( identifier='result', title='timeseries data', type=types.StringType )

    def execute(self):
        try:
            data = loadValue( self.data )
            region = loadValue( self.region )
            operation = loadValue( self.operation )
            wpsLog.debug( " $$$ CDAS Process: DataIn='%s', Domain='%s', Operation='%s' " % ( str( data ), str( region ), str( operation ) ) )
            t0 = time.time()
            engine = engineRegistry.getComputeEngine( settings.COMPUTE_ENGINE )
            result =  engine.execute( data, region, operation )
            result_json = json.dumps( result )
            t1 = time.time()
            wpsLog.debug( " $$$ CDAS Process (response time: %.3f sec): Result='%s' " %  ( (t1-t0), str( result_json )) )
            self.result.setValue( result_json )
        except Exception, err:
             wpsLog.debug( "Exception executing CDAS process:\n " + traceback.format_exc() )
             self.result.setValue( '' )




# def record_attributes( var, attr_name_list, additional_attributes = {} ):
#     mdata = {}
#     for attr_name in attr_name_list:
#         if attr_name == '_data_' and hasattr(var,"getValue"):
#             attr_val =  var.getValue()
#         else:
#             attr_val = var.__dict__.get(attr_name,None)
#         if attr_val is None:
#             attr_val = var.attributes.get(attr_name,None)
#         if attr_val is not None:
#             if isinstance( attr_val, numpy.ndarray ):
#                attr_val = attr_val.tolist()
#             mdata[attr_name] = attr_val
#     for attr_name in additional_attributes:
#         mdata[attr_name] = additional_attributes[attr_name]
#     return mdata

# Test arguments for run configuration:
# version=1.0.0&service=wps&request=Execute&RawDataOutput=result&identifier=timeseries&datainputs=[domain={\"longitude\":10.0,\"latitude\":10.0,\"level\":1000.0};variable={\"url\":\"file://Users/tpmaxwel/Data/AConaty/comp-ECMWF/geos5.xml\",\"id\":\"uwnd\"}]

    # def execute(self):
    #     dataIn=self.loadData()[0]
    #     location = self.loadDomain()
    #     cdms2keyargs = self.domain2cdms(location)
    #     url = dataIn["url"]
    #     id = dataIn["id"]
    #     dataset = self.loadFileFromURL( url )
    #     logging.debug( " $$$ Data Request: '%s', '%s' ", url, str( cdms2keyargs ) )
    #
    #     variable = dataset[ id ]
    #     result_variable = variable(**cdms2keyargs)
    #     result_data = result_variable.squeeze().tolist( numpy.nan )
    #     time_axis = result_variable.getTime()
    #
    #     result_obj = {}
    #
    #     result_obj['variable'] = record_attributes( variable, [ 'long_name', 'name', 'units' ], { 'id': dataIn["id"] } )
    #     result_obj['dataset'] = record_attributes( dataset, [ 'id', 'uri' ])
    #     if time_axis is not None:
    #         result_obj['time'] = record_attributes( time_axis, [ 'units', 'calendar', '_data_' ] )
    #     result_obj['data'] = result_data
    #     result_json = json.dumps( result_obj )
    #     self.result.setValue( result_json )
    #     return

