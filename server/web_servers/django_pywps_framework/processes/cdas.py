from pywps.Process import WPSProcess
import os, time
import logging
import json, types, traceback
from cdasProcess import CDASProcess, loadValue
from request.manager import taskManager
from modules.utilities import wpsLog, debug_trace

class Process(CDASProcess):
    def __init__(self):
        """Process initialization"""
        WPSProcess.__init__(self, identifier=os.path.split(__file__)[-1].split('.')[0], title='CDAS', version=0.1, abstract='Climate Data Analytic Services', storeSupported='true', statusSupported='true')
        self.region = self.addComplexInput(identifier='domain', title='Spatial location of timeseries', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}], minOccurs=0, maxOccurs=1 )
        self.data = self.addComplexInput(identifier='variable', title='Variables to process', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}], minOccurs=1, maxOccurs=1)
        self.operation = self.addComplexInput(identifier='operation', title='Analysis operation', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}], minOccurs=0, maxOccurs=1)
        self.async = self.addLiteralInput(identifier='async', title='Async operation', default='true', type=types.StringType, minOccurs=0, maxOccurs=1)
        self.embedded = self.addLiteralInput(identifier='embedded', title='Embedded result', default='false', type=types.StringType, minOccurs=0, maxOccurs=1)
        self.result = self.addLiteralOutput( identifier='result', title='timeseries data', type=types.StringType )

    def execute(self):
        try:
            # wpsLog.debug( "  -------------- Execution stack:  -------------- ")
            # wpsLog.debug( traceback.format_stack() )
            # wpsLog.debug( "  -------------- ________________  -------------- ")
#            debug_trace()
            data = loadValue( self.data )
            region = loadValue( self.region )
            operation = loadValue( self.operation )
            request = { 'datainputs': { 'data':data, 'region':region, 'operation':operation }, 'async':self.async.value, 'embedded':self.embedded.value }
            wpsLog.debug( " $$$ CDAS Django-WPS Process,  Request: %s, Operation: %s " % ( str( request ), str(operation) ) )
            response = taskManager.processRequest( request )
            self.result.setValue( response )
        except Exception, err:
             wpsLog.debug( "Exception executing CDAS process:\n " + traceback.format_exc() )
             self.result.setValue( '' )


    # def execute1(self):
    #     try:
    #         # wpsLog.debug( "  -------------- Execution stack:  -------------- ")
    #         # wpsLog.debug( traceback.format_stack() )
    #         # wpsLog.debug( "  -------------- ________________  -------------- ")
    #
    #         data = loadValue( self.data )
    #         region = loadValue( self.region )
    #         operation = loadValue( self.operation )
    #         wpsLog.debug( " $$$ CDAS Process: DataIn='%s', Domain='%s', Operation='%s', ---> Time=%.3f " % ( str( data ), str( region ), str( operation ), time.time() ) )
    #         t0 = time.time()
    #         handler = stagingRegistry.getInstance( configuration.CDAS_STAGING  )
    #         if handler is None:
    #             wpsLog.warning( " Staging method not configured. Running locally on wps server. " )
    #             handler = stagingRegistry.getInstance( 'local' )
    #         result_obj =  handler.execute( { 'data':data, 'region':region, 'operation':operation, 'engine': configuration.CDAS_COMPUTE_ENGINE } )
    #         wpsLog.debug( " $$$*** CDAS Process (response time: %.3f sec):\n Result='%s' " %  ( (time.time()-t0), str(result_obj) ) )
    #         result_json = json.dumps( result_obj )
    #         self.result.setValue( result_json )
    #     except Exception, err:
    #          wpsLog.debug( "Exception executing CDAS process:\n " + traceback.format_exc() )
    #          self.result.setValue( '' )




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


