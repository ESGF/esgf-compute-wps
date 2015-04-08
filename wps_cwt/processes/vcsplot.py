from pywps.Process import WPSProcess
import os
import logging, time
from wps_uvcdat_utilities import importEnvironment, record_attributes
import json, types
import numpy
import cdutil
import random
from tools import ESGFCWTProcess
env = { 'uvcdatSetupPath':"UVCDAT_SETUP_PATH", 'ldLibraryPath':"LD_LIBRARY_PATH", 'pythonPath':"PYTHONPATH", 'dyldFallbackLibraryPath':'DYLD_FALLBACK_LIBRARY_PATH' }
importEnvironment( env )

# Test arguments for run configuration:
# version=1.0.0&service=wps&request=Execute&RawDataOutput=result&identifier=timeseries&datainputs=[domain={\"longitude\":10.0,\"latitude\":10.0,\"level\":1000.0};variable={\"url\":\"file://Users/tpmaxwel/Data/AConaty/comp-ECMWF/geos5.xml\",\"id\":\"uwnd\"}]

class Process(ESGFCWTProcess):
    def __init__(self):
        """Process initialization"""
        WPSProcess.__init__(self, identifier=os.path.split(__file__)[-1].split('.')[0], title='timeseries', version=0.1, abstract='Extract a timeseries at a spatial location', storeSupported='true', statusSupported='true')
        self.domain = self.addComplexInput(identifier='domain', title='spatiotemporal domain of plot', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}])
#        self.download = self.addLiteralInput(identifier='download', type=bool, title='download output', default=False)
        self.dataIn = self.addComplexInput(identifier='variable', title='variable to average', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}], minOccurs=1, maxOccurs=1)
        self.plotSpec = self.addComplexInput(identifier='plot', title='plot specification', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}], minOccurs=1, maxOccurs=1)
        self.result = self.addLiteralOutput( identifier='result', title='result URL', type=types.StringType )
        self.cacheVariableData = False

    def execute(self):
        import cdms2, vcs
        cdms2.setNetcdfShuffleFlag(0)
        cdms2.setNetcdfDeflateFlag(0)
        cdms2.setNetcdfDeflateLevelFlag(0)
        start_time = time.time()
        dataIn=self.loadData()[0]
        location = self.loadDomain()
        cdms2keyargs = self.domain2cdms(location)
        url = dataIn["url"]
        id = dataIn["id"]
        var_cache_id =  ":".join( [url,id] )
        dataset = self.loadFileFromURL( url )
        logging.debug( " $$$ Data Request: '%s', '%s' ", var_cache_id, str( cdms2keyargs ) )
        variable = dataset[ id ]

        read_start_time = time.time()
        result_variable = variable(**cdms2keyargs)
        result_data = result_variable.squeeze()[...]
        time_axis = result_variable.getTime()
        read_end_time = time.time()

        x = vcs.init()
        bf = x.createboxfill('new')
        x.plot( result_data, bf, 'default', variable=result_variable, bg=1 )
        x.gif(  OutputPath + '/plot.gif' )

        result_obj = {}

        result_obj['url'] = OutputDir + '/plot.gif'
        result_json = json.dumps( result_obj )
        self.result.setValue( result_json )
        final_end_time = time.time()
        logging.debug( " $$$ Execution time: %f (with init: %f) sec", (final_end_time-start_time), (final_end_time-self.init_time) )
        
        return

# if __name__ == "__main__":
#     dataset = cdms2.open( '/usr/local/cds/web/data/MERRA/Temp2D/MERRA_3Hr_Temp.xml' )
#     t = dataset('t')
#     result_obj = {}
#     time_axis = t.getTime()
#     
#     time_obj = record_attributes( time_axis, [ 'units', 'calendar' ] )
#     time_data = time_axis.getValue().tolist()
#     isLinear = True
#     if isLinear:
#         time_obj['t0'] = time_data[0]
#         time_obj['dt'] = time_data[1] - time_data[0]
#     else:
#         time_obj['data'] = time_data
#      
#     result_obj['time'] = time_obj           
#     result_json = json.dumps( result_obj )
#     merged_dataset = cdms2.open( '/usr/local/cds/web/data/MERRA/Temp2D/MERRA_3Hr_Temp.nc', 'w' )
#     merged_dataset.write( t )
#     merged_dataset.close()
#     dataset.close()
#    result_variable = variable( latitude = ( 59.0,59.0,"cob"), longitude= (103.0,103.0,"cob") )
#    result_data = result_variable.squeeze().tolist( numpy.nan )
#    result_json = json.dumps( result_data )
#    print " "