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

from esgf_process import ESGFProcess

class Process(ESGFProcess):
    def __init__(self):
        """Process initialization"""
        WPSProcess.__init__(self, identifier=os.path.split(__file__)[-1].split('.')[0], title='vcs plot', version=0.1, abstract='Generate a plot using vcs', storeSupported='true', statusSupported='true')
        self.domain = self.addComplexInput(identifier='domain', title='spatiotemporal domain of plot', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}])
#        self.download = self.addLiteralInput(identifier='download', type=bool, title='download output', default=False)
        self.dataIn = self.addComplexInput(identifier='variable', title='variable to average', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}], minOccurs=1, maxOccurs=1)
        self.plotSpec = self.addComplexInput(identifier='plot', title='plot specification', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}], minOccurs=1, maxOccurs=1)
        self.result = self.addLiteralOutput( identifier='result', title='result URL', type=types.StringType )

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
   

# 
