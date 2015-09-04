import os
import json
import random, traceback
import cdms2
from modules.utilities import *
from modules.containers import *
from datacache.manager import dataManager

cdms2.setNetcdfShuffleFlag(0)
cdms2.setNetcdfDeflateFlag(0)
cdms2.setNetcdfDeflateLevelFlag(0)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'output'))

class DatasetContainer(JSONObjectContainer):

    def process_spec( self, spec ):
        if spec:
            spec = convert_json_str( spec )
            if isinstance( spec, dict ):
                keys = []
                for item in spec.iteritems():
                    collection = item[0]
                    varlist = item[1]
                    for var_spec in varlist:
                        tokens = var_spec.split(':')
                        varid = tokens[0]
                        varname = tokens[-1]
                        if varid in keys:
                            raise Exception( "Error, Duplicate variable id in request data inputs: %s." % varid )
                        object_spec = { 'collection':collection, 'name':varname, 'id':varid }
                        self._objects.append( self.newObject( object_spec) )
                        keys.append( varid )
            else:
                raise Exception( "Unrecognized DatasetContainer spec: " + str(spec) )

    def newObject( self, spec ):
        return Dataset(spec)

class Dataset(JSONObject):

    def __init__( self, spec={} ):
        JSONObject.__init__( self, spec )

class OperationContainer(JSONObjectContainer):

    def newObject( self, spec ):
        return Operation(spec)

class Operation(JSONObject):

    def load_spec( self, spec ):
        self.spec = spec

    def process_spec( self, **args ):
        toks = self.spec.split('()')
        args = toks[1].split(',')
        task = toks[0].split('.')
        self.items = { 'kernel': task[0], 'method': task[1], 'input': args[0] }
        if len(args>1): self.items['slice'] = args[2]
        if len(args>2): self.items['bounds'] = args[1]

class DataAnalytics:

    def __init__( self,  **args  ):
        self.use_cache = args.get( 'cache', True )

        # self.envs = {
        #         "path":"PATH",
        #         "addonPath":"GRASS_ADDON_PATH",
        #         "version":"GRASS_VERSION",
        #         "gui":"GRASS_GUI",
        #         "gisbase": "GISBASE",
        #         "ldLibraryPath": "LD_LIBRARY_PATH"
        # }


    def run( self, data, region, operation ):
        wpsLog.debug( " DataAnalytics RUN, time = %.3f " % time.time() )
        result_obj = {}
        try:
            if operation:
                raise Exception( "Error, processing operation with undefined kernel: " + operation )
            else:
                read_start_time = time.time()
                variable, result_obj = dataManager.loadVariable( data, region, cache_type )
                read_end_time = time.time()
                wpsLog.debug( " $$$ DATA CACHE Complete: " + str( (read_end_time-read_start_time) ) )

        except Exception, err:
            wpsLog.debug( "Exception executing timeseries process:\n " + traceback.format_exc() )
        return result_obj

    def saveVariable(self,data,dest,type="json"):
        cont = True
        while cont:
            rndm = random.randint(0,100000000000)
            fout = os.path.join(BASE_DIR,"%i.nc" % rndm)
            fjson = os.path.join(BASE_DIR,"%i.json" % rndm)
            cont = os.path.exists(fout) or os.path.exists(fjson)
        f=cdms2.open(fout,"w")
        f.write(data)
        f.close()
        out = {}
        out["url"] = "file:/"+fout
        out["id"]=data.id
        Fjson=open(fjson,"w")
        json.dump(out,Fjson)
        Fjson.close()
        dest.setValue(fjson)

    # def breakpoint(self):
    #     try:
    #         import pydevd
    #         pydevd.settrace('localhost', port=8030, stdoutToServer=False, stderrToServer=True)
    #     except: pass


if __name__ == "__main__":
    da = DataAnalytics('')
    id = 'clt'
    ds = da.loadFileFromCollection( 'MERRA/mon/atmos', id )
    v = ds[id]