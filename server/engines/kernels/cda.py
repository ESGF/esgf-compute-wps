import os
import json
import random, traceback
import cdms2
from modules.utilities import *
from datacache.manager import dataManager

cdms2.setNetcdfShuffleFlag(0)
cdms2.setNetcdfDeflateFlag(0)
cdms2.setNetcdfDeflateLevelFlag(0)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'output'))

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


    def run( self, run_args ):
        wpsLog.debug( " DataAnalytics RUN, time = %.3f " % time.time() )
        data = get_json_arg( 'data', run_args )
        region = get_json_arg( 'region', run_args )
        operation = get_json_arg( 'operation', run_args )
        result_obj = {}
        try:
            if operation:
                raise Exception( "Error, processing operation with undefined kernel: " + operation )
            else:
                read_start_time = time.time()
                vardata = run_args.get( "dataSlice", None )
                if vardata is not None: data['variable'] = vardata
                if region:
                    cdms2keyargs = region2cdms( region )
                    data['region'] = cdms2keyargs
                variable, result_obj = dataManager.loadVariable( cache=True, **data )
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