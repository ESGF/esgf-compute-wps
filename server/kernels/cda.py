import os
import json
import random, traceback
import cdms2
from modules.utilities import *
from modules.containers import *

cdms2.setNetcdfShuffleFlag(0)
cdms2.setNetcdfDeflateFlag(0)
cdms2.setNetcdfDeflateLevelFlag(0)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'output'))

class DatasetContainer(JSONObjectContainer):

    def process_spec( self, spec ):
        if spec:
            spec = convert_json_str( spec )
            if isinstance( spec, dict ):
                if ('name' in spec.keys()) or ('id' in spec.keys()) or ('collection' in spec.keys()):
                    self._objects.append( self.newObject( spec) )
                else:
                    keys = []
                    for item in spec.iteritems():
                        collection = item[0]
                        varlist = item[1]
                        if isinstance(varlist,basestring): varlist = [ varlist ]
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
        self.spec = str(spec)

    def process_spec( self, **args ):
        toks = self.spec.split('(')
        fn_args = toks[1].strip(')').split(',')
        task = toks[0].split('.')
        self.items = { 'kernel': task[0], 'method': task[1] }
        inputs = []
        for fn_arg in fn_args:
            if ':' in fn_arg:
                arg_items = fn_arg.split(':')
                self.items[ arg_items[0] ]  = arg_items[1]
            else:
                inputs.append( fn_arg )
        self.items[ 'inputs' ]  = inputs

class CDASKernel:

    def __init__( self, name,  **args  ):
        self.name = name
        self.use_cache = args.get( 'cache', True )


    def run( self, subsetted_variables, metadata_recs, region, operation ):
        pass

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
