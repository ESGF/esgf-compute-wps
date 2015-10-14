import os
import json
import random, traceback
import cdms2, cdutil, numpy
from modules.utilities import *
from modules.containers import *
from modules import configuration

cdms2.setNetcdfShuffleFlag(0)
cdms2.setNetcdfDeflateFlag(0)
cdms2.setNetcdfDeflateLevelFlag(0)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'output'))

class Aliases:

    def __init__( self, keys, aliases ):
        self.keys = keys
        self.aliases = aliases

    def hasKeys( self, key_map ):
        for key in key_map:
            if key in self.keys: return True
        return False

    def insertAliases(self, key_map, clear_old=False ):
        for key_alias, alt_key in self.aliases.items():
            if key_alias in key_map:
                key_map[ alt_key ] = key_map[ key_alias ]
                if clear_old: del key_map[ key_alias ]


class DatasetContainer(JSONObjectContainer):

    DataAliases = Aliases( [ 'name', 'id', 'collection', 'dset', 'dataset', 'url' ], { 'dset':'collection', 'dataset':'collection' } )

    def process_spec( self, specs ):
        if specs:
            specs = convert_json_str( specs )
            if isinstance( specs, dict ): specs = [ specs ]
            for spec in specs:
                if isinstance( spec, dict ):
                    if self.DataAliases.hasKeys( spec ):
                        self.DataAliases.insertAliases( spec )
                        var_id = spec.get('id',None)
                        if var_id is not None:
                            tokens = var_id.split(':')
                            spec['id'] = tokens[0]
                            if spec.get( 'name', None ) is None:
                                spec['name'] = tokens[-1]
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


    def run( self, subsetted_variables, metadata_recs, operation ):
        pass

    def saveVariable(self, data ):
        varname = "var-%d.nc"%int(10*time.time())
        data_file = os.path.join( configuration.CDAS_OUTGOING_DATA_DIR, varname )
        data_url = configuration.CDAS_OUTGOING_DATA_URL + varname
        f=cdms2.open(data_file,"w")
        f.write(data)
        f.close()
        return data_url

    def toList( self, result, missing ): # input_variable.getMissing()
        time_axis = result.getTime()
        if isinstance( result, float ):
            result_data = [ result ]
        elif result is not None:
            if result.__class__.__name__ == 'TransientVariable':
                result = numpy.ma.masked_equal( result.squeeze().getValue(), result.getMissing() )
            result_data = result.tolist( numpy.nan )
        else:
            result_data = None
        return result_data, time_axis

    def getEmbedded( self, metadata ):
        if isinstance( metadata, list ): metadata = metadata[0]
        embedded = metadata.get('embedded',['f'])[0]
        return ( embedded.lower()[0] == 't' )

    def getResultObject(self, metadata_recs, result ):
        result_mdata = metadata_recs[0]
        embedded = self.getEmbedded( result_mdata )
        if embedded:
            result_data, time_axis  = self.toList( result )

            if time_axis is not None:
                try:
                    time_obj = record_attributes( time_axis, [ 'units', 'calendar' ] )
                    time_data = time_axis.getValue().tolist()
                    time_obj['t0'] = time_data[0]
                    time_obj['dt'] = time_data[1] - time_data[0]
                except Exception, err:
                    time_obj['data'] = time_data
                result_mdata['time'] = time_obj
            result_mdata['data'] = result_data
        else:
            result_mdata['result.url'] = self.saveVariable( result )
        return result_mdata

    def setTimeBounds( self, var ):
        time_axis = var.getTime()
        if time_axis.bounds is None:
            try:
                time_unit = time_axis.units.split(' since ')[0].strip()
                if time_unit == 'hours':
                    values = time_axis.getValue()
                    freq = 24/( values[1]-values[0] )
                    cdutil.setTimeBoundsDaily( time_axis, freq )
                elif time_unit == 'days':
                    cdutil.setTimeBoundsDaily( time_axis )
                elif time_unit == 'months':
                    cdutil.setTimeBoundsMonthly( time_axis )
                elif time_unit == 'years':
                    cdutil.setTimeBoundsYearly( time_axis )
            except Exception, err:
                wpsLog.debug( "Exception in setTimeBounds:\n " + traceback.format_exc() )
