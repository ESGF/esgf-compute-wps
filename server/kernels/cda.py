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

def getRawValue( metadata, key, default ):
    if isinstance( metadata, list ): metadata = metadata[0]
    value = metadata.get( key, default )
    if isinstance( value, list ): value = value[0]
    return value

def getBoolValue( metadata, key, default ):
    value = getRawValue( metadata, key, default )
    return ( value.lower()[0] == 't' ) if isinstance( value, str ) else bool(value)

def getFloatValue( metadata, key, default ):
    return float( getRawValue( metadata, key, default ) )

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

    def applyOperation( self, input_variables, operation ):
        wpsLog.error( "Unimplemented applyOperation method in class %s" + self.__class__.__name__ )

    def run( self, subsetted_variables, metadata_recs, operation ):
        result_obj = None
        try:
            start_time = time.time()
            self.setBounds(subsetted_variables)
            result, result_mdata  = self.applyOperation( subsetted_variables, operation )
            result_obj = self.getResultObject( metadata_recs, result, result_mdata  )
            end_time = time.time()
            wpsLog.debug( "Computed operation %s: time = %.4f" % ( str(operation), (end_time-start_time) ) )
        except Exception, err:
            wpsLog.debug( "Exception executing timeseries process:\n " + traceback.format_exc() )
        return result_obj

    def setBounds(self, input_variables ):
        for input_variable in input_variables:
            self.setTimeBounds( input_variable )

    def saveVariable(self, data, varname ):
        data_file = os.path.join( configuration.CDAS_OUTGOING_DATA_DIR, varname )
        data_url = configuration.CDAS_OUTGOING_DATA_URL + varname
        f=cdms2.open(data_file,"w")
        f.write(data)
        f.close()
        wpsLog.debug( "Saved result to file: '%s', url: '%s' " % ( data_file, data_url ) )
        return data_url

    def toList( self, result, missing=None ):
        if isinstance( result, float ):
            result_data = [ result ]
        elif result is not None:
            if result.__class__.__name__ == 'TransientVariable':
                if missing is None: missing = result.getMissing()
                result = numpy.ma.masked_equal( result.squeeze().getValue(), missing )
            result_data = result.tolist( numpy.nan )
        else:
            result_data = None
        return result_data

    def getResultObject(self, request_mdatas, result, result_mdata ):
        request_mdata = request_mdatas[0]
        embedded = getBoolValue( request_mdata,'embedded', False )
        if embedded:
            vstat = request_mdata['domain_spec'].vstat
            missing = getFloatValue( vstat,'missing', None )
            if type( result ) in ( list, tuple, dict, float, int ):
                result_data = result
                time_axis = result_mdata['time']
            else:
                time_axis = result.getTime()
                result_data = self.toList( result, missing )

            if time_axis is not None:
                try:
                    time_obj = record_attributes( time_axis, [ 'units', 'calendar' ] )
                    time_data = time_axis.getValue().tolist()
                    time_obj['t0'] = time_data[0]
                    time_obj['dt'] = time_data[1] - time_data[0]
                except Exception, err:
                    time_obj['data'] = time_data
                request_mdata['time'] = time_obj
            request_mdata['data'] = result_data
        else:
            request_mdata['result.url'] = self.saveVariable( result, request_mdata['result_name'] )
        return request_mdata

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
