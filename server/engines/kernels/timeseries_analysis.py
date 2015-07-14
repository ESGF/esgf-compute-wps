import traceback
import sys
import time
import logging
import pprint

import numpy
import numpy.ma as ma
import cdutil

from cda import DataAnalytics
from modules.utilities import get_json_arg, wpsLog


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

class TimeseriesAnalytics( DataAnalytics ):

    def __init__( self, operation ):
        DataAnalytics.__init__( self, operation )

    def compress( self, variable, precision=4 ):
        maxval = variable.max()
        minval = variable.min()
        scale = ( pow(10,precision) - 0.01 ) / ( maxval - minval )
        scaled_variable = ( variable - minval ) * scale
        return { 'range': [ minval, maxval ], 'data': scaled_variable.tolist( numpy.nan ) }

    def run( self, run_args ):
        data = get_json_arg( 'data', run_args )
        region = get_json_arg( 'region', run_args )
        result_obj = {}
        try:
            start_time = time.time()
            cdms2keyargs = self.region2cdms( region )
            variable = run_args.get( "dataSlice", None )
            if variable is None:
                url = data["url"]
                id = data["id"]
                var_cache_id =  ":".join( [url,id] )
                dataset = self.loadFileFromURL( url )
                wpsLog.debug( " $$$ Data Request: '%s', '%s' ", var_cache_id, str( cdms2keyargs ) )
                variable = dataset[ id ]
                result_obj['variable'] = record_attributes( variable, [ 'long_name', 'name', 'units' ], { 'id': id } )
                result_obj['dataset'] = record_attributes( dataset, [ 'id', 'uri' ])
            else:
                result_obj['variable'] = record_attributes( variable, [ 'long_name', 'name', 'id', 'units' ]  )

            read_start_time = time.time()
            subsetted_variable = variable(**cdms2keyargs)
            read_end_time = time.time()
            wpsLog.debug( " $$$ DATA READ Complete: " + str( (read_end_time-read_start_time) ) )

            process_start_time = time.time()
            ( result_data, time_axis ) = self.applyOperation( subsetted_variable, self.operation )
            process_end_time = time.time()
            wpsLog.debug( " $$$ DATA PROCESSING Complete: " + str( (process_end_time-process_start_time) ) )
            #            pydevd.settrace('localhost', port=8030, stdoutToServer=False, stderrToServer=True)

            if time_axis is not None:
                time_obj = record_attributes( time_axis, [ 'units', 'calendar' ] )
                time_data = time_axis.getValue().tolist()
                try:
                    time_obj['t0'] = time_data[0]
                    time_obj['dt'] = time_data[1] - time_data[0]
                except Exception, err:
                    time_obj['data'] = time_data
                result_obj['time'] = time_obj
            result_obj['data'] = result_data
            end_time = time.time()
            wpsLog.debug( " $$$ Execution complete, total time: %.2f sec", (end_time-start_time) )
        except Exception, err:
            wpsLog.debug( "Exception executing timeseries process:\n " + traceback.format_exc() )
        return result_obj

    def applyOperation( self, input_variable, operation ):
        result = None
        try:
            self.setTimeBounds( input_variable )
            operator = None
#            pydevd.settrace('localhost', port=8030, stdoutToServer=False, stderrToServer=True)
            wpsLog.debug( " $$$ ApplyOperation: %s " % str( operation ) )
            if operation is not None:
                type = operation.get('type','').lower()
                bounds = operation.get('bounds','').lower()
                op_start_time = time.clock() # time.time()
                if not bounds:
                    if type == 'departures':
                        ave = cdutil.averager( input_variable, axis='t', weights='equal' )
                        result = input_variable - ave
                    elif type == 'climatology':
                        result = cdutil.averager( input_variable, axis='t', weights='equal' )
                    else:
                        result = input_variable
                    time_axis = input_variable.getTime()
                elif bounds == 'np':
                    if   type == 'departures':
                        result = ma.anomalies( input_variable ).squeeze()
                    elif type == 'climatology':
                        result = ma.average( input_variable ).squeeze()
                    else:
                        result = input_variable
                    time_axis = input_variable.getTime()
                else:
                    if bounds == 'djf': operator = cdutil.DJF
                    elif bounds == 'mam': operator = cdutil.MAM
                    elif bounds == 'jja': operator = cdutil.JJA
                    elif bounds == 'son': operator = cdutil.SON
                    elif bounds == 'year':          operator = cdutil.YEAR
                    elif bounds == 'annualcycle':   operator = cdutil.ANNUALCYCLE
                    elif bounds == 'seasonalcycle': operator = cdutil.SEASONALCYCLE
                    if operator <> None:
                        if   type == 'departures':    result = operator.departures( input_variable ).squeeze()
                        elif type == 'climatology':   result = operator.climatology( input_variable ).squeeze()
                        else:                         result = operator( input_variable ).squeeze()
                    time_axis = result.getTime()
                op_end_time = time.clock() # time.time()
                wpsLog.debug( " ---> Base Operation Time: %.5f" % (op_end_time-op_start_time) )
            else:
                result = input_variable
                time_axis = input_variable.getTime()

            if isinstance( result, float ):
                result_data = [ result ]
            elif result is not None:
                if result.__class__.__name__ == 'TransientVariable':
                    result = ma.masked_equal( result.squeeze().getValue(), input_variable.getMissing() )
                result_data = result.tolist( numpy.nan )
            else: result_data = None
        except Exception, err:
            wpsLog.debug( "Exception applying Operation '%s':\n %s" % ( str(operation), traceback.format_exc() ) )
            return ( None, None )
        return (input_variable, input_variable.getTime()) if result is None else ( result_data, time_axis )

    def setTimeBounds( self, var ):
        time_axis = var.getTime()
        if time_axis._bounds_ == None:
            try:
                time_unit = time_axis.units.split(' since ')[0].strip()
                if time_unit == 'hours':
                    values = time_axis.getValue()
                    freq = 24/( values[1]-values[0] )
                    cdutil.setTimeBoundsDaily( time_axis, freq )
#                    cdutil.setTimeBoundsDaily( time_axis )
                elif time_unit == 'days':
                    cdutil.setTimeBoundsDaily( time_axis )
                elif time_unit == 'months':
                    cdutil.setTimeBoundsMonthly( time_axis )
                elif time_unit == 'years':
                    cdutil.setTimeBoundsYearly( time_axis )
            except Exception, err:
                wpsLog.debug( "Exception in setTimeBounds:\n " + traceback.format_exc() )

if __name__ == "__main__":
    wpsLog.addHandler( logging.StreamHandler(sys.stdout) ) #logging.FileHandler( os.path.abspath( os.path.join(os.path.dirname(__file__), '..', 'logs', 'wps.log') ) ) )
    wpsLog.setLevel(logging.DEBUG)
    pp = pprint.PrettyPrinter(indent=4)

    variables = [ { 'url': 'file://usr/local/web/data/MERRA/u750/merra_u750.xml', 'id': 'u' },
                  { 'url': 'file://usr/local/web/data/MERRA/MERRA100.xml', 'id': 't' },
                  { 'url': 'file://usr/local/web/data/MERRA/u750/merra_u750.nc', 'id': 'u' },
                  { 'url': 'file://usr/local/web/data/MERRA/u750/merra_u750_1979_1982.nc', 'id': 'u' },
                  { 'url': 'file://usr/local/web/WPCDAS/data/TestData.nc', 'id': 't' } ]
    var_index = 4
    region    = { 'latitude': -18.2, 'longitude': -134.6 }
    operations = [ { 'type': '', 'bounds': 'annualcycle' },
                   { 'type': 'departures', 'bounds': '' },
                   { 'type': 'climatology', 'bounds': 'annualcycle' },
                   { 'type': 'climatology', 'bounds': '' },
                   { 'type': 'departures', 'bounds': 'np' },
                   { 'type': 'climatology', 'bounds': 'np' },
                   { 'type': '', 'bounds': '' } ]
    operation_index = 0

    processor = TimeseriesAnalytics( operations[operation_index]  )
    result = processor.run( { 'data':variables[var_index], 'region': region } )
    print "\n ---------- Result: ---------- "
    pp.pprint(result)
