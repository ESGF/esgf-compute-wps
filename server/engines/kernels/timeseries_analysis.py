import traceback
import sys
import time
import logging
import pprint

import numpy, math
import numpy.ma as ma
import cdutil, cdms2

from cda import DataAnalytics
from modules.utilities import *

class TimeseriesAnalytics( DataAnalytics ):

    def __init__( self, **args ):
        DataAnalytics.__init__( self, **args  )

    def compress( self, variable, precision=4 ):
        maxval = variable.max()
        minval = variable.min()
        scale = ( pow(10,precision) - 0.01 ) / ( maxval - minval )
        scaled_variable = ( variable - minval ) * scale
        return { 'range': [ minval, maxval ], 'data': scaled_variable.tolist( numpy.nan ) }

    def run( self, data, region, operations ):
        wpsLog.debug( " TimeseriesAnalytics RUN, time = %.3f " % time.time() )
        if not isinstance(operations, (list, tuple)): operations = [ operations ]
        results = []
        try:
            [ subsetted_variable ] = data['variables']
            for operation in operations:
                start_time = time.time()
                result_obj = dict( data['result'] )
                ( result_data, time_axis ) = self.applyOperation( subsetted_variable, operation )
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
                results.append( result_obj )
                end_time = time.time()
                wpsLog.debug( " $$$ Execution complete, total time: %.2f sec [%.2f]", (end_time-start_time), end_time )

        except Exception, err:
            wpsLog.debug( "Exception executing timeseries process:\n " + traceback.format_exc() )

        return results

    def applyOperation( self, input_variable, operation ):
        result = None
        try:
            self.setTimeBounds( input_variable )
            operator = None
            time_axis = None
#            pydevd.settrace('localhost', port=8030, stdoutToServer=False, stderrToServer=True)
            wpsLog.debug( " $$$ ApplyOperation: %s to variable shape %s" % ( str( operation ), str(input_variable.shape) ) )
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
                    if bounds == 'djf':
                        operator = cdutil.DJF
                    elif bounds == 'mam':
                        operator = cdutil.MAM
                    elif bounds == 'jja':
                        operator = cdutil.JJA
                    elif bounds == 'son':
                        operator = cdutil.SON
                    elif bounds == 'year':
                        operator = cdutil.YEAR
                    elif bounds == 'annualcycle':
                        operator = cdutil.ANNUALCYCLE
                    elif bounds == 'seasonalcycle':
                        operator = cdutil.SEASONALCYCLE
                    if operator <> None:
                        if   type == 'departures':
                            result = operator.departures( input_variable ).squeeze()
                        elif type == 'climatology':
                            result = operator.climatology( input_variable ).squeeze()
                            if bounds == 'annualcycle':
                                time_axis = cdms2.createAxis( range( len(result) ) )
                                time_axis.units = "months"
                            elif bounds == 'seasonalcycle':
                                time_axis = cdms2.createAxis( range( len(result) ) )
                                time_axis.units = "seasons"
                        else:
                            result = operator( input_variable ).squeeze()
                    if time_axis is None:
                        time_axis = result.getTime()
                op_end_time = time.clock() # time.time()
                wpsLog.debug( " ---> Base Operation Time: %.5f, result = %s " % ( op_end_time-op_start_time, str(result) ) )
                # if math.isnan( result[0] ):
                #     pp = pprint.PrettyPrinter(indent=4)
                #     print "\n ---------- NaN in Result, Input: ---------- "
                #     print str( input_variable.data )
            else:
                result = input_variable
                time_axis = input_variable.getTime()

            if isinstance( result, float ):
                result_data = [ result ]
            elif result is not None:
                if result.__class__.__name__ == 'TransientVariable':
                    result = ma.masked_equal( result.squeeze().getValue(), input_variable.getMissing() )
                result_data = result.tolist( numpy.nan )
            else:
                result_data = None
                time_axis = input_variable.getTime()
        except Exception, err:
            wpsLog.debug( "Exception applying Operation '%s':\n %s" % ( str(operation), traceback.format_exc() ) )
            return ( None, None )

        if time_axis is not None:
            units = time_axis.units.split()
            if( len(units) == 3 ) and ( units[1] == 'since' ):
                newunits = "%s since 1970-1-1" % units[0]
                time_axis.toRelativeTime(newunits)
        rv = input_variable if result is None else result_data
        return ( rv, time_axis )

    def setTimeBounds( self, var ):
        time_axis = var.getTime()
        if time_axis._bounds_ is None:
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

