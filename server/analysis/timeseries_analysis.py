import os, traceback, sys
import time, pydevd
import cdms2, logging, pprint
import numpy
import cdutil
cdms2.setNetcdfShuffleFlag(0)
cdms2.setNetcdfDeflateFlag(0)
cdms2.setNetcdfDeflateLevelFlag(0)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'output'))
wpsLog = logging.getLogger('wps')
from cda import DataAnalytics

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

    def __init__( self, variable ):
        self.variable = variable

    def compress( self, variable, precision=4 ):
        maxval = variable.max()
        minval = variable.min()
        scale = ( pow(10,precision) - 0.01 ) / ( maxval - minval )
        scaled_variable = ( variable - minval ) * scale
        return { 'range': [ minval, maxval ], 'data': scaled_variable.tolist( numpy.nan ) }

    def execute( self, operation, domain ):
        result_obj = {}
        try:
            start_time = time.time()
            cdms2keyargs = self.domain2cdms(domain)
            self.operation = operation
            url = self.variable["url"]
            id = self.variable["id"]
            var_cache_id =  ":".join( [url,id] )
            dataset = self.loadFileFromURL( url )
            wpsLog.debug( " $$$ Data Request: '%s', '%s' ", var_cache_id, str( cdms2keyargs ) )
            variable = dataset[ id ]
            wpsLog.debug( " $$$ Starting DATA READ" )
            read_start_time = time.time()
            subsetted_variable = variable(**cdms2keyargs)
            read_end_time = time.time()
            wpsLog.debug( " $$$ DATA READ Complete: " + str( (read_end_time-read_start_time) ) )

            process_start_time = time.time()
            result_variable = self.applyOperation( subsetted_variable, operation )
            result_data = result_variable.squeeze().tolist( numpy.nan )
            time_axis = result_variable.getTime()
            process_end_time = time.time()
            wpsLog.debug( " $$$ DATA PROCESSING Complete: " + str( (process_end_time-process_start_time) ) )
            #            pydevd.settrace('localhost', port=8030, stdoutToServer=False, stderrToServer=True)

            result_obj['variable'] = record_attributes( variable, [ 'long_name', 'name', 'units' ], { 'id': id } )
            result_obj['dataset'] = record_attributes( dataset, [ 'id', 'uri' ])
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
            timings = [ (end_time-start_time), (read_end_time-read_start_time), (process_end_time-process_start_time) ]
            result_obj['timings'] = timings
            wpsLog.debug( " $$$ Execution complete, total time: %.2f sec\n -------------- Result : \n %s ", timings[0],  str(result_obj) )
        except Exception, err:
            wpsLog.debug( "Exception executing timeseries process:\n " + traceback.format_exc() )
        return result_obj

    def applyOperation( self, input_variable, operation ):
        self.setTimeBounds( input_variable )
        result = None
        operator = None
        if operation is not None:
            type   = operation.get('type','').lower()
            bounds = operation.get('bounds','').lower()
            if   bounds == 'djf': operator = cdutil.DJF
            elif bounds == 'mam': operator = cdutil.MAM
            elif bounds == 'jja': operator = cdutil.JJA
            elif bounds == 'son': operator = cdutil.SON
            elif bounds == 'year':          operator = cdutil.YEAR
            elif bounds == 'annualcycle':   operator = cdutil.ANNUALCYCLE
            elif bounds == 'seasonalcycle': operator = cdutil.SEASONALCYCLE
            if operator <> None:
                if   type == 'departures':    result = operator.departures( input_variable )
                elif type == 'climatology':   result = operator.climatology( input_variable )
        return input_variable if result is None else result

    def setTimeBounds( self, var ):
        time_axis = var.getTime()
        if time_axis._bounds_ == None:
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

if __name__ == "__main__":
    wpsLog.addHandler( logging.StreamHandler(sys.stdout) ) #logging.FileHandler( os.path.abspath( os.path.join(os.path.dirname(__file__), '..', 'logs', 'wps.log') ) ) )
    wpsLog.setLevel(logging.DEBUG)
    pp = pprint.PrettyPrinter(indent=4)

    variable1  = { 'url': 'file://usr/local/web/data/MERRA/u750/merra_u750.xml', 'id': 'u' }
    variable2  = { 'url': 'file://usr/local/web/data/MERRA/MERRA100.xml', 'id': 't' }
    domain    = { 'latitude': -18.2, 'longitude': -134.6 }
    operation = { 'type': 'departures', 'bounds': 'annualcycle' }

    processor = TimeseriesAnalytics( variable2 )
    result = processor.execute( operation, domain )
    print "\n ---------- Result: ---------- "
    pp.pprint(result)
