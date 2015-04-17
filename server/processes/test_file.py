#import cdms2, cdutil
import os

if __name__ == "__main__":
    print os.path.abspath( os.path.join(os.path.dirname(__file__), '..', '..', 'logs', 'wps.log') )
    exit(1)   
    dataset = cdms2.open( '/usr/local/cds/web/data/MERRA/Temp2D/MERRA_3Hr_Temp.xml' )
    variable = dataset['t']
    subsetted_variable = variable()
    cdutil.setTimeBoundsDaily( subsetted_variable.getTime(), 8 )
    departures = cdutil.ANNUALCYCLE.climatology( subsetted_variable )
    print str( departures )
