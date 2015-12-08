from request.api.dialects import WPSDialect
from modules.utilities import wpsLog, convert_json_str
import json

class CDASDialect( WPSDialect ):

    def __init__(self):
        WPSDialect.__init__(self,'cdas')

if __name__ == "__main__":

    cd = CDASDialect()
    request = { 'datainputs': [u'[region={"level":"100000"};data={"collection":"MERRA/mon/atmos","id":"hur"};]'] }
    result = cd.getTaskRequestData( request )
    print result