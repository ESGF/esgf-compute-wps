import flask, logging, sys, json, time
from staging import stagingRegistry
from modules import configuration
from modules.utilities import  wpsLog
from flask.ext.cors import CORS
app = flask.Flask(__name__)
CORS(app)
app.logger.addHandler( logging.StreamHandler(sys.stdout) )
app.logger.setLevel(logging.DEBUG)

def get_request_datainputs():
    inputs = {}
    inputs_str = flask.request.args.get('datainputs','')
    if inputs_str:
        lines = inputs_str.split(';')
        for line in lines:
            items = line.split('=')
            if len( items ) > 1:
                inputs[ str(items[0]).strip(" []") ] = json.loads( items[1] )
    return inputs


@app.route("/cdas/")
def cdas():
    task_run_args = get_request_datainputs()

    data = task_run_args['data']
    region = task_run_args['region']
    operation = task_run_args['operation']
    wpsLog.debug( " $$$ CDAS Process: DataIn='%s', Domain='%s', Operation='%s', ---> Time=%.3f " % ( str( data ), str( region ), str( operation ), time.time() ) )
    t0 = time.time()
    handler = stagingRegistry.getInstance( configuration.CDAS_STAGING  )
    if handler is None:
        wpsLog.warning( " Staging method not configured. Running locally on wps server. " )
        handler = stagingRegistry.getInstance( 'local' )
    result_obj =  handler.execute( { 'data':data, 'region':region, 'operation':operation, 'engine': configuration.CDAS_COMPUTE_ENGINE + "Engine"  } )
    wpsLog.debug( " $$$*** CDAS Process (response time: %.3f sec):\n Result='%s' " %  ( (time.time()-t0), str(result_obj) ) )
    result_json = json.dumps( result_obj )

#    resp = flask.make_response( json.dumps(result), 200 )
    return result_json


if __name__ == "__main__":
    app.run()
