import flask, time
from request.manager import taskManager
from modules.utilities import wpsLog, DebugLogger
from flask.ext.cors import CORS
app = flask.Flask(__name__)
CORS(app)
dlog = DebugLogger('flask')
#app.logger.addHandler( logging.StreamHandler(sys.stdout) )
#app.logger.setLevel(logging.DEBUG)

@app.route("/cdas/")
def cdas():
    request_parms = dict(flask.request.args)
    t0 = time.time()
    dlog.log( "NEW Flask request: %s" %  str(request_parms) )
    response = taskManager.processRequest( request_parms )
    dlog.log( "END Flask request, response time = %.2f" % (time.time()-t0) )
    return response

    #    resp = flask.make_response( json.dumps(result), 200 )

if __name__ == "__main__":
    app.run()
