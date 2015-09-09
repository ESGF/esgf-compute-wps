import flask, logging, sys, json
from request.manager import taskManager
from modules.utilities import wpsLog
from flask.ext.cors import CORS
app = flask.Flask(__name__)
CORS(app)
#app.logger.addHandler( logging.StreamHandler(sys.stdout) )
#app.logger.setLevel(logging.DEBUG)

@app.route("/cdas/")
def cdas():
    request_parms = dict(flask.request.args)
    wpsLog.debug( "FLASK request_parms: %s" % str(request_parms))
    response = taskManager.processRequest( request_parms )
    return response

    #    resp = flask.make_response( json.dumps(result), 200 )

if __name__ == "__main__":
    app.run()
