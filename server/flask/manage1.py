import flask, logging, sys, json
from request.manager import taskManager
from flask.ext.cors import CORS
app = flask.Flask(__name__)
CORS(app)
app.logger.addHandler( logging.StreamHandler(sys.stdout) )
app.logger.setLevel(logging.DEBUG)

@app.route("/cdas/")
def cdas():
    response = taskManager.processRequest( flask.request.args )
    return response

    #    resp = flask.make_response( json.dumps(result), 200 )

if __name__ == "__main__":
    app.run()
