from pywps.Process import WPSProcess
import os
import json
import cdms2
import cdutil
cdms2.setNetcdfShuffleFlag(0)
cdms2.setNetcdfDeflateFlag(0)
cdms2.setNetcdfDeflateLevelFlag(0)
import random
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'output'))
from esgfcwtProcess import esgfcwtProcess
from PyOphidia import client, cube
import ConfigParser
# Path where output will be stored/cached
wps_config = ConfigParser.ConfigParser()
wps_config.read(os.path.join(os.path.dirname(__file__),"..","wps.cfg"))



class Process(esgfcwtProcess):

    def __init__(self):
        """Process initialization"""
        WPSProcess.__init__(self, identifier=os.path.split(__file__)[-1].split('.')[0], title='ophidia_averager', version=0.1, abstract='Average a variable over a (many) dimension', storeSupported='true', statusSupported='true')

        #TODO add Ophidia specific support (ncores, etc)
        self.dataIn = self.addComplexInput(identifier='variable', title='Variable to average', formats=[{'mimeType': 'text/json'}], minOccurs=1, maxOccurs=1)
        self.axes = self.addLiteralInput(identifier = "axes", type=str,title = "Axes to average over", default = 'time')
        self.outputformat = self.addLiteralInput(identifier = "outputformat", type=str,title = "Output Format", default = 'opendap')
        # TODO application/netcdf ???
        self.output = self.addComplexOutput(identifier='output', title='averaged variable', formats=[{'mimeType': 'text/json'},{'mimeType': "application/netcdf"}, {"mimeType":"image/png"}])

    def execute(self):

        #Even though this is on aims2 or any machine, IP of Ophidia probably should still be 127.0.0.1
        oph_ip = "127.0.0.1"

        #The location of the file, either as a path of a local file or OpenDAP URL
        #Obtained from the URL
        filepath = self.loadData()[0]['uri']
        #If 'file:/' is in filepath, remove it because it messes stuff up
        filepath = filepath.replace('file:/','')

        #Axis to average over, which is also obtained from the URL
        axes = self.axes.getValue()

        #Connect to the Ophidia server
        #Arguments are: username on Ophidia server, password, IP, port
        ophclient = client.Client("oph-test","llnlaims",oph_ip,"11732")


        #Create a new container
        #The name of the container must be different each time
        container_name = 'test-' + str(random.random())
        ophclient.submit("oph_createcontainer container=%s;dim=lat|lon|time;dim_type=double|double|double;hierachy=oph_base|oph_base|oph_time;units=d;base_time=1850-01-01;"%container_name)


        #import the file located at filepath into the container with name container_name
        ophclient.submit('oph_importnc3 measure=tas;src_path=%s;container=%s;imp_dim=time;ncores=4;'%(filepath,container_name))
        #TODO add below line in final version. Probably should add option to get user value for ncores.
        #ophclient.submit('oph_importnc3 measure=tas;src_path=%s;container=%s;imp_dim=%s;ncores=4;'%(filepath,container_name,axes))

        #These two lines are used to reference the last response, which is idenitified
        # by a PID. Specifically, reference the container that had something imported into it,
        # one command ago.
        json_data = json.loads(ophclient.last_response)
        raw_pid_container = json_data["response"][0]["objcontent"][0]["message"]
        #actually get the average. Note: oph_reduce2 and oph_reduce3 might increase performance
        ophclient.submit("oph_reduce cube=%s;operation=avg;ncores=4;"%str(raw_pid_container))

        json_data = json.loads(ophclient.last_response)
        raw_pid_output_cube = json_data["response"][0]["objcontent"][0]["message"]
        #export the output cube as a file
        ophclient.submit("oph_exportnc2 cube=%s;"%str(raw_pid_output_cube))

        #Get the location of the cube on the server
        json_data = json.loads(ophclient.last_response)
        cube_location = json_data["response"][0]["objcontent"][0]["message"]


    	self.saveVariable(None, self.output, oph_loc=cube_location, type="ophidia")
    	return
