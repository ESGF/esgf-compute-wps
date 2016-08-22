from pywps.Process import WPSProcess
import os,numpy,sys
import logging, json
import cdms2
import random
from pywps import config
import ConfigParser
# Path where output will be stored/cached

cdms2.setNetcdfShuffleFlag(0) ## where value is either 0 or 1
cdms2.setNetcdfDeflateFlag(0) ## where value is either 0 or 1
cdms2.setNetcdfDeflateLevelFlag(0) ## where value is a integer between 0 and 9 included

wps_config = ConfigParser.ConfigParser()
wps_config.read(os.path.join(os.path.dirname(__file__),"..","wps.cfg"))
try:
    DAP_DATA = wps_config.get("dapserver","dap_data")
except:
    warnings.warn("Could not READ DAP_DATA from wps.cfg will store files in /tmp")
    DAP_DATA = "/tmp"
try:
    DAP_INI = wps_config.get("dapserver","dap_ini")
except:
    DAP_INI = None
try:
    DAP_HOST = wps_config.get("dapserver","dap_host")
except:
    DAP_HOST = None
try:
    DAP_PORT = wps_config.get("dapserver","dap_port")
except:
    DAP_PORT = None

if DAP_INI is not None:
    with open(DAP_INI) as dapini:
        dapconfig = dapini.read()
        if DAP_HOST is None:
            DAP_HOST = dapconfig.split("host")[1].split()[1]
        if DAP_PORT is None:
            DAP_PORT = dapconfig.split("port")[1].split()[1]

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'output'))
wpsLog = logging.getLogger( 'wps' )

def loadValue( wpsInput ):
    valueHandle = wpsInput.getValue()
    if valueHandle:
        f=open(valueHandle)
        return f.read()
    return ""

class esgfcwtProcess(WPSProcess):

    def saveVariable(self,data,dest,type=None):
        cont = True
        while cont:
            rndm = random.randint(0,100000000000)
            fout = os.path.join(DAP_DATA,"%i.nc" % rndm)
            fjson = os.path.join(BASE_DIR,"%i.json" % rndm)
            cont = os.path.exists(fout) or os.path.exists(fjson)
        cdms2.setNetcdf4Flag(False)

       #Writing data to fout is not needed when using Ophidia because:
	   # 1. calling this function via ophidia_averager has no value for data
	   # 2. nothing needs to be written because Ophidia already created the file
	   if type!= "ophidia":
            f=cdms2.open(fout,"w")
            f.write(data)
            f.close()

        if type is None:  # not user specified
            type = self.outputformat.getValue()
        if type == "opendap":
            out = {}
            out["uri"] = "http://%s:%s/%s" % (DAP_HOST,DAP_PORT, os.path.split(fout)[1])
            out["id"]=data.id
            Fjson=open(fjson,"w")
            json.dump(out,Fjson)
            Fjson.close()
            dest.format = {"mimetype":"text/json",'encoding':"utf-8",'schema':""}
            dest.setValue(fjson)
        elif type == "ophidia":
            #Dont need fout because it's used in the uri of out variable, and we already know it
            out = {}
            out["uri"] = oph_loc
            #out["uri"] = "http://%s:%s/%s" % (DAP_HOST,DAP_PORT, oph_loc.replace('SOMETHING'.'SOMETHING'))
            #out["uri"] = oph_loc.replace('http://aims2.llnl.gov', 'localhost')
            out["id"] = self.getVariableName(self.loadData()[0])
            Fjson=open(fjson,"w")
            json.dump(out,Fjson)
            Fjson.close()
            dest.format = {"mimetype":"text/json",'encoding':"utf-8",'schema':""}
            dest.setValue(fjson)
        elif type == "netcdf":
            dest.format = {"mimetype":"application/netcdf",'encoding':"utf-8",'schema':""}
            dest.setValue(fout)
        elif type == "png":
            import vcs
            x=vcs.init()
            x.plot(data,bg=False)
            x.png(fjson)
            dest.format = {"mimetype":"image/png",'encoding':"base64",'schema':""}
            dest.setValue(fjson+".png")
        else:
            raise "UNKnwon format"


    def breakpoint(self):
        try:
            import pydevd
            pydevd.settrace('localhost', port=8030, stdoutToServer=False, stderrToServer=True)
        except:
            pass

    def loadOperation(self,origin=None):
        if origin is None:
            origin = self.operation
        if origin is None: return None
        opStr = origin.getValue()
        if opStr:
            op_json = open(opStr).read().strip()
            if op_json: return json.loads(op_json)
        return None

    def loadFileFromURI(self,uri):
        if uri[:7].lower()=="file://":
            f=cdms2.open(uri[6:])
        elif uri[:13].lower() == "collection://":
            # TODO @ThomasMaxwell please add code here
            f = None
        elif uri[:7].lower() == "esgf://":
            # TODO placeholder we don't know wht this will be yet
            f = None
        else:  # cdms2 knows how to open http://
            f=cdms2.open(uri)
        return f

    def getVariableName(self,variable):
        # "var1:clt" clt is real name var1 is used for expressions
        return variable["id"].split(":")[-1]

    def loadVariables(self,variables):
        domains = self.loadDomain()
        out = []
        for variable in variables:
            out.append(self.loadVariable(variable,domains))
        return out

    def loadVariable(self,variable,domains=[]):
        if len(domains)==0:
            domains = self.loadDomain()
        cdms2keyargs = self.domain2cdms(variable.get("domain",None),domains)
        f=self.loadFileFromURI(variable["uri"])
        var = self.getVariableName(variable)
        data = f(var,**cdms2keyargs)
        return data,cdms2keyargs

    def loadData(self,origin=None):
        if origin is None:
            origin = self.dataIn
        if origin is None: return None
        dataFiles = origin.getValue()
        dataIn = []
        if isinstance(dataFiles,basestring):
            dataFiles = [dataFiles,]
        for fnm in dataFiles:
            f=open(fnm)
            dataIn.append(self.loadJSONVariable(f.read()))
        return dataIn

    def loadJSONVariable(self,data):
        """loads in data, right now can only be json but i guess could have to determine between json and xml"""
        return json.loads(data)

    def loadDomain(self,origin=None):
        if origin is None:
            origin = self.domain
        if origin is None: return None
        domain = origin.getValue()
        f=open(domain)
        domains = json.loads(f.read())
        if not isinstance(domains,(list,tuple)):
            domains = [domains,]
        return domains

    def domain2cdms(self,domain,domains):
        kargs = {}
        if domain is None:
            # datInput did not provide anything
            return {}
        elif isinstance(domain,basestring):
            for d in domains:
                if d["id"]==domain:
                    domain = d
                    break

        for kw,definition in domain.iteritems():
            if kw not in ["id","version"]:
                crs = definition.get("crs","values").lower()
                if crs in ["values","epsg:4326"]:
                    start = definition["start"]
                    end = definition["end"]
                    if isinstance(start,basestring):
                        start = str(start)  # cdms2 does not like unicode
                    if isinstance(end,basestring):
                        end = str(end)  # cdms2 does not like unicode
                    val = (start,end)
                    cdms_selection = definition.get("cdms_selection","")
                    if cdms_selection != "":
                        val.append(cdms_selection)
                elif crs == "indices":
                    step = defintion.get("step",None)
                    val = slice(definition["start"],definition["end"],step)
                kargs[str(kw)] = val
        return kargs


def setEnv( key, value = None ):
    current_value = os.getenv( key )
    if value == None: value = current_value
    else: os.putenv( key, value )
    os.environ[key] = value


def updateEnv( envs ):
    for key in envs.keys():
        try:
            value = config.getConfigValue("cds",key)
            setEnv( envs[key], value )
            logging.info("CDS environment variable %s set to %s" % ( key, value ) )
        except :
            logging.info("Error setting CDS environment variable %s to %s" % ( key, envs[key]) )
            pass

def importEnvironmentVariable( env_var ):
    try:
        value = config.getConfigValue( "cds", env_var )
    except:
        value = os.getenv( env_var )
    if value <> None:
        os.environ[ env_var ] = value
        logging.info("CDS environment variable %s set to %s" % ( env_var, value ) )

def setEnvVariable( key, env_var ):
    try:
        value = config.getConfigValue( "cds", key )
        os.environ[ env_var ] = value
        setEnv( env_var, value )
        logging.info("CDS environment variable %s set to %s" % ( env_var, value ) )
        for identifier in [ 'path', 'library' ]:
            if identifier in env_var.lower():
                for path in value.split(':'):
                    if path not in sys.path:
                        sys.path.append(path)
                        logging.info("Adding %s to sys.path" % ( path ) )
    except:
        logging.info("Error setting CDS environment variable %s" % ( env_var ) )


def importEnvironment( env_vars ):
    for key in env_vars.keys():
        setEnvVariable( key, env_vars[key] )
    logging.debug( " *** Current Environment: %s " % str( os.environ ) )

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
