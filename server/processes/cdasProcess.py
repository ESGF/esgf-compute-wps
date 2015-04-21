from pywps.Process import WPSProcess
import os,numpy,sys
import logging, json
import cdms2, pydevd
import random
from pywps import config
# Path where output will be stored/cached

cdms2.setNetcdfShuffleFlag(0) ## where value is either 0 or 1
cdms2.setNetcdfDeflateFlag(0) ## where value is either 0 or 1
cdms2.setNetcdfDeflateLevelFlag(0) ## where value is a integer between 0 and 9 included

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'output'))
OutputDir = 'wpsoutputs'
OutputPath = os.environ['DOCUMENT_ROOT'] + "/" + OutputDir
wpsLog = logging.getLogger('wps')
wpsLog.setLevel(logging.DEBUG)
wpsLog.addHandler( logging.FileHandler( os.path.abspath( os.path.join(os.path.dirname(__file__), '..', 'logs', 'wps.log') ) ) )

class CDASProcess(WPSProcess):
    """Main process class"""
    def __init__(self):
        self.operation = None
        self.dataIn = None
        self.domain = None
        self.result = None

    def saveVariable(self,data,dest,type="json"):
        cont = True
        while cont:
            rndm = random.randint(0,100000000000)
            fout = os.path.join(BASE_DIR,"%i.nc" % rndm)
            fjson = os.path.join(BASE_DIR,"%i.json" % rndm)
            cont = os.path.exists(fout) or os.path.exists(fjson)
        f=cdms2.open(fout,"w")
        f.write(data)
        f.close()
        out = {}
        out["url"] = "file:/"+fout
        out["id"]=data.id
        Fjson=open(fjson,"w")
        json.dump(out,Fjson)
        Fjson.close()
        dest.setValue(fjson)

    def breakpoint(self):
        pydevd.settrace('localhost', port=8030, stdoutToServer=False, stderrToServer=True)

    def loadOperation(self,origin=None):
        if origin is None:
            origin = self.operation
        if origin is None: return None
        opStr = origin.getValue()
        f=open(opStr)
        return json.loads(f.read())

    def loadData(self,origin=None):
        if origin is None:
            origin = self.dataIn
        if origin is None: return None
        dataFiles = origin.getValue()
        dataIn = []
        if isinstance(dataFiles,str):
            dataFiles = [dataFiles,]
        for fnm in dataFiles:
            f=open(fnm)
            dataIn.append(self.loadVariable(f.read()))
        return dataIn

    def loadVariable(self,data):
        """loads in data, right now can only be json but i guess could have to determine between json and xml"""
        return json.loads(data)

    def  loadDomain(self,origin=None):
        if origin is None:
            origin = self.domain
        if origin is None: return None
        domain = origin.getValue()
        f=open(domain)
        return json.loads(f.read())

    def location2cdms(self,domain):
        kargs = {}
        for k,v in domain.iteritems():
            if k not in ["id","version"]:
                kargs[str(k)] = float( str(v) )
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

