#from pywps.Process import WPSProcess
import sys, os
import json




import cdms2

import cdutil

cdms2.setNetcdfShuffleFlag(0)
cdms2.setNetcdfDeflateFlag(0)
cdms2.setNetcdfDeflateLevelFlag(0)

import random
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'output'))

#from cdasProcess import CDASProcess

# Test arguments for run configuration:
# version=1.0.0&service=wps&request=Execute&identifier=averager&datainputs=[domain={\"longitude\":{\"start\":-180.0,\"end\":180.0}};variable={\"url\":\"file://Users/tpmaxwel/Data/AConaty/comp-ECMWF/geos5.xml\",\"id\":\"uwnd\"}]

def domain2cdms(domain):
      kargs = {}
      for k,v in domain.iteritems():
          if k in ["id","version"]:
              continue
          system = v.get("system","value").lower()
          if isinstance(v["start"],unicode):
              v["start"] = str(v["start"])
          if isinstance(v["end"],unicode):
              v["end"] = str(v["end"])
          if system == "value":
              kargs[str(k)]=(v["start"],v["end"])
          elif system == "index":
              kargs[str(k)] = slice(v["start"],v["end"])
      return kargs


def saveVariable(data,fout,type="json"):


    f=cdms2.open(fout,"w")
    f.write(data)
    f.close()

def loadData(dataFiles):


    dataIn = []
    if isinstance(dataFiles,str):
        dataFiles = [dataFiles,]
    for fnm in dataFiles:
        f=open(fnm)
        dataIn.append(loadVariable(f.read()))
    return dataIn




def loadVariable(data):
    """loads in data, right now can only be json but i guess could have to determine between json and xml"""
    return json.loads(data)

def  loadDomain(domain):
    
    f=open(domain)
    return json.loads(f.read())


def location2cdms(domain):
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



print "Script started with", sys.argv[1:]

dataIn= loadData(sys.argv[2])[0]
                
domain = loadDomain(sys.argv[1])

cdms2keyargs = domain2cdms(domain)

f=cdms2.open(dataIn["url"])

data = f(dataIn["id"],**cdms2keyargs)
dims = "".join(["(%s)" % x for x in cdms2keyargs.keys()])

print dims

data = cdutil.averager(data,axis=dims)
data.id=dataIn["id"]

saveVariable(data,sys.argv[3],"json")

