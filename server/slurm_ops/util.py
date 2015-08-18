#__author__ = 'tpmaxwel'

import os, traceback, sys, numpy

import cdms2, logging
import json


cdms2.setNetcdfShuffleFlag(0)
cdms2.setNetcdfDeflateFlag(0)
cdms2.setNetcdfDeflateLevelFlag(0)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..', 'output'))
slurmLog = logging.getLogger('slurm_proc')
slurmLog.setLevel(logging.DEBUG)
slurmLog.addHandler( logging.FileHandler( os.path.abspath( os.path.join(os.path.dirname(__file__), '..', 'logs', 'slurm_proc.log') ) ) )



def domain2cdms(domain):
    kargs = {}
    for k,v in domain.iteritems():
            if k in ["id","version"]:
                continue
            if isinstance( v, float ) or isinstance( v, int ):
                kargs[str(k)] = (v,v,"cob")
            else:
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
    
def loadFileFromURL(url):
        ## let's figure out between dap or local
        if url[:7].lower()=="http://":
            f=cdms2.open(str(url))
        elif url[:7]=="file://":

            urlst = str(url[6:])
            print "   String is    ", urlst 
            f=cdms2.open(urlst)
        else:
            # can't figure it out skipping
            f=None
        return f

        # self.envs = {
        #         "path":"PATH",
        #         "addonPath":"GRASS_ADDON_PATH",
        #         "version":"GRASS_VERSION",
        #         "gui":"GRASS_GUI",
        #         "gisbase": "GISBASE",
        #         "ldLibraryPath": "LD_LIBRARY_PATH"
        # }


wpsLog = logging.getLogger('wps')


def saveVariable(data,fout):

    
    
    f=cdms2.open(fout,"w")
    f.write(data)
    f.close()

def breakpoint():
    import pydevd
    pydevd.settrace('localhost', port=8030, stdoutToServer=False, stderrToServer=True)


def loadData(dataFiles):

    f = open(dataFiles) 
    
    data_tmp=f.read()
    print data_tmp
    dataIn = json.loads(data_tmp)
    f.close()
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

