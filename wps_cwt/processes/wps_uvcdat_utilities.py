from pywps import config
import logging,os,numpy,sys


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
        if env_var == 'PYTHONPATH':
            for path in value.split(':'):
                sys.path.append(path)
                logging.info("Adding %s to sys.path" % ( path ) )
        else:
            setEnv( env_var, value )
            logging.info("CDS environment variable %s set to %s" % ( env_var, value ) )
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

