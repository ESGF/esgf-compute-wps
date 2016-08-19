from pywps import config
import os, logging

wpsLog = logging.getLogger('wps')
wpsLog.setLevel(logging.DEBUG)

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
            wpsLog.info("CDS environment variable %s set to %s" % ( key, value ) )
        except :
            wpsLog.info("Error setting CDS environment variable %s to %s" % ( key, envs[key]) )
            pass

def importEnvironmentVariable( env_var ):
    try:
        value = config.getConfigValue( "cds", env_var )
    except:
        value = os.getenv( env_var )
    if value <> None:
        os.environ[ env_var ] = value
        wpsLog.info("CDS environment variable %s set to %s" % ( env_var, value ) )

def setEnvVariable( key, env_var ):
    try:
        value = config.getConfigValue( "cds", key )
        os.environ[ env_var ] = value
        setEnv( env_var, value )
        wpsLog.info("CDS environment variable %s set to %s" % ( env_var, value ) )
        for identifier in [ 'path', 'library' ]:
            if identifier in env_var.lower():
                for path in value.split(':'):
                    if path not in sys.path:
                        sys.path.append(path)
                        wpsLog.info("Adding %s to sys.path" % ( path ) )
    except:
        wpsLog.info("Error setting CDS environment variable %s" % ( env_var ) )


def importEnvironment( env_vars ):
    for key in env_vars.keys():
        setEnvVariable( key, env_vars[key] )
    wpsLog.debug( " *** Current Environment: %s " % str( os.environ ) )


