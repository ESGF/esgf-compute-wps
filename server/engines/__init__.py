import os, logging

def getLogger( name, level=logging.DEBUG ):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if len( logger.handlers ) == 0:
        logger.addHandler( logging.FileHandler( os.path.abspath( os.path.join(os.path.dirname(__file__), '..', 'logs', '%s.log' % name ) ) ) )
    return logger

wpsLog = getLogger('wps')

