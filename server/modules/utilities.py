import logging, time, os, json

LogDir = os.path.abspath( os.path.join( os.path.dirname(__file__), '../', 'logs' ) )
DefaultLogLevel = logging.DEBUG
wpsLog = logging.getLogger('wps')
wpsLog.setLevel(DefaultLogLevel)
if len( wpsLog.handlers ) == 0: wpsLog.addHandler( logging.FileHandler( os.path.join( LogDir, 'wps.log') ) )

def get_json_arg( id, args ):
        json_arg = args.get( id, None )
        if json_arg is not None:
            try:
                return convert_json_str( json_arg )
            except:
                wpsLog.error( "Can't recognize json '%s' from args: '%s" % ( str(json_arg), str(args) ) )
        return "" if json_arg is None else json_arg

def convert_json_str( json_arg ):
    return json.loads( json_arg ) if isinstance(json_arg, basestring) else json_arg

class Profiler(object):

    def __init__(self):
        self.t0 = None
        self.marks = []

    def mark( self, label="" ):
        if self.t0 is None:
           self.t0 = time.time()
        else:
            t1 = time.time()
            self.marks.append(  (label, (t1-self.t0) ) )
            self.t0 = t1

    def dump( self, title ):
        print title
        for mark in self.marks:
            print " %s: %.4f " % ( mark[0], mark[1] )

    @staticmethod
    def getLogger( name, level=DefaultLogLevel ):
        logger = logging.getLogger(name)
        logger.setLevel(level)
        if len( logger.handlers ) == 0:
            logger.addHandler( logging.FileHandler( os.path.join( LogDir, '%s.log' % name ) ) )
        return logger


