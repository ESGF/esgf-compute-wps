import logging, time, os


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
    def getLogger( name, level=logging.DEBUG ):
        logger = logging.getLogger(name)
        logger.setLevel(level)
        if len( logger.handlers ) == 0:
            logger.addHandler( logging.FileHandler( os.path.abspath( os.path.join(os.path.dirname(__file__), '../', 'logs', '%s.log' % name ) ) ) )
        return logger

