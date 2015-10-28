from mpi4py import MPI
from modules import configuration
from modules.utilities import *
import sys, os

class WorkerManager:
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    WORKER_SCRIPT = os.path.join( CURRENT_DIR, 'worker.py' )

    def __init__(self):
        self.comm=None
        self._nworkers = None
        self.startup( configuration.CDAS_NUM_WORKERS )

    def startup( self, nworkers ):
        if self.comm is None :
            wpsLog.debug( "WorkerManager[%x:%x]--> Spawn %d workers." % ( id(self), os.getpid(), nworkers ) )
            self.comm = MPI.COMM_SELF.Spawn( sys.executable, args=[ self.WORKER_SCRIPT ], maxprocs=nworkers )

    def nworkers(self):
        if self._nworkers is None:
            self._nworkers = self.comm.Get_remote_size()
        return self._nworkers

    def broadcast( self, msg, tag, destination_list=None ):
        if destination_list is None:
            destination_list = range( self.nworkers() )
        for destination in destination_list:
            self.send( msg, tag, destination )
        return self.comm, len(destination_list)

    def send( self, msg, tag, destination ):
        self.comm.send( msg, tag=tag, dest=wrank(destination) )
        return self.comm, 1

    def recv( self, source, tag ):
        response = self.comm.recv( source=source, tag=tag )
        wpsLog.debug( "WorkerManager--> receiving response %d from [%s]: %s" % ( tag, source, str(response) ) )
        return response

    def close(self):
        self.comm.Disconnect()

    def shutdown(self):
        self.broadcast( {'config':'exit'} )
        self.comm.Disconnect()

    def getProcessStats(self):
        rv = {}
        for destination in range( self.nworkers() ):
            w_id = wid(destination)
            rv[w_id] = { 'name': w_id, 'rank': destination }
        return rv

if __name__ == "__main__":
    import cdms2
    wpsLog.addHandler( logging.StreamHandler(sys.stdout) )
    wpsLog.setLevel(logging.DEBUG)

    def worker_exe1():
        dfile = 'http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA/mon/atmos/hur.ncml'
        slice_args = {'lev': (100000.0, 100000.0, 'cob')}
        dataset = f=cdms2.open(dfile)
        dset = dataset( "hur", **slice_args )
        print str(dset.shape)

    def worker_exe2():
        dfile = 'http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA/mon/atmos/hur.ncml'
        dataset = f=cdms2.open(dfile)
        slice_args1 = {"longitude": (-10.0, -10.0, 'cob'), "latitude": (10.0, 10.0, 'cob'), 'lev': (100000.0, 100000.0, 'cob')}
        dset1 = dataset( "hur", **slice_args1 )
        print str(dset1.shape)

