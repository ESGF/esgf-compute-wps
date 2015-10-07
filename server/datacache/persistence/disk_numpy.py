from datacache.persistence.engine import DataPersistenceEngineBase
from modules import configuration
from modules.utilities import  *
import os, time
import numpy as np

class DataPersistenceEngine( DataPersistenceEngineBase ):

    PersistenceDirectory = None

    @classmethod
    def update_directory(cls):
        if cls.PersistenceDirectory is None:
            cls.PersistenceDirectory = os.path.join( os.path.expanduser( configuration.CDAS_PERSISTENCE_DIRECTORY ), 'data' )
            if not os.path.exists(cls.PersistenceDirectory):
                os.makedirs(cls.PersistenceDirectory)

    def __init__(self, **args ):
        self.update_directory()

    def store(self, data, stat, **args ):
        try:
            pid = stat['persist_id']
            persisted = stat.get('persisted',False)
            if pid and not persisted and (data is not None):
                file = os.path.join(self.PersistenceDirectory,pid)
                t0 = time.time()
                data.tofile( file )
                t1 = time.time()
                wpsLog.debug( " Data %s persisted in %.3f: %s " % ( str(data.shape), (t1-t0), str(stat) ) )
        except Exception, err:
            wpsLog.debug( " Error storing data for %s: %s " % ( pid, str(err) ) )


    def load( self, stat, **args ):
        try:
            pid = stat['persist_id']
            if pid:
                dtype = stat.get('dtype',np.float32)
                shape = stat['shape']
                file = os.path.join(self.PersistenceDirectory,pid)
                t0 = time.time()
                data = np.fromfile( file, dtype=dtype )
                t1 = time.time()
                rv = data.reshape( shape )
                wpsLog.debug( " Data %s loaded in %.3f " % ( str(rv.shape), (t1-t0)) )
                return rv
        except Exception, err:
            wpsLog.debug( " Error loading data for %s: %s " % ( pid, str(err) ) )

    def release(self, stat, **args ):
        pid = stat['persist_id']
        if pid:
            filename = os.path.join(self.PersistenceDirectory,pid)
            if os.path.isfile(filename): os.remove(filename)

    def is_stored( self, stat ):
        pid = stat.get('persist_id',None)
        if not pid: return False
        file = os.path.join(self.PersistenceDirectory,pid)
        return os.path.isfile( file )


if __name__ == "__main__":
    import pprint, cdms2, sys
    from datacache.data_collections import CollectionManager
    from modules.configuration import MERRA_TEST_VARIABLES
    wpsLog.addHandler( logging.StreamHandler(sys.stdout) )
    wpsLog.setLevel(logging.DEBUG)
    pp = pprint.PrettyPrinter(indent=4)
    test_array = np.array( range(0,1000), np.int32)
    pid = 'test1'

    collection = MERRA_TEST_VARIABLES["collection"]
    id = MERRA_TEST_VARIABLES["vars"][0]
    cm = CollectionManager.getInstance('CreateV')
    url = cm.getURL( collection, id )
    d = cdms2.open( url )
    v = d[ id ]

    data = v( level=10000.0 )
    pp.pprint( data )

    engine = DataPersistenceEngine()

    engine.store( data.data, pid )

    result = engine.load( pid )

    pp.pprint(result)



