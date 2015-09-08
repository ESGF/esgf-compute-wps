from datacache.persistence.engine import DataPersistenceEngineBase
from modules import configuration
from modules.utilities import  *
import os, time
import numpy as np

class PersistenceRecord:

    PersistenceDirectory = None

    @classmethod
    def update_directory(cls):
        if cls.PersistenceDirectory is None:
            cls.PersistenceDirectory = os.path.expanduser( configuration.CDAS_PERSISTENCE_DIRECTORY )
            if not os.path.exists(cls.PersistenceDirectory):
                os.makedirs(cls.PersistenceDirectory)

    def __init__(self, pid, **args ):
        self._id = pid
        self.update_directory()
        self._file = os.path.join(self.PersistenceDirectory,pid)
        self._dtype = None
        self._shape = None

    @property
    def id(self):
        return self._id

    @property
    def dtype(self):
        return self._dtype

    @property
    def file(self):
        return self._file

    def store( self, data ):
        self._dtype = data.dtype
        self._shape = data.shape
        t0 = time.time()
        data.tofile(self._file)
        t1 = time.time()
        wpsLog.debug( " Data %s persisted in %.3f " % ( str(data.shape), (t1-t0) ) )

    def load(self, **args ):
        t0 = time.time()
        data = np.fromfile( self._file, dtype=self._dtype )
        t1 = time.time()
        rv = data.reshape( self._shape )
        wpsLog.debug( " Data %s loaded in %.3f " % ( str(rv.shape), (t1-t0)) )
        return rv

class DataPersistenceEngine( DataPersistenceEngineBase ):

    def __init__(self, **args ):
        self.precs = { }

    def store(self, data, pid, **args ):
        prec = self.precs.setdefault( pid, PersistenceRecord(pid) )
        prec.store( data )

    def load(self, pid, **args ):
        prec = self.precs.get( pid, None )
        assert (prec is not None), "Error, undefined persistence id: %s" % pid
        return prec.load()


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



