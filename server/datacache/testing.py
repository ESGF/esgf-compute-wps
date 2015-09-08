import unittest, cdms2, logging, sys
from modules.utilities import wpsLog
from datacache.data_collections import CollectionManager
from modules.configuration import MERRA_TEST_VARIABLES
from datacache.persistence.manager import persistenceManager

class PersistenceTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        self.cache_level = 10000.0
        wpsLog.addHandler( logging.StreamHandler(sys.stdout) )
        wpsLog.setLevel(logging.DEBUG)
        self.data_chunk = self.getDataChunk()
        unittest.TestCase. __init__( self, methodName )

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def getDataChunk(self, ivar=0):
        collection = MERRA_TEST_VARIABLES["collection"]
        id = MERRA_TEST_VARIABLES["vars"][ivar]
        cm = CollectionManager.getInstance('CreateV')
        url = cm.getURL( collection, id )
        d = cdms2.open( url )
        v = d[ id ]
        data = v( level=self.cache_level )
        return data.data

    def test01_cache(self):
        pid = persistenceManager.store( self.data_chunk, pid='testing' )
        result = persistenceManager.load( pid )
        self.assertEqual( self.data_chunk.shape, result.shape )
        sample0 = self.data_chunk.flatten()[0:5].tolist()
        sample1 = result.flatten()[0:5].tolist()
        self.assertEqual( sample0, sample1 )
