import unittest, cdms2, logging, sys
from modules.utilities import wpsLog
wpsLog.addHandler( logging.StreamHandler(sys.stdout) )
wpsLog.setLevel(logging.DEBUG)

def getVariable( ivar, cache_level ):
    from modules.configuration import MERRA_TEST_VARIABLES
    from datacache.data_collections import CollectionManager
    collection = MERRA_TEST_VARIABLES["collection"]
    id = MERRA_TEST_VARIABLES["vars"][ivar]
    cm = CollectionManager.getInstance('CreateV')
    url = cm.getURL( collection, id )
    dset = cdms2.open( url )
    return dset( id, level=cache_level )  #, latitude=[self.cache_lat,self.cache_lat,'cob'] )

CacheLevel = 10000.0
TestVariable = getVariable( 0, CacheLevel )

class PersistenceTests(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test01_persist_mgr_cache(self):
        from datacache.persistence.manager import persistenceManager
        data_chunk = TestVariable.data
        pid = persistenceManager.store( data_chunk, pid='testing' )
        result = persistenceManager.load( pid )
        self.assertEqual( data_chunk.shape, result.shape )
        sample0 = data_chunk.flatten()[0:5].tolist()
        sample1 = result.flatten()[0:5].tolist()
        self.assertEqual( sample0, sample1 )

    def test02_domain_cache(self):
        from datacache.domains import Domain
        data_chunk = TestVariable.data
        domain = Domain( { 'level': CacheLevel }, TestVariable )  # , 'latitude': self.cache_lat
        domain.persist()
        result = domain.getData()
        self.assertEqual( data_chunk.shape, result.shape )
        sample0 = data_chunk.flatten()[0:5].tolist()
        sample1 = result.flatten()[0:5].tolist()
        self.assertEqual( sample0, sample1 )
