import unittest, cdms2, logging, sys
from modules.utilities import wpsLog
from datacache.data_collections import CollectionManager
from modules.configuration import MERRA_TEST_VARIABLES
from datacache.persistence.manager import persistenceManager
from datacache.domains import Domain
wpsLog.addHandler( logging.StreamHandler(sys.stdout) )
wpsLog.setLevel(logging.DEBUG)

class PersistenceTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        self.cache_level = 10000.0
        self.variable = self.getVariable()
        unittest.TestCase. __init__( self, methodName )

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def getVariable(self, ivar=0):
        collection = MERRA_TEST_VARIABLES["collection"]
        id = MERRA_TEST_VARIABLES["vars"][ivar]
        cm = CollectionManager.getInstance('CreateV')
        url = cm.getURL( collection, id )
        dset = cdms2.open( url )
        return dset( id, level=self.cache_level )

    def test01_persist_mgr_cache(self):
        data_chunk = self.variable.data
        pid = persistenceManager.store( data_chunk, pid='testing' )
        result = persistenceManager.load( pid )
        self.assertEqual( data_chunk.shape, result.shape )
        sample0 = data_chunk.flatten()[0:5].tolist()
        sample1 = result.flatten()[0:5].tolist()
        self.assertEqual( sample0, sample1 )

    def test02_domain_cache(self):
        data_chunk = self.variable.data
        domain = Domain( { 'level': self.cache_level }, self.variable )
        domain.persist()
        result = domain.load_persisted_data()
        self.assertEqual( data_chunk.shape, result.shape )
        sample0 = data_chunk.flatten()[0:5].tolist()
        sample1 = result.flatten()[0:5].tolist()
        self.assertEqual( sample0, sample1 )
