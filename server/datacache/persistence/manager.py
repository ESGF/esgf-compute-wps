from modules import configuration

def getDataPersistenceEngine():
    if configuration.CDAS_DATA_PERSISTENCE_ENGINE == 'disk.numpy':
        from disk_numpy import DataPersistenceEngine
        return DataPersistenceEngine()

class DataPersistenceManager:

    def __init__(self, **args ):
        self.engine = getDataPersistenceEngine()

    def store(self, data, **args ):
        pid = self.get_data_storage_id( **args )
        self.engine.store( data, pid )
        return pid

    def get_data_storage_id( self, **args ):
        import re
        pid = args.get('pid')
        return re.sub("[/:]","_",pid)

    def load(self, id, **args ):
        data = self.engine.load( id )
        return data


persistenceManager = DataPersistenceManager()


if __name__ == "__main__":

    import cdms2, sys, time
    from datacache.domains import Domain

    def getVariable( ivar, **subset_args ):
        from modules.configuration import MERRA_TEST_VARIABLES
        from datacache.data_collections import CollectionManager
        collection = MERRA_TEST_VARIABLES["collection"]
        id = MERRA_TEST_VARIABLES["vars"][ivar]
        cm = CollectionManager.getInstance('CreateV')
        url = cm.getURL( collection, id )
        dset = cdms2.open( url )
        t0 = time.time()
        rv = dset( id, **subset_args )  #, latitude=[self.cache_lat,self.cache_lat,'cob'] )
        t1 = time.time()
        print "Loaded subset %s in %.2f " % ( subset_args, ( t1 - t0 ) )
        return rv

    CacheLevel = 10000.0
    domains = [ { 'level': CacheLevel }, {} ]
    iDom = 1
    TestVariable = getVariable( 0, **domains[iDom] )
    data_chunk = TestVariable.data
    domain = Domain( domains[iDom], TestVariable )  # , 'latitude': self.cache_lat
    t0 = time.time()
    domain.persist()
    t1 = time.time()
    result = domain.getData()
    t2 = time.time()
    sample0 = data_chunk.flatten()[0:5].tolist()
    sample1 = result.flatten()[0:5].tolist()

    print " Persist time: %.3f" % ( t1 - t0 )
    print " Restore time: %.3f" % ( t2 - t1 )
    print " Data shape: %s " % str( result.shape )
    print " Data pre-sample: %s " % str( sample0 )
    print " Data post-sample: %s " % str( sample1 )
