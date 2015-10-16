from modules import configuration
from modules.utilities import  *

class Collection:
    def __init__( self, name, collection_spec ):
        self.open_files = {}
        self.name = name
        self.cache_open_files = True
        self.server_type = collection_spec.get('type', 'file')
        self.base_url = collection_spec['url']
 #       self.initialize( collection_spec.get( 'open', [] ) )

    def initialize( self, open_list ):
        for var_id in open_list:
            self.getFile( var_id )

    def close(self):
        rv = [ f.close() for f in self.open_files.values() ]
        self.open_files = {}
        return rv

    def getURL(self, var_id ):
        if self.server_type in [ 'dods', ]:
            return "%s/%s.ncml" % ( self.base_url, var_id )
        elif self.server_type == 'file':
            return self.base_url

    def getFile( self, var_id ):
        pid = str(os.getpid())
        cache_key = pid if self.server_type == 'file' else '.'.join([var_id,pid] )
        file = self.open_files.get( cache_key, None )
        if file is None:
            file = self.loadFile( var_id )
            if (file is not None) and self.cache_open_files: self.open_files[ cache_key ] = file
        return file

    def loadFile(self, var_id=None ):
        import cdms2
        wpsLog.debug( " CDAS Collection: Opening File <%s>, var: %s " % ( self.base_url, var_id ) )
        if self.server_type == 'file':
            if self.base_url[:7]=="file://":
                f=cdms2.open(str(self.base_url[6:]),'r')
            else:
                f=cdms2.open(str(self.base_url),'r')
        else:
            url = self.getURL( var_id )
            f=cdms2.open(str(url),'r')
        return f

class CollectionManager:
    CollectionManagers = {}

    @classmethod
    def getInstance(cls,name,**args):
        return cls.CollectionManagers.setdefault( name, CollectionManager(name,**args) )

    def __init__( self, name ):
        self.collections = {}

    def close(self):
        return [ collection.close() for collection in self.collections.values() ]

    def getFile( self, collection_name, var_id ):
        collection = self.getCollection( collection_name )
        return collection.getFile( var_id )

    def getURL( self, collection_name, var_id ):
        collection = self.getCollection( collection_name )
        return collection.getURL( var_id )

    def addCollection(self, collection_name, collection_rec ):
        self.collections[ collection_name ] = Collection( collection_name, collection_rec )

    def getCollection(self, collection_name  ):
        try:
            return self.collections.get( collection_name )
        except KeyError:
            raise Exception( "Error, attempt to access undefined collection: %s " % collection_name )


def getCollectionManger(**args):
    collectionManager = CollectionManager.getInstance( configuration.CDAS_APPLICATION, **args )
    collections = configuration.CDAS_COLLECTIONS
    for collection_spec in collections:
        collectionManager.addCollection( collection_spec[0], collection_spec[1] )
    return collectionManager

def cache_load_test():
    from modules.configuration import CDAS_PERSISTENCE_DIRECTORY
    import os, numpy
    cache_dir = os.path.expanduser( CDAS_PERSISTENCE_DIRECTORY )
    cached_files = [ os.path.join(cache_dir,f) for f in os.listdir(cache_dir) if os.path.isfile(os.path.join(cache_dir,f)) ]

    max_load_time = 0.0
    max_file = None
    for cached_file in cached_files:
        t0 = time.time()
        data = numpy.fromfile( cached_file, dtype=numpy.float32 )
        t1 = time.time()
        load_time = t1-t0
        print "Loaded file %s in %.3f" % ( cached_file, load_time )
        if load_time > max_load_time:
            max_load_time = load_time
            max_file = cached_file

    print "--- "*20
    print "MAX Load time: file %s in %.3f" % ( max_file, max_load_time )

if __name__ == "__main__":
#    from testing import getVariable
#    CacheLevel = 10000.0
#    TestVariable = getVariable( 0, CacheLevel )
#    data = TestVariable.data

    import os, numpy, numpy.ma as ma
    cached_file = os.path.expanduser( '~/.cdas/testing/MERRA_mon_atmos_hur' )
    data = numpy.fromfile( cached_file, dtype=numpy.float32 ).reshape( (432, 144, 288) )
    FillVal = 1.00000002e+20
    mdata = ma.masked_greater( data, 0.90000002e+20 )
    t0 = time.time()
    a = ma.average(data,0)
    t1 = time.time()
    print "Result computed in %.2f, shape = %s, sample=%s" % ( (t1-t0), str(a.shape), a.flatten()[0:10])






