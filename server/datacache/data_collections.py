from modules import configuration
import cdms2
from modules.utilities import  *

class CollectionManager:
    CollectionManagers = {}

    @classmethod
    def getInstance(cls,name):
        return cls.CollectionManagers.setdefault( name, CollectionManager(name) )

    def __init__( self, name ):
        self.collections = {}

    def getURL(self, collection_name, var_id  ):
        wpsLog.debug( " getURL: %s %s " % ( collection_name, var_id ) )
        collection_rec = self.getCollectionRecord( collection_name )
        return self.constructURL( collection_rec[0], collection_rec[1], var_id )

    def addCollectionRecord(self, collection_name, collection_rec ):
        self.collections[ collection_name ] = collection_rec

    def getCollectionRecord(self, collection_name  ):
        try:
            return self.collections.get( collection_name )
        except KeyError:
            raise Exception( "Error, attempt to access undefined collection: %s " % collection_name )

    def constructURL(self, server_type, collection_base_url, var_id ):
        if server_type in [ 'dods', ]:
            return "%s/%s.ncml" % ( collection_base_url, var_id )

cm = CollectionManager.getInstance( configuration.CDAS_APPLICATION )

collections = configuration.CDAS_COLLECTIONS
for collection_spec in collections:
    cm.addCollectionRecord( collection_spec[0], collection_spec[1] )

if __name__ == "__main__":
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