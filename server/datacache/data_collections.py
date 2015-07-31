from modules.utilities import wpsLog
from modules import configuration
import cdms2

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
    collection_name = 'MERRA/mon/atmos'
    id = 'clt'
    cm = CollectionManager.getInstance('CreateV')
    url = cm.getURL( collection_name, id )
    d = cdms2.open( url )
    v = d[ id ]
    print url, v.shape

