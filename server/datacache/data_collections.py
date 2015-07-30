from modules.utilities import wpsLog
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

cm = CollectionManager.getInstance('CreateV')
cm.addCollectionRecord('MERRA/mon/atmos',       [ 'dods', 'http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/MERRA/mon/atmos' ] )
cm.addCollectionRecord('CFSR/mon/atmos',        [ 'dods', 'http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/CFSR/mon/atmos' ] )
cm.addCollectionRecord('ECMWF/mon/atmos',       [ 'dods', 'http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/ECMWF/mon/atmos' ] )


if __name__ == "__main__":
    collection_name = 'MERRA/mon/atmos'
    id = 'clt'
    cm = CollectionManager.getInstance('CreateV')
    url = cm.getURL( collection_name, id )
    d = cdms2.open( url )
    v = d[ id ]
    print url, v.shape

