from modules import configuration

def getDataPersistenceEngine():
    if configuration.CDAS_DATA_PERSISTENCE_ENGINE == 'disk.numpy':
        from disk_numpy import DataPersistenceEngine
        return DataPersistenceEngine()

class DataPersistenceManager:

    def __init__(self, **args ):
        self.engine = getDataPersistenceEngine()

    def store(self, data, **args ):
        dsid = self.get_data_storage_id( **args )
        self.engine.store( data, dsid )
        return dsid

    def get_data_storage_id( self, **args ):
        return 'id'

    def load(self, id, **args ):
        data = self.engine.load( id )
        return data


persistenceManager = DataPersistenceManager()
