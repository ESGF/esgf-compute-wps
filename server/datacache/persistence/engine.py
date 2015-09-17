from modules.utilities import  *

class DataPersistenceEngineBase:

    def __init__(self, **args ):
        pass

    def store(self, data, **args ):
        wpsLog.error( " DataPersistenceEngine not implemented- no data is being persisted." )


    def load(self, id, **args ):
        wpsLog.error( " DataPersistenceEngine not implemented- no persisted data available" )

    def is_stored(self, id ):
        wpsLog.error( " DataPersistenceEngine not implemented- no info available" )


