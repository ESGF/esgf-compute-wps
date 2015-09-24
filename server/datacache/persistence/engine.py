from modules.utilities import  *

class DataPersistenceEngineBase:

    def __init__(self, **args ):
        pass

    def store(self, data, stat, **args ):
        wpsLog.error( " DataPersistenceEngine not implemented- no data is being persisted." )

    def release(self, stat, **args ):
        wpsLog.error( " DataPersistenceEngine not implemented- no data is being released." )

    def load(self, stat, **args ):
        wpsLog.error( " DataPersistenceEngine not implemented- no persisted data available" )

    def is_stored(self, stat ):
        wpsLog.error( " DataPersistenceEngine not implemented- no info available" )
