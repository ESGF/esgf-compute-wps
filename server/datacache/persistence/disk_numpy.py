from datacache.persistence.engine import DataPersistenceEngineBase
from modules import configuration
from modules.utilities import  *
import os, time
import numpy as np
import numpy.ma as ma

class PersistenceRecord:

    def __init__(self, file_path, **args ):
        self.path = file_path

class DataPersistenceEngine( DataPersistenceEngineBase ):

    def __init__(self, **args ):
        self.file_paths = { }
        self.persistence_directory = os.path.expanduser( configuration.CDAS_PERSISTENCE_DIRECTORY )
        if not os.path.exists(self.persistence_directory):
            os.makedirs(self.persistence_directory)

    def store(self, data, id, **args ):
        file_path = self.get_file_path( id, **args )
        t0 = time.time()
        npdata = data.data
        npdata.tofile(file_path)
        t1 = time.time()
        wpsLog.debug( " Data %s persisted in %.3f " % ( str(data.shape), (t1-t0) ) )
        return id

    def load(self, id, **args ):
        file_path = self.get_file_path( id, **args )
        t0 = time.time()
        data = np.fromfile(file_path)
        t1 = time.time()
        wpsLog.debug( " Data %s loaded in %.3f " % ( str(data.shape), (t1-t0)) )
        return data

    def get_file_path( self, id ):
        prec = self.file_paths.get( id, None )
        if prec is None:
            file_path = os.path.join(self.persistence_directory,id)
            prec = PersistenceRecord( file_path )
            self.file_paths[ id ] = prec
        return prec.path


