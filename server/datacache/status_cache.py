import os, pickle
from modules.utilities import *
from modules.configuration import CDAS_STATUS_CACHE_METHOD

class StatusCacheManager:

    def __init__( self, **args ):
        self._cache_data = {}
        if CDAS_STATUS_CACHE_METHOD == 'disk':
            self.cacheDir = args.get( 'cache_dir', os.path.expanduser( "~/.cdas_cache") )
            if not os.path.exists( self.cacheDir ):
                try:
                    os.makedirs( self.cacheDir )
                except OSError, err:
                    if not os.path.exists( self.cacheDir ):
                        wpsLog.error( "Failed to create cache dir: %s ", str( err ) )
                        self.cacheDir = None

    def getCacheFilePath( self, cache_key ):
        if CDAS_STATUS_CACHE_METHOD == 'disk':
            return os.path.join( self.cacheDir, cache_key + ".pkl" ) if self.cacheDir else "UNDEF"
        else: return None

    def restore(self, cache_key):
        if CDAS_STATUS_CACHE_METHOD == 'memory':
            return self._cache_data.get( cache_key, None )
        elif CDAS_STATUS_CACHE_METHOD == 'disk':
            try:
                cacheFile = self.getCacheFilePath( cache_key )
                with open( cacheFile ) as cache_file:
                    return pickle.load( cache_file )
            except IOError, err:
                wpsLog.error( " Error reading cache file '%s': %s" % ( cacheFile, str(err) ) )
            except EOFError:
                wpsLog.warning( " Empty cache file '%s'" % ( cacheFile  ) )

    def cache( self, cache_key, status_data ):
        if CDAS_STATUS_CACHE_METHOD == 'memory':
            self._cache_data[cache_key] = status_data
        elif CDAS_STATUS_CACHE_METHOD == 'disk':
            try:
                cacheFile = self.getCacheFilePath( cache_key )
                with open( cacheFile, 'w' ) as cache_file:
                    pickle.dump( status_data, cache_file )
            except IOError, err:
                wpsLog.error( " Error writing to cache file '%s': %s" % ( cacheFile, str(err) ) )

