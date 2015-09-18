import os, cPickle, shelve
from modules.utilities import *

class PersistentStatusManager:

    def __init__( self, cache_name, **args ):
        self._extension = ""
        self.cache_name = cache_name
        self.cacheDir = args.get( 'cache_dir', os.path.expanduser( "~/.cdas_cache") )
        if not os.path.exists( self.cacheDir ):
            try:
                os.makedirs( self.cacheDir )
            except OSError, err:
                if not os.path.exists( self.cacheDir ):
                    wpsLog.error( "Failed to create cache dir: %s ", str( err ) )
                    self.cacheDir = None

    def getCacheFilePath( self ):
        return os.path.join( self.cacheDir, self.cache_name + self._extension ) if self.cacheDir else "UNDEF"

    def __getitem__(self, key):
        pass

    def get(self, key, defval ):
        pass

    def __setitem__(self, key, value):
        pass

    def valid_entry( self, value ):
        try:
            test = cPickle.dumps(value)
            return True
        except (cPickle.PicklingError, TypeError) as err:
            wpsLog.error( " \n **** Error, attempt to cache an unpicklable object: %s **** \n%s\n" % ( str(value), str(err) ) )
            return False

class StatusPickleMgr(PersistentStatusManager):

    def __init__( self, cache_name, **args ):
        PersistentStatusManager.__init__(self, cache_name, **args)
        self._extension = ".pkl"
        self._cache_data = {}
        self.restore()

    def __getitem__(self, key):
        return self._cache_data.get( key, None )

    def get(self, key, defval ):
        return self._cache_data.get( key, defval )

    def __setitem__(self, key, value):
        if self.valid_entry(value):
            self._cache_data[ key ] = value
            self.save()

    def restore(self ):
        try:
            cacheFile = self.getCacheFilePath()
            if os.path.isfile(cacheFile):
                with open( cacheFile ) as cache_file:
                    self._cache_data = cPickle.load( cache_file )
        except IOError, err:
            wpsLog.error( " Error reading cache file '%s': %s" % ( cacheFile, str(err) ) )
        except EOFError:
            wpsLog.warning( " Empty cache file '%s'" % ( cacheFile  ) )

    def save( self ):
        try:
            cacheFile = self.getCacheFilePath( )
            with open( cacheFile, 'w' ) as cache_file:
                cPickle.dump( self._cache_data, cache_file )
        except IOError, err:
            wpsLog.error( " Error writing to cache file '%s': %s" % ( cacheFile, str(err) ) )


class StatusShelveMgr(PersistentStatusManager):

    def __init__( self, cache_name, **args ):
        PersistentStatusManager.__init__( self, cache_name, **args )
        self._extension = ""
        self._cache_data = shelve.open( self.getCacheFilePath() )

    def __getitem__(self, key):
        return self._cache_data.get( key, None )

    def get(self, key, defval ):
        return self._cache_data.get( key, defval )


    def __setitem__(self, key, value):
        if self.valid_entry(value):
            self._cache_data[ key ] = value



if __name__ == "__main__":

    import shelve

    filename = os.path.expanduser( "~/.cdas_cache/test_shelve")
    d = shelve.open(filename)
    print d.get('test', None)


