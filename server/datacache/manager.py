from modules.utilities import  *
from data_collections import CollectionManager
from domains import *
from decomposition.manager import decompositionManager
import numpy, sys

import cdms2, time

def load_variable_region( dataset, name, cdms2_cache_args=None ):
    t0 = time.time()
    rv = numpy.ma.fix_invalid( dataset( name, **cdms2_cache_args ) )
    t1 = time.time()
    wpsLog.debug( " $$$ Variable '%s' %s loaded from dataset '%s' --> TIME: %.2f " %  ( name, str(rv.shape), dataset.id, (t1-t0) ) )
    return rv

def subset_variable_region( variable, cdms2_cache_args=None ):
    wpsLog.debug( " $$$ Subsetting Variable '%s' (%s): args='%s' " %  ( variable.id, str(variable.shape), str(cdms2_cache_args) ) )
    t0 = time.time()
    rv = numpy.ma.fix_invalid( variable( **cdms2_cache_args ) )
    t1 = time.time()
    wpsLog.debug( "    ---->>> Subset-> %s: TIME: %.2f " %  ( str(rv.shape), (t1-t0) ) )
    return rv

class CachedVariable:

    CACHE_NONE = 0
    CACHE_OP = 1
    CACHE_REGION = 2

    def __init__(self, **args ):
        self.id = args.get('id',None)
        self.type = args.get('type',None)
        self.dataset = args.get('dataset',None)
        self.specs = args
        self.domainManager = DomainManager()

    def persist( self, scope='all' ):
        self.domainManager.persist( scope=scope, cid=self.id )

    @classmethod
    def getCacheType( cls, use_cache, operation ):
        if not use_cache: return cls.CACHE_NONE
        return cls.CACHE_REGION if operation is None else cls.CACHE_OP

    def addDomain(self, region, **args ):
        data = args.get( 'data', None )
        domain = Domain( region, data )
        request_queue = args.get( 'queue', None )
        if request_queue: domain.cacheRequestSubmitted( request_queue )
        self.domainManager.addDomain( domain )
        return domain

    def cacheType(self):
        return self.specs.get( 'cache_type', None )

    def getSpec( self, name ):
        return self.specs.get( name, None )

    def findDomain( self, search_region  ):
        return self.domainManager.findDomain( Domain( search_region ) )

class CacheManager:
    RESULT = 1
    modifiers = { '@': RESULT };

    def __init__( self, name ):
        self._cache = {}
        self.name = name

    def persist( self, scope='all' ):
        if scope == 'all':
            for cached_cvar in self._cache.values():
               cached_cvar.persist(scope)

    @classmethod
    def getModifiers( cls, variable_name ):
        var_type = cls.modifiers.get( variable_name[0], None )
        return ( var_type, variable_name if ( var_type == None ) else variable_name[1:] )

    def getVariable( self, cache_id, new_region ):
        cached_cvar = self._cache.get( cache_id, None )
        if cached_cvar is None: return Domain.DISJOINT, []
        return cached_cvar.findDomain( new_region )

    def addVariable( self, cache_id, data, specs ):
        cached_cvar = self._cache.get( cache_id, None )
        if cached_cvar is None:
            var_type, var_id = self.getModifiers( cache_id )
            cached_cvar = CachedVariable( type=var_type, id=cache_id, specs=specs )
            self._cache[ cache_id ] = cached_cvar
        region = specs.get( 'region', {} )
        cached_cvar.addDomain( region, data=data )

    def getResults(self):
        return self.filterVariables( { 'type': CachedVariable.RESULT } )

    def filterVariables( self, filter ):
        accepted_list = [ ]
        for var_spec in self._cache.values():
            accepted = True
            for filter_item in filter.items():
                if ( len(filter) > 1 ) and (filter[0] is not None) and (filter[1] is not None):
                    spec_value = var_spec.getSpec( filter[0] )
                    if (spec_value <> None) and (spec_value <> filter[1]):
                        accepted = False
                        break
            if accepted: accepted_list.append( var_spec )

class DataManager:

    def __init__( self ):
        self.cacheManager = CacheManager( 'default' )

    def persist( self, scope='all' ):
        self.cacheManager.persist( scope )

    def loadVariable( self, data, region, cache_type ):
        data_specs = {}
        name =  data.get( 'name', None ); id
        variable = data.get('variable',None)
        t0 = time.time()
        wpsLog.debug( " #@@ DataManager:LoadVariable %s (time = %.2f), region = [%s], cache_type = %d" %  ( str( data ), t0, str(region), cache_type ) )
        if variable is None:
            url = data.get('url',None)
            if url is not None:
                var_cache_id =  ":".join( [url,name] )
                status, domain = self.cacheManager.getVariable( var_cache_id, region )
                if status is not Domain.CONTAINED:
                    dataset = self.loadFileFromURL( url )
                else:
                    variable = domain.getVariable()
                    data_specs['dataset']  = domain.spec
                data_specs['cache_id']  = var_cache_id
            else:
                collection = data.get('collection',None)
                if collection is not None:
                    var_cache_id =  ":".join( [collection,name] )
                    status, domain = self.cacheManager.getVariable( var_cache_id, region )
                    if status is not Domain.CONTAINED:
                        dataset = self.loadFileFromCollection( collection, name )
                        data_specs['dataset'] = record_attributes( dataset, [ 'name', 'id', 'uri' ])
                    else:
                        variable = domain.getVariable()
                        data_specs['dataset']  = domain.spec.get( 'dataset', None )
                        data_specs['region'] = str(domain.getRegion())
                    data_specs['cache_id']  = var_cache_id
                else:
                    wpsLog.debug( " $$$ Empty Data Request: '%s' ",  str( data ) )
                    return None, data_specs

            if (variable is None):
                if (cache_type == CachedVariable.CACHE_NONE):
                    variable = dataset[name]
                    data_specs['region'] = str(region)
                else:
                    load_region = decompositionManager.getNodeRegion( region ) if (cache_type == CachedVariable.CACHE_REGION) else region
                    cache_region = Region( load_region, axes=[ Region.LATITUDE, Region.LONGITUDE, Region.LEVEL ] )
                    variable = load_variable_region( dataset, name, cache_region.toCDMS() )
                    data_specs['region'] = str(cache_region)

            else:
                wpsLog.debug( ">>----------------->>> Loading data from cache, cached region= %s" % data_specs['region'] )
                if (cache_type == CachedVariable.CACHE_OP) and (region is not None):
                    variable = subset_variable_region( variable, region.toCDMS() )
                    data_specs['region'] = str(region)

            data_specs['variable'] = record_attributes( variable, [ 'long_name', 'name', 'units' ], { 'id': id } )
        else:
            data_specs['variable'] = record_attributes( variable, [ 'long_name', 'name', 'id', 'units' ]  )

        if (variable is not None) and (cache_type <> CachedVariable.CACHE_NONE):
            data_specs['cache_type'] = cache_type
            self.cacheManager.addVariable( var_cache_id, variable, data_specs )
        t1 = time.time()
        wpsLog.debug( " #@@ DataManager:FinishedLoadVariable %s (time = %.2f, dt = %.2f), shape = %s" %  ( str( data_specs ), t1, (t1-t0), str(variable.shape) ) )
        return variable, data_specs

    def loadFileFromCollection( self, collection, id=None ):
        collectionManager = CollectionManager.getInstance( 'CreateV') # getConfigSetting( 'CDAS_APPLICATION' ) )
        url = collectionManager.getURL( collection, id )
        wpsLog.debug( "loadFileFromCollection: '%s' '%s': %s " % ( collection, id, url ) )
        return self.loadFileFromURL( url )
        wpsLog.debug( "Done loadFileFromCollection!" )

    def loadFileFromURL(self,url):
        ## let's figure out between dap or local
        if url[:7].lower()=="http://":
            f=cdms2.open(str(url))
        elif url[:7]=="file://":
            f=cdms2.open(str(url[6:]))
        else:
            # can't figure it out skipping
            f=None
        return f

dataManager = DataManager()


if __name__ == "__main__":
    wpsLog.addHandler( logging.StreamHandler(sys.stdout) )
    wpsLog.setLevel(logging.DEBUG)

    from modules.configuration import MERRA_TEST_VARIABLES
    test_point = [ -137.0, 35.0, 85000 ]
    test_time = '2010-01-16T12:00:00'
    collection = MERRA_TEST_VARIABLES["collection"]
    varname = MERRA_TEST_VARIABLES["vars"][0]

    cache_type = CachedVariable.getCacheType( True, None )
    region =  { "level":test_point[2] }
    data = { 'name': varname, 'collection': collection }
    variable, result_obj = dataManager.loadVariable( data, region, cache_type )
    cached_data = dataManager.cacheManager.getVariable( result_obj['cache_id'], region )
    cached_domain = cached_data[1]
    cache_id = cached_domain.persist()
    cached_domain.load_persisted_data()



