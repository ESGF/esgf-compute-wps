from modules.utilities import  *
from data_collections import CollectionManager
from domains import *
from decomposition.manager import decompositionManager
import numpy

import cdms2, time

def load_variable_region( dataset, name, cdms2_cache_args=None ):   # TODO
    wpsLog.debug( " $$$ Loading Variable '%s', region: '%s'  " %  ( name, cdms2_cache_args ) )
    t0 = time.time()
    rv = numpy.ma.fix_invalid( dataset( name, **cdms2_cache_args ) )
    t1 = time.time()
    wpsLog.debug( " $$$ Variable '%s' %s loaded from dataset '%s' --> TIME: %.2f " %  ( name, str(rv.shape), dataset.id, (t1-t0) ) )
    return rv

class CachedVariable:

    CACHE_NONE = 0
    CACHE_OP = 1
    CACHE_REGION = 2

    def __init__(self, **args ):
        self.id = args.get('id',None)
        self.type = args.get('type',None)
        self.specs = args.get('specs',None)
        self.specs = args
        self.domainManager = DomainManager()

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


    def loadVariable( self, **run_args ):
        data = get_json_arg( 'data', run_args )
        region = get_json_arg( 'region', run_args )
        operation = get_json_arg( 'operation', run_args )
        use_cache =  get_json_arg( 'cache', run_args, True )
        cache_type = CachedVariable.getCacheType( use_cache, operation )

        data_specs = {}
        id =  data.get( 'id', None )
        variable = data.get('variable',None)
        t0 = time.time()
        wpsLog.debug( " #@@ DataManager:LoadVariable %s (time = %.2f)" %  ( str( data ), t0 ) )
        if variable is None:
            url = data.get('url',None)
            if url is not None:
                var_cache_id =  ":".join( [url,id] )
                status, domain = self.cacheManager.getVariable( var_cache_id, region )
                if status is not Domain.CONTAINED:
                    dataset = self.loadFileFromURL( url )
                else:
                    variable = domain.data
                    data_specs['dataset']  = domain.spec
                data_specs['cache_id']  = var_cache_id
            else:
                collection = data.get('collection',None)
                if collection is not None:
                    var_cache_id =  ":".join( [collection,id] )
                    status, domain = self.cacheManager.getVariable( var_cache_id, region )
                    if status is not Domain.CONTAINED:
                        dataset = self.loadFileFromCollection( collection, id )
                        data_specs['dataset'] = record_attributes( dataset, [ 'id', 'uri' ])
                    else:
                        variable = domain.data
                        data_specs['dataset']  = domain.spec.get( 'dataset', None )
                    data_specs['cache_id']  = var_cache_id
                else:
                    wpsLog.debug( " $$$ Empty Data Request: '%s' ",  str( data ) )
                    return None, data_specs

            if (variable is None):
                if (cache_type == CachedVariable.CACHE_NONE):
                    variable = dataset[id]
                    data_specs['region'] = region
                else:
                    cache_region = decompositionManager.getNodeRegion( region ) if (cache_type == CachedVariable.CACHE_REGION) else region2cdms(region);
                    variable = load_variable_region( dataset, id, cache_region )
                    data_specs['region'] = cache_region

            data_specs['variable'] = record_attributes( variable, [ 'long_name', 'name', 'units' ], { 'id': id } )
        else:
            data_specs['variable'] = record_attributes( variable, [ 'long_name', 'name', 'id', 'units' ]  )

        if (variable is not None) and (cache_type <> CachedVariable.CACHE_NONE):
            data_specs['cache_type'] = cache_type
            self.cacheManager.addVariable( var_cache_id, variable, data_specs )
        t1 = time.time()
        wpsLog.debug( " #@@ DataManager:FinishedLoadVariable %s (time = %.2f, dt = %.2f)" %  ( str( data_specs ), t1, (t1-t0) ) )
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



