from modules.utilities import  wpsLog, record_attributes, getConfigSetting
from data_collections import CollectionManager
import cdms2, time

def load_variable_region( dataset, name, cache_region=None ):   # TODO
    t0 = time.time()
    rv = dataset(name)
    t1 = time.time()
    wpsLog.debug( " $$$ Variable %s%s loaded --> TIME: %.2f " %  ( id, str(rv.shape), (t1-t0) ) )
    return rv

class CachedVariable:

    def __init__(self, **args ):
        self.id = args.get('id',None)
        self.type = args.get('type',None)
        self.specs = args.get('specs',None)
        self.data = args.get('data',None)
        self.operation = args.get('operation',None)
        self.region = args.get('region',None)
        self.specs = args

    def getSpec( self, name ):
        return self.specs.get( name, None )


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

    def getVariable( self, cache_id ):
        return self._cache.get( cache_id, None )

    def addVariable( self, cache_id, data, specs, operation=None ):
#        var_type, var_id = self.getModifiers( cache_id )
        self._cache[ cache_id ] = CachedVariable( type=type, id=cache_id, data=data, specs=specs, operation=operation )

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

    def loadVariable( self, **args ):
        data_specs = {}
        id =  args.get( 'id', None )
        use_cache =  args.get( 'cache', False )
        cache_region = args.get( 'region', None )
        variable = args.get('variable',None)
        if variable is None:
            url = args.get('url',None)
            if url is not None:
                var_cache_id =  ":".join( [url,id] )
                cached_variable = self.cacheManager.getVariable( var_cache_id )
                if cached_variable is None:
                    dataset = self.loadFileFromURL( url )
                else:
                    variable = cached_variable.data
                    data_specs['dataset']  = cached_variable.specs
            else:
                collection = args.get('collection',None)
                if collection is not None:
                    var_cache_id =  ":".join( [collection,id] )
                    cached_variable = self.cacheManager.getVariable( var_cache_id )
                    if cached_variable is None:
                        dataset = self.loadFileFromCollection( collection, id )
                        data_specs['dataset'] = record_attributes( dataset, [ 'id', 'uri' ])
                    else:
                        variable = cached_variable.data
                        data_specs['dataset']  = cached_variable.specs.get( 'dataset', None )
                else:
                    wpsLog.debug( " $$$ Empty Data Request: '%s' ",  str( args ) )
                    return None

            if variable is None:
                variable = load_variable_region( dataset, id, cache_region ) if use_cache else dataset[id]
                data_specs['region'] = cache_region
                data_specs['variable'] = record_attributes( variable, [ 'long_name', 'name', 'units' ], { 'id': id } )
                if use_cache: self.cacheManager.addVariable( var_cache_id, variable, data_specs )
        else:
            data_specs['variable'] = record_attributes( variable, [ 'long_name', 'name', 'id', 'units' ]  )

        if (variable is not None) and use_cache:
            self.cacheManager.addVariable( var_cache_id, variable, data_specs )
        return variable, data_specs

    def loadFileFromCollection( self, collection, id=None ):
        collectionManager = CollectionManager.getInstance( 'CreateV') # getConfigSetting( 'CDAS_APPLICATION' ) )
        url = collectionManager.getURL( collection, id )
        wpsLog.debug( "loadFileFromCollection: '%s' '%s': %s " % ( collection, id, url ) )
        return self.loadFileFromURL( url )

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



