
class CachedVariable:

    def __init__(self, **args ):
        self.id = args.get('id',None)
        self.type = args.get('type',None)
        self.operation = args.get('operation',None)
        self.region = args.get('region',None)
        self.specs = args

    def getSpec( self, id ):
        return self.specs.get( id, None )


class CacheManagerBase:
    RESULT = 1
    modifiers = { '@': RESULT };

    def __init__( self, id ):
        self._cache = {}
        self.id = id

    @classmethod
    def getModifiers( cls, variable_name ):
        var_type = cls.modifiers.get( variable_name[0], None )
        return ( var_type, variable_name if ( var_type == None ) else variable_name[1:] )

    def getVariable( self, id ):
        return self._cache.get( id, None )

    def addCachedVariable( self, var_id, operation ):
        var_type, var_id = self.getModifiers( var_id )
        self._cache[id] = CachedVariable( type=type, id=id, operation=operation )

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






