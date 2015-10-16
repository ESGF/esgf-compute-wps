from modules.utilities import  *
from modules.containers import  *
from datacache.persistence.manager import persistenceManager
import re, traceback

def filter_attributes( attr, keys, include_keys = True ):
    rv = {}
    for key in attr.iterkeys():
        if ( include_keys and (key in keys) ) or (not include_keys and (key not in keys)):
            rv[key] = attr[key]
    return rv

class RegionContainer(JSONObjectContainer):

    def newObject( self, spec ):
        return Region(spec)


class CDAxis(JSONObject):
    LEVEL = 'lev'
    LATITUDE = 'lat'
    LONGITUDE = 'lon'
    TIME = 'time'
    AXIS_LIST = { 'y' : LATITUDE, 'x' : LONGITUDE, 'z' : LEVEL, 't' : TIME }

    @classmethod
    def getInstance(cls, axis_id, value):
        if isinstance( value, CDAxis ): return value
        axis = CDAxis()
        axis.init( axis_id, value )
        return axis

    def init( self, axis, values, **args ):
        JSONObject.__init__( self )
        self.tolerance=0.001
        self.items['config'] = {}
        self.items['bounds'] = {}
        self.items['axis'] = CDAxis.identify_axis( axis )
        self.init_axis_items( values )

    @classmethod
    def is_axis(cls, spec):
        return isinstance(spec,dict) and isinstance( spec.get('config',None), dict ) and isinstance( spec.get('bounds',None), list )

    @classmethod
    def identify_axis( cls, axis_name ):
        for axis_id in cls.AXIS_LIST.values():
            if axis_name.lower().find( axis_id ) >= 0: return axis_id

    def __str__(self):
        return JSONObject.__str__(self)

    def __eq__(self, axis ):
        if axis is None: return False
        if self['axis'] <> axis['axis']: return False
        r0 = self['bounds']
        r1 = axis['bounds']
        if  ( len(r0) <> len(r1) ): return False
        if self['axis'] == self.TIME:
            for x0, x1 in zip(r0, r1):
                if x0 <> x1: return False
        else:
            for x0, x1 in zip(r0, r1):
                if ( abs(x1-x0) > self.tolerance ): return False
        return True

    def __ne__(self, axis ):
        return not self.__eq__( axis )

    def init_axis_items( self, values ):
        if values is None: return
        if hasattr( values, '__iter__' ):
            if isinstance( values, dict ):
                if CDAxis.is_axis(values):
                    self.items.update(values)
                else:
                    try:
                        start = values.get('start',None)
                        end = values.get('end',None)
                        if start == end:
                            if start is None:
                               start =  values.get('value',None)
                               if start is None:
                                   wpsLog.error( "Warning, no bounds specified for axis: %s " % str(values) )
                            self['bounds'] = [ float(start) ]
                        else:
                            self['bounds'] = [ float(start), float(end) ]
                        self['config'] = filter_attributes( values, ['start','end','value'], False )
                    except KeyError:
                        wpsLog.error( "Error, can't recognize region values keys: %s " % values.keys() )
            else:
                self['bounds'] = [ float(v) for v in values ] if self['axis'] <> CDAxis.TIME else values
        else:
            try:
                self['bounds'] = [ float(values) ] if self['axis'] <> CDAxis.TIME else [ values ]
            except Exception, err:
                wpsLog.error( "Error, unknown region axis value: %s, axis: %s " % ( str(values), self['axis'] )  )
                axis_bounds = values

class Region(JSONObject):

    def __init__( self, region_spec={}, **args ):
        if isinstance( region_spec, Region ): region_spec = region_spec.items
        JSONObject.__init__( self, region_spec, **args )

    def getAxisRange( self, axis_name ):
        try:
            axis_spec = self.getItem( axis_name )
            return axis_spec['bounds'] if axis_spec else None
        except Exception, err:
            wpsLog.error( "Error in getAxisRange( %s ): %s" % ( axis_name, str(err) ) )
            return None

    def filter_spec(self, valid_axes ):
        new_spec = {}
        for spec_item in self.spec.items():
            key = spec_item[0].lower()
            if key in [ 'id', 'grid' ]:
                new_spec[key] = spec_item[1]
            else:
                for axis in CDAxis.AXIS_LIST.itervalues():
                    if key.startswith(axis) and (axis in valid_axes):
                        new_spec[key] = spec_item[1]
                        break
        return new_spec

    def process_spec(self, **args):
        slice = args.get('slice',None)
        if slice: axes = [ item[1] if item[0] not in slice else None for item in CDAxis.AXIS_LIST.iteritems() ]
        if self.spec is None: self.spec = {}
        for spec_item in self.spec.items():
            key = spec_item[0].lower()
            if key in [ 'id', 'grid' ]:
                self[key] = spec_item[1]
            else:
                for axis in CDAxis.AXIS_LIST.itervalues():
                    if key.startswith(axis):
                        cdaxis = CDAxis.getInstance( key, spec_item[1] )
                        self[axis] = cdaxis.get_spec()
                        break

    def __eq__(self, reqion1 ):
        if reqion1 is None: return False
        if self.size() <> reqion1.size(): return False
        for k0,r0 in self.iteritems():
            if isinstance( r0, basestring ):
                pass
            else:
                r1 = reqion1.getAxisRange( k0 )
                if (r0['bounds'] <> r1): return False
        return True

    def __ne__(self, reqion1 ):
        return not self.__eq__( reqion1 )

    def size(self):
        axis_count = 0
        for k,axis_spec in self.iteritems():
             if CDAxis.is_axis( axis_spec ):
                 axis_count = axis_count + 1
        return axis_count

    def toCDMS( self, **args ):
        active_axes = args.get('axes',None)
        kargs = {}
        for k,axis_spec in self.iteritems():
            if not active_axes or k in active_axes:
                try:
                    if CDAxis.is_axis( axis_spec ):
                        v = axis_spec['bounds']
                        c = axis_spec['config']
                        system = c.get('system','value')
                        is_indexed = ( system == 'indices' )
                        if isinstance( v, list ) or isinstance( v, tuple ):
                            if not is_indexed:
                                if k == CDAxis.TIME:
                                    kargs[str(k)] = ( str(v[0]), str(v[1]), "cob" ) if ( len( v ) > 1 ) else ( str(v[0]), str(v[0]), "cob" )
                                else:
                                    kargs[str(k)] = ( float(v[0]), float(v[1]), "cob" ) if ( len( v ) > 1 ) else ( float(v[0]), float(v[0]), "cob" )
                            else:
                                    kargs[str(k)] = slice(int(v[0]),int(v[1])) if ( len( v ) > 1 ) else slice(int(v[0]),int(v[0])+1)
                except Exception, err:
                    wpsLog.error( "Error processing axis '%s' spec '%s': %s\n %s " % ( k, str(axis_spec), str(err), traceback.format_exc() ) )


            # elif isinstance( v, dict ):
            #     system = v.get("system","value").lower()
            #     if isinstance(v["start"],unicode):
            #         v["start"] = str(v["start"])
            #     if isinstance(v["end"],unicode):
            #         v["end"] = str(v["end"])
            #     if system == "value":
            #         kargs[str(k)]=(v["start"],v["end"])
            #     elif system == "index":
            #         kargs[str(k)] = slice(v["start"],v["end"])
        return kargs

# class DomainStats:
#
#     def __init__( self ):
#         self.fill_value = None
#         self.attributes = None
#         self.domain = None
#         self.grid = None
#         self.id = None
#         self.dtype = None
#         self.axes = None

class Domain(Region):

    DISJOINT = 0
    CONTAINED = 1
    OVERLAP = 2

    PENDING = 0
    COMPLETE = 1

    def __init__( self, domain_spec=None,  **args ):
        self.stat = args.get( 'dstat', { 'persist_id':None } )
        Region.__init__( self, domain_spec )
        self._variable = None
        self.vstat = args.get('vstat', None )
        self.setVariable( args.get('tvar', None ) )                   # TransientVariable
        self.cache_request_status = Domain.COMPLETE if self.isCached() else None

    def getPersistId(self):
        return self.stat.get( 'persist_id', None )

    def isCached(self):
        return self.stat.get( 'persist_id', None ) or self.stat.get( 'inMemory', False )

    def getData(self):
        v = self.getVariable()
        return None if v is None else v.data

    def setData( self, data ):
        import cdms2
        self._variable = cdms2.createVariable( data, fill_value=self.vstat['fill_value'], grid=self.vstat['grid'], axes=self.vstat['axes'], id=self.vstat['id'], dtype=self.vstat['dtype'], attributes=self.vstat['attributes'] )

    def getVariable(self):
        if self._variable is None:
            self.load_persisted_data()
        return self._variable

    def setVariable( self, tvar ):
        self._variable = tvar

    def getRegion(self):
        return Region( self.spec )

    def persist(self,**args):
        if not 'persist_id' in self.stat:
            cid = args.get( 'cid', self.stat.get('cid',None) )
            if cid:  self.stat['persist_id'] = '_'.join([ re.sub("[/:]","_",cid), str(int(10*time.time()))])
        if not persistenceManager.is_stored( self.stat ):
            data = self.getData()
            if data is not None:
                persistenceManager.store( data, self.stat )
                self.stat['shape'] = list(data.shape)
                self.stat['dtype'] = data.dtype
        flush = args.get('flush',False)
        if flush and self.stat['persist_id']:
            self._variable = None

    def getCacheSize( self, cache_map ):
        wid = self.stat['wid']
        iSize = 1
        for iS in self.stat.get('shape',[]): iSize = iSize * iS
        cache_map[wid] = cache_map.get(wid,0) + iSize

    def release(self):
        persistenceManager.release( self.stat )

    def stats(self,**args):
        self.stat['cid'] = args.get( 'cid', self.vstat['id'] )
        self.stat['rid'] = args.get( 'rid', self.vstat['id'] )
        wid = args.get( 'wid', None )
        if wid:
            self.stat['wid'] = wid
            self.stat['cache_queue'] = wid
        self.stat['inMemory'] = ( self._variable is not None )
        self.stat['persisted'] = persistenceManager.is_stored( self.stat )
        self.stat['region'] = self.spec
        return self.stat

    def load_persisted_data(self,**args):
        restore = args.get('restore',False)
        if restore or (self._variable == None):
            if persistenceManager.is_stored( self.stat ):
                restored_data = persistenceManager.load( self.stat )
                if restored_data is not None:
                    self.setData( restored_data )
            if self._variable is None:
                wpsLog.error( "  ERROR: Failed to retreive persisted variable '%s'", self.stat['cid'] )

    def getSize(self):
        features = [ 'lat', 'lon' ]
        sizes = [ float('Inf'), float('Inf') ]
        bounds = [ 180.0, 360.0]
        for iAxis, axis in enumerate( features ):
            cached_axis_range = self.getAxisRange( axis )
            if cached_axis_range is None:
                sizes[ iAxis ] = bounds[ iAxis ]
            elif isinstance( cached_axis_range, (list, tuple) ):
                if (len( cached_axis_range ) == 1):
                    sizes[ iAxis ] = 0.0
                else:
                    sizes[ iAxis ] = cached_axis_range[1] - cached_axis_range[0]
        return sizes[ 0 ] * sizes[ 1 ]

    def cacheRequestSubmitted( self, cache_queue = None ):
        if cache_queue: self.stat['cache_queue'] = cache_queue
        self.cache_request_status = Domain.PENDING

    def cacheRequestComplete( self, cache_queue = None  ):
        self.cache_request_status = Domain.COMPLETE
        if cache_queue: self.stat['cache_queue'] = cache_queue

    def getCacheStatus(self):
        return self.stat.get('cache_queue',None), self.cache_request_status

    def overlap(self, new_domain ):  # self = cached domain
        for grid_axis in [ 'lat', 'lon', 'lev' ]:
            cached_axis_range = self.getAxisRange( grid_axis )
            if cached_axis_range is not None:
                new_axis_range =  new_domain.getAxisRange( grid_axis )
                if new_axis_range is None: return self.OVERLAP
                overlap = self.compare_axes( grid_axis, cached_axis_range, new_axis_range )
                if overlap == 0.0: return self.DISJOINT
                if overlap == 1.0: return self.CONTAINED
                else: return self.OVERLAP
        return self.CONTAINED

    def compare_axes(self, axis_label, cached_axis_range, new_axis_range ):
        if len( new_axis_range ) == 1:
            if len( cached_axis_range ) == 1:
                return 1.0 if cached_axis_range[0] == new_axis_range[0] else 0.0
            else:
                return 1.0 if (( new_axis_range[0] >= cached_axis_range[0] ) and ( new_axis_range[0] <= cached_axis_range[1] )) else 0.0
        elif len( cached_axis_range ) == 1:
            return 0.0

        if (new_axis_range[0] <= cached_axis_range[0]):
            return  min( max( new_axis_range[1] - cached_axis_range[0], 0.0 ) / ( cached_axis_range[1] - cached_axis_range[0] ), 1.0 )
        elif (new_axis_range[1] <= cached_axis_range[1]):
            return  1.0
        else:
            return ( cached_axis_range[1] - new_axis_range[0] ) / ( cached_axis_range[1] - cached_axis_range[0] )

class DomainManager:

    def __init__( self ):
        self.domains = []

    def persist( self, **args ):
        scope = args.get( 'scope', 'all' )
        if scope=='all':
            for domain in self.domains:
                domain.persist(**args)

    def getCacheSize(self, cache_map ):
        for d in self.domains:
            d.getCacheSize( cache_map )

    def stats( self, **args ):
        domains = []
        for domain in self.domains:
           domains.append( domain.stats(**args) )
        return domains

    def addDomain(self, new_domain ):
        self.domains.append( new_domain )

    def findDomain( self, new_domain  ):
        overlap_list = [ ]
        contained_list = [ ]
        for cache_domain in self.domains:
            overlap_status = cache_domain.overlap( new_domain )
            if overlap_status == Domain.CONTAINED:
                contained_list.append( cache_domain )
            elif overlap_status == Domain.OVERLAP:
                overlap_list.append( cache_domain )
        if len( contained_list ) > 0:
            return Domain.CONTAINED, self.findSmallestDomain( contained_list )
        if len( overlap_list ) > 0:
            return Domain.OVERLAP, overlap_list[0]
        else: return Domain.DISJOINT, None

    def uncache( self, region ):
        uncache_list = [ ]
        wids = []
        for cache_domain in self.domains:
            overlap_status = cache_domain.overlap( region )
            if overlap_status == Domain.CONTAINED:
                 uncache_list.append( cache_domain )
        for domain in uncache_list:
           wid = domain.stat.get('wid',None)
           if wid: wids.append( wid )
           domain.release()
           self.domains.remove( domain )
           wpsLog.error( "\n  ****uncache**** Removing domain from cache: %s " % str(domain) )
        wpsLog.error( " ----> Remaining domains: %s " % str(self.domains) )
        return wids

    def findSmallestDomain(self, domain_list ):
        if len( domain_list ) == 1:
            return domain_list[0]
        min_size = float('Inf')
        smallest_domain = None
        for cache_domain in domain_list:
            csize = cache_domain.getSize()
            if csize < min_size:
                min_size = csize
                smallest_domain = cache_domain
        return smallest_domain


if __name__ == "__main__":

    import cdms2, logging, sys
    from modules.utilities import wpsLog
    from datacache.domains import Domain

    wpsLog.addHandler( logging.StreamHandler(sys.stdout) )
    wpsLog.setLevel(logging.DEBUG)

    def getVariable( ivar, cache_level ):
        from modules.configuration import MERRA_TEST_VARIABLES
        from datacache.data_collections import CollectionManager
        collection = MERRA_TEST_VARIABLES["collection"]
        id = MERRA_TEST_VARIABLES["vars"][ivar]
        cm = CollectionManager.getInstance('CreateV')
        url = cm.getURL( collection, id )
        dset = cdms2.open( url )
        return dset( id, level=cache_level )  #, latitude=[self.cache_lat,self.cache_lat,'cob'] )

    CacheLevel = 10000.0
    TestVariable = getVariable( 0, CacheLevel )
    data_chunk = TestVariable.data
    domain = Domain( { 'level': CacheLevel }, TestVariable )  # , 'latitude': self.cache_lat
    t0 = time.time()
    domain.persist()
    t1 = time.time()
    result = domain.getData()
    t2 = time.time()
    sample0 = data_chunk.flatten()[0:5].tolist()
    sample1 = result.flatten()[0:5].tolist()

    print " Persist time: %.3f" % ( t1 - t0 )
    print " Restore time: %.3f" % ( t2 - t1 )
    print " Data shape: %s " % str( result.shape )
    print " Data pre-sample: %s " % str( sample0 )
    print " Data post-sample: %s " % str( sample1 )