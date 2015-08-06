

class Domain:

    DISJOINT = 0
    CONTAINED = 1
    OVERLAP = 2

    PENDING = 0
    COMPLETE = 1

    def __init__( self, domain_spec=None, data=None ):
        self.spec = domain_spec if domain_spec else {}
        self.axes = {}
        self.process_spec()
        self.data = data
        self.cache_queue = None
        self.cache_request_status = None

    def cacheRequestSubmitted( self, cache_queue = None ):
        self.cache_queue = cache_queue
        self.cache_request_status = Domain.PENDING

    def cacheRequestComplete( self, cache_queue = None  ):
        self.cache_request_status = Domain.COMPLETE
        self.cache_queue = cache_queue

    def getCacheStatus(self):
        return self.cache_queue, self.cache_request_status

    def getAxisRange( self, axis_name ):
        return self.axes.get( axis_name, None )

    def process_spec(self):
        for spec_item in self.spec:
            key = spec_item[0].lower()
            if key.startswith('lat'):
                self.axes['lat'] = spec_item[1]
            elif key.startswith('lon'):
                self.axes['lon'] = spec_item[1]
            elif key.startswith('lev'):
                self.axes['lev'] = spec_item[1]
            elif key.startswith('time'):
                self.axes['time'] = spec_item[1]
            elif key.startswith('grid'):
                self.axes['grid'] = spec_item[1]

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
        if (new_axis_range[0] <= cached_axis_range[0]):
            return  min( max( new_axis_range[1] - cached_axis_range[0], 0.0 ) / ( cached_axis_range[1] - cached_axis_range[0] ), 1.0 )
        elif (new_axis_range[1] <= cached_axis_range[1]):
            return  1.0
        else:
            return ( cached_axis_range[1] - new_axis_range[0] ) / ( cached_axis_range[1] - cached_axis_range[0] )

class DomainManager:

    def __init__( self ):
        self.domains = []

    def addDomain(self, new_domain ):
        self.domains.append( new_domain )

    def findDomain( self, new_domain  ):
        overlap_list = [ ]
        for cache_domain in self.domains:
            overlap_status = cache_domain.overlap( new_domain )
            if overlap_status == Domain.CONTAINED: return Domain.CONTAINED, cache_domain
            elif overlap_status == Domain.OVERLAP: overlap_list.append( cache_domain )
        if len( overlap_list ) == 0: return Domain.OVERLAP, overlap_list
        else: return Domain.DISJOINT, []