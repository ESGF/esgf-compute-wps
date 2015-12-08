from modules import configuration
from modules.utilities import *
from datacache.domains import Region, CDAxis
from decomposition.manager import StrategyManager, RegionReductionStrategy

class DecompositionStrategy(RegionReductionStrategy):
    pass

class DecimationStrategy(RegionReductionStrategy):
    pass


class SpaceStrategy( DecompositionStrategy ):

    ID = 'space.lon'

    def __init__( self, **args ):
        RegionReductionStrategy.__init__( self, **args )

    def getReducedRegion( self, global_region, **args ):
        inode=args.get('node',0)
        num_nodes=args.get('node_count',configuration.CDAS_DEFAULT_NUM_NODES )
        return global_region
        if global_region == None: return None
        node_region = global_region
        for dim_name, range_val in global_region.items():
           if dim_name.startswith('lon'):
               if ( range_val[0] == range_val[1] ) or ( num_nodes <= 1 ):
                   return global_region if inode == 0 else None
               else:
                   dx = ( range_val[1] - range_val[0] ) / num_nodes
                   r0 = range_val[0] + dx * inode
                   r1 = r0 + dx
                   node_region[ dim_name ] = ( r0, r1 )
                   return node_region

class TimeSubsetStrategy( DecimationStrategy ):

    ID = 'time.subset'

    def __init__( self, **args ):
        RegionReductionStrategy.__init__( self, **args )
        self.max_size = args.get( 'max_size', 1000 )

    def getTimeAxis(self, **args):
        axes = args.get( 'axes', None )
        for axis in axes:
            if axis.isTime():
                return axis

    def getReducedRegion( self, region, **args ):
        time_axis = self.getTimeAxis( **args )
        if time_axis:
            region_size = region.getIndexedAxisSize( CDAxis.AXIS_LIST['t'], time_axis )
            step_size = int(region_size/self.max_size) + 1
            region.setStepSize( 't', step_size, time_axis )
        return region


class DecompositionManager(StrategyManager):
    StrategyClass = DecompositionStrategy

decompositionManager = DecompositionManager()

class DecimationManager(StrategyManager):
    StrategyClass = DecimationStrategy

decimationManager = DecimationManager()







