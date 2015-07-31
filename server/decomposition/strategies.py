from modules import configuration
from modules.utilities import *

class DecompositionStrategy:

    def __init__( self, name ):
        self.name = name

    def getNodeRegion( self, region, inode, num_nodes ):
        return region


class SpaceStrategy( DecompositionStrategy ):

    def __init__( self ):
        DecompositionStrategy.__init__( self, 'space' )

    def getNodeRegion( self, global_region, inode=0, num_nodes=configuration.CDAS_DEFAULT_NUM_NODES ):
        if global_region == None: return None
        node_region = dict( global_region )
        for dim_name, range_val in global_region.items():
           if dim_name.startswith('lon'):
               if ( range_val[0] == range_val[1] ) or ( num_nodes <= 1 ):
                   return global_region if inode == 0 else None
               else:
                   dx = ( range_val[1] - range_val[0] ) / num_nodes
                   r0 = range_val[0] + dx * inode
                   r1 = r0 + dx
                   node_region[ dim_name ] = ( r0, r1, 'cob' )
                   return global_region






