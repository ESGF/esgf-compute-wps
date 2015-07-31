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
        node_region = global_region

        # for k,v in global_region.iteritems():
        #     if k in ["id","version"]:
        #         continue
        #     if k.starts_with('lon'):
        #         kargs[str(k)] = (v,v,"cob")
        #     else:
        #         system = v.get("system","value").lower()
        #         if isinstance(v["start"],unicode):
        #             v["start"] = str(v["start"])
        #         if isinstance(v["end"],unicode):
        #             v["end"] = str(v["end"])
        #         if system == "value":
        #             kargs[str(k)]=(v["start"],v["end"])
        #         elif system == "index":
        #             kargs[str(k)] = slice(v["start"],v["end"])

        return region2cdms( node_region )


