from modules import configuration
import strategies

class DecompositionManager:

    def __init__( self ):
        self.strategies = {}
        self.load()

    def getStrategy(self, global_region, nnodes=configuration.CDAS_DEFAULT_NUM_NODES ):
        return self.strategies[ configuration.CDAS_DEFAULT_DECOMP_STRATEGY ]

    def getNodeRegion( self, region, inode=0, num_nodes=configuration.CDAS_DEFAULT_NUM_NODES ):
        strategy = self.getStrategy( region, num_nodes )
        assert strategy is not None, "Error, undefined decomposition strategy."
        return strategy.getNodeRegion( region, inode, num_nodes )

    def load(self):
        for name, cls in strategies.__dict__.items():
            try:
                if ( strategies.DecompositionStrategy in cls.__bases__ ):
                    strategy_instance = cls()
                    self.strategies[ strategy_instance.name ] = strategy_instance
            except: pass
        pass



decompositionManager = DecompositionManager()
