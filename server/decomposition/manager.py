from modules import configuration
import strategies

class DecompositionManager:

    def __init__( self ):
        self.strategies = {}
        self.load()

    def getStrategy(self, global_region, nnodes=configuration.CDAS_DEFAULT_NUM_NODES ):
        return self.strategies[ configuration.CDAS_DEFAULT_DECOMP_STRATEGY ]

    def load(self):
        for name, cls in strategies.__dict__.items():
            try:
                if cls.__bases__[0] == strategies.DecompositionStrategy:
                    strategy_instance = cls()
                    self.strategies[ strategy_instance.name ] = strategy_instance
            except: pass
        pass



decompositionManager = DecompositionManager()
