from modules import configuration
from modules.utilities import *
import inspect

class RegionReductionStrategy:

    def __init__( self, **args ):
        pass

    def getReducedRegion( self, region, **args ):
        return region

class StrategyManager:

    def __init__( self ):
        self.strategies = {}
        self.strategy_spec = configuration.CDAS_REDUCTION_STRATEGY[ self.StrategyClass.__name__ ]
        self.load()

    def getStrategy(self, region, **args ):
        return self.strategies[ self.strategy_spec['id'] ]

    def getReducedRegion( self, region, **args ):
        strategy = self.getStrategy( region, **args  )
        assert strategy is not None, "Error, undefined decomposition strategy."
        return strategy.getReducedRegion( region, **args  )

    def load(self):
        import strategies
        class_map = strategies.__dict__
        for name, cls in class_map.items():
            if inspect.isclass(cls) and issubclass( cls, self.StrategyClass ) and cls <> self.StrategyClass:
                try:
                    self.strategies[ cls.ID ] = cls( **self.strategy_spec )
                except Exception, err:
                    wpsLog.error( "StrategyManager load error: %s " % str( err ) )

