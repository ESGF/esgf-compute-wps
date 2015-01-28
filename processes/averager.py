from pywps.Process.Process import WPSProcess
import os
import time
import types

class Process(WPSProcess):
  """Main process class"""
  def __init__(self):
    """Process initialization"""
    # init process
    WPSProcess.__init__(self,
        identifier = os.path.split(__file__)[-1].split(".")[0],
        title="averager",
        version = 0.1,
        abstract = "Average a variable over a (many) dimension",
        storeSupported = "true",
        statusSupported = "true",
        )
    self.axis = self.addLiteralInput(identifier = "axes", type=str,title = "Dimensions to average over", default = '0')
    self.dataIn = self.addComplexInput(identifier = "variable",title="variable to average",formats = [{"mimeType":"esgf/variable"},{"mimeType":"application/x-netcdf"}],maxmegabytes=50)

  def execute(self):
    self.status.set("Starting %i, %i",0)
    return


