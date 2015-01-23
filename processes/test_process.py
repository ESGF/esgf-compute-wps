from pywps.Process.Process import WPSProcess
import os

class Process(WPSProcess):
  """Main process class"""
  def __init__(self):
    """Process initialization"""
    # init process
    WPSProcess.__init__(self,
        identifier = os.path.split(__file__)[-1][:-3],
        title="TESTit",
        version = "0.1",
        storeSupported = "true",
        statusSupported = "true",
        abstract="Just a simple test to learn",
        )
  def execute(self):
    return


