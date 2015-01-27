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
        title="TESTit",
        version = 0.1,
        abstract = "Just testing basic functions, adding input, checking status, etc...",
        storeSupported = "true",
        statusSupported = "true",
        )
    self.valueIn = self.addLiteralInput(identifier = "val", type=types.IntType,title = "User Input Value", default = 6)
    self.timePauseIn = self.addLiteralInput(identifier = "pauseTime", type=types.FloatType,title = "User Value for Pause Time", default = 1.)
    self.numberPauseIn = self.addLiteralInput(identifier = "pauseNumber", type=types.IntType,title = "User Value for Number of Pauses", default = 3)
    self.pauseIn = self.addLiteralInput(identifier = "pause", type=bool,title = "User Input Value", default = True)
    self.textOut = self.addLiteralOutput(identifier="text", title="just some text")
  def execute(self):
    self.status.set("Starting",0)
    self.textOut.setValue("%i * 2. = %f" % (int(self.valueIn.getValue()),int(self.valueIn.getValue())*2.))
    N = self.numberPauseIn.getValue()
    n = self.timePauseIn.getValue()
    p = self.pauseIn.getValue()
    if p:
      for i in range(N):
        self.status.set("Waiting %f second for the %i time (out of %i), just because" % (n,i+1,N),100-N+i)
        time.sleep(n)
    return


