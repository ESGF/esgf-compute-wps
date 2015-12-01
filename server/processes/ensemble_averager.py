from pywps.Process import WPSProcess
import os
import json
import cdms2
cdms2.setNetcdfShuffleFlag(0) ## where value is either 0 or 1
cdms2.setNetcdfDeflateFlag(0) ## where value is either 0 or 1
cdms2.setNetcdfDeflateLevelFlag(0) ## where value is a integer between 0 and 9 included
import random
# Path where output will be stored/cached
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__),"..","output"))
from cdasProcess import CDASProcess

class Process(CDASProcess):
  """Main process class"""
  def __init__(self):
    """Process initialization"""
    # init process
    WPSProcess.__init__(self,
        identifier = os.path.split(__file__)[-1].split(".")[0],
        title="ensemble averager",
        version = 0.1,
        abstract = "Average multiple runs together to create the ensemble mean",
        storeSupported = "true",
        statusSupported = "true",
        )
    self.dataIn = self.addComplexInput(identifier = "variable",title="variable to average",formats = [{"mimeType":"text/json"}], minOccurs=1, maxOccurs=10)
    self.ensemble = self.addComplexOutput(identifier = "ensemble",title="ensemble average",formats = [{"mimeType":"text/json"}])

  def execute(self):
    ## Ok now loop thru models
    # Not tring to regrid it's an ensmeble over same model for this basic demo
    # When node manager is in place we code add code to see load balance and maybe spwan new request to avergae on different nodes, or whatever
    out = None
    N=0
    dataIn = self.loadData()
    Ntot = len(dataIn)+1
    for d in dataIn:
        fnm = str(d["url"])
        self.status.set("Reading file: %s" % fnm,100*float(N)/Ntot)
        f = self.loadFileFromURL(d["url"])
        if f is None:
            continue
        N+=1
        V=f(d["id"])
        f.close()
        if out is None:
            out = V
        else:
            out+=V
    out/=N
    out.id = d["id"]
    cont = True
    self.status.set("Preparing Output",100*float(Ntot-1)/Ntot)
    self.saveVariable(out,self.ensemble,"json")
    return
