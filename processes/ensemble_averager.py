from pywps.Process.Process import WPSProcess
import os
import time
import types
import json
import cdms2
import random

class Process(WPSProcess):
  """Main process class"""
  def __init__(self):
    """Process initialization"""
    # init process
    WPSProcess.__init__(self,
        identifier = os.path.split(__file__)[-1].split(".")[0],
        title="averager",
        version = 0.1,
        abstract = "Average multiple runs together to create the ensemble mean",
        storeSupported = "true",
        statusSupported = "true",
        )
    self.dataIn = self.addComplexInput(identifier = "variable",title="variable to average",formats = [{"mimeType":"text/json"}], minOccurs=1, maxOccurs=10)
    self.domain = self.addComplexInput(identifier = "domain",title="domain over which to average",formats = [{"mimeType":"text/json","encoding":"utf-8","schema":None},])
    self.ensemble = self.addComplexOutput(identifier = "ensemble",title="ensemble average",formats = [{"mimeType":"text/json"}])

  def execute(self):
    self.status.set("Starting %i, %i" % (0,0),0)
    dataFiles = self.dataIn.getValue()
    dataIn = []
    for fnm in dataFiles:
        f=open(fnm)
        dataIn.append(self.loadVariable(f.read()))
    ## Ok now loop thru models
    # Not tring to regrid it's an ensmeble over same model for this basic demo
    # When node manager is in place we code add code to see load balance and maybe spwan new request to avergae on different nodes, or whatever
    out = None
    N=0
    Ntot = len(dataFiles)+1
    for d in enumerate(dataIn):
        self.status.set("Reading file: %s" % d["url"],float(i)/Ntot)
        ## let's figure out between dap or local
        fnm = d["url"]
        if fnm[:7].lower()=="http://":
            f=cdms2.open(fnm)
        elif fnm[:7]=="file://":
            f=cdms2.open(fnm[7:])
        else:
            # can't figure it out skipping
            continue
        N+=1
        V=fr(d["id"])
        f.close()
        if out is None:
            out = V
        else:
            out+=V
    out/=N
    cont = True
    self.status.set("Preparing Output" % d["url"],float(Ntot-1)/Ntot)
    while cont:
        rndm = random.randint(0,100000000000)
        fout = "%i.nc"
        cont = os.path.exists(fout)
    out.id = d["id"]
    f=cdms2.open(fout,"w")
    f.write(out)
    f.close()
    out = {}
    out["url"] = fout
    out["id"]=d["id"]
    self.ensemble.setValue(json.dumps(out))
    return

  def loadVariable(self,data):
    """loads in data, right now can only be json but i guess could have to determine between json and xml"""
    return json.loads(data)
