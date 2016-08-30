from pywps.Process import WPSProcess
import os
import json
import cdms2
cdms2.setNetcdfShuffleFlag(0) ## where value is either 0 or 1
cdms2.setNetcdfDeflateFlag(0) ## where value is either 0 or 1
cdms2.setNetcdfDeflateLevelFlag(0) ## where value is a integer between 0 and 9 included
import random

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'output'))

from esgf_process import ESGFProcess

class Process(ESGFProcess):
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
    self.domain = self.addComplexInput(identifier='domain', title='domain over which to average', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}])
    self.dataIn = self.addComplexInput(identifier = "variable",title="variable to average",formats = [{"mimeType":"text/json"}], minOccurs=1, maxOccurs=10)
    self.ensemble = self.addComplexOutput(identifier = "ensemble",title="ensemble average",formats = [{"mimeType":"text/json"}])

  def execute(self):
    ## Ok now loop thru models
    # Not tring to regrid it's an ensmeble over same model for this basic demo
    # When node manager is in place we code add code to see load balance and maybe spwan new request to avergae on different nodes, or whatever
    out = None
    dataIn = self.loadData()
    Ntot = len(dataIn)+1
    for i,d in enumerate(dataIn):
        self.status.set("Preparing Output from :%s" % d["uri"],100*i/Ntot)
        data,dummy = self.loadVariable(d)
        if out is None:
            out = data
        else:
            out += data
    out/=len(data)
    out.id = self.getVariableName(dataIn[0])
    cont = True
    self.status.set("Preparing Output",100*float(Ntot-1)/Ntot)
    self.saveVariable(out,self.ensemble,"json")
    return
