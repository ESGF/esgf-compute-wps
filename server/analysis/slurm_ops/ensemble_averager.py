import os
import json
import cdms2
cdms2.setNetcdfShuffleFlag(0) ## where value is either 0 or 1
cdms2.setNetcdfDeflateFlag(0) ## where value is either 0 or 1
cdms2.setNetcdfDeflateLevelFlag(0) ## where value is a integer between 0 and 9 included
import random
# Path where output will be stored/cached
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__),"..","output"))
from util import *

def execute(data_str, domain_str, res_str):

    ## Ok now loop thru models
    # Not tring to regrid it's an ensmeble over same model for this basic demo
    # When node manager is in place we code add code to see load balance and maybe spwan new request to avergae on different nodes, or whatever
    
#    print "Ensemble average here!"

    out = None
    N=0
    dataIn = loadData(data_str)
    Ntot = len(dataIn)+1
    for d in dataIn:
        fnm = str(d["url"])
# TODO figure out status updates
#        self.status.set("Reading file: %s" % fnm,100*float(N)/Ntot)

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

    saveVariable(out,res_str,"json")

