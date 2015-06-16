#from pywps.Process import WPSProcess
import sys, os
import json




import cdms2

import cdutil

cdms2.setNetcdfShuffleFlag(0)
cdms2.setNetcdfDeflateFlag(0)
cdms2.setNetcdfDeflateLevelFlag(0)

import random
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'output'))

#from cdasProcess import CDASProcess

# Test arguments for run configuration:
# version=1.0.0&service=wps&request=Execute&identifier=averager&datainputs=[domain={\"longitude\":{\"start\":-180.0,\"end\":180.0}};variable={\"url\":\"file://Users/tpmaxwel/Data/AConaty/comp-ECMWF/geos5.xml\",\"id\":\"uwnd\"}]

from util import *

def execute(data_str, domain_str, res_str):

      dataIn= loadData(data_str)
                
      domain = loadDomain(domain_str)

      cdms2keyargs = domain2cdms(domain)

      f=loadFileFromURL(dataIn["url"])

      data = f(dataIn["id"],**cdms2keyargs)
      dims = "".join(["(%s)" % x for x in cdms2keyargs.keys()])

      data = cdutil.averager(data,axis=dims)
      data.id=dataIn["id"]

      saveVariable(data, res_str)

