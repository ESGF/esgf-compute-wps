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


class ESGFCWTProcess(WPSProcess):
  """Main process class"""
  def __init__(self):
      pass

  def saveVariable(self,data,dest,type="json"):
    cont = True
    while cont:
        rndm = random.randint(0,100000000000)
        fout = os.path.join(BASE_DIR,"%i.nc" % rndm)
        fjson = os.path.join(BASE_DIR,"%i.json" % rndm)
        cont = os.path.exists(fout) or os.path.exists(fjson)
    f=cdms2.open(fout,"w")
    f.write(data)
    f.close()
    out = {}
    out["url"] = "file:/"+fout
    out["id"]=data.id
    Fjson=open(fjson,"w")
    json.dump(out,Fjson)
    Fjson.close()
    dest.setValue(fjson)

  def loadData(self,origin=None):
    if origin is None:
          origin = self.dataIn
    dataFiles = origin.getValue()
    dataIn = []
    if isinstance(dataFiles,str):
        dataFiles = [dataFiles,]
    for fnm in dataFiles:
        f=open(fnm)
        dataIn.append(self.loadVariable(f.read()))
    return dataIn

  def loadVariable(self,data):
    """loads in data, right now can only be json but i guess could have to determine between json and xml"""
    return json.loads(data)

  def  loadDomain(self,origin=None):
      if origin is None:
          origin = self.domain
      domain = origin.getValue()
      f=open(domain)
      return json.loads(f.read())

  def domain2cdms(self,domain):
      kargs = {}
      for k,v in domain.iteritems():
          if k in ["id","version"]:
              continue
          if isinstance( v, float ) or isinstance( v, int ):
              kargs[str(k)] = (v,v,"cob")
          else:
              system = v.get("system","value").lower()
              if isinstance(v["start"],unicode):
                  v["start"] = str(v["start"])
              if isinstance(v["end"],unicode):
                  v["end"] = str(v["end"])
              if system == "value":
                  kargs[str(k)]=(v["start"],v["end"])
              elif system == "index":
                  kargs[str(k)] = slice(v["start"],v["end"])
      return kargs

  def location2cdms(self,domain):
      kargs = {}
      for k,v in domain.iteritems():
          if k not in ["id","version"]:
            kargs[str(k)] = float( str(v) )
      return kargs

  def loadFileFromURL(self,url):
    ## let's figure out between dap or local
    if url[:7].lower()=="http://":
        f=cdms2.open(str(url))
    elif url[:7]=="file://":
        f=cdms2.open(str(url[6:]))
    else:
        # can't figure it out skipping
        f=None
    return f

