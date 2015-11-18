from django.http import HttpResponse,HttpRequest
from django.shortcuts import redirect
import sys
import os
import random
os.environ["PYWPS_PROCESSES"]=os.path.realpath(os.path.join(os.path.dirname(__file__),"..","processes"))
import pywps
import glob
import threading
import subprocess
#from processes.cdasProcess import wpsLog
from django import template
from django.template.loader import get_template
import settings


enable_debug=False
if enable_debug:
    import pydevd

class TestRequest:
    def __init__(self, request_str ):
        self.META["QUERY_STRING"] = request_str

def process_status(nm):
    f=open(nm)
    st=f.read()
    iok = st.find("[processsucceeded]")
    istart = st.rfind("Status [processstarted]")
    ifailed = st.rfind("Status [processfailed]")
    if istart>-1:
        line = st[24+istart:].split("\n")[0]
        p = line.split("]")[0]
    else:
        p=0
    if iok>-1:
        return "Done",100.,st
    elif ifailed>-1:
        return "Failed",p,st[ifailed+23:].strip()
    elif istart>-1:
        msg = ":".join(line.split(":")[1:])
        return "Running",p,msg
    else:
        return "Unknown",-1,"???"

def status(request):
    processes = glob.glob("err*.txt")
    done =[]
    running=[]
    unknown = []
    failed=[]
    for nm in sorted(processes):
        id = nm[4:-4]
        status,p,msg = process_status(nm)
        if status == "Done":
            done.append(id)
        elif status == "Failed":
            failed.append([id,p,msg])
        elif status == "Running":
            running.append([id,p,msg])
        else:
            unknown.append(id)
    st="""<h1>Finished Processes</h1>
    <ul>"""
    host = request.get_host()
    for d in done:
        st+="<li><a href='http://%s/view/%s'>%s</a> <a href='http://%s/clear/%s'>clear</a></li>" % (host,d,d,host,d)
    st+="</ul><h1>Runnning Processes</h1><ul>"
    for r in running:
        st+="<li>%s - %s - %s</li>" % (r[0],r[1],r[2])
    st+="</ul><h1>Failed Processes</h1><ul>"
    for f in failed:
        st+="<li>%s (%s %%) -%s<a href='http://%s/clear/%s'>clear</a></li>" % (f[0],f[1],f[2],host,f[0])
    st+="</ul>%s " % processes

    return HttpResponse(st)

def view_main(request):
    t = get_template("test_urls.html")
    html = t.render()
    return HttpResponse(html)

def view_process(request,id):
    nm = os.path.join(settings.PROCESS_TEMPORARY_FILES,"out_%s.txt" % id)
    if os.path.exists(nm):
        f=open(nm)
        msg = f.read()
    else:
        msg = "Unknown process"
    return HttpResponse(msg)

def clear_process(request,id):
    os.remove(os.path.join(settings.PROCESS_TEMPORARY_FILES,"out_%i.txt" % int(id)))
    os.remove(os.path.join(settings.PROCESS_TEMPORARY_FILES,"err_%i.txt" % int(id)))
    return redirect("/status")

def getRequestParms( request ):
  parmMap = { 'embedded': False, 'execute':False }
  queryStr = request.META["QUERY_STRING"]
#  wpsLog.debug("Query String: " + queryStr)
  for requestParm in queryStr.split('&'):
      rParmElems = requestParm.split('=')
      param = rParmElems[0].strip().lower()
      paramVal = rParmElems[1].strip().lower()
      if ( param == 'datainputs'):
        sIndex = paramVal.find('embedded')
        if sIndex > 0:
            arg_value = paramVal[sIndex+9:sIndex+17]
            if ( arg_value.find( 'true' ) >= 0 ):
                parmMap['embedded']  = True
      if ( param == 'request' ) and ( paramVal == 'execute' ): parmMap['execute']  = True
  return parmMap

def wps(request):
  print "DO WE EVEN COME HERE",os.getcwd()
  rndm = random.randint(0,100000000000)
  out = open(os.path.join(settings.PROCESS_TEMPORARY_FILES,"out_%i.txt" % rndm), "w")
  err = open(os.path.join(settings.PROCESS_TEMPORARY_FILES,"err_%i.txt" % rndm), "w")
  if enable_debug: pydevd.settrace('localhost', port=8030, stdoutToServer=False, stderrToServer=True)
  requestParams = getRequestParms(request)

  T=threading.Thread(target=run_wps,args=(request,out,err,rndm))
  T.start()
  if not requestParams['embedded'] and requestParams['execute']:
      return HttpResponse("Started Request Process id: <a href='http://%s/view/%i'>%i</a>" % (request.get_host(),rndm,rndm))
  else:
      T.join()
      out = open(os.path.join(settings.PROCESS_TEMPORARY_FILES,"out_%i.txt" % rndm))
      print "READING IN:",out
      st = out.read()
      out.close()
      return HttpResponse(st)

def run_wps(request,out,err,rndm):
  inputQuery = request.META["QUERY_STRING"]
  print "QUERY:",inputQuery,rndm,out,err
  P=subprocess.Popen(["wps.py",inputQuery],bufsize=0,stdin=None,stdout=out,stderr=err)
  P.wait()
  print "Colsing:",out
  out.close()
  print "Colsing:",err
  err.close()
  
if __name__ == "__main__":
    project_dir = os.path.dirname( os.path.dirname( __file__ ) )
    sys.path.append( project_dir )
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "wps.settings")
    os.environ.setdefault("PYWPS_CFG", os.path.join( project_dir, "wps.cfg" ) )
    os.environ.setdefault("DOCUMENT_ROOT", os.path.join( project_dir, "wps") )

    testRequest = TestRequest('service=WPS&version=1.0.0&request=Execute&rawDataOutput=result&identifier=timeseries&datainputs=%5Bdomain%3D%7B%22longitude%22%3A-130.4296875%2C%22latitude%22%3A-28.828125%7D%3Bvariable%3D%7B%22url%22%3A%22file%3A%2F%2Fusr%2Flocal%2Fweb%2Fdata%2FMERRA%2FTemp2D%2FMERRA_3Hr_Temp.xml%22%2C%22id%22%3A%22t%22%7D%3Boperation%3D%7B%22bounds%22%3A%22DJF%22%2C%22type%22%3A%22climatology%22%7D%3Bembedded%3Dtrue%3B')
    run_wps( testRequest, sys.stdout, sys.stderr, 0)



#  inputQuery = "version=1.0.0&service=wps&request=DescribeProcess&identifier=timeseries"
#  P=subprocess.Popen(["wps.py",inputQuery],bufsize=0,stdin=None,stdout=out,stderr=err)
#  P.wait()
#  out.close()
#  err.close()
