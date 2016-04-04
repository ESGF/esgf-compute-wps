from django.http import HttpResponse,HttpRequest
from django.shortcuts import redirect
from django.template.loader import get_template
import os
import random
import settings
server_dir = settings.CWT_SERVER_DIR
os.environ["PYWPS_PROCESSES"]=os.path.realpath(os.path.join(server_dir,"processes"))
os.environ["PYWPS_SERVER_DIR"]=os.path.realpath(server_dir)
import glob
import logging
import threading
import subprocess
import sys
sys.path.insert(0,server_dir)
from modules.utilities import *
debug = False

class TestRequest:
    def __init__(self, request_str ):
        self.META = {}
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
    if debug: print "LOOKING AT FILES IN:",settings.PROCESS_TEMPORARY_FILES
    processes = glob.glob(os.path.join(settings.PROCESS_TEMPORARY_FILES,"err*.txt"))
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
  parmMap = { 'embedded': False, 'execute':False, 'async':False }
  try:
      queryStr = request.META["QUERY_STRING"]
      for requestParm in queryStr.split('&'):
          rParmElems = requestParm.split('=')
          param = rParmElems[0].strip().lower()
          paramVal = rParmElems[1].strip().lower()
          if ( param == 'request' ) and ( paramVal == 'execute' ): parmMap['execute']  = True
          if ( param == 'embedded' ) and ( paramVal == 'true' ):   parmMap['embedded']  = True
          if ( param == 'async' ) and ( paramVal == 'true' ):   parmMap['async']  = True
          if ( param == 'datainputs' ):
            debug_trace()
            value_cut = requestParm.find('=')
            di_params = requestParm[value_cut+1:].strip('[]').lower().split(';')
            for di_param in di_params:
                diparam, diparamval = di_param.split('=')
                if ( diparam == 'embedded' ) and ( diparamval == 'true' ):   parmMap['embedded']  = True
                if ( diparam == 'async' ) and ( diparamval == 'true' ):      parmMap['async']  = True
  except Exception, err:
      wpsLog.error( "Error in getRequestParms: %s " % str(err) )
  return parmMap

def wps(request):
  wpsLog.debug( "WPS-> process request: %s" % str(request) )
  rndm = random.randint(0,100000000000)
  out = open(os.path.join(settings.PROCESS_TEMPORARY_FILES,"out_%i.txt" % rndm), "w")
  err = open(os.path.join(settings.PROCESS_TEMPORARY_FILES,"err_%i.txt" % rndm), "w")
  requestParams = getRequestParms(request)
  T=threading.Thread(target=run_wps,args=(request,out,err,rndm))
  T.start()
  if requestParams['async'] and requestParams['execute']:
      return HttpResponse("Started Request Process id: <a href='http://%s/view_process/%i'>%i</a>" % (request.get_host(),rndm,rndm))
  else:
      T.join()
      out = open(os.path.join(settings.PROCESS_TEMPORARY_FILES,"out_%i.txt" % rndm))
      st = out.readlines()
      out.close()
      # os.remove(os.path.join(settings.PROCESS_TEMPORARY_FILES,"out_%i.txt" % rndm))
      # os.remove(os.path.join(settings.PROCESS_TEMPORARY_FILES,"err_%i.txt" % rndm))
      return HttpResponse("".join(st[2:])) # skips first two lines that confuses Maarten web

def run_wps(request,out,err,rndm):
  inputQuery = request.META["QUERY_STRING"]
  if debug:
      print "Dumping to:",out,err
  P=subprocess.Popen(["wps.py",inputQuery],bufsize=0,stdin=None,stdout=out,stderr=err)
  P.wait()
  out.close()
  err.close()

def postprocess(request):
    # First run the actual wps
    out = wps(request)
    # isolate data section
    start = out.find("<wps:ExecuteResponse")
    from xml.dom import minidom
    outxml = minidom.parseString(out[start:])


    response = HttpResponse()
    response['content_type'] =  'image/png'
    response['content'] = '<html>test123</html>'
    response['Content-Length'] = len(response.content)

    return response

if __name__ == "__main__":
    project_dir = os.path.dirname( os.path.dirname( __file__ ) )
    sys.path.append( project_dir )
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "wps.settings")
    os.environ.setdefault("PYWPS_CFG", os.path.join( project_dir, "wps.cfg" ) )
    os.environ.setdefault("DOCUMENT_ROOT", os.path.join( project_dir, "wps") )

    testRequest = TestRequest('version=1.0.0&service=wps&request=getcapabilities')
    run_wps( testRequest, sys.stdout, sys.stderr, 0)

#DJANGO_SETTINGS_MODULE wps.settings
#PYWPS_CFG /usr/local/web/WPCDAS/server/wps.cfg
#DOCUMENT_ROOT  /usr/local/web/WPCDAS/server/wps

#  inputQuery = "version=1.0.0&service=wps&request=DescribeProcess&identifier=timeseries"
#  P=subprocess.Popen(["wps.py",inputQuery],bufsize=0,stdin=None,stdout=out,stderr=err)
#  P.wait()
#  out.close()
#  err.close()
