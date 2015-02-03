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


def status(request):
    processes = glob.glob("err*.txt")
    done =[]
    running=[]
    unknown = []
    failed=[]
    for nm in sorted(processes):
        f=open(nm)
        st=f.read()
        iok = st.find("[processsucceeded]")
        istart = st.rfind("Status [processstarted]")
        ifailed = st.rfind("Status [processfailed]")
        if iok>-1:
            done.append(nm[4:-4])
        elif ifailed>-1:
            failed.append([nm[4:-4],st[ifailed+23:].strip()])
        elif istart>-1:
            line = st[24+istart:].split("\n")[0]
            p = line.split("]")[0]
            msg = ":".join(line.split(":")[1:])
            running.append([nm[4:-4],p,msg])
        else:
            unknown.append(nm[-4:4])

    st="""<h1>Finished Processes</h1>
    <ul>"""
    for d in done:
        st+="<li>%s<a href='http://%s/clear/%s'>clear</a></li>" % (d,request.get_host(),d)
    st+="</ul><h1>Runnning Processes</h1><ul>"
    for r in running:
        st+="<li>%s - %s - %s</li>" % (r[0],r[1],r[2])
    st+="</ul><h1>Failed Processes</h1><ul>"
    for f in failed:
        st+="<li>%s -%s<a href='http://%s/clear/%s'>clear</a></li>" % (f[0],f[1],request.get_host(),f[0])
    st+="</ul>%s " % processes

    return HttpResponse(st)

def clear_process(request,id):
    if 1:
        os.remove("out_%i.txt" % int(id))
        os.remove("err_%i.txt" % int(id))
    return redirect("/status")

def wps(request):
  rndm = random.randint(0,100000000000)
  print "PROCESS:",rndm
  out = open("out_%i.txt" % rndm, "w")
  err = open("err_%i.txt" % rndm, "w")
  T=threading.Thread(target=run_wps,args=(request,out,err,rndm))
  T.start()
  if request.META["QUERY_STRING"].find("request=Execute")>-1:
      return HttpResponse("Staaaaaarted Request Process id: %i" % rndm)
  else:
      T.join()
      out = open("out_%i.txt" % rndm)
      return HttpResponse(out.read())

def run_wps(request,out,err,rndm):

  inputQuery = request.META["QUERY_STRING"]
  P=subprocess.Popen(["wps.py",inputQuery],bufsize=0,stdin=None,stdout=out,stderr=err)
  P.wait()
  out.close()
  err.close()
