from django.http import HttpResponse,HttpRequest
from django.shortcuts import redirect, render

from pywps import Pywps

import os
import sys
import glob
import pywps
import random
import logging
import settings
import threading
import subprocess

debug = False

os.environ['PYWPS_PROCESSES'] = os.path.join(settings.WPS_SERVER_DIR, 'processes')

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
    return render(request, 'wps/test_urls.html')

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
        print err
        return parmMap

def wps(request):
    service = Pywps(pywps.METHOD_GET)

    service_inputs = service.parseRequest(request.META['QUERY_STRING'])

    service_response = service.performRequest(service_inputs)

    return HttpResponse(service_response)

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
