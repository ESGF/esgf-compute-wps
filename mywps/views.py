from django.http import HttpResponse
import sys
import pywps
import StringIO

def wps(request):
    output = open("crap.txt","w")
    keep =sys.stdout
    sys.stdout = output
    a = pywps.Pywps(pywps.METHOD_GET)
    inputQuery = "service=wps&request=getcapabilities"
    print >>output, "BRGININNG"
    if a.parseRequest(inputQuery):
        pywps.debug(a.inputs)
        response = a.performRequest()


        if response:
            # print only to standard out
            pywps.response.response(a.response,
            sys.stdout,a.parser.soapVersion,a.parser.isSoap,a.parser.isSoapExecute, a.request.contentType)
        else:
            return HttpResponse("Failed")
    sys.stdout=keep
    output.close()
    a=open("crap.txt")
    return HttpResponse(a.read())
