from django.http import HttpResponse
import sys
import os
os.environ["PYWPS_PROCESSES"]=os.path.realpath(os.path.join(os.path.dirname(__file__),"..","processes"))
import pywps

def wps(request):
  output = open("crap.txt","w")
  keep =sys.stdout
  sys.stdout = output
  a = pywps.Pywps(request.META["REQUEST_METHOD"])
  inputQuery = request.META["QUERY_STRING"]
  print >>output, __file__
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
