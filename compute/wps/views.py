from django import http

def wps(request):
    return http.HttpResponse('Welcome')
