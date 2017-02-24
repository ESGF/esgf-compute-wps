from django import http

from wps import tasks

def wps(request):
    if request.method == 'GET':
        result = tasks.handle_get.delay(request.GET)

        response = result.get()
    elif request.method == 'POST':
        raise NotImplementedError()

    return http.HttpResponse(response)
