from django import http

from wps import models
from wps import tasks
from wps import node_manager

def wps(request):
    if request.method == 'GET':
        result = tasks.handle_get.delay(request.GET)

        response = result.get()
    elif request.method == 'POST':
        raise NotImplementedError()

    return http.HttpResponse(response)

def status(request, job_id):
    manager = node_manager.NodeManager()

    status = manager.get_status(job_id) 

    return http.HttpResponse(status)
