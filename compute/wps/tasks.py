#! /usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals

import threading

import zmq

from esgf.wps_lib import metadata as md

from celery import shared_task
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

class ResponseThread(threading.Thread):
    def __init__(self, context, host, response_port):
        super(ResponseThread, self).__init__()

        self.response_socket = context.socket(zmq.PULL)
        self.host = host
        self.response_port = response_port

    def run(self):
        self.response_socket.connect('tcp://%s:%s' % (self.host, self.response_port))

        self.response = self.response_socket.recv()

        self.response_socket.close()

@shared_task
def handle_get(request, host, request_port, response_port):
    logger.info(request.GET)

    required = ('service', 'request')

    for para in required:
        if para not in request.GET:
            er = md.ExceptionReport('1.0.0')

            er.add_exception(md.Exception.MissingParameterValue,
                    'Missing required parameter',
                    para)

            return er.xml()

    logger.info('CDAS2 host: %s request: %s response: %s', host, request_port, response_port)

    context = zmq.Context.instance()

    request_socket = context.socket(zmq.PUSH)
    request_socket.connect('tcp://%s:%s' % (host, request_port))

    response_thread = ResponseThread(context, host, response_port)
    response_thread.start()

    request = request.GET['request'].lower()

    if request == 'getcapabilities':
        request_socket.send(str('12345678!getCapabilities!WPS'))
    elif request == 'describeprocess':
        pass
    elif request == 'execute':
        pass

    response_thread.join()

    request_socket.close()

    result = response_thread.response

    logger.info(result)

    return result.split('!')[-1]

@shared_task
def handle_post(request):
    logger.info(request.POST)

    raise NotImplementedError()

#@app.task
#def describe_process(host, request_port, response_port, identifier):
#    context = zmq.Context.instance()
#
#    request_socket = context.socket(zmq.PUSH)
#    request_socket.connect('tcp://%s:%s' % (host, request_port))
#
#    response_thread = ResponseThread(context, host, response_port)
#    response_thread.start()
#
#    request_socket.send('12345678!describeProcess!%s' % (str(identifier),))
#
#    response_thread.join()
#
#    request_socket.close()
#
#    result = response_thread.response
#
#    return result
#
#@app.task
#def execute(host, request_port, response_port, identifier, data_inputs):
#    context = zmq.Context.instance()
#
#    request_socket = context.socket(zmq.PUSH)
#    request_socket.connect('tcp://%s:%s' % (host, request_port))
#
#    response_thread = ResponseThread(context, host, response_port)
#    response_thread.start()
#
#    request_socket.send('12345678!execute!%s!%s' % (str(identifier), str(data_inputs)))
#
#    response_thread.join()
#
#    request_socket.close()
#
#    result = response_thread.response
#
#    # Temporary fix, eventuall we'll completely rewrap the CDAS response
#    result_doc = result.split('!')[-1]
#
#    from lxml import etree
#
#    tree = etree.fromstring(result_doc)
#
#    # Update status to reflect ProcessSucceeded
#    old_tag = tree.xpath('/wps:ExecuteResponse/wps:Status', namespaces=tree.nsmap)[0].getchildren()[0].tag
#        
#    import re
#
#    new_tag = re.sub('Process.*', 'ProcessSucceeded', old_tag)
#
#    tree.xpath('/wps:ExecuteResponse/wps:Status', namespaces=tree.nsmap)[0].getchildren()[0].tag = new_tag
#
#    # Remove bogus output references that confuse the end-user API
#    matches = tree.xpath('/wps:ExecuteResponse/wps:ProcessOutputs/wps:Output/wps:Reference', namespaces=tree.nsmap)
#
#    for x in matches:
#        x.getparent().remove(x)
#        
#    result = etree.tostring(tree)
#
#    return result
