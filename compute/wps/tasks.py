#! /usr/bin/env python

from __future__ import absolute_import

import zmq
import threading
from pycdas.portal import cdas

from wps import logger
from wps.celery import app

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

@app.task
def get_capabilities(host, request_port, response_port):
    context = zmq.Context.instance()

    request_socket = context.socket(zmq.PUSH)
    request_socket.connect('tcp://%s:%s' % (host, request_port))

    response_thread = ResponseThread(context, host, response_port)
    response_thread.start()

    request_socket.send('12345678!getCapabilities!WPS')

    response_thread.join()

    request_socket.close()

    result = response_thread.response

    return result.split('!')[-1]

@app.task
def describe_process(host, request_port, response_port, identifier):
    context = zmq.Context.instance()

    request_socket = context.socket(zmq.PUSH)
    request_socket.connect('tcp://%s:%s' % (host, request_port))

    response_thread = ResponseThread(context, host, response_port)
    response_thread.start()

    request_socket.send('12345678!describeProcess!%s' % (str(identifier),))

    response_thread.join()

    request_socket.close()

    result = response_thread.response

    return result

@app.task
def execute(host, request_port, response_port, identifier, data_inputs):
    context = zmq.Context.instance()

    request_socket = context.socket(zmq.PUSH)
    request_socket.connect('tcp://%s:%s' % (host, request_port))

    response_thread = ResponseThread(context, host, response_port)
    response_thread.start()

    request_socket.send('12345678!execute!%s!%s' % (str(identifier), str(data_inputs)))

    response_thread.join()

    request_socket.close()

    result = response_thread.response

    # Temporary fix, eventuall we'll completely rewrap the CDAS response
    result_doc = result.split('!')[-1]

    from lxml import etree

    tree = etree.fromstring(result_doc)

    # Update status to reflect ProcessSucceeded
    old_tag = tree.xpath('/wps:ExecuteResponse/wps:Status', namespaces=tree.nsmap)[0].getchildren()[0].tag
        
    import re

    new_tag = re.sub('Process.*', 'ProcessSucceeded', old_tag)

    tree.xpath('/wps:ExecuteResponse/wps:Status', namespaces=tree.nsmap)[0].getchildren()[0].tag = new_tag

    # Remove bogus output references that confuse the end-user API
    matches = tree.xpath('/wps:ExecuteResponse/wps:ProcessOutputs/wps:Output/wps:Reference', namespaces=tree.nsmap)

    for x in matches:
        x.getparent().remove(x)
        
    result = etree.tostring(tree)

    return result
