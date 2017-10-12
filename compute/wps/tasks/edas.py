#! /usr/bin/env python

from xml.etree import ElementTree as ET

import zmq
from celery.utils.log import get_task_logger

from wps import settings
from wps import processes

logger = get_task_logger('wps.tasks.edas')

def check_exceptions(data):
    if '<exceptions>' in data:
        index = data.index('!')

        data = data[index+1:]

        root = ET.fromstring(data)

        exceptions = root.findall('./exceptions/*')

        if len(exceptions) > 0:
            raise Exception(exceptions[0].text)

@processes.cwt_shared_task()
def edas_submit(self, data_inputs, identifier, **kwargs):
    self.PUBLISH = processes.ALL

    job_id = kwargs.get('job_id')

    context = zmq.Context.instance()

    req_sock = context.socket(zmq.REQ)

    sub_sock = context.socket(zmq.SUB)

    req_sock.connect('tcp://{}:{}'.format(settings.EDAS_HOST, settings.EDAS_REQ_PORT))

    sub_sock.connect('tcp://{}:{}'.format(settings.EDAS_HOST, settings.EDAS_RES_PORT))

    sub_sock.subscribe('{}'.format(job_id))

    extra = '{"reponse":"file"}'

    req_sock.send(str('{}!execute!{}!{}!{}'.format(job_id, identifier, data_inputs, extra)))

    data = req_sock.recv()

    try:
        check_exceptions(data)
    except:
        req_sock.close()

        sub_sock.close()

        raise

    req_sock.close()

    while True:
        if sub_sock.poll(10 * 1000) == 0:
            logger.warning('EDAS response socket timed out')

            sub_sock.close()

            raise Exception('EDAS server timed out')

        data = sub_sock.recv()

        try:
            check_exceptions(data)
        except:
            sub_sock.close()

            raise

    sub_sock.close()

    raise Exception('Parse results')
