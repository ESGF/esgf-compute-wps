import json
import logging
import os
import uuid

import celery
import cwt
import numpy as np
from django.conf import settings

from wps import helpers
from wps import models
from wps import tasks
from wps.backends import backend
from wps.tasks import base

__ALL__ = ['CDAT']

logger = logging.getLogger('wps.backends')

class CDAT(backend.Backend):
    def initialize(self):
        pass

    def populate_processes(self):
        logger.info('Registering processes for backend "local"')

        for name, proc in base.REGISTRY.iteritems():
            self.add_process(name, name.title(), data_inputs=proc.INPUT, 
                             process_outputs=proc.OUTPUT, abstract=proc.ABSTRACT)

    def load_data_inputs(self, variable_raw, domain_raw, operation_raw):
        variable = {}

        for item in json.loads(variable_raw):
            v = cwt.Variable.from_dict(item)

            variable[v.name] = v

        domain = {}

        for item in json.loads(domain_raw):
            d = cwt.Domain.from_dict(item)

            domain[d.name] = d

        operation = {}

        for item in json.loads(operation_raw):
            o = cwt.Process.from_dict(item)

            operation[o.name] = o

        for o in operation.values():
            o.domain = domain[o.domain]

            inputs = []

            for v in o.inputs:
                inputs.append(variable[v])

            o.inputs = inputs

        return variable, domain, operation

    def execute(self, **kwargs):
        identifier = kwargs['identifier']

        variable = kwargs['variable']

        domain = kwargs['domain']

        operation = kwargs['operation']

        user = kwargs['user']

        job = kwargs['job']

        variable, domain, operation = self.load_data_inputs(variable, domain, operation)
