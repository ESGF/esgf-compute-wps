#! /usr/bin/env python

import importlib
import json
import os
import pkgutil
import types
from builtins import str
from functools import partial
from functools import wraps

import celery
import cwt
from celery import shared_task
from celery.utils.log import get_task_logger
from jinja2 import Environment, BaseLoader

from compute_tasks import AccessError
from compute_tasks import WPSError

logger = get_task_logger('wps.tasks.base')

REGISTRY = {}
BINDINGS = {}


def discover_processes(ignore_modules=None):
    if not ignore_modules:
        ignore_modules = ['base', 'tests']

    processes = []

    logger.info('Discovering processes for module %r', os.path.dirname(__file__))

    for _, name, _ in pkgutil.iter_modules([os.path.dirname(__file__)]):
        if name in ignore_modules:
            logger.info('Skipping %s module', name)

            continue

        mod = importlib.import_module('.{!s}'.format(name), package='compute_tasks')

        logger.info('Processing module %r', name)

        if 'discover_processes' in dir(mod):
            logger.info('Found discover_processes method in module %r', name)

            method = getattr(mod, 'discover_processes')

            data = method()

            logger.info('Extending processes by %r', len(data))

            processes.extend(data)

    logger.info('Extending processes from local registry by %r', len(REGISTRY.values()))

    processes.extend(REGISTRY.values())

    return processes


def get_process(identifier):
    try:
        return BINDINGS[identifier]
    except KeyError as e:
        raise WPSError('Unknown process {!r}', e)


def build_parameter(name, desc, type, subtype=None, min=0, max=1, validate_func=None):
    return {
        'name': name,
        'desc': desc,
        'type': type,
        'subtype': subtype,
        'min': min,
        'max': max,
        'validate_func': validate_func,
    }


def parameter(name, desc, type, subtype=None, min=0, max=1, validate_func=None):
    def _wrapper(func):
        if name in func._parameters:
            raise Exception('Parameter {!s} already exists'.format(name))

        func._parameters[name] = build_parameter(name, desc, type, subtype, min, max, validate_func)

        return func
    return _wrapper


def abstract(txt):
    def _wrapper(func):
        if func._abstract is None:
            func._abstract = txt

        return func
    return _wrapper


class ValidationError(WPSError):
    pass


def validate_parameter(param, name, type, subtype, min, max, validate_func, num_inputs, input_var_names, **kwargs):
    if param is None and min > 0:
        raise ValidationError(f'Parameter {name!r} is required')

    if type in (list, tuple):
        num = len(param.values)

        if validate_func is not None:
            validate_kwargs = {
                'values': param.values,
                'num_param': num,
                'num_inputs': num_inputs,
                'input_var_names': input_var_names,
            }

            try:
                valid = validate_func(**validate_kwargs)
            except ValidationError as e:
                raise e
            except Exception:
                raise ValidationError(f'Parameter {name!r} failed validation, check abstract for details.')

            if not valid:
                raise ValidationError(f'Parameter {name!r} failed validation, check abstract for details.')

        if num < min or num > max:
            raise ValidationError(f'The number of parameter values {num!r} for {name!r} is out of range min {min!r} max {max!r}.')

        for x in param.values:
            try:
                subtype(x)
            except ValueError:
                raise ValidationError(f'Could not convert parameter {name!r} to {type.__name__!s}.')
    else:
        try:
            type(param.values[0])
        except ValueError:
            raise ValidationError(f'Could not convert parameter {name!r} to {type.__name__!s}.')


def validate(self, context, process, input_var_names):
    context.message(f'Validating inputs and parameters of {process.identifier!s} ({process.name!s})')

    num = len(process.inputs)

    if num < self._min or num > self._max:
        raise ValidationError(f'The number of inputs {num!r} is out of range min {self._min!r} max {self._max!r}.')

    if (all([isinstance(x, cwt.Variable) for x in process.inputs]) and
            len(input_var_names) > 1 and
            process.identifier == 'CDAT.aggregate'):
        raise ValidationError(f'Expecting the same variable for all inputs to {process.identifier!r}, got {input_var_names!r}')

    for x in self._parameters.values():
        p = process.get_parameter(x['name'])

        if p is not None:
            validate_parameter(p, num_inputs=num, input_var_names=input_var_names, **x)

            if x['name'] == 'variable':
                for y in p.values:
                    if y not in input_var_names:
                        raise ValidationError(f'Variable {p.values[0]!r} is not present in input, found values {input_var_names!r}')


def validate_workflow(context):
    context.message('Validating workflow')

    for next, var_names in context.topo_sort():
        logger.info('Validating %r candidate variable names %r', next.identifier, var_names)

        process = get_process(next.identifier)

        # Need to call attached method since its bootstrapped with self
        process._validate(context, next, var_names)


BASE_ABSTRACT = """
{{- description }}
{% if min > 0 and (max == infinity) %}
Accepts a minimum of {{ min }} inputs.
{% elif min == max %}
Accepts exactly {{ min }} input.
{% elif max > min %}
Accepts {{ min }} to {{ max }} inputs.
{% endif %}
{%- if params|length > 0 %}
Parameters:
{%- for item in params %}
{%- if item["min"] > 0 %}
    {{ item["name"] }} ({{ item["type"].__name__ }}): {{ item["desc"] }}
{%- endif %}
{%- endfor %}
{%- for item in params %}
{%- if item["min"] <= 0 %}
    {{ item["name"] }} ({{ item["type"].__name__ }}): {{ item["desc"] }}
{%- endif %}
{%- endfor %}
{% endif %}
"""


template = Environment(loader=BaseLoader).from_string(BASE_ABSTRACT)


def render_abstract(self):
    kwargs = {
        'description': self._abstract,
        'min': self._min,
        'max': self._max,
        'params': [],
        'infinity': float('inf'),
    }

    for x in self._parameters.values():
        if x is None:
            continue

        kwargs['params'].append({
            'name': x['name'],
            'desc': x['desc'],
            'type': x['type'],
            'subtype': x['subtype'],
            'min': x['min'],
            'max': x['max'],
        })

    return template.render(**kwargs)


def get_parameters(self, process):
    params = {}

    for x in self._parameters.values():
        name, type, subtype = x['name'], x['type'], x['subtype']

        proc_param = process.get_parameter(name)

        if proc_param is None:
            params[name] = None
        else:
            if type in (list, tuple):
                params[name] = [subtype(x) for x in proc_param.values]
            else:
                params[name] = type(proc_param.values[0])

    return params


def register_process(identifier, min=1, max=1, abstract=None, version=None, **metadata):
    def _wrapper(func):
        def _inner_wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        if func.__doc__ is not None:
            _abstract = func.__doc__
        else:
            _abstract = abstract

        # Setting self using partial
        process = partial(_inner_wrapper, _inner_wrapper)

        setattr(process, '_identififer', identifier)
        setattr(process, '_parameters', {})
        setattr(process, '_abstract', _abstract)
        setattr(process, '_validate', partial(validate, process))
        setattr(process, '_render_abstract', partial(render_abstract, process))
        setattr(process, '_get_parameters', partial(get_parameters, process))
        setattr(process, '_task', cwt_shared_task(serializer='cwt_json')(func))
        setattr(process, '_min', min)
        setattr(process, '_max', max)

        metadata['inputs'] = min

        backend, _ = identifier.split('.')

        REGISTRY[identifier] = {
            'identifier': identifier,
            'backend': backend,
            'abstract': _abstract or None,
            'metadata': json.dumps(metadata),
            'version': version or 'devel',
        }

        BINDINGS[identifier] = process

        return process
    return _wrapper


class CWTBaseTask(celery.Task):
    def on_retry(self, exc, task_id, args, kwargs, einfo):
        logger.info('Retry %r %r %r %r', task_id, args, kwargs, exc)

        try:
            args[0].message('Retrying from error: {!s}', exc)
        except WPSError:
            raise

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.info('Failure %r %r %r %r', task_id, args, kwargs, exc)

        from compute_tasks import context

        try:
            args[0].failed(str(exc))

            args[0].update_metrics(context.FAILURE)
        except WPSError:
            raise


cwt_shared_task = partial(shared_task,
                          bind=True,
                          base=CWTBaseTask,
                          autoretry_for=(AccessError,),
                          retry_kwargs={
                              'max_retries': 4,
                          },
                          retry_backoff=10)
