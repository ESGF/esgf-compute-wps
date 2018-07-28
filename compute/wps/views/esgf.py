#! /usr/bin/env python

import collections
import datetime
import json
import re

import cdms2
import cdtime
import requests
from django.conf import settings
from django.core.cache import cache
from django.views.decorators.cache import cache_control
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods

from . import common
from wps import WPSError
from wps import tasks

logger = common.logger

def retrieve_axes(user, dataset_id, variable, urls):
    base_cache_id = '{}|{}'.format(dataset_id, variable)

    axes = []
    base_units = None

    tasks.load_certificate(user)

    for i, url in enumerate(sorted(urls)):
        cache_id = '{}|{}'.format(base_cache_id, url)

        data = cache.get(cache_id)

        if data is None:
            logger.info('Retrieving axes fro %r', url)

            start = datetime.datetime.now()

            data = {
                'url': url,
                'temporal': None,
                'spatial': None,
            }

            try:
                with cdms2.open(url) as infile:
                    header = infile[variable]

                    for x in header.getAxisList():
                        if x.isTime():
                            if base_units is None:
                                base_units = x.units

                            x_clone = x.clone()

                            x_clone.toRelativeTime(base_units)

                            data['temporal'] = {
                                'id': x.id,
                                'start': float(x_clone[0]),
                                'stop': float(x_clone[-1]),
                                'units': x.units or None,
                                'length': len(x_clone),
                            }
                        else:
                            axis = {
                                'id': x.id,
                                'start': float(x[0]),
                                'stop': float(x[-1]),
                                'units': x.units or None,
                                'length': len(x),
                            }

                            if data['spatial'] is None:
                                data['spatial'] = [axis]
                            else:
                                data['spatial'].append(axis)
            except cdms2.CDMSError:
                logger.error('Failed to open %r', url)

                raise Exception('Failed to open {}'.format(url))

            #cache.set(data, 24*60*60)

            logger.info('Completed axes retrieval in %r', datetime.datetime.now()-start)
        else:
            logger.info('Loaded axes for %r from cache', url)

        axes.append(data)

    logger.info('%r', axes)

    return axes

def search_solr(dataset_id, index_node, shard=None, query=None):
    data = cache.get(dataset_id)

    if data is None:
        logger.info('Dataset "{}" not in cache'.format(dataset_id))

        start = datetime.datetime.now()

        params = {
            'type': 'File',
            'dataset_id': dataset_id,
            'format': 'application/solr+json',
            'offset': 0,
            'limit': 8000
        }

        if query is not None and len(query.strip()) > 0:
            params['query'] = query.strip()

        if shard is not None and len(shard.strip()) > 0:
            params['shards'] = '{}/solr'.format(shard.strip())

        # enabled distrib search by default
        params['distrib'] = 'true'

        url = 'http://{}/esg-search/search'.format(index_node)

        logger.info('Requesting "{}" {}'.format(url, params))

        try:
            response = requests.get(url, params)
        except requests.ConnectionError:
            raise Exception('Connection timed out')
        except requests.RequestException as e:
            raise Exception('Request failed: "{}"'.format(e))

        try:
            response_json = json.loads(response.content)
        except:
            raise Exception('Failed to load JSON response')

        data = parse_solr_docs(response_json['response']['docs'])

        # Cache for 1 day
        cache.set(dataset_id, data, 24*60*60)

        logger.debug('search_solr elapsed time {}'.format(datetime.datetime.now()-start))
    else:
        logger.info('Dataset "{}" retrieved from cache'.format(dataset_id))

    return data

def parse_solr_docs(docs):
    variables = {}
    files = []

    for doc in docs:
        variable = doc['variable']

        try:
            open_dap = [x for x in doc['url'] if 'opendap' in x.lower()][0]
        except IndexError:
            logger.warning('Skipping %r, missing OpenDAP url', doc['master_id'])

            continue

        url, _, _ = open_dap.split('|')

        url = url.replace('.html', '')

        if url not in files:
            files.append(url)

        for var in variable:
            if var not in variables:
                variables[var] = []

            index = files.index(url)

            variables[var].append(index)

    return { 'variables': variables, 'files': files }

@require_http_methods(['GET'])
@ensure_csrf_cookie
def search_variable(request):
    try:
        common.authentication_required(request)

        try:
            dataset_id = request.GET['dataset_id']

            index_node = request.GET['index_node']

            variable = request.GET['variable']

            files = request.GET['files']
        except KeyError as e:
            raise common.MissingParameterError(name=e.message)

        files = json.loads(files)
        
        if not isinstance(files, list):
            files = [files]

        shard = request.GET.get('shard', None)

        query = request.GET.get('query', None)

        logger.info('Searching for variable %r', variable)
        logger.info('Dataset ID %r', dataset_id)
        logger.info('Index Node %r', index_node)
        logger.info('Files %r', files)

        dataset_variables = search_solr(dataset_id, index_node, shard, query)

        urls = [dataset_variables['files'][int(x)] for x in files]

        axes = retrieve_axes(request.user, dataset_id, variable, urls)
    except WPSError as e:
        logger.exception('Error retrieving ESGF search results')

        return common.failed(e.message)
    else:
        return common.success(axes)

@require_http_methods(['GET'])
@ensure_csrf_cookie
@cache_control(private=True, max_age=3600)
def search_dataset(request):
    try:
        common.authentication_required(request)

        try:
            dataset_id = request.GET['dataset_id']

            index_node = request.GET['index_node']
        except KeyError as e:
            raise common.MissingParameterError(name=e.message)

        shard = request.GET.get('shard', None)

        query = request.GET.get('query', None)

        dataset_variables = search_solr(dataset_id, index_node, shard, query)
    except WPSError as e:
        logger.exception('Error retrieving ESGF search results')

        return common.failed(e.message)
    else:
        return common.success(dataset_variables)
