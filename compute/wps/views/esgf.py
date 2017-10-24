#! /usr/bin/env python

import collections
import datetime
import json
import re

import cdms2
import requests
from django.core.cache import cache
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods

from . import common
from wps.tasks import process

logger = common.logger

TIME_AXIS_IDS = ('time', 't')

def retrieve_axes(user, dataset_id, query_variable, query_files):
    cache_id = '{}|{}'.format(dataset_id, query_variable)

    axes = cache.get(cache_id)

    if axes is None:
        logger.debug('Dataset variable "{}" not in cache'.format(cache_id))

        if len(query_files) == 0:
            raise Exception('No files to retrieve axes for')

        start = datetime.datetime.now()

        axes = {}

        task = process.CWTBaseTask()

        task.load_certificate(user)

        for i, url in enumerate([query_files[0], query_files[-1]]):
            try:
                with cdms2.open(url) as infile:
                    header = infile[query_variable]

                    if i == 0:
                        for x in header.getAxisList():
                            axis_data = {
                                'id': x.id,
                                'start': x[0],
                                'stop': x[-1],
                                'id_alt': x.attributes['axis'].lower(),
                                'units': x.attributes['units']
                            }

                            axes[x.id] = axis_data
                    else:
                        time = header.getAxisList(axes=('time'))

                        if len(time) > 0:
                            time = time[0]

                            axes[time.id]['stop'] = time[-1]
            except:
                raise Exception('Error accessing OpenDAP url: "{}"'.format(url))

        cache.set(cache_id, axes, 24*60*60)

        logger.debug('retrieve_axes elapsed time {}'.format(datetime.datetime.now()-start))

    return axes

def search_solr(dataset_id, index_node, shard=None, query=None):
    data = cache.get(dataset_id)

    if data is None:
        logger.debug('Dataset "{}" not in cache'.format(dataset_id))

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
        else:
            params['distrib'] = 'false'

        url = 'http://{}/esg-search/search'.format(index_node)

        try:
            response = requests.get(url, params)
        except requests.ConnectionError:
            raise Exception('Connection timed out')
        except requests.RequestException as e:
            raise Exception('Request failed: "{}"'.format(e))

        try:
            data = json.loads(response.content)
        except:
            raise Exception('Failed to load JSON response')

        # Cache for 1 day
        cache.set(dataset_id, data, 24*60*60)

        logger.debug('search_solr elapsed time {}'.format(datetime.datetime.now()-start))

    return data['response']['docs']

def parse_solr_docs(docs):
    variables = collections.OrderedDict()

    for doc in docs:
        var = doc['variable'][0]

        for item in doc['url']:
            url, mime, urlType = item.split('|')

            if urlType.lower() == 'opendap':
                url = url.replace('.html', '')

                if var in variables:
                    variables[var]['files'].append(url)
                else:
                    variables[var] = {'files': [url] }

    return variables

@require_http_methods(['GET'])
@ensure_csrf_cookie
def search_axes(request):
    try:
        common.authentication_required(request)

        dataset_id = request.GET['dataset_id']

        index_node = request.GET['index_node']

        query_variable = request.GET['variable']

        shard = request.GET.get('shard', None)

        query = request.GET.get('query', None)

        docs = search_solr(dataset_id, index_node, shard, query)

        dataset_variables = parse_solr_docs(docs)

        query_files = dataset_variables[query_variable]['files']

        axes = retrieve_axes(request.user, dataset_id, query_variable, query_files)
    except KeyError as e:
        logger.exception('Missing required parameter')

        return common.failed({'message': 'Mising required parameter "{}"'.format(e.message)})
    except Exception as e:
        logger.exception('Error retrieving ESGF search results')

        return common.failed(e.message)
    else:
        return common.success(axes)

@require_http_methods(['GET'])
@ensure_csrf_cookie
def search_dataset(request):
    try:
        common.authentication_required(request)

        dataset_id = request.GET['dataset_id']

        index_node = request.GET['index_node']

        shard = request.GET.get('shard', None)

        query = request.GET.get('query', None)

        docs = search_solr(dataset_id, index_node, shard, query)

        dataset_variables = parse_solr_docs(docs)

        if len(dataset_variables.keys()) == 0:
            raise Exception('Dataset contains no variables')

        query_variable = dataset_variables.keys()[0]

        query_files = dataset_variables[query_variable]['files']

        axes = retrieve_axes(request.user, dataset_id, query_variable, query_files)

        dataset_variables[query_variable]['axes'] = axes.values()
    except KeyError as e:
        logger.exception('Missing required parameter')

        return common.failed({'message': 'Mising required parameter "{}"'.format(e.message)})
    except Exception as e:
        logger.exception('Error retrieving ESGF search results')

        return common.failed(e.message)
    else:
        return common.success(dataset_variables)
