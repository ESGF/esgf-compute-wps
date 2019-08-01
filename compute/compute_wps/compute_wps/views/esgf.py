#! /usr/bin/env python

import contextlib
import os
import hashlib
import json
import tempfile
import urllib
import xml.etree.ElementTree as ET

import cdms2
import cdtime
import cwt
import requests
from django.conf import settings
from django.core.cache import cache
from django.http import HttpResponse
from django.http import JsonResponse
from django.views.decorators.cache import cache_page
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods

from . import common
from compute_wps import metrics
from compute_wps.exceptions import AccessError
from compute_wps.exceptions import WPSError
from compute_wps.auth import oauth2

logger = common.logger


class AxisConversionError(WPSError):
    def __init__(self, axis, base_units=None):
        msg = 'Error converting axis {axis.id}'

        if base_units is not None:
            msg += ' with base units {base_units}'

        super(AxisConversionError, self).__init__(msg, axis=axis, base_units=base_units)


def load_certificate(user):
    """ Loads a user certificate.

    First the users certificate is checked and refreshed if needed. It's
    then written to disk and the processes current working directory is
    set, allowing calls to NetCDF library to use the certificate.

    Args:
        user: User object.
    """
    if not oauth2.check_certificate(user):
        oauth2.refresh_certificate(user)

    cert_data = user.auth.cert

    cwd = os.getcwd()

    cert_path = os.path.join(cwd, 'cert.pem')

    with open(cert_path, 'w') as outfile:
        outfile.write(cert_data)

    logger.info('Wrote user certificate')

    dodsrc_path = os.path.join(cwd, '.dodsrc')

    with open(dodsrc_path, 'w') as outfile:
        outfile.write('HTTP.COOKIEJAR=.dods_cookies\n')
        outfile.write('HTTP.SSL.CERTIFICATE={}\n'.format(cert_path))
        outfile.write('HTTP.SSL.KEY={}\n'.format(cert_path))
        outfile.write('HTTP.SSL.VERIFY=0\n')

    logger.info('Wrote .dodsrc file {}'.format(dodsrc_path))

    return cert_path


def describe_axis(axis):
    """ Describe an axis.
    Args:
        axis: A cdms2.axis.TransientAxis or cdms2.axis.FileAxis.

    Returns:
        A dict describing the axis.
    """
    data = {
        'id': axis.id,
        'start': float(axis[0]),
        'stop': float(axis[-1]),
        'units': axis.units or None,
        'length': len(axis),
    }

    logger.info('Described axis %r', data)

    if axis.isTime():
        component = axis.asComponentTime()

        data['start_timestamp'] = str(component[0])

        data['stop_timestamp'] = str(component[-1])

    return data


def process_axes(header):
    """ Processes the axes of a file.
    Args:
        header: A cdms2.fvariable.FileVariable.

    Returns:
        A dict containing the url, temporal and spatial axes.
    """
    data = {}
    base_units = None

    for axis in header.getAxisList():
        logger.info('Processing axis %r', axis.id)

        if axis.isTime():
            if base_units is None:
                base_units = axis.units

            logger.info('Process %r with units %r', axis.id, base_units)

            axis_clone = axis.clone()

            try:
                axis_clone.toRelativeTime(base_units)
            except Exception:
                raise AxisConversionError(axis_clone, base_units)

            data['temporal'] = describe_axis(axis_clone)
        else:
            desc = describe_axis(axis)

            if 'spatial' not in data:
                data['spatial'] = [desc]
            else:
                data['spatial'].append(desc)

    return data


def check_access(uri, cert=None):
    url = '{!s}.dds'.format(uri)

    logger.info('Checking access with %r', url)

    parts = urllib.parse.urlparse(url)

    try:
        response = requests.get(url, timeout=(10, 30), cert=cert, verify=False)
    except requests.ConnectionError:
        logger.exception('Connection error %r', parts.hostname)

        metrics.WPS_DATA_ACCESS_FAILED.labels(parts.hostname).inc()

        raise AccessError(url, 'Connection error to {!r}'.format(parts.hostname))
    except requests.ConnectTimeout:
        logger.exception('Timeout connecting to %r', parts.hostname)

        metrics.WPS_DATA_ACCESS_FAILED.labels(parts.hostname).inc()

        raise AccessError(url, 'Timeout connecting to {!r}'.format(parts.hostname))
    except requests.ReadTimeout:
        logger.exception('Timeout reading from %r', parts.hostname)

        metrics.WPS_DATA_ACCESS_FAILED.labels(parts.hostname).inc()

        raise AccessError(url, 'Timeout reading from {!r}'.format(parts.hostname))

    if response.status_code == 200:
        return True

    logger.info('Checking url failed with status code %r', response.status_code)

    metrics.WPS_DATA_ACCESS_FAILED.labels(parts.hostname).inc()

    return False


@contextlib.contextmanager
def open_file(user, uri):
    try:
        temp_file = tempfile.TemporaryDirectory()

        if not check_access(uri):
            cert_path = load_certificate(user)

            if not check_access(uri, cert_path):
                raise WPSError('File {!r} is not accessible, check the OpenDAP service', uri)

            logger.info('File %r requires certificate', uri)

        try:
            yield cdms2.open(uri)
        except Exception:
            raise WPSError('Error open file %r', uri)
    finally:
        temp_file.cleanup()


def process_url(user, prefix_id, var):
    """ Processes a url.
    Args:
        prefix_id: A str prefix to build the cache id.
        url: A str url path.
        variable: A str variable name.

    Returns:
        A list of dicts describing each files axes.
    """
    cache_id = '{}|{}'.format(prefix_id, var.uri)

    cache_id = hashlib.md5(cache_id.encode()).hexdigest()

    data = cache.get(cache_id)

    logger.info('Processing %r', var)

    if data is None:
        data = {'url': var.uri}

        with open_file(user, var.uri) as handle:
            axes = process_axes(handle[var.var_name])

        data.update(axes)

        cache.set(cache_id, data, 24*60*60)

    return data


def retrieve_axes(user, dataset_id, variable, urls):
    """ Retrieves the axes for a set of urls.
    Args:
        user: A wps.models.User object.
        dataset_id: A str dataset id.
        variable: A str variable name.
        urls: A list of str url paths.

    Returns:
        A list of dicts containing the axes of each file.
    """
    prefix_id = '{}|{}'.format(dataset_id, variable)

    axes = []

    for url in sorted(urls):
        var = cwt.Variable(url, variable)

        data = process_url(user, prefix_id, var)

        axes.append(data)

    return axes


def search_params(dataset_id, query, shard):
    """ Prepares search params for ESGF.
    Args:
        dataset_id: A str dataset id.
        query: A str search query.
        shard: A str shard to search.

    Returns:
        A dict containing the search params.
    """
    params = {
        'type': 'File',
        'dataset_id': dataset_id.strip(),
        'format': 'application/solr+json',
        'offset': 0,
        'limit': 10000,
    }

    if query is not None and len(query.strip()) > 0:
        params['query'] = query.strip()

    if shard is not None and len(shard.strip()) > 0:
        params['shards'] = '{}/solr'.format(shard.strip())

    # enabled distrib search by default
    params['distrib'] = 'true'

    logger.info('ESGF search params %r', params)

    return params


def parse_solr_docs(response):
    """ Parses the solr response docs.
    Args:
        response: A str response from a solr search in json format.

    Returns:
        A dict containing the parsed variables and files.

        {
            "variables": {
                "tas": [0,2,3,4]
            },
            "files": [
                'file1.nc',
                'file2.nc',
                ...
                'file20.nc',
            ]
        }
    """
    variables = {}
    files = []

    for doc in response['response']['docs']:
        variable = doc['variable']

        try:
            open_dap = [x for x in doc['url'] if 'opendap' in x.lower()][0]
        except IndexError:
            logger.error('Skipping %r, missing OpenDAP url', doc['master_id'])

            raise WPSError('Dataset {!r} is not accessible through OpenDAP', doc['id'])

        url, _, _ = open_dap.split('|')

        url = url.replace('.html', '')

        if url not in files:
            files.append(url)

        for x, var in enumerate(variable):
            if var not in variables:
                variables[var] = []

            index = files.index(url)

            # Collect the indexes of the files containing this variable
            variables[var].append(index)

    return {'variables': variables, 'files': files}


def search_solr(dataset_id, index_node, shard=None, query=None):
    """ Search ESGF solr.
    Args:
        dataset_id: A str dataset id.
        index_node: A str of the host to run the search on.
        shard: A str shard name to pass.
        query: A str query to pass.

    Returns:
        A dict containing the parsed solr documents.
    """
    cache_id = hashlib.md5(dataset_id.encode()).hexdigest()

    data = cache.get(cache_id)

    if data is None:
        params = search_params(dataset_id, query, shard)

        url = 'http://{}/esg-search/search'.format(index_node)

        logger.info('Searching %r', url)

        with metrics.WPS_ESGF_SEARCH.time():
            try:
                response = requests.get(url, params)

                metrics.WPS_ESGF_SEARCH_SUCCESS.inc()
            except requests.ConnectionError:
                metrics.WPS_ESGF_SEARCH_FAILED.inc()

                raise Exception('Connection to ESGF search {!s} timed out'.format(index_node))
            except requests.RequestException:
                metrics.WPS_ESGF_SEARCH_FAILED.inc()

                raise Exception('Request to ESGF search {!s} failed'.format(index_node))

        try:
            response_json = json.loads(response.content)
        except Exception:
            raise Exception('Failed to load JSON response')

        data = parse_solr_docs(response_json)

        cache.set(dataset_id, data, 24*60*60)

    return data


@require_http_methods(['GET'])
@ensure_csrf_cookie
def search_variable(request):
    try:
        common.authentication_required(request)

        try:
            dataset_id = request.GET['dataset_id']

            variable = request.GET['variable']

            files = request.GET['files']
        except KeyError as e:
            raise common.MissingParameterError(name=str(e))

        files = json.loads(files)

        if not isinstance(files, list):
            files = [files]

        index_node = request.GET.get('index_node', settings.ESGF_SEARCH)

        shard = request.GET.get('shard', None)

        query = request.GET.get('query', None)

        dataset_variables = search_solr(dataset_id, index_node, shard, query)

        urls = [dataset_variables['files'][int(x)] for x in files]

        axes = retrieve_axes(request.user, dataset_id, variable, urls)
    except WPSError as e:
        logger.exception('Error retrieving ESGF search results')

        return common.failed(str(e))
    else:
        return common.success(axes)


@require_http_methods(['GET'])
@ensure_csrf_cookie
def search_dataset(request):
    try:
        common.authentication_required(request)

        try:
            dataset_id = request.GET['dataset_id']
        except KeyError as e:
            raise common.MissingParameterError(name=str(e))

        index_node = request.GET.get('index_node', settings.ESGF_SEARCH)

        shard = request.GET.get('shard', None)

        query = request.GET.get('query', None)

        dataset_variables = search_solr(dataset_id, index_node, shard, query)
    except WPSError as e:
        logger.exception('Error retrieving ESGF search results')

        return common.failed(str(e))
    else:
        return common.success(dataset_variables)


@require_http_methods(['POST'])
@ensure_csrf_cookie
def combine(request):
    try:
        try:
            axes = json.loads(request.body)['axes']
        except KeyError as e:
            raise common.MissingParameterError(name=e)

        axes = sorted(axes, key=lambda x: x['units'])

        base_units = axes[0]['units']

        start = cdtime.reltime(axes[0]['start'], axes[0]['units'])

        stop = cdtime.reltime(axes[-1]['stop'], axes[-1]['units'])

        data = {
            'units': base_units,
            'start': start.torel(base_units).value,
            'stop': stop.torel(base_units).value,
            'start_timestamp': str(start.tocomponent()),
            'stop_timestamp': str(stop.tocomponent()),
        }
    except WPSError as e:
        logger.exception('Error combining temporal axes')

        return common.failed(str(e))
    else:
        return common.success(data)


@require_http_methods(['GET'])
@cache_page(60 * 15)
def providers(request):
    try:
        response = requests.get('https://raw.githubusercontent.com/ESGF/config/master/esgf-prod/esgf_known_providers.xml')

        response.raise_for_status()
    except Exception:
        raise Exception('Error retrieving ESGF known providers list')

    try:
        root = ET.fromstring(response.text)

        data = {}

        for child in root.iter('OP'):
            name = child.find('NAME').text

            if name == None:
                continue

            url = child.find('URL').text

            data[name] = url
    except Exception:
        raise Exception('Error parsing ESGF known provider list document')

    return JsonResponse(data)
