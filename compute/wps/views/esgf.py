#! /usr/bin/env python

from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods

from . import common

logger = common.logger

TIME_FREQ = {
    '3hr': '3 Hours',
    '6hr': '6 Hours',
    'day': 'Daily',
    'mon': 'Monthly',
    'monClim': 'Monthly',
    'subhr': 'Sub Hourly',
    'yr': 'Yearly'
}

TIME_FMT = {
    '3hr': '%Y%m%d%H%M',
    '6hr': '%Y%m%d%H',
    'day': '%Y%m%d',
    'mon': '%Y%m',
    'monClim': '%Y%m',
    'subhr': '%Y%m%d%H%M%S',
    'yr': '%Y'
}

CDAT_TIME_FMT = '{0.year:04d}-{0.month:02d}-{0.day:02d} {0.hour:02d}:{0.minute:02d}:{0.second:02d}.0'

@require_http_methods(['GET'])
@ensure_csrf_cookie
def search_esgf(request):
    try:
        common.authentication_required(request)

        dataset_ids = request.GET['dataset_id']

        index_node = request.GET['index_node']

        shard = request.GET.get('shard', None)

        query = request.GET.get('query', None)

        dataset_ids = dataset_ids.split(',')

        files = []
        variables = []
        time_freq = None

        for dataset_id in dataset_ids:
            logger.info('Searching for dataset {}'.format(dataset_id))

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
            except:
                raise Exception('Failed to retrieve search results')

            try:
                data = json.loads(response.content)
            except:
                raise Exception('Failed to load JSON response')

            docs = data['response']['docs']

            logger.debug('ESGF search returned keys {}'.format(docs[0].keys()))

            files = []
            variables = []
            time_freqs = []

            for doc in docs:
                files.extend(doc['url'])

                variables.extend(doc['variable'])

                time_freqs.extend(doc['time_frequency'])

            uniq_files = []
            time_ranges = []

            time_pattern = re.compile('.*_([0-9]*)-([0-9]*)\.nc')

            for f in files:
                if 'opendap' in f.lower():
                    url = f.split('|')[0].replace('.html', '')

                    try:
                        time_ranges.extend(time_pattern.match(url).groups())
                    except:
                        raise Exception('Failed to parse time range from url "{}"'.format(url))

                    uniq_files.append(url)

            uniq_files = list(uniq_files)

            time_ranges = sorted(set(time_ranges))

            variables = list(set(variables))

            time_freqs = list(set(time_freqs))

            time_fmt = TIME_FMT.get(time_freqs[0], None)

            if time_fmt is None:
                raise Exception('Could not parse time formats for time frequency "{}"'.format(time_freqs))

            time_start = datetime.datetime.strptime(time_ranges[0], time_fmt)

            time_stop = datetime.datetime.strptime(time_ranges[-1], time_fmt)

            base = process.CWTBaseTask()

            base.load_certificate(request.user)

            axes = {}

            logger.info('Checking file "{}"'.format(uniq_files[0]))

            try:
                with cdms2.open(uniq_files[0]) as infile:
                    variableHeader = None
                    fileVariables = infile.getVariables()

                    for v in fileVariables:
                        if v.id in variables:
                            variableHeader = v

                            break

                    if variableHeader is None:
                        raise Exception('Failed to match variables in dataset')

                    for x in variableHeader.getAxisList():
                        if x.id not in ('time', 'lat', 'lon'):
                            continue

                        if 'axis' in x.attributes:
                            axes[x.id] = {
                                'id': x.id,
                                'id_alt': x.attributes['axis'].lower(),
                                'start': x[0],
                                'stop': x[-1],
                                'units': x.attributes['units']
                            }
            except:
                logger.exception('Failed to open netcdf file')

                raise Exception('Failed to open netcdf file, might need to check certificate')

            axes['time']['start'] = CDAT_TIME_FMT.format(time_start)

            axes['time']['stop'] = CDAT_TIME_FMT.format(time_stop)

            axes['time']['units'] = time_freqs[0]

            data = {
                'files': uniq_files,
                'variables': variables,
                'axes': axes.values()
            }
    except KeyError as e:
        logger.exception('Missing required parameter')

        return common.failed({'message': 'Mising required parameter "{}"'.format(e.message)})
    except Exception as e:
        logger.exception('Error retrieving ESGF search results')

        return common.failed(e.message)
    else:
        return common.success(data)
