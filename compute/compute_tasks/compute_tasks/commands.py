import argparse
import json
import logging

from distributed import LocalCluster

from compute_tasks import base
from compute_tasks import cdat
from compute_tasks import context

logger = logging.getLogger(__name__)

def get_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('data_inputs', type=str)

    parser.add_argument('identifier', type=str)

    parser.add_argument('--output-path', type=str, default='/data')

    parser.add_argument('--workers', type=int, default=2)

    parser.add_argument('--threads-per-worker', type=int, default=2)

    return vars(parser.parse_args())

def local_execute():
    logging.basicConfig(level=logging.INFO)

    args = get_arguments()

    cluster = LocalCluster(n_workers=args['workers'], threads_per_worker=args['threads_per_worker'])

    extra = {
        'DASK_SCHEDULER': cluster.scheduler.address,
        'output_path': args['output_path'],
    }

    context = context.LocalContext.from_data_inputs(
        args['identifier'],
        json.loads(args['data_inputs']),
        extra=extra,
        user=0,
        job=0)

    base.validate_workflow(context)

    context = cdat.workflow(context)

    cluster.close()

    logger.info(f'Listing {len(context.output)} outputs')

    for output in context.output:
        logger.info(f'Output {output.uri}')
