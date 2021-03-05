import argparse
import json
import logging

from compute_tasks import base
from compute_tasks import cdat
from compute_tasks import context
from distributed import LocalCluster

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

    ctx = context.LocalContext.from_data_inputs(
        args['identifier'],
        json.loads(args['data_inputs']),
        extra=extra)

    base.validate_workflow(ctx)

    ctx = cdat.workflow(ctx)

    cluster.close()

    logger.info(f'Listing {len(ctx._output)} outputs')

    for local, output in ctx._output.items():
        logger.info(f'Output {local!r} -> {output.uri!r}')
