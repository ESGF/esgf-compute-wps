def main():
    import argparse
    import logging

    from .cluster import Cluster
    from .cluster_manager import ClusterManager

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='Run Kubernetes monitor')

    parser.add_argument('scheduler', help='Dask Scheduler')
    parser.add_argument('worker_spec', help='Worker YAML file')
    parser.add_argument('--namespace', default='default', help='Kubernetes namespace')
    parser.add_argument('--timeout', default=60, type=int, help='Time between attempts to remove pods')

    args = parser.parse_args()

    cluster = Cluster.from_yaml(args.namespace, args.worker_spec)

    manager = ClusterManager(args.scheduler, cluster, timeout=args.timeout)

    manager.run()
