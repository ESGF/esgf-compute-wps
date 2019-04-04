def main():
    import argparse

    from .cluster import Cluster
    from .cluster_manager import ClusterManager

    parser = argparse.ArgumentParser(description='Run Kubernetes monitor')

    parser.add_argument('scheduler', help='Dask Scheduler')
    parser.add_argument('--namespace', default='default', help='Kubernetes namespace')

    args = parser.parse_args()

    cluster = Cluster(args.namespace)

    manager = ClusterManager(args.scheduler, cluster)

    manager.run()
