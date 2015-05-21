# Run using 'MASTER=local[4] spark-submit ./latency_test.py'
# or MASTER=local[4] pyspark

from pyspark import SparkContext
from utilities import Profiler


profiler = Profiler()
num_procs = 4


sc = SparkContext('local[%d]' % num_procs, "latencyTest")

import time

def compute( value ):
    return value * 2

def run_comp_test():
    partitions = sc.parallelize( range(8) )
    part = partitions.collect()
    t0 = time.time()
    import cdutil, cdms2
    result = partitions.map(compute).collect()
    t1 = time.time()
    print " Result = %s, time = %.3f " % ( str( result ), (t1-t0) )
    return result

run_comp_test()





