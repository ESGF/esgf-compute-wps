from pyspark import SparkContext
import time

sc = SparkContext()

num_procs = 4
start = 1
partitions = sc.parallelize( range( start, start+num_procs) )

def run_comp_test( taskindex=0 ):
    def compute( value ): return value * taskindex
    t0 = time.time()
    result = partitions.map(compute).collect()
    t1 = time.time()
    print " Result-%d = %s, time = %.3f " % ( taskindex, str( result ), (t1-t0) )
    return result

for taskindex in range(5):
   run_comp_test(taskindex)





