#from pywps.Process import WPSProcess
import sys, os
import json

operation_fn = sys.argv[1]
data_fn = sys.argv[2]
reg_fn = sys.argv[3]
res_fn = sys.argv[4]

import slurm_ops


f=open(operation_fn)
op_j = json.loads(f.read())
f.close()


eval("slurm_ops." + op_j + ".execute(" + data_fn + ", " + reg_fn + ", " + res_fn + ")" )
