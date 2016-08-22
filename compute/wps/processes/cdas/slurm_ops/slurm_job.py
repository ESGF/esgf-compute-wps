#from pywps.Process import WPSProcess
import sys, os
import json

op_str = sys.argv[1]
data_fn = sys.argv[2]
reg_fn = sys.argv[3]
res_fn = sys.argv[4]



oper = __import__(op_str)



oper.execute(data_fn, reg_fn, res_fn ) 
