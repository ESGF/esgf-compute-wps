from pywps.Process import WPSProcess

import os
import json
import random

import logging

from subprocess import call 

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'output'))

wpsLog = logging.getLogger('wps')

class Process(WPSProcess):

    def __init__(self):
        
        """Process initialization"""
        WPSProcess.__init__(self, 
                            identifier=os.path.split(__file__)[-1].split('.')[0], 
                            title='slurm_averager', 
                            version=1.0, abstract='Slurm Average a variable over a (many) dimension', 
                            storeSupported=True, 
                            statusSupported=True)
        self.domain = self.addComplexInput(identifier='domain', title='domain over which to average', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}])
        self.download = self.addLiteralInput(identifier='download', type=bool, title='download output', default=False)
        self.dataIn = self.addComplexInput(identifier='variable', title='variable to average', formats=[{'mimeType': 'text/json'}], minOccurs=1, maxOccurs=1)
        self.average = self.addComplexOutput(identifier='average', title='averaged variable', formats=[{'mimeType': 'text/json'}])
        self.operation = self.addLiteralInput(identifier='operation', type=str, title='download output', default="FooBar")
        self.result = None


    def loadOperation(self,origin=None):
        if origin is None:
            origin = self.operation
        if origin is None: return None
        opStr = origin.getValue()
        if opStr:
            op_json = open(opStr).read().strip()
            if op_json: return json.loads(op_json)
        return None


    
    def execute(self):

        cont = True

        rndm = 0

        while cont:
            rndm = random.randint(0,100000000000)
            fout = os.path.join(BASE_DIR,"%i.nc" % rndm)
            fjson = os.path.join(BASE_DIR,"%i.json" % rndm)
            cont = os.path.exists(fout) or os.path.exists(fjson)


#        wpsLog.info("WPS at %s", os.getcwd())

            
        domain = self.domain.getValue()

        dataIn = self.dataIn.getValue()

#        wpsLog.info( "SLURM Working at %s" , BASE_DIR )
#        if  call("srun -o " + BASE_DIR + "/" + str(rndm)+".log -D " + BASE_DIR + "/../analysis python avg_tester.py " + domain + " " + dataIn + " " + fout, shell=True) > 0:
#        if  call("srun -w greyworm1 -o " + BASE_DIR + "/" + str(rndm)+".log -D " + BASE_DIR + "/../analysis sh run_job.sh avg_tester.py " + domain + " " + dataIn + " " + fout, shell=True) > 0:
        if  call("time srun -w greyworm1 -o " + BASE_DIR + "/" + str(rndm)+".log sh "+ BASE_DIR + "/../analysis/run_job.sh " + BASE_DIR + "/../analysis/avg_tester.py " + domain + " " + dataIn + " " + fout, shell=True) > 0:

            return "Slurm returned an error!"
        

        
   #     self.cmd(["/usr/bin/srun", "-o " + BASE_DIR + "/" + str(rndm)  + ".log pwd")

#        self.cmd(["/usr/bin/srun", "-D "+BASE_DIR+" -o " + BASE_DIR + "/" + str(rndm)  + ".log whoami"])

#        call("/usr/bin/srun -D "+BASE_DIR+" -o " + BASE_DIR + "/" + str(rndm)  + ".log whoami", shell=True)

        sz = 0
        try:
            sz = os.stat(fout).st_size
        except:
            return "Couldn't open output file!"
        
        if sz == 0:
            return "Output file empty!"
                

        out = {}
        out["url"] = "file:/"+fout
        out["id"]="variable"
        Fjson=open(fjson,"w")
        json.dump(out,Fjson)
        Fjson.close()

        self.average.setValue(fjson)
        return
