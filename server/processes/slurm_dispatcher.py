from pywps.Process import WPSProcess

import os
import json
import random

import logging

from subprocess import call 

# TODO - get the output directory from an environment variable or external config
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'output'))

wpsLog = logging.getLogger('wps')
wpsLog.setLevel(logging.DEBUG)
wpsLog.addHandler( logging.FileHandler( os.path.abspath( os.path.join(os.path.dirname(__file__), '..', 'logs', 'wps.log') ) ) )


class Process(WPSProcess):

    def __init__(self):
        
        """Process initialization"""
        WPSProcess.__init__(self, 
                            identifier=os.path.split(__file__)[-1].split('.')[0], 
                            title='slurm_dispatcher', 
                            version=0.2.1, abstract='Pass esgf compute operation to slurm for processing on cluster', 
                            storeSupported=True, 
                            statusSupported=True)
        # this will be operation specific
        self.region = self.addComplexInput(identifier='region', title='region of interest and grid specs', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}])

        # one or more files will be operaton specific
        self.dataIn = self.addComplexInput(identifier='data', title='one or more data files', formats=[{'mimeType': 'text/json'}], minOccurs=1, maxOccurs=1)

        self.operation = self.addLiteralInput(identifier='operation', type=str, title='operation')
        self.result = self.addComplexOutput(identifier='result', title='result of operation', formats=[{'mimeType': 'text/json'}])


    def execute(self):

        cont = True

        rndm = 0

        while cont:
            rndm = random.randint(0,100000000000)
            fout = os.path.join(BASE_DIR,"%i.nc" % rndm)
            fjson = os.path.join(BASE_DIR,"%i.json" % rndm)
            cont = os.path.exists(fout) or os.path.exists(fjson)


#        wpsLog.info("WPS at %s", os.getcwd())

            

        operation = self.operation.getValue()
        dataIn = self.dataIn.getValue()
        region = self.region.getValue()

#        wpsLog.info( "SLURM Working at %s" , BASE_DIR )
#        if  call("srun -o " + BASE_DIR + "/" + str(rndm)+".log -D " + BASE_DIR + "/../analysis python avg_tester.py " + domain + " " + dataIn + " " + fout, shell=True) > 0:
#        if  call("srun -w greyworm1 -o " + BASE_DIR + "/" + str(rndm)+".log -D " + BASE_DIR + "/../analysis sh run_job.sh avg_tester.py " + domain + " " + dataIn + " " + fout, shell=True) > 0:
        if  call("time srun -o " + BASE_DIR + "/" + str(rndm)+".log sh "+ BASE_DIR + "/../analysis/run_job.sh " + BASE_DIR + "/../analysis/slurm_job.py " + operation + " " + dataIn + " " + region + " "  + fout, shell=True) > 0:

            return "Slurm returned an error!"
        

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

        self.result.setValue(fjson)
        return
