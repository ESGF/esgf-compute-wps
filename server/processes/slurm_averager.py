from pywps.Process import WPSProcess

import os
import json

from subprocess import system

class Process(WPSProcess):

    def __init__(self):
                
       """Process initialization"""
        WPSProcess.__init__(self, identifier=os.path.split(__file__)[-1].split('.')[0], title='averager', version=0.1, abstract='Average a variable over a (many) dimension', storeSupported='true', statusSupported='true')
        self.domain = self.addComplexInput(identifier='domain', title='domain over which to average', formats=[{'mimeType': 'text/json', 'encoding': 'utf-8', 'schema': None}])
        self.download = self.addLiteralInput(identifier='download', type=bool, title='download output', default=False)
        self.dataIn = self.addComplexInput(identifier='variable', title='variable to average', formats=[{'mimeType': 'text/json'}], minOccurs=1, maxOccurs=1)
        self.average = self.addComplexOutput(identifier='average', title='averaged variable', formats=[{'mimeType': 'text/json'}])





    
    def execute(self):

        cont = True

        rndm = 0

        while cont:
            rndm = random.randint(0,100000000000)
            fout = os.path.join(BASE_DIR,"%i.nc" % rndm)
            fjson = os.path.join(BASE_DIR,"%i.json" % rndm)
            cont = os.path.exists(fout) or os.path.exists(fjson)



        domain = self.domain.getValue()
        dataIn = self.dataIn.getValue()

        system("srun -o " + rndm  + ".log python avg_tester.py " + domain + " " + dataIn + " " + fout)
        out = {}
        out["url"] = "file:/"+fout
        out["id"]=data.id
        Fjson=open(fjson,"w")
        json.dump(out,Fjson)
        Fjson.close()

        self.average.setValue(fjson)
