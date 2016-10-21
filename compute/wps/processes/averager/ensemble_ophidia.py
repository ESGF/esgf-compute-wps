from wps.processes import esgf_operation

class EnsembleOphidiaAverage(esgf_operation.ESGFOperation):
    def __init__(self):
        super(EnsembleOphidiaAverager, self).__init__()
