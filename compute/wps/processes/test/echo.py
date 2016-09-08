from ..esgf_process import ESGFProcess

import json

class EchoProcess(ESGFProcess):
    def __init__(self):
        ESGFProcess.__init__(self, 'Echo')

    def __call__(self, *args, **kwargs):
        response = {}

        response['variable'] = self._variable.parameterize()
        response['domain'] = [x.parameterize() for x in self._domains]
        response['operation'] = self._operation.parameterize()

        output_file = self.output_file('application/json')

        with open(output_file, 'w') as json_file:
            json.dump(response, json_file)

        self.process_output(output_file)
