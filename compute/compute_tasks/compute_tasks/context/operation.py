import json

import cwt
from celery.utils.log import get_task_logger

from compute_tasks import WPSError
from compute_tasks.context import state_mixin

logger = get_task_logger('wps.context.operation')


class OperationContext(state_mixin.StateMixin, object):
    def __init__(self, variable=None, domain=None, operation=None):
        super(OperationContext, self).__init__()

        self._variable = variable or {}
        self._domain = domain or {}
        self._operation = operation or {}

        self.output = []

        self.gdomain = None
        self.gparameters = {}

        self._sorted = []
        self.input_var_names = {}

    def __repr__(self):
        return (f'{self.__class__.__name__}('
                f'{self._variable!r}, {self._domain!r}, {self._operation!r},'
                f' {self.gdomain!r}, {self.gparameters!r}, {self._sorted!r},'
                f' {self.output!r}, {self.input_var_names!r})')

    @staticmethod
    def decode_data_inputs(data_inputs):
        items = {}

        for id in ('variable', 'domain', 'operation'):
            try:
                data = data_inputs[id]
            except KeyError as e:
                raise WPSError('Missing required data input "{!s}"', e)

            if id == 'variable':
                data = [cwt.Variable.from_dict(x) for x in data]

                items['variable'] = dict((x.name, x) for x in data)
            elif id == 'domain':
                data = [cwt.Domain.from_dict(x) for x in data]

                items['domain'] = dict((x.name, x) for x in data)
            elif id == 'operation':
                data = [cwt.Process.from_dict(x) for x in data]

                items['operation'] = dict((x.name, x) for x in data)

        return items['variable'], items['domain'], items['operation']

    @staticmethod
    def resolve_dependencies(variable, domain, operation, gdomain=None, gparameters=None):
        for item in operation.values():
            inputs = []

            for x in item.inputs:
                if x in variable:
                    inputs.append(variable[x])
                elif x in operation:
                    inputs.append(operation[x])
                else:
                    raise WPSError('Error finding input {!s}', x)

            item.inputs = inputs

            # Default to global domain
            item.domain = domain.get(item.domain, gdomain)

            if gparameters is not None:
                item.add_parameters(**gparameters)

    @classmethod
    def from_data_inputs(cls, identifier, data_inputs):
        variable, domain, operation = OperationContext.decode_data_inputs(data_inputs)

        try:
            root_op = [x for x in operation.values() if x.identifier == identifier][0]
        except IndexError:
            raise WPSError('Error finding operation {!r}', identifier)

        gdomain = None

        gparameters = None

        if identifier == 'CDAT.workflow':
            # Remove workflow operation servers no further purpose
            operation.pop(root_op.name)

            gdomain = domain.get(root_op.domain, None)

            gparameters = root_op.parameters

        OperationContext.resolve_dependencies(variable, domain, operation, gdomain, gparameters)

        ctx = cls(variable, domain, operation)

        ctx.gdomain = gdomain

        ctx.gparameters = gparameters or {}

        return ctx

    @classmethod
    def from_dict(cls, data):
        gdomain = data.pop('gdomain')

        gparameters = data.pop('gparameters')

        variable = data.pop('_variable')

        domain = data.pop('_domain')

        operation = data.pop('_operation')

        OperationContext.resolve_dependencies(variable, domain, operation, gdomain, gparameters)

        obj = cls(variable, domain, operation)

        obj.output = data.pop('output')

        obj._sorted = data.pop('_sorted')

        obj.input_var_names = data.pop('input_var_names')

        obj.init_state(data)

        return obj

    def to_dict(self):
        data = {
            'gdomain': self.gdomain,
            'gparameters': self.gparameters,
            '_variable': self._variable,
            '_domain': self._domain,
            '_operation': self._operation,
            'output': self.output,
            '_sorted': self._sorted,
            'input_var_names': self.input_var_names,
        }

        data.update(self.store_state())

        return data

    @property
    def variable(self):
        return list(set([x.var_name for x in self._variable.values()]))[0]

    @property
    def sorted(self):
        for x in self._sorted:
            yield self._operation[x]

    def output_ops(self):
        out_deg = dict((x, self.node_out_deg(y)) for x, y in self._operation.items())

        return [self._operation[x] for x, y in out_deg.items() if y == 0]

    def interm_ops(self):
        out_deg = dict((x, self.node_out_deg(y)) for x, y in self._operation.items())

        return [self._operation[x] for x, y in out_deg.items() if y > 0]

    def node_in_deg(self, node):
        return len([x for x in node.inputs if x.name in self._operation])

    def node_out_deg(self, node):
        return len([x for x, y in self._operation.items() if any(node.name == z.name for z in y.inputs)])

    def find_neighbors(self, node):
        return [x for x, y in self._operation.items() if node in [x.name for x in y.inputs]]

    def topo_sort(self):
        in_deg = dict((x, self.node_in_deg(y)) for x, y in self._operation.items())

        neigh = dict((x, self.find_neighbors(x)) for x in in_deg.keys())

        queue = [x for x, y in in_deg.items() if y == 0]

        while queue:
            next = queue.pop(0)

            self._sorted.append(next)

            operation = self._operation[next]

            if all([isinstance(x, cwt.Variable) for x in operation.inputs]):
                var_names = set([x.var_name for x in operation.inputs])

                self.input_var_names[next] = list(var_names)
            else:
                var_names = set([y for x in operation.inputs for y in self.input_var_names[x.name]])

                self.input_var_names[next] = list(var_names)

            yield operation, self.input_var_names[next]
            # yield self._operation[next], self.input_var_names[next.name]

            for x in neigh[next]:
                in_deg[x] -= 1

                if in_deg[x] == 0:
                    queue.append(x)
