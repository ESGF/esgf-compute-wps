from compute_tasks import base

def test_register_process():
    @base.register_process('CDAT', 'subset', 'abstract data', '1.0.0', 10, extra_data='extra_data_content')
    def test_task(self, context):
        return context

    registry_entry = {
        'identifier': 'CDAT.subset',
        'backend': 'CDAT',
        'abstract': 'abstract data',
        'metadata': '{"extra_data": "extra_data_content", "inputs": 10}',
        'version': '1.0.0',
    }

    assert 'CDAT.subset' in base.REGISTRY
    assert base.REGISTRY['CDAT.subset'] == registry_entry
    assert base.BINDINGS['CDAT.subset'] == test_task

def test_build_process_bindings():
    base.build_process_bindings()

    assert len(base.BINDINGS) > 0

def test_discover_processes():
    processes = base.discover_processes()

    assert len(processes) > 0
