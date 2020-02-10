import cwt
from compute_tasks import cdat
from compute_tasks import base
from compute_tasks import celery_app
from compute_tasks import metrics_
from compute_tasks.context import operation

def test_task():
    v0 = cwt.Variable('https://aims3.llnl.gov/thredds/dodsC/css03_data/cmip5/output1/MRI/MRI-CGCM3/amip/3hr/atmos/cf3hr/r1i1p1/v20131009/pr/pr_cf3hr_MRI-CGCM3_amip_r1i1p1_200801010000-200812312100.nc', 'pr')

    d = cwt.Domain(time=slice(0, 10))

    s = cwt.Process('CDAT.subset')
    s.add_inputs(v0)
    s.set_domain(d)

    data_inputs = {
        'variable': [v0.to_dict()],
        'domain': [d.to_dict()],
        'operation': [s.to_dict()],
    }

    ctx = operation.OperationContext.from_data_inputs('CDAT.subset', data_inputs)
    type(ctx).action = lambda *args, **kwargs: None
    # type(ctx).failed = lambda *args, **kwargs: None
    [x for x in ctx.topo_sort()]

    p = base.get_process('CDAT.subset')

    # p._validate(s)
    print(p._render_abstract())
    assert False
    # p._task.s(ctx).apply_async()
