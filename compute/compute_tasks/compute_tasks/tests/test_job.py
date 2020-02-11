import cwt
import json

from compute_tasks import job
from compute_tasks.context import operation


def test_job_succeeded_multiple_variable(mocker):
    v0 = cwt.Variable('file:///test1.nc', 'tas')
    v1 = cwt.Variable('file:///test2.nc', 'tas')

    context = mocker.MagicMock()
    context.output = [v0, v1]

    output = job.job_succeeded(context)

    assert output == context

    context.succeeded.assert_called_with(json.dumps([v0.to_dict(), v1.to_dict()]))


def test_job_succeeded_single_variable(mocker):
    v0 = cwt.Variable('file:///test1.nc', 'tas')

    context = mocker.MagicMock()
    context.output = [v0]

    output = job.job_succeeded(context)

    assert output == context

    context.succeeded.assert_called_with(json.dumps(v0.to_dict()))


def test_job_succeeded(mocker):
    context = mocker.MagicMock()
    context.output = ['test']

    output = job.job_succeeded(context)

    assert output == context

    context.succeeded.assert_called_with('test')


def test_job_started(mocker):
    context = operation.OperationContext()

    mocker.patch.object(context, 'action')
    mocker.spy(context, 'started')

    output = job.job_started(context)

    output.started.assert_called()
