import cwt
import json

from compute_tasks import job


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
    mocker.patch.object(job, 'ctx')

    data_inputs = {
        'variable': '[]',
        'domain': '[]',
        'operation': json.dumps({
            'op1': cwt.Process(identifier='CDAT.subset').to_dict(),
        }),
    }

    output = job.job_started('CDAT.subset', data_inputs, 0, 0, 0, {})

    assert output == job.ctx.OperationContext.from_data_inputs.return_value

    job.ctx.OperationContext.from_data_inputs.assert_called_with('CDAT.subset', data_inputs)

    output.init_state.assert_called_with({'extra': {}, 'job': 0, 'user': 0, 'process': 0})

    output.started.assert_called()
