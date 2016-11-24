from wps import logger
from wps.processes import ophidia_operation
from wps.processes import ophidia_workflow_builder

class MultiModelEnsembleOphidia(ophidia_operation.OphidiaOperation):
    def __init__(self):
        super(MultiModelEnsembleOphidia, self).__init__()

    @property
    def title(self):
        return 'Ophidia Multi Model Ensemble Averager'

    def __call__(self, data_manager, status):
        inputs = self.input()

        workflow = ophidia_workflow_builder.OphidiaWorkflowBuilder(8)

        container = workflow.create_task('container',
                                         'oph_createcontainer',
                                         container='wps_time_lat_lon',
                                         dim='time|lat|lon',
                                         on_error='skip')

        cubes = [
            [
                'http://aims2.llnl.gov/ophidia/107/813',
                'http://aims2.llnl.gov/ophidia/107/814',
            ],
            [
                'http://aims2.llnl.gov/ophidia/107/797',
                'http://aims2.llnl.gov/ophidia/107/798',
            ],
        ]

        for model_index, model in enumerate(inputs):
            for src_file in model.inputs:
                # import source files and track to set as inter_cubes dependencies
                pass

            inter_cube_name = 'intercube model %s file %s' % (model_index, 0)

            last_inter_cube = workflow.create_task(inter_cube_name,
                                                   'oph_intercube',
                                                   cube = cubes[model_index][0],
                                                   cube2 = cubes[model_index][1],
                                                   operation = 'sum',
                                                   output_measure = 'summation')
        
            last_inter_cube.depends(container)

            for src_idx, src_cube in enumerate(cubes[model_index][2:]):
                inter_cube_name = 'intercube model %s file %s' % (model_index,
                                                                  src_idx + 1)

                inter_cube = workflow.create_task(inter_cube_name,
                                                  operation = 'sum',
                                                  output_measure = 'summation')

                inter_cube.depends(last_inter_cube)

                last_inter_cube = inter_cube

            divisor = 1.0/len(cubes[model_index])

            apply_task_name = 'apply model %s' % (model_index,)

            apply_task = workflow.create_task(apply_task_name,
                                              'oph_apply',
                                              query = "oph_mul_scalar('oph_double', 'oph_double', measure, %s" % (divisor,),
                                              measure='ensemble_avg')

            apply_task.depends(last_inter_cube)

            export_task_name = 'export model %s' % (model_index,)

            export_task = workflow.create_task(export_task_name,
                                               'oph_exportnc2',
                                               misc = True,
                                               force = True)

            export_task.depends(apply_task)

        result = self.submit_workflow(workflow)

        # run oph_script to apply regrid

        # second workflow to import, average and export

        self.set_output('', '')
