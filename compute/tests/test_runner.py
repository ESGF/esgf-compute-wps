import os
import re

import pytest
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

@pytest.mark.parametrize('notebook', [
      'workflow.ipynb',
      'aggregate.ipynb',
      'subset_indices.ipynb',
      'subset_values.ipynb',
])
def test_notebooks(wps_env, notebook):
    base_path = os.path.dirname(__file__)

    nb_path = os.path.join(base_path, notebook)

    with open(nb_path) as f:
        nb = nbformat.read(f, as_version=4)

    ep = ExecutePreprocessor(timeout=600, kernel_name=os.environ['WPS_KERNEL'])

    ep.preprocess(nb, {'metadata': {'path': 'notebooks/'}})
