import os
import re

import pytest
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

os.environ['WPS_TOKEN'] = 'ANXnEmQgMOf6XQOqywVhJvuj7kTzmjNxzjEcHAQRlKTaBzfsIZyNs4qCYrW3cA4r'

@pytest.mark.parametrize('notebook', [
    'workflow.ipynb',
    'aggregate.ipynb',
    'subset_indices.ipynb',
    'subset_values.ipynb',
])
def test_notebooks(notebook):
    with open(notebook) as f:
        nb = nbformat.read(f, as_version=4)

    ep = ExecutePreprocessor(timeout=600, kernel_name='test-compute')

    ep.preprocess(nb, {'metadata': {'path': 'notebooks/'}})
