#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import pytest
import luigi
import disdat.api as dsdt_api
from disdatluigi.pipe import PipeTask
import disdatluigi.api as api
from tests.functional.common import run_test, TEST_CONTEXT

test_luigi_args_data = {'str_arg': 'some string',
                    'int_arg': 10,
                    'list_arg': [1,3,5],
                    'list_str_arg': ['farg','barg'],
                    'dict_float_arg': {'farg': 0.01, 'barg': 3.14},
                    'date_arg': datetime.date(2020,4,1)}


class ArgTask(PipeTask):
    str_arg = luigi.Parameter(default=None)
    int_arg = luigi.IntParameter(default=None)
    list_arg = luigi.ListParameter(default=None)
    list_str_arg = luigi.ListParameter(default=None)
    dict_float_arg = luigi.DictParameter(default=None)
    date_arg = luigi.DateParameter(default=None)

    def pipe_run(self):
        return True


def test_luigi_args(run_test):
    """ Create a task, store args, retrieve from bundle api.
    Pass in python objects as the values for Luigi parameters.
    Stored as serialized json objects.   Bundle presents the parameters
    as the serialized objects (Disdat isn't aware they were Luigi serialized).
    """

    api.apply(TEST_CONTEXT, ArgTask, output_bundle='output', params=test_luigi_args_data)
    b = dsdt_api.get(TEST_CONTEXT, 'output')
    found_p = {}
    for k, p in b.params.items():
        attribute = getattr(ArgTask, k)
        found_p[k] = attribute.parse(p)
    assert(found_p == test_luigi_args_data)


if __name__ == "__main__":
    pytest.main([__file__])
