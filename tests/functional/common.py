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
#

import pytest

import disdat.api as api

TEST_CONTEXT = '___test_context___'


@pytest.fixture(autouse=True)
def run_test():
    # Remove test context before running test
    setup()
    yield
    api.delete_context(context_name=TEST_CONTEXT)


def setup():
    if TEST_CONTEXT in api.ls_contexts():
        api.delete_context(context_name=TEST_CONTEXT)

    api.context(context_name=TEST_CONTEXT)
