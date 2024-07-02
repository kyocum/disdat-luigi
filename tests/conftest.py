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

import os
import pytest
import disdat.api as api

TEST_CONTEXT = '___test_context___'
    
"""
NOTE: When testing we try to use Moto to mock-out the s3 calls.  Pyarrow sidesteps boto by using its own S3FileSystem.  This is a problem
because that c++ code is no longer accessing s3 URLs through the botocore Python library.  So you get errors about INVALID CREDENTIALS. 
The primary solution for testing is to simply use fastparquet instead of pyarrow.  The other alternative is to use the moto_server or, as the 
comment below suggests, use fsspec's s3fs.  At this time (7-2024) we simply use fastparquet for testing.

From: https://issues.apache.org/jira/browse/ARROW-16437?page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel&focusedCommentId=17530795#comment-17530795

    I don't think you can use the "mock_s3" method of moto directly with our S3 filesystem integration (as Antoine said, we don't use boto). 
    A possible way to do this is to use fsspec's s3fs, and then pass this filesystem object (and pyarrow will wrap that in a PyFileSystem), 
    but that adds another layer of indirection which you don't need here (and which can be another source of failures).
    Another option is to use the "moto_server" feature of moto (http://docs.getmoto.org/en/latest/docs/getting_started.html#stand-alone-server-mode), 
    which gives you an endpoint url, that can be used to construct a pyarrow S3FileSystem that interacts with the moto server.
    This is basically the approach that we use ourselves (but with MinIO instead of moto), and eg also dask switched from mock_s3 to 
    moto_server in their tests (see eg https://github.com/dask/dask/blob/4d6a5f08c45be56302f696ca4ef6038a1cd1e734/dask/bytes/tests/test_s3.py#L84)

"""

@pytest.fixture(autouse=True, scope='module')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    _aws_credentials()

def _aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

@pytest.fixture(autouse=True, scope='function')
def run_test():
    # Remove test context before running test
    if TEST_CONTEXT in api.ls_contexts():
        api.delete_context(context_name=TEST_CONTEXT)
    api.context(context_name=TEST_CONTEXT)
    yield
    api.delete_context(context_name=TEST_CONTEXT)
