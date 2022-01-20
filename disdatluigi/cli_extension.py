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

"""
Disdat comes with a CLI.  It looks for the presence of certain packages that might extend that CLI.
One of them is "disdatluigi".  It then looks for a module "cli_extension.py", and the "add_arg_parser" method.
The 3rd-party package is then able to add commands to the CLI so users have a single interface to using
Disdat on the command line.

Disdat-Luigi needs to add a number of subparsers.  These include:
apply.py:
dockerize.py:
run.py:
"""

import disdatluigi.apply
import disdatluigi.dockerize
import disdatluigi.run
from disdatluigi import logger as _logger


def add_arg_parser(subparsers):
    _logger.debug("Disdat-Luigi: extending Disdat CLI...")
    ls_p = subparsers.add_parser('luigi')
    ls_p.set_defaults(func=lambda args: DisdatConfig.init())

    disdatluigi.apply.add_arg_parser(subparsers)
    disdatluigi.dockerize.add_arg_parser(subparsers)
    disdatluigi.run.add_arg_parser(subparsers)
    return
