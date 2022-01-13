#!/usr/bin/env python

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
AWS SageMaker entrypoint wrapper for Disdatified pipelines.
"""

import argparse
import json
import logging
import os
import sys
import docker_ep

from multiprocessing import Process

_HELP = """ AWS SageMaker Disdat pipeline wrapper. This script will call the main entrypoint
to execute the pipeline, parsing arguments from hyperparameter.json.
"""

_logger = logging.getLogger(__name__)

# This is a hard-coded path that will be present in the container according to AWS SageMaker
# https://docs.aws.amazon.com/sagemaker/latest/dg/your-algorithms-training-algo.html
_HYPERPARAMETERS = "/opt/ml/input/config/hyperparameters.json"


def add_argument_help_string(help_string, default=None):
    if default is None:
        return '{}'.format(help_string)
    else:
        return "{} (default '{}')".format(help_string, default)


def train():

    with open(_HYPERPARAMETERS) as hp:
        args = json.load(hp)

    arglist = json.loads(args['arglist'])

    _logger.info("Disdat SageMaker Train calling entrypoint with json loads arglist {}".format(arglist))

    p = Process(target=docker_ep.main, args=[arglist,])
    p.start()
    p.join()
    return p.exitcode == 0


if __name__ == '__main__':
    """ SageMaker invokes the container with 'train' or 'serve'.
    Train jobs support arbitrary 'hyperparameter' params inside a json blob.
    We read the json and interpret them as arguments to the Disdat entrypoint.

    Note:
    1.) We ignore the input S3 path (inputs come as bundles)
    2.) We use the output S3 path to store the output bundle context, remote, name and UUID
    """

    parser = argparse.ArgumentParser(
        description=_HELP,
    )

    parser.add_argument(
        'purpose',
        type=str,
        help="'train' or 'serve'",
    )

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    _logger.setLevel(logging.INFO)

    if args.purpose == 'train':
        if not train():
            _logger.error("Disdat SageMaker train entrypoint failed.")
            sys.exit(os.EX_IOERR)
    elif args.purpose == 'serve':
        _logger.warn("Disdat does not yet support SageMaker serve.")
        sys.exit(os.EX_UNAVAILABLE)
    else:
        _logger.error("Disdat SageMaker invoked entrypoint with {}, not 'train' or 'serve'".format(args.purpose))
        sys.exit(os.EX_USAGE)

    sys.exit(os.EX_OK)

