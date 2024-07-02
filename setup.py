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
from setuptools import setup, find_packages

setup(
    use_scm_version={
        'write_to': 'disdatluigi/version.py',
        'write_to_template': '__version__ = "{version}"'
    },
    setup_requires=['setuptools_scm'],

    name='disdat-luigi',
    description='Disdat-Luigi: Data-versioned Luigi Workflows',
    author='Ken Yocum',
    author_email='kyocum@gmail.com',
    url='https://github.com/kyocum/disdat-luigi',

    # Choose your license
    license='Apache License, version 2.0',

    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 5 - Production/Stable',

        # Indicate who your project is intended for
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: Apache Software License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Operating System :: OS Independent',
        'Natural Language :: English',
    ],

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(exclude=['tests*']),

    # Include non-python files found in each package in the install, if in your MANIFEST.in
    include_package_data=True,

    # If any package contains other resource files, include them here.
    # We copy config/disdat so 'dsdt init' still runs from an installed
    # .egg.  This package_data only works if you're not using "sdist"
    # Otherwise only MANIFEST.in actually works, and then only if include_package_data=True
    package_data={
        '': ['*.json'],
        'disdatluigi': [
            'config/disdatluigi/*',
            'VERSION',
        ],
        'infrastructure': [
            'Dockerfiles/hyperframe_def/*'
            'dockerizer/Makefile',
            'dockerizer/Dockerfiles/*',
            'dockerizer/kickstart/bin/*',
            'dockerizer/kickstart/etc/*',
        ],
    },

    exclude_package_data={
        'disdatluigi': [
            'dockerizer/kickstart/bin/*.pyc',
        ]
    },

    data_files=[('', ['setup.py'])],

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed.  If >=, means it worked with the base version.
    # If <= means higher versions broke something.

    install_requires=[
        'disdat>=1.1.3,<1.2',
        'luigi>=3.0,<3.6',
        'boto3>=1.14.49,<2.0',
        'docker>=7.0.0,<7.2.0',
    ],

    # List additional groups of dependencies here (e.g. development
    # dependencies). You can install these using the following syntax,
    # for example:
    # $ pip install -e '.[dev, rel]'
    extras_require={
        'dev': [
            'pytest',
            'ipython',
            'mock',
            'pylint',
            'coverage',
            'tox',
            'moto>=5',
            'fastparquet',
            's3fs<=0.4.2' # 0.5.0 breaks with aiobotocore and missing AWS headers
        ],
        'rel': [
            'wheel',
        ]
    },

    entry_points={
        'console_scripts': [
            'dsdt_docker = disdatluigi.entrypoints.docker_ep:main'
        ],
        'distutils.commands': [
            "dsdt_distname = disdatluigi.infrastructure.dockerizer.setup_tools_commands:DistributionName",
        ]
    },
)
