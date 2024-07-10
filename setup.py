from __future__ import absolute_import

########################################################################
#
# License: MIT
# Created: August 8, 2020
#       Author:  Carst Vaartjes - cvaartjes@visualfabriq.com
#
########################################################################
import codecs
import os

from setuptools import setup, find_packages
from os.path import abspath
from sys import version_info as v
from setuptools.command.build_ext import build_ext as _build_ext


# Check this Python version is supported
if v > (3, 7):
    raise Exception("Unsupported Python version %d.%d. Requires Python >= 3.7." % v[:2])


class build_ext(_build_ext):
    def finalize_options(self):
        _build_ext.finalize_options(self)
        # Prevent numpy from thinking it is still in its setup process:
        __builtins__.__NUMPY_SETUP__ = False
        import numpy
        self.include_dirs.append(numpy.get_include())


HERE = os.path.abspath(os.path.dirname(__file__))

def read(*parts):
    """
    Build an absolute path from *parts* and and return the contents of the
    resulting file.  Assume UTF-8 encoding.
    """
    with codecs.open(os.path.join(HERE, *parts), "rb", "utf-8") as f:
        return f.read()


# Sources & libraries
inc_dirs = [abspath('parqueryd')]
try:
    import numpy as np
    inc_dirs.append(np.get_include())
except ImportError as e:
    pass
lib_dirs = []
libs = []
def_macros = []
sources = []

cmdclass = {'build_ext': build_ext}

optional_libs = ['numexpr>=2.6.9']

install_requires = [
    'azure-storage-blob>=12.4.0;python_version>="3.7"',
    'boto3>=1.17.95;python_version>="3.7"',
    'configobj>=5.0.6',
    'netifaces>=0.10.9',
    'numexpr==2.8.4;python_version>="3.7"',
    'numpy>=2;python_version>="3.7"',
    'pyarrow>=1.0.0;python_version>="3.7"',
    'pandas~=2.2.2;python_version>="3.7"',
    'parquery~=0.3.dev0, >=0.3.2',
    'psutil>=5.7.2',
    'pyzmq==19.0.2',
    'redis>=3.5;python_version>="3.7"',
    "sentry-sdk",
    'smart-open>=1.11.1;python_version>="3.7"'
]
setup_requires = []
tests_requires = [
    'pytest>=4.6.11',
    'pytest-cov>=2.10.0',
    'codacy-coverage>=1.3.11',
    'moto;python_version>="3.7"',
]
dev_requires = [
    "ipython>=7.20;python_version>='3.7'"
]
extras_requires = {
    'all': tests_requires + dev_requires,
    'test': tests_requires,
    'dev': dev_requires
}
ext_modules = []
package_data = {}
classifiers = [
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'Intended Audience :: Information Technology',
    'Intended Audience :: Science/Research',
    'License :: OSI Approved :: BSD License',
    'Programming Language :: Python',
    'Topic :: Software Development :: Libraries :: Python Modules',
    'Operating System :: Microsoft :: Windows',
    'Operating System :: Unix',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9',
]

setup(
    name="parqueryd",
    description='A distribution framework for parquery',
    long_description=read("README.md"),
    long_description_content_type='text/markdown',
    classifiers=classifiers,
    author='Carst Vaartjes',
    author_email='cvaartjes@visualfabriq.com',
    maintainer='Jelle Verstraaten',
    maintainer_email='jverstraaten@visualfabriq.com',
    url='https://github.com/visualfabriq/parqueryd',
    license='GPL2',
    platforms=['any'],
    ext_modules=ext_modules,
    cmdclass=cmdclass,
    install_requires=install_requires,
    setup_requires=setup_requires,
    tests_require=tests_requires,
    extras_require=extras_requires,
    packages=find_packages(),
    package_data=package_data,
    include_package_data=True,
    zip_safe=True,
    entry_points={
        'console_scripts': [
            'parqueryd = parqueryd.node:main',
            'parqueryd_local = parqueryd.connect_local:main',
        ]
    }
)

