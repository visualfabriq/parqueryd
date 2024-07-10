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
from sys import version_info as v

# Check this Python version is supported
if v < (3, 11):
    raise Exception("Unsupported Python version %d.%d. Requires Python >= 3.11 " % v[:2])


HERE = os.path.abspath(os.path.dirname(__file__))

def read(*parts):
    """
    Build an absolute path from *parts* and and return the contents of the
    resulting file.  Assume UTF-8 encoding.
    """
    with codecs.open(os.path.join(HERE, *parts), "rb", "utf-8") as f:
        return f.read()


# Sources & libraries
cmdclass = {}

install_requires = [
    'azure-storage-blob>=12.4.0',
    'boto3>=1.17.95',
    'configobj>=5.0.6',
    'netifaces>=0.10.9',
    'numpy>=2',
    'pyarrow>=1.0.0',
    'pandas~=2.2.2',
    'parquery==1.1.2.dev2', # TODO: update version
    'psutil>=5.7.2',
    'pyzmq==25.1.2',
    'redis>=3.5',
    "sentry-sdk",
    'smart-open>=1.11.1'
]
setup_requires = []
tests_requires = [
    'pytest>=4.6.11',
    'pytest-cov>=2.10.0',
    'codacy-coverage>=1.3.11',
    'moto',
]
dev_requires = [
    "ipython>=7.20"
]
extras_requires = {
    'all': tests_requires + dev_requires,
    'test': tests_requires,
    'dev': dev_requires
}
ext_modules = []
package_data = {}
classifiers = [
    'Development Status :: 5 - Production/Stable',
    'Intended Audience :: Developers',
    'Intended Audience :: Information Technology',
    'Intended Audience :: Science/Research',
    'License :: OSI Approved :: BSD License',
    'Programming Language :: Python',
    'Topic :: Software Development :: Libraries :: Python Modules',
    'Operating System :: Microsoft :: Windows',
    'Operating System :: Unix',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.11',
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

