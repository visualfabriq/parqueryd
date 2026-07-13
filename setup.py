########################################################################
#
# License: MIT
# Created: August 8, 2020
#       Author:  Carst Vaartjes - cvaartjes@visualfabriq.com
#
########################################################################
import codecs
import os

from setuptools import find_packages, setup

# Check this Python version is supported


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
    "azure-storage-blob>=12.4,<13",
    "boto3>=1.17,<2",
    "configobj>=5,<6",
    "netifaces>=0.10,<1",
    "pyarrow>=20,<26",
    "parquery>=2.0.6,<3",
    "psutil>=5.7,<8",
    "pyzmq>=25.1.2,<28",
    "redis>=3.5,<7",
    "sentry-sdk>=1,<3",
    "smart-open>=1.11,<8",
]
setup_requires = []
tests_requires = [
    "pytest>=7.4,<9",
    "pytest-cov>=4,<7",
    "coverage>=5,<8",
    "codacy-coverage>=1.3,<2",
    "moto>=4,<6",
    "numpy>=1.24,<3",
    "pandas>=1.5.3,<3",
]
dev_requires = ["ipython>=7.20,<10", "ruff>=0.8,<1"]
extras_requires = {
    "all": tests_requires + dev_requires,
    "test": tests_requires,
    "dev": dev_requires,
}
ext_modules = []
package_data = {}
classifiers = [
    "Development Status :: 5 - Production/Stable",
    "Intended Audience :: Developers",
    "Intended Audience :: Information Technology",
    "Intended Audience :: Science/Research",
    "License :: OSI Approved :: BSD License",
    "Programming Language :: Python",
    "Topic :: Software Development :: Libraries :: Python Modules",
    "Operating System :: Microsoft :: Windows",
    "Operating System :: Unix",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Programming Language :: Python :: 3.14",
]

setup(
    name="parqueryd",
    description="A distribution framework for parquery",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    classifiers=classifiers,
    author="Carst Vaartjes",
    author_email="cvaartjes@visualfabriq.com",
    maintainer="Jelle Verstraaten",
    maintainer_email="jverstraaten@visualfabriq.com",
    url="https://github.com/visualfabriq/parqueryd",
    license="GPL2",
    python_requires=">=3.11",
    platforms=["any"],
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
        "console_scripts": [
            "parqueryd = parqueryd.node:main",
            "parqueryd_local = parqueryd.connect_local:main",
        ]
    },
)
