#!/usr/bin/env python3
"""Setup module.

A way to install this:
python3 setup.py bdist_wheel
pip3 install dist/py_nf-0.1.0-py3-none-any.whl

Load to test-pypi:
python3 setup.py sdist bdist_wheel
python3 -m twine upload --repository testpypi dist/*
Install from test-pypi:
pip install -i https://test.pypi.org/simple/ py-nf==0.1.0 --user
"""
from setuptools import find_packages, setup

with open("README.md", "r") as f:
    long_description = f.read()


classifiers=[
    "Programming Language :: Python :: 3",
    "License :: OSI Approved :: MIT License",
    "Operating System :: POSIX",
    "Development Status :: 2 - Pre-Alpha",
    "Topic :: Scientific/Engineering :: Bio-Informatics"
]

setup(name="py_nf",
      packages=find_packages(include=["py_nf"]),
      version="0.1.0",
      description="Execute batches of jobs with nextflow",
      long_description=long_description,
      long_description_content_type="text/markdown",
      url="https://github.com/kirilenkobm/py_nf",
      author="Bogdan Kirilenko",
      author_email="kirilenkobm@gmail.com",
      license="MIT",
      classifiers=classifiers,
      )
