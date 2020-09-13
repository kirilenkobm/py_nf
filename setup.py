#!/usr/bin/env python3
"""Setup module.

Was not properly tested!
"""
from setuptools import find_packages, setup

with open("README.md", "r") as f:
    long_description = f.read()

setup(name="py_nf",
      packages=find_packages(include=["py_nf"]),
      version="0.1.0",
      description="Library to execute job lists with nextflow",
      long_description=long_description,
      long_description_content_type="text/markdown",
      url="https://github.com/kirilenkobm/py_nf",
      author="Bogdan Kirilenko",
      author_email="kirilenkobm@gmail.com",
      license="MIT",
      )
