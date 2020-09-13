#!/usr/bin/env python3
"""Setup module.

Was not properly tested!
"""
from setuptools import find_packages, setup

setup(name="py_nf",
      packages=find_packages(include=["py_nf"]),
      version="0.1.0",
      description="Library to execute job lists with nextflow",
      author="Bogdan Kirilenko",
      license="MIT",
      )
