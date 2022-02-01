#!/bin/bash
rm -rf dist/
rm -rf py_nf.egg-info
python3 setup.py sdist
twine upload dist/*
