#!/usr/bin/env bash
rm -rf build
rm -rf dist

python3 setup.py bdist_wheel
pip3 install dist/*.whl --user
