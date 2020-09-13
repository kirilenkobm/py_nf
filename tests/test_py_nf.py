#!/usr/bin/env python3
"""Test nextflow wrapper."""
import sys
import os
import pytest
# a temporary solution for import error:
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from py_nf.py_nf import Nextflow


def get_joblist(sample_num):
    """Generate joblist."""
    abs_test_path = os.path.abspath(os.path.dirname(__file__))
    sample_script = os.path.join(abs_test_path, "sample_script.py")
    if sample_num == 1:
        jobs = []
        out_files = []
        ethalon_files = []
        in_dir = os.path.join(abs_test_path, "input_test", "sample_1")
        out_dir = os.path.join(abs_test_path, "output", "sample_1")
        ethalon_dir = os.path.join(abs_test_path, "ethalon_output", "sample_1")
        for i in range(1, 5):
            in_out_filename = f"file_{i}.fa"
            in_path = os.path.join(in_dir, in_out_filename)
            out_path = os.path.join(out_dir, in_out_filename)
            ethalon_out_path = os.path.join(ethalon_dir, in_out_filename)
            cmd = f"python3 {sample_script} {in_path} {out_path}"
            jobs.append(cmd)
            out_files.append(out_path)
            ethalon_files.append(ethalon_out_path)
        os.mkdir(out_dir) if not os.path.isdir(out_dir) else None
        to_compare = list(zip(out_files, ethalon_files))
        return jobs, to_compare
    else:
        raise ValueError(f"Test number {sample_num} doesn't exist")


def have_same_content(files_list):
    """Check that out and ethalon files have the same content."""
    for elem in files_list:
        out_file = elem[0]
        ethalon = elem[1]
        if not os.path.isfile(out_file):
            return False
        with open(out_file, "r") as f:
            out_str = f.read()
        with open(ethalon, "r") as f:
            ethalon_str = f.read()
        if out_str != ethalon_str:
            return False
    return True


if __name__ == "__main__":
    project_name_1 = "test_project_1"
    nf_instance = Nextflow(project_name=project_name_1)
    joblist, to_check = get_joblist(1)
    nf_instance.execute(joblist)
    assert(have_same_content(to_check))
