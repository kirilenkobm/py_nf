#!/usr/bin/env python3
"""Test nextflow wrapper."""
import sys
import os
import pytest
# a temporary solution for import error:
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from py_nf.py_nf import Nextflow
from py_nf.py_nf import pick_executor


def get_joblist(sample_num):
    """Generate joblist."""
    abs_test_path = os.path.abspath(os.path.dirname(__file__))
    sample_script = os.path.join(abs_test_path, "sample_script.py")
    if sample_num == 1:
        jobs = []
        out_files = []
        ref_result_files = []
        in_dir = os.path.join(abs_test_path, "input_test", "sample_1")
        out_dir = os.path.join(abs_test_path, "output", "sample_1")
        exp_out_dir = os.path.join(abs_test_path, "expected_output", "sample_1")
        for i in range(1, 5):
            in_out_filename = f"file_{i}.fa"
            in_path = os.path.join(in_dir, in_out_filename)
            out_path = os.path.join(out_dir, in_out_filename)
            exp_out_path = os.path.join(exp_out_dir, in_out_filename)
            cmd = f"python3 {sample_script} {in_path} {out_path}"
            jobs.append(cmd)
            out_files.append(out_path)
            ref_result_files.append(exp_out_path)
        os.mkdir(out_dir) if not os.path.isdir(out_dir) else None
        to_compare = list(zip(out_files, ref_result_files))
        return jobs, to_compare
    else:
        raise ValueError(f"Test number {sample_num} doesn't exist")


def have_same_content(files_list):
    """Check that out and reference (expected output) files have the same content."""
    for elem in files_list:
        out_file = elem[0]
        exp_out = elem[1]
        if not os.path.isfile(out_file):
            return False
        with open(out_file, "r") as f:
            out_str = f.read()
        with open(exp_out, "r") as f:
            exp_out_str = f.read()
        if out_str != exp_out_str:
            return False
    return True


if __name__ == "__main__":
    project_name_1 = "test_project_1"
    exe = pick_executor()
    nf_instance = Nextflow(project_name=project_name_1, executor="slurm", switch_to_local=True)
    joblist, to_check = get_joblist(1)
    status = nf_instance.execute(joblist)
    # if status != 0: nextflow subprocess crashed
    # if status == 0 -> nextflow process finished without errors
    assert(status == 0)
    # check that results are correct:
    assert(have_same_content(to_check))
