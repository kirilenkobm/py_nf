#!/usr/bin/env python3
"""Test nextflow wrapper."""
import sys
import os
# a temporary solution for import error:
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from py_nf.py_nf import Nextflow


def get_joblist(sample_num):
    """Generate joblist."""
    abs_test_path = os.path.abspath(os.path.dirname(__file__))
    sample_script = os.path.join(abs_test_path, "sample_script.py")
    if sample_num == 1:
        jobs = []
        in_dir = os.path.join(abs_test_path, "input_test", "sample_1")
        out_dir = os.path.join(abs_test_path, "output", "sample_1")
        for i in range(1, 5):
            in_out_filename = f"file_{i}.fa"
            in_path = os.path.join(in_dir, in_out_filename)
            out_path = os.path.join(out_dir, in_out_filename)
            cmd = f"python3 {sample_script} {in_path} {out_path}"
            jobs.append(cmd)
        os.mkdir(out_dir) if not os.path.isdir(out_dir) else None
    else:
        raise ValueError(f"Test number {sample_num} doesn't exist")
    return jobs


if __name__ == "__main__":
    nf_instance = Nextflow()
    joblist = get_joblist(1)
    nf_instance.execute(joblist)
    pass
