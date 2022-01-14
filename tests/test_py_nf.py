#!/usr/bin/env python3
"""Test nextflow wrapper."""
# TODO: do with pytest
import sys
import os
import shutil
import pytest

# a temporary solution for import error:
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from py_nf.py_nf import Nextflow
from py_nf.py_nf import pick_executor
from py_nf.utils import paths_to_abspaths_in_joblist


def get_joblist(sample_num):
    """Generate joblist."""
    test_path = os.path.dirname(__file__)
    sample_script = os.path.join(test_path, "sample_script.py")
    if sample_num == 1:
        jobs = []
        out_files = []
        ref_result_files = []
        in_dir = os.path.join(test_path, "input_test", "sample_1")
        out_dir = os.path.join(test_path, "output", "sample_1")
        exp_out_dir = os.path.join(test_path, "expected_output", "sample_1")
        for i in range(1, 5):
            in_out_filename = f"file_{i}.fa"
            in_path = os.path.abspath(os.path.join(in_dir, in_out_filename))
            out_path = os.path.abspath(os.path.join(out_dir, in_out_filename))
            exp_out_path = os.path.abspath(os.path.join(exp_out_dir, in_out_filename))
            cmd = f"python3 {sample_script} {in_path} {out_path}"
            jobs.append(cmd)
            out_files.append(out_path)
            ref_result_files.append(exp_out_path)
        os.mkdir(out_dir) if not os.path.isdir(out_dir) else None
        to_compare = list(zip(out_files, ref_result_files))
        return jobs, to_compare
    if sample_num == 2:
        jobs = [f"not_a_script.py in/{x}.txt out/{x}.txt" for x in range(10)]
        return jobs, None
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
    project_name_2 = "test_project_2"
    if "clean" in sys.argv:
        projects = [project_name_1, project_name_2]
        for project in projects:
            shutil.rmtree(project) if os.path.isdir(project) else None
        sys.exit("Cleaned")
    print("### Running test 1\n")
    nf_instance = Nextflow(
        project_name=project_name_1,
        executor="slurm",
        switch_to_local=True,
        verbose=True,
    )
    joblist, to_check = get_joblist(1)
    abs_joblist = paths_to_abspaths_in_joblist(joblist)
    status = nf_instance.execute(abs_joblist)
    # if status != 0: nextflow subprocess crashed
    # if status == 0 -> nextflow process finished without errors
    assert status == 0
    # check that results are correct:
    assert have_same_content(to_check)
    del nf_instance
    print("### Running test 2 -> should fail\n")
    project_name_1 = "test_project_2"
    exe = pick_executor()
    nf_instance = Nextflow(
        project_name=project_name_2,
        executor=exe,
        switch_to_local=True,
        verbose=True,
        max_retries=3,
        retry_increase_mem=True,
    )
    joblist_2, _ = get_joblist(2)
    status = nf_instance.execute(joblist_2)
    assert status != 0
    print("Test 2: success (NF pipeline should fail)")
    del nf_instance
    print("### Running test 3 -> should fail\n")
    try:
        nf_instance = Nextflow(project_name=None, executor="non_existent")
    except NotImplementedError as err:
        print(err)
        print("Test 3: OK")
    else:
        print("Test 3 -> not raised an error -> failed")
        sys.exit(1)

    print("### Running test 4 (should fail)\n")
    try:
        nf_instance = Nextflow(project_name=None, executor="lsf")
    except ValueError as err:
        print(err)
        print("Test 4: OK")
    else:
        print("Test 4: Fail")
        sys.exit(1)
