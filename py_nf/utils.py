"""Utils that make using py_nf easier."""
import os
import sys
import shutil
import subprocess
from collections import Iterable
from .version import __version__

LOCAL = "local"
NEXTFLOW = "nextflow"


def paths_to_abspaths_in_line(line):
    """Replace all relative paths to absolute paths in a string.

    For example, a line:
    script.py in/file1.txt out/file1.txt -v is transformed into:
    /home/user/proj/script.py /home/user/proj/in/file1.txt /home/user/proj/out/file1.txt -v
    """
    if not isinstance(line, str):
        err_msg = (
            f"paths_to_abspaths_in_line expects a string as input, got {type(line)}"
        )
        raise ValueError(err_msg)
    # command is a space-separated list of arguments, some of them are paths
    line_pieces = line.split()
    upd_pieces = []
    for elem in line_pieces:
        # is parameter is not a path: we are not interested in it
        # else, it might be either a file or a directory
        is_file = os.path.isfile(elem)
        is_dir = os.path.isdir(elem)
        is_path = is_file or is_dir
        if not is_path:
            upd_pieces.append(elem)
            continue
        abspath = os.path.abspath(elem)
        upd_pieces.append(abspath)
    upd_string = " ".join(upd_pieces)
    return upd_string


def paths_to_abspaths_in_joblist(joblist):
    """Just apply replace_all_paths_to_abspaths_in_line to each job."""
    if not isinstance(joblist, Iterable):  # must be a list or other iterable
        raise TypeError(f"Error! Joblist must be an iterable! Got {type(joblist)}")
    upd_joblist = []
    for line in joblist:
        # just apply func to each line
        upd_line = paths_to_abspaths_in_line(line)
        upd_joblist.append(upd_line)
    return upd_joblist


def install_nf_if_not_installed():
    """Install nextflow if it's not installed.

    If already installed: return path to nextflow.
    If not: install and return abspath to installed NF.
    If impossible: raise an Error."""
    nf_here = shutil.which(NEXTFLOW)
    if nf_here is not None:
        # this is likely installed
        return nf_here
    # no installed, try to install
    cmd_1 = "curl -fsSL https://get.nextflow.io | bash"
    cmd_2 = "conda install -c bioconda nextflow"
    pwd = os.path.abspath(os.getcwd())
    try:
        subprocess.call(cmd_1, shell=True)
        nf_path = os.path.join(pwd, NEXTFLOW)
        return nf_path
    except subprocess.CalledProcessError:
        sys.stderr.write(f"Error! Command {cmd_1} failed\n")
        sys.stderr.write(f"Trying to install nextflow with conda...\n")
    try:
        subprocess.call(cmd_2, shell=True)
        return shutil.which(NEXTFLOW)
    except subprocess.CalledProcessError:
        sys.stderr.write("Error! Could not install nextflow.\nAbort\n")
        sys.exit(1)
