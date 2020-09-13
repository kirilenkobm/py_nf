"""Nextflow wrapper."""
import subprocess
import os
import sys
import time
from datetime import datetime as dt
from collections import Iterable
import shutil
import warnings

__author__ = "Bogdan Kirilenko"
__version__ = "0.1 alpha"

# nextflow params constants
NEXTFLOW_DEFAULT_EXE = "nextflow"
NEXTFLOW_EXE_PARAM = "nextflow_executable"
EXECUTOR_PARAM = "executor"
ERROR_STRATEGY_PARAM = "error_strategy"
MAX_RETRIES_PARAM = "max_retries"
QUEUE_PARAM = "queue"
MEMORY_PARAM = "memory"
TIME_PARAM = "time"
CPUS_PARAM = "cpus"
QUEUE_SIZE_PARAM = "queue_size"
REMOVE_LOGS_PARAM = "remove_logs"
FORCE_REMOVE_LOGS_PARAM = "force_remove_logs"
WD_PARAM = "wd"
PROJECT_NAME_PARAM = "project_name"
NO_NF_CHECK_PARAM = "no_nf_check"
SWITCH_TO_LOCAL_PARAM = "switch_to_local"
LOCAL = "local"


class Nextflow:
    """Nextflow wrapper."""
    # each executor requires some binary to be accessible
    # for instance, slurm requires sbatch and lsf needs bsub
    executor_to_depend = {"slurm": "sbatch",
                          "lsf": "bsub",
                          "sge": "qsub",
                          "psb": "qsub",
                          "pbspro": "qsub",
                          "moab": "msub",
                          "nqsii": "qsub",
                          "condor": "condor_submit"}

    def __init__(self, **kwargs):
        """Init nextflow wrapper."""
        # TODO: proper documentation of course
        # list of acceptable parameters
        self.params_list = {EXECUTOR_PARAM, NEXTFLOW_EXE_PARAM, ERROR_STRATEGY_PARAM,
                            MAX_RETRIES_PARAM, QUEUE_PARAM, MEMORY_PARAM, TIME_PARAM,
                            CPUS_PARAM, QUEUE_SIZE_PARAM, REMOVE_LOGS_PARAM, WD_PARAM,
                            PROJECT_NAME_PARAM, NO_NF_CHECK_PARAM, FORCE_REMOVE_LOGS_PARAM,
                            SWITCH_TO_LOCAL_PARAM}
        if kwargs.get(NEXTFLOW_EXE_PARAM):
            # in case if user provided a path to nextflow executable manually:
            self.nextflow_exe = os.path.abspath(kwargs[NEXTFLOW_EXE_PARAM])
        else:  # otherwise try default nextflow (must be in $PATH)
            self.nextflow_exe = NEXTFLOW_DEFAULT_EXE
        # check whether nextflow is installed and reachable
        # except user asked not to check for this here
        self.nextflow_checked = False
        dont_check_nf = kwargs.get(NO_NF_CHECK_PARAM, False)
        self.__check_nextflow() if dont_check_nf is False else None
        # set nextflow parameters
        self.executor = kwargs.get(EXECUTOR_PARAM, LOCAL)  # local executor (CPU) is default
        # if user picked "slurm" on a machine without sbatch nextflow will raise an error
        # if "switch_to_local" value is True, py_nf will replace "slurm" to "local", if
        # there is no slurm
        self.switch_to_local = kwargs.get(SWITCH_TO_LOCAL_PARAM, False)
        self.__check_executor()
        self.error_strategy = kwargs.get(ERROR_STRATEGY_PARAM, "retry")
        self.max_retries = kwargs.get(MAX_RETRIES_PARAM, 3)
        self.queue = kwargs.get(QUEUE_PARAM, "batch")
        self.memory = kwargs.get(MEMORY_PARAM, "10G")
        self.time = kwargs.get(TIME_PARAM, "1h")
        self.cpus = kwargs.get(CPUS_PARAM, 1)
        self.queue_size = kwargs.get(QUEUE_SIZE_PARAM, 100)
        # set directory parameters
        # remove logs will remove project directory only in case of successful pipe execution
        # force_remove_logs will remove this anyway
        self.remove_logs = kwargs.get(REMOVE_LOGS_PARAM, False)
        self.force_remove_logs = kwargs.get(FORCE_REMOVE_LOGS_PARAM, False)
        wd = os.getcwd()  # if we like to run nextflow from some specific directory
        self.wd = kwargs.get(WD_PARAM, wd)
        self.__check_dir_exists(self.wd)
        self.project_name = None
        self.project_dir = None
        project_opt = kwargs.get(PROJECT_NAME_PARAM, None)
        self.set_project_name_and_dir(project_name=project_opt)
        self.jobs_num = 0
        self.joblist_path = None
        self.nextflow_script_path = None
        self.nextflow_config_path = None
        # show warnings if user provided not supported arguments
        not_acceptable_args = set(kwargs.keys()).difference(self.params_list)
        for elem in not_acceptable_args:
            msg = f"py_nf: Argument {elem} is not supported."
            warnings.warn(msg)

    def set_project_name_and_dir(self, project_name=None):
        """Set project name and directory.

        Default value: nextflow_project_at_$timestamp.
        """
        if project_name is None:
            # set default project name then
            timestamp = str(time.time()).split(".")[0]
            project_name = f"nextflow_project_at_{timestamp}"
        self.project_name = project_name
        self.project_dir = os.path.abspath(os.path.join(self.wd, self.project_name))

    def __check_executor(self):
        """Check executor parameter correctness.

        For instance, if executor == 'slurm', but there is no slurm,
        must not be overlooked. Please see documentation:
        https://www.nextflow.io/docs/latest/executor.html
        for details.
        """
        if self.executor == LOCAL:
            # local executor must be reachable on any machine
            return True
        # not local executor: requires extra check
        # each executor requires some binary to be accessible
        # for instance, slurm requires sbatch and lsf needs bsub
        # TODO: handle ignite, kubernetes, awsbatch, tes and google-lifesciences
        if self.executor not in self.executor_to_depend.keys():
            msg = f"Executor {self.executor} is not supported, abort"
            raise NotImplementedError(msg)
        # we have a supported executor, need to check wheter the required
        # executable exists
        depend_exe = self.executor_to_depend[self.executor]
        depend_exists = shutil.which(depend_exe)
        if depend_exists:
            # this is fine, let's go further
            return True
        # no way to call the required executor
        if self.switch_to_local:
            # if this flag set: just call with 'local' executor
            # warn user anyway
            msg = f"Cannot call nextflow pipe with {self.executor} executor: " \
                  f"command {depend_exe} is not available. Switching to 'local' executor."
            warnings.warn(msg)
            self.executor = LOCAL
            return True
        # in this case we should terminate the program
        err_msg = f"Cannot call nextflow pipeline with {self.executor} executor: " \
                  f"command {depend_exe} is not available. Either call it on another " \
                  f"machine or set switch_to_local parameter to True."
        raise ValueError(err_msg)

    def __check_nextflow(self):
        """Check that nextflow is installed."""
        self.nextflow_checked = True
        cmd = f"{self.nextflow_exe} -v"
        nf_here = shutil.which(self.nextflow_exe)
        if nf_here:
            return True
        err_msg = f"Nextflow installation not found." \
                  f"Command {cmd} failed. Please find nextflow installation guide " \
                  f"here: https://www.nextflow.io/"
        if nf_here is None:
            raise ChildProcessError(err_msg)

    @staticmethod
    def __check_dir_exists(directory):
        """Check that dir exists."""
        if not os.path.isdir(directory):
            raise OSError(f"Error! Directory {directory} does not exist!")

    def __create_nf_script(self):
        """Create nextflow script and config file"""
        self.nextflow_script_path = os.path.abspath(os.path.join(self.project_dir, "script.nf"))
        self.nextflow_config_path = os.path.abspath(os.path.join(self.project_dir, "config.nf"))
        # write config file
        now = dt.now().isoformat()
        f = open(self.nextflow_config_path, "w")
        # TODO: depending on executor parameters list might differ
        f.write(f"// automatically generated config file for project {self.project_name}\n")
        f.write(f"// at: {now}\n")
        f.write(f"process.executor = '{self.executor}'\n")
        f.write(f"process.queue = '{self.queue}'\n")
        f.write(f"process.memory = '{self.memory}'\n")
        f.write(f"process.time = '{self.time}'\n")
        f.write(f"process.cpus = '{self.cpus}'\n")
        f.close()
        # write script
        f = open(self.nextflow_script_path, "w")
        f.write(f"// automatically generated script for project {self.project_name}\n")
        f.write(f"// at: {now}\n")
        f.write(f"joblist_path = '{self.joblist_path}'\n")
        f.write(f"joblist = file(joblist_path)\n")
        f.write(f"lines = Channel.from(joblist.readLines())\n\n")
        f.write("process execute_jobs {\n")
        f.write(f"    errorStrategy '{self.error_strategy}'\n")
        f.write(f"    maxRetries {self.max_retries}\n")
        f.write("\n")
        f.write("    input:\n")
        f.write("    val line from lines\n\n")
        f.write("    \"${line}\"\n")
        f.write("}\n")
        f.close()

    def execute(self, joblist, config_file=None):
        """Execute jobs in parallel."""
        if not self.nextflow_checked:
            # TODO: show warning
            self.__check_nextflow()
        os.mkdir(self.project_dir) if not os.path.isdir(self.project_dir) else None
        self.__generate_joblist_file(joblist)
        self.__create_nf_script()
        if config_file:  # in case if user wants to execute with pre-defined parameters
            self.nextflow_script_path = config_file
        cmd = f"{self.nextflow_exe} {self.nextflow_script_path} -c {self.nextflow_config_path}"
        rc = subprocess.call(cmd, shell=True, cwd=self.project_dir)
        # remove project files logic: if pipeline fails, remove_logs keep all files
        # in case of force_remove_logs we delete them anyway
        remove_files = self.force_remove_logs or (self.remove_logs and rc == 0)
        if remove_files:
            shutil.rmtree(self.project_dir) if os.path.isdir(self.project_dir) else None

        if rc != 0:
            # Nextflow pipe failed: we return 1.
            # User should decide whether to halt the upstream
            # functions or not (maybe do some garbage collecting or so)
            msg = f"Nextflow pipeline {self.project_name} failed!" \
                  f"execute function returns 1."
            warnings.warn(msg)
            return 1
        else:  # everything is fine
            return 0

    def __generate_joblist_file(self, joblist):
        """Generate joblist file.

        Joblist expected type: list of strings.
        """
        self.joblist_path = os.path.abspath(os.path.join(self.project_dir, "joblist.txt"))
        if not isinstance(joblist, Iterable):  # must be a list or other iterable
            raise TypeError(f"Error! Joblist must be an iterable! Got {type(joblist)}")
        f = open(self.joblist_path, "w")
        for elem in joblist:
            if type(elem) is not str:
                raise TypeError(f"Error! Jobs type must be string! Got {type(elem)}")
            f.write(f"{elem.rstrip()}\n")
            self.jobs_num += 1
        f.close()

    def __repr__(self):
        """Show parameters."""
        line = "Nextflow wrapper, parameters:\n"
        for k, v in self.__dict__.items():
            # TODO: improve this
            line += f"{k}: {v}\n"
        return line


def paths_to_abspaths_in_line(line):
    """Replace all relative paths to absolute paths in a string.

    For example, a line:
    script.py in/file1.txt out/file1.txt -v is transformed into:
    /home/user/proj/script.py /home/user/proj/in/file1.txt /home/user/proj/out/file1.txt -v
    """
    if not isinstance(line, str):
        err_msg = f"paths_to_abspaths_in_line expects a string as input, got {type(line)}"
        raise ValueError(err_msg)
    # command is a space-separated list of arguments, some of them are paths
    # TODO: what if someone separate arguments with tabs?
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


def pick_executor():
    """Pick the best possible executor."""
    # TODO: if qsub is available then we need some extra procedure
    for executor, dep_bin in Nextflow.executor_to_depend.items():
        depend_available = shutil.which(dep_bin)
        if depend_available is None:
            continue
        return executor
    # didn't find any supported executor, use local
    return LOCAL
