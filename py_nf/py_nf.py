"""Nextflow wrapper."""
import subprocess
import os
import sys
import time
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
WD_PARAM = "wd"
PROJECT_NAME_PARAM = "project_name"
NO_NF_CHECK_PARAM = "no_nf_check"


class Nextflow:
    """Nextflow wrapper."""
    def __init__(self, **kwargs):
        """Init nextflow wrapper."""
        # TODO: proper documentation of course
        # list of acceptable parameters
        self.params_list = {EXECUTOR_PARAM, NEXTFLOW_EXE_PARAM, ERROR_STRATEGY_PARAM,
                            MAX_RETRIES_PARAM, QUEUE_PARAM, MEMORY_PARAM, TIME_PARAM,
                            CPUS_PARAM, QUEUE_SIZE_PARAM, REMOVE_LOGS_PARAM, WD_PARAM,
                            PROJECT_NAME_PARAM, NO_NF_CHECK_PARAM}
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
        self.executor = kwargs.get(EXECUTOR_PARAM, "local")  # local executor (CPU) is default
        self.error_strategy = kwargs.get(ERROR_STRATEGY_PARAM, "retry")
        self.max_retries = kwargs.get(MAX_RETRIES_PARAM, 3)
        self.queue = kwargs.get(QUEUE_PARAM, "batch")
        self.memory = kwargs.get(MEMORY_PARAM, "10G")
        self.time = kwargs.get(TIME_PARAM, "1h")
        self.cpus = kwargs.get(CPUS_PARAM, 1)
        self.queue_size = kwargs.get(QUEUE_SIZE_PARAM, 100)
        # set directory parameters
        self.remove_logs = kwargs.get(REMOVE_LOGS_PARAM, False)
        wd = os.getcwd()  # if we like to run nextflow from some specific directory
        self.wd = kwargs.get(WD_PARAM, wd)
        self.__check_dir_exists(self.wd)
        timestamp = str(time.time()).split(".")[0]
        project_name = f"nextflow_project_at_{timestamp}"
        self.project_name = kwargs.get(PROJECT_NAME_PARAM, project_name)
        self.project_dir = os.path.abspath(os.path.join(self.wd, self.project_name))
        self.jobs_num = 0
        self.joblist_path = None
        self.nextflow_script_path = None
        self.nextflow_config_path = None
        # show warnings if user provided not supported arguments
        not_acceptable_args = set(kwargs.keys()).difference(self.params_list)
        for elem in not_acceptable_args:
            msg = f"py_nf: Argument {elem} is not supported."
            warnings.warn(msg)

    def __check_nextflow(self):
        """Check that nextflow is installed."""
        self.nextflow_checked = True
        cmd = f"{self.nextflow_exe} -v"
        rc = subprocess.call(cmd, shell=True)
        err_msg = f"Nextflow installation not found." \
                  f"Command {cmd} failed. Please find nextflow installation guide " \
                  f"here: https://www.nextflow.io/"
        if rc != 0:
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
        f = open(self.nextflow_config_path, "w")
        # TODO: depending on executor parameters list might differ
        f.write(f"// automatically generated config file for project {self.project_name}\n")
        f.write(f"process.executor = '{self.executor}'\n")
        f.write(f"process.queue = '{self.queue}'\n")
        f.write(f"process.memory = '{self.memory}'\n")
        f.write(f"process.time = '{self.time}'\n")
        f.write(f"process.cpus = '{self.cpus}'\n")
        f.close()
        # write script
        f = open(self.nextflow_script_path, "w")
        f.write(f"// automatically generated script for project {self.project_name}\n")
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

    def execute(self, joblist):
        """Execute jobs in parallel."""
        if not self.nextflow_checked:
            self.__check_nextflow()
        os.mkdir(self.project_dir) if not os.path.isdir(self.project_dir) else None
        self.__generate_joblist_file(joblist)
        self.__create_nf_script()
        cmd = f"{self.nextflow_exe} {self.nextflow_script_path} -c {self.nextflow_config_path}"
        rc = subprocess.call(cmd, shell=True, cwd=self.project_dir)
        if self.remove_logs:
            shutil.rmtree(self.project_dir) if os.path.isdir(self.project_dir) else None

        if rc != 0:
            err_msg = "Warning! Nextflow pipeline failed!\n"
            sys.stderr.write(err_msg)
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

    @staticmethod
    def _all_paths_to_abspaths(line):
        """Replace all relative paths to absolute paths in a string.

        For example, a line:
        script.py in/file1.txt out/file1.txt -v is transformed into:
        /home/user/proj/script.py /home/user/proj/in/file1.txt /home/user/proj/out/file1.txt -v
        """
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
