"""Py-nf core functionality."""
import re
import subprocess
import os
import sys
import time
from datetime import datetime as dt
from collections import Iterable
import shutil
import inspect
import warnings
from .version import __version__


__author__ = "Bogdan Kirilenko"
CURRENT_DIR = os.path.dirname(__file__)


# nextflow params constants
NEXTFLOW_DEFAULT_EXE = "nextflow"
NEXTFLOW_EXE_PARAM = "nextflow_executable"
EXECUTOR_PARAM = "executor"
ERROR_STRATEGY_PARAM = "error_strategy"
MAX_RETRIES_PARAM = "max_retries"
QUEUE_PARAM = "queue"
MEMORY_PARAM = "memory"
TIME_PARAM = "time"
MEMORY_UNITS_PARAM = "memory_units"
TIME_UNITS_PARAM = "time_units"
CPUS_PARAM = "cpus"
VERBOSE = "verbose"
REMOVE_LOGS_PARAM = "remove_logs"
FORCE_REMOVE_LOGS_PARAM = "force_remove_logs"
WD_PARAM = "wd"
EXECUTOR_QUEUE_SIZE_PARAM = "executor_queuesize"
PROJECT_NAME_PARAM = "project_name"
NO_NF_CHECK_PARAM = "no_nf_check"
SWITCH_TO_LOCAL_PARAM = "switch_to_local"
ALREADY_EXISING_CONFIG = "predef_config"
LOCAL = "local"
RETRY_INCREASE_MEMORY_PARAM = "retry_increase_mem"
RETRY_INCREASE_TIME_PARAM = "retry_increase_time"
PARTITION_PARAM = "partition"
CLUSTER_OPTIONS_PARAM = "cluster_options"
NEXTFLOW_LOG_FILENAME = ".nextflow.log"

DEFAULT_SCRIPT_NAME = "script.nf"
DEFAULT_CONFIG_NAME = "config.nf"

AVAILABLE_MEMORY_UNITS = {"B", "KB", "MB", "GB", "TB"}
AVAILABLE_TIME_UNITS = {"ms", "milli", "millis",
                        "s", "sec", "seconds",
                        "m", "min", "minute", "minutes",
                        "h", "hour", "hours",
                        "d", "day", "days"}

class Nextflow:
    """Nextflow wrapper."""

    # each executor requires some binary to be accessible
    # for instance, slurm requires sbatch and lsf needs bsub
    executor_to_depend = {
        "slurm": "sbatch",
        "lsf": "bsub",
        "sge": "qsub",
        "pbs": "qsub",
        "pbspro": "qsub",
        "moab": "msub",
        "nqsii": "qsub",
        "condor": "condor_submit",
        # I didn't find any known binaries to check
        # whether they are valud upfront
        "ignite": None,
        "kubernetes": None,
        "awsbatch": None,
        "google-lifesciences": None,
        "tes": None,
    }

    def __init__(self, **kwargs):
        """Init nextflow wrapper."""
        # TODO: proper documentation of course
        # list of acceptable parameters
        self.params_list = {
            EXECUTOR_PARAM,
            NEXTFLOW_EXE_PARAM,
            ERROR_STRATEGY_PARAM,
            MAX_RETRIES_PARAM,
            RETRY_INCREASE_MEMORY_PARAM,
            RETRY_INCREASE_TIME_PARAM,
            QUEUE_PARAM,
            MEMORY_PARAM,
            MEMORY_UNITS_PARAM,
            TIME_PARAM,
            TIME_UNITS_PARAM,
            CPUS_PARAM,
            EXECUTOR_QUEUE_SIZE_PARAM,
            REMOVE_LOGS_PARAM,
            WD_PARAM,
            PROJECT_NAME_PARAM,
            NO_NF_CHECK_PARAM,
            FORCE_REMOVE_LOGS_PARAM,
            SWITCH_TO_LOCAL_PARAM,
            VERBOSE,
            CLUSTER_OPTIONS_PARAM,
        }
        self.verbosity_on = True if kwargs.get(VERBOSE) else False
        if kwargs.get(NEXTFLOW_EXE_PARAM):
            # in case if user provided a path to nextflow executable manually:
            self.nextflow_exe = os.path.abspath(kwargs[NEXTFLOW_EXE_PARAM])
        else:  # otherwise try default nextflow (must be in $PATH)
            self.nextflow_exe = NEXTFLOW_DEFAULT_EXE
        # check whether nextflow is installed and reachable
        # except user asked not to check for this here
        self.__nextflow_checked = False
        dont_check_nf = kwargs.get(NO_NF_CHECK_PARAM, False)
        self.__check_nextflow() if dont_check_nf is False else None
        # set nextflow parameters
        self.executor = kwargs.get(
            EXECUTOR_PARAM, LOCAL
        )  # local executor (CPU) is default
        # if user picked "slurm" on a machine without sbatch nextflow will raise an error
        # if "switch_to_local" value is True, py_nf will replace "slurm" to "local", if
        # there is no slurm
        self.switch_to_local = kwargs.get(SWITCH_TO_LOCAL_PARAM, False)
        self.__check_executor()
        self.error_strategy = kwargs.get(ERROR_STRATEGY_PARAM, "retry")
        self.max_retries = kwargs.get(MAX_RETRIES_PARAM, 3)
        self.queue = kwargs.get(QUEUE_PARAM, "batch")
        # TODO: fix this, must always be {number}.{units}
        self.memory = self.__set_memory(kwargs.get(MEMORY_PARAM, "10"), kwargs.get(MEMORY_UNITS_PARAM, "GB"))
        self.time = self.__set_time(kwargs.get(TIME_PARAM, "1"), kwargs.get(TIME_UNITS_PARAM, "h"))
        self.cpus = kwargs.get(CPUS_PARAM, 1)
        self.retry_increase_mem = kwargs.get(RETRY_INCREASE_MEMORY_PARAM, False)
        self.retry_increase_time = kwargs.get(RETRY_INCREASE_TIME_PARAM, False)
        self.executor_queuesize = kwargs.get(EXECUTOR_QUEUE_SIZE_PARAM, None)
        # set directory parameters
        # remove logs will remove project directory only in case of successful pipe execution
        # force_remove_logs will remove this anyway
        self.remove_logs = kwargs.get(REMOVE_LOGS_PARAM, False)
        self.force_remove_logs = kwargs.get(FORCE_REMOVE_LOGS_PARAM, False)
        wd = os.getcwd()  # if we like to run nextflow from some specific directory
        self.wd = kwargs.get(WD_PARAM, wd)
        self.cluster_options = kwargs.get(CLUSTER_OPTIONS_PARAM, None)
        self.__check_dir_exists(self.wd)
        self.project_name = None
        self.project_dir = None
        project_opt = kwargs.get(PROJECT_NAME_PARAM, None)
        self.set_project_name_and_dir(project_name=project_opt)
        self.jobs_num = 0
        self.joblist_path = None
        self.nextflow_script_path = None
        self.nextflow_config_path = None
        self.executed_with_success = None
        self.executed_at = "N/A"

        # show warnings if user provided not supported arguments
        not_acceptable_args = set(kwargs.keys()).difference(self.params_list)
        for elem in not_acceptable_args:
            # TODO: maybe crash then?
            msg = f"py_nf: Argument {elem} is not supported."
            warnings.warn(msg)
        if len(not_acceptable_args) > 0:
            warnings.warn("### Please find a list of supported options in the README.md")
        self.__v(f"Initiated py_nf with the following params:\n{self.__repr__()}")

    def __set_memory(self, quantity, units):
        """Get memory parameter as a string."""
        if units not in AVAILABLE_MEMORY_UNITS:
            raise ValueError(f"Invalid {MEMORY_UNITS_PARAM} parameter {units}, "
                             f"please select one of these:\n{AVAILABLE_MEMORY_UNITS}")
        return f"{quantity}.{units}"
    
    def __set_time(self, quantity, units):
        """Get memory parameter as a string."""
        if units not in AVAILABLE_TIME_UNITS:
            raise ValueError(f"Invalid {TIME_UNITS_PARAM} parameter {units}, "
                             f"please select one of these:\n{AVAILABLE_TIME_UNITS}")
        return f"{quantity}.{units}"

    def __v(self, msg):
        """Verbosity message."""
        sys.stderr.write(f"{msg}\n") if self.verbosity_on else None

    def set_project_name_and_dir(self, project_name=None):
        """Set project name and directory.

        Default value: nextflow_project_at_$timestamp.
        """
        self.__v(
            f"Calling {inspect.currentframe()}\nwith params: project_name={project_name}"
        )
        if project_name is None:
            # set default project name then
            timestamp = self._get_tmstmp()
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
        self.__v(f"Calling {inspect.currentframe()}; self.executor={self.executor}")
        if self.executor == LOCAL:
            # local executor must be reachable on any machine
            return True
        # not local executor: requires extra check
        # each executor requires some binary to be accessible
        # for instance, slurm requires sbatch and lsf needs bsub
        # TODO: handle ignite, kubernetes, awsbatch, tes and google-lifesciences, see issue #2
        if self.executor not in self.executor_to_depend.keys():
            msg = f"Executor {self.executor} is not supported, abort"
            raise NotImplementedError(msg)
        # we have a supported executor, need to check whether the required
        # executable exists
        depend_exe = self.executor_to_depend[self.executor]
        if depend_exe is None:
            # no way to check, just return True -> let the nextflow and user to figure this out
            return True
        depend_exists = shutil.which(depend_exe)
        if depend_exists:
            # this is fine, let's go further
            return True
        # no way to call the required executor
        if self.switch_to_local:
            # if this flag set: just call with 'local' executor
            # warn user anyway
            msg = (
                f"Cannot call nextflow pipe with {self.executor} executor: "
                f"command {depend_exe} is not available. Switching to 'local' executor."
            )
            warnings.warn(msg)
            self.executor = LOCAL
            return True
        # in this case we should terminate the program
        err_msg = (
            f"Cannot call nextflow pipeline with {self.executor} executor: "
            f"command {depend_exe} is not available. Either call it on another "
            f"machine or set switch_to_local parameter to True."
        )
        raise ValueError(err_msg)

    def __check_nextflow(self):
        """Check that nextflow is installed."""
        self.__v(
            f"Calling {inspect.currentframe()}; self.nextflow_exe={self.nextflow_exe}"
        )
        self.__nextflow_checked = True
        cmd = f"{self.nextflow_exe} -v"
        nf_here = shutil.which(self.nextflow_exe)
        if nf_here:
            return True
        err_msg = (
            f"Nextflow installation not found."
            f"Command {cmd} failed. Please find nextflow installation guide "
            f"here: https://www.nextflow.io/"
        )
        if nf_here is None:
            raise ChildProcessError(err_msg)

    @staticmethod
    def __check_dir_exists(directory):
        """Check that dir exists."""
        if not os.path.isdir(directory):
            raise OSError(f"Error! Directory {directory} does not exist!")

    def __create_config_file(self, config_exists=False):
        self.__v(f"Calling {inspect.currentframe()}")
        if config_exists:
            # in this case no need to create any additional config files
            return
        self.nextflow_config_path = os.path.abspath(
            os.path.join(self.project_dir, DEFAULT_CONFIG_NAME)
        )
        now = dt.now().isoformat()
        f = open(self.nextflow_config_path, "w")

        if self.executor_queuesize:
            f.write(f"executor.queueSize = {self.executor_queuesize}\n")

        f.write(
            f"// automatically generated config file for project {self.project_name}\n"
        )

        # TODO: depending on executor parameters the list of opts might differ
        f.write(f"// at: {now}\n")
        f.write("process {\n")
        f.write(f"    executor = '{self.executor}'\n")
        f.write(f"    queue = '{self.queue}'\n")
        f.write(f"    memory = '{self.memory}'\n")
        f.write(f"    time = '{self.time}'\n")
        f.write(f"    cpus = '{self.cpus}'\n")

                # with label config extensions
        if self.retry_increase_mem:
            # add extension to increase memory each time pipeline fails
            f.write("\n")
            f.write("    withLabel: retry_increase_mem {\n")
            f.write(f"        memory = {{{self.memory} * task.attempt}}\n")
            f.write(f"        errorStrategy = 'retry'\n")
            f.write("    }\n")

        if self.retry_increase_time:
            # add extension to increase time each time pipeline fails
            f.write("\n")
            f.write("    withLabel: retry_increase_time {\n")
            f.write(f"        time = {{{self.time} * task.attempt}}\n")
            f.write(f"        errorStrategy = 'retry'\n")
            f.write("    }\n")

        f.write("}\n")

        f.close()
        self.__v(f"Created config file at {self.nextflow_config_path}")

    def __create_nf_script(self):
        """Create nextflow script and config file"""
        self.__v(f"Calling {inspect.currentframe()}")
        self.nextflow_script_path = os.path.abspath(
            os.path.join(self.project_dir, DEFAULT_SCRIPT_NAME)
        )

        # write config file
        now = dt.now().isoformat()

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
        # optional parameters:
        f.write(f"    clusterOptions \"{self.cluster_options}\"\n") if self.cluster_options else None
        f.write(f"    label 'retry_increasing_mem'\n") if self.retry_increase_mem is True else None
        f.write(f"    label 'retry_increase_time'\n") if self.retry_increase_time is True else None
        f.write("\n")
        # calling the jobs itself:
        f.write("    input:\n")
        f.write("    val line from lines\n\n")
        f.write('    "${line}"\n')
        f.write("}\n")
        f.close()
        self.__v(f"Created script at {self.nextflow_script_path}")

    def execute(self, joblist, config_file=None):
        """Execute jobs in parallel."""
        self.__v(f"Calling {inspect.currentframe()}")
        self.__v(f"self.project_dir = {self.project_dir}")

        if not self.__nextflow_checked:
            self.__check_nextflow()
        os.mkdir(self.project_dir) if not os.path.isdir(self.project_dir) else None

        _config_exists = config_file is not None
        self.__generate_joblist_file(joblist)
        self.__create_config_file(config_exists=_config_exists)
        self.__create_nf_script()
        if config_file:  # in case user wants to execute with pre-defined config file
            self.nextflow_script_path = config_file

        # TODO: add detach process feature (useful if user wants to execute several pipelines at once)
        
        ### Important, recent versions of nextflow use DSL2. The py_nf library runs with DSL1 only ###
        # TODO: adapt for DSL2 version
        # Temporary solution for now: force DSL1 use
        os.environ["NXF_DEFAULT_DSL"] = "1"

        cmd = f"{self.nextflow_exe} {self.nextflow_script_path} -c {self.nextflow_config_path}"
        self.executed_at = self._get_tmstmp()
        self.__v(f"Executing command:\n{cmd}")
        rc = subprocess.call(cmd, shell=True, cwd=self.project_dir)
        # remove project files logic: if pipeline fails, remove_logs keep all files
        # in case of force_remove_logs we delete them anyway
        remove_files = self.force_remove_logs or (self.remove_logs and rc == 0)
        if remove_files:
            self.__v(f"Removing temporary files at {self.project_dir}")
            shutil.rmtree(self.project_dir) if os.path.isdir(self.project_dir) else None

        # TODO: maybe add a param to not kill program if nf pipe fails (kill by default)
        if rc != 0:
            # Nextflow pipe failed: we return 1.
            # User should decide whether to halt the upstream
            # functions or not (maybe do some garbage collecting or so)
            self.__v("Nextflow pipeline failed!")
            msg = (
                f"Nextflow pipeline {self.project_name} failed! "
                f"Execute function returns 1."
            )
            warnings.warn(msg)
            self.executed_with_success = False
            return 1
        else:  # everything is fine
            self.__v("Nextflow pipeline executed successfully")
            self.executed_with_success = True
            return 0

    def __generate_joblist_file(self, joblist):
        """Generate joblist file.

        Joblist expected type: list of strings.
        """
        self.__v(f"Calling {inspect.currentframe()}")
        self.joblist_path = os.path.abspath(
            os.path.join(self.project_dir, "joblist.txt")
        )
        self.__v(f"Saving joblist to: {self.joblist_path}")
        if not isinstance(joblist, Iterable):  # must be a list or other iterable
            raise TypeError(f"Error! Joblist must be an iterable! Got {type(joblist)}")
        f = open(self.joblist_path, "w")
        for elem in joblist:
            if type(elem) is not str:
                raise TypeError(f"Error! Jobs type must be string! Got {type(elem)}")
            f.write(f"{elem.rstrip()}\n")
            self.jobs_num += 1
        f.close()

    def get_nf_log(self, first=False):
        """Retrieve nextflow log.

        Return None if nextflow logs are absent.
        As default shows the latest log.
        If first flag is set, returns the first one."""
        if os.path.isdir(self.project_dir):
            files_in_proj_dir = os.listdir(self.project_dir)
        else:  # no dir: no logs
            return None
        nf_log_files = [
            f for f in files_in_proj_dir if f.startswith(NEXTFLOW_LOG_FILENAME)
        ]
        if len(nf_log_files) == 0:
            # no logs: nothing to return
            return None
        # may be something like ['.nextflow.log.2', '.nextflow.log', '.nextflow.log.1']
        # if there is only file: return this
        if len(nf_log_files) == 1:
            to_open = nf_log_files[0]
            path_to_open = os.path.join(self.project_dir, to_open)
            return self._get_file_content(path_to_open)
        # if we required the first one and there is .nextflow.log inside: we need this
        if NEXTFLOW_LOG_FILENAME in nf_log_files and first:
            path_to_open = os.path.join(self.project_dir, NEXTFLOW_LOG_FILENAME)
            return self._get_file_content(path_to_open)
        # if we required the first one BUT there is no '.nextflow.log'
        # at least show a warning
        if first:
            msg = (
                "get_nf_log(): requested the first log but .nextflow.log not "
                f"found in the {self.project_dir}"
            )
            warnings.warn(msg)
        # we are here: let's remove .nextflow.log for simplicity, other files can be split(".")
        # -> lets us to get a number
        nf_log_files = [x for x in nf_log_files if x != NEXTFLOW_LOG_FILENAME]
        print(nf_log_files)
        dot_splits = [x.split(".") for x in nf_log_files]
        # maybe a bit paranoid, but what if someone put something line .nextflow.logxxxx inside?
        # we need only the splits with 4 elements: ["", "nextflow", "log", "X"]
        dot_splits_len = [x for x in dot_splits if len(x) == 4]
        if len(dot_splits_len) == 0:
            return None
        # also we need the 3th element to be a number, what if there is something
        # .nextflow.log.trash inside?
        dot_splits_numeric = [x for x in dot_splits_len if x[3].isnumeric()]
        if len(dot_splits_numeric) == 0:
            return None
        # well, seems like there are numbers only
        num_to_filename = [(int(x[3]), ".".join(x)) for x in dot_splits_numeric]
        sorted_by_num = sorted(num_to_filename, key=lambda x: x[0])
        to_open = sorted_by_num[0][1] if first else sorted_by_num[-1][1]
        path_to_open = os.path.join(self.project_dir, to_open)
        return self._get_file_content(path_to_open)

    def __repr__(self):
        """Show parameters."""
        lines = [
            "<nf_py Nextflow wrapper>\n",
            f"project_name: {self.project_name}\n",
            f"nextflow executable: {self.nextflow_exe}\n",
            f"executor: {self.executor}\n",
            f"wd: {self.wd}\n",
            f"executed_success: {self.executed_with_success}\n",
            f"executed_at: {self.executed_at}\n",
            f"queue: {self.queue}\n",
            f"memory: {self.memory}\n",
            f"time: {self.time}\n",
            f"queue_size: {self.executor_queuesize}\n",
        ]
        return "".join(lines)

    @staticmethod
    def _get_tmstmp():
        """Get current timestamp."""
        return str(time.time()).split(".")[0]

    @staticmethod
    def _get_file_content(path):
        """Just return file content."""
        if not os.path.isfile(path):
            raise ValueError(f"File {path} not found!")
        with open(path, "r") as f:
            content = f.read()
        return content


def pick_executor():
    """Pick the best possible executor."""
    # TODO: if qsub is available then we need some extra procedure
    # please see issue #1
    for executor, dep_bin in Nextflow.executor_to_depend.items():
        if dep_bin is None:
            # here we cannot say for sure
            continue
        depend_available = shutil.which(dep_bin)
        if depend_available is None:
            continue
        return executor
    # didn't find any supported executor, use local
    return LOCAL
