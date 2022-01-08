#!/usr/bin/env python3
"""Analyse nextflow logs."""
import sys
import os
from collections import defaultdict
import subprocess


class NFTaskHandler:
    """Nextflow task execution handler data."""
    def __init__(self, task_id, name, status, exit, work_dir):
        """Parse log related to task execution."""
        self.task_id = task_id
        self.name = name
        self.status = status
        self.exit = exit
        self.wd = work_dir

        # self.stdout = self.__read_task_stdout()
        # self.stderr = self.__read_task_stderr()


    def get_task_stdout(self):
        """Get command stdout."""
        stdout_path = os.path.join(self.wd, ".command.out")
        with open(stdout_path, "r") as f:
            return f.read()

    def get_task_stderr(self):
        """Get command stderr."""
        stderr_path = os.path.join(self.wd, ".command.err")
        with open(stderr_path, "r") as f:
            return f.read()

    def get_cmd(self):
        """Extract exact task command."""
        cmd_path = os.path.join(self.wd, ".command.sh")
        with open(cmd_path, "r") as f:
            return f.read()

    def execute_again(self):
        """Execute the command again."""
        # TODO: make a better implementation
        # handle errors etc
        exe_file = os.path.join(self.wd, ".command.run")
        subprocess.call(exe_file, shell=True)

    # def __repr__(self):  # TODO


class NextflowLog:
    def __init__(self, logs_dir):
        """Read logs from nextflow dir."""
        self.logs_dir = logs_dir
        self.nextflow_log_file = os.path.join(self.logs_dir, ".nextflow.log")
        self.nextflow_work_dir = os.path.join(self.logs_dir, "work")
        self.nextflow_nf_dir = os.path.join(self.logs_dir, ".nextflow")
        self.__check_valid_nf_dir()
        # init fields
        self.launcher_cmd = ""
        self.command_to_tasks = defaultdict(list)
        self.__parse_nf_log_file()

        pass

    def __check_valid_nf_dir(self):
        """Check that provided directory is actual nextflow project dir."""
        if not os.path.isfile(self.nextflow_log_file):
            err_msg = (f"{self.nextflow_log_file} is not a nextflow project dir"
                       f".nextflow.log file not found")
            raise ValueError(err_msg)
        if not os.path.isdir(self.nextflow_work_dir):
            err_msg = (f"{self.nextflow_log_file} is not a nextflow project dir"
                       f".work directory not found")
            raise ValueError(err_msg)
        if not os.path.isdir(self.nextflow_work_dir):
            err_msg = (f"{self.nextflow_nf_dir} is not a nextflow project dir"
                       f".nextflow directory not found")
            raise ValueError(err_msg)

    def __parse_nf_log_file(self):
        """Parse .nextflow.log file."""
        f = open(self.nextflow_log_file, "r")
        for line in f:
            # extract specific project data here
            # TODO: rm magic strings
            if "DEBUG nextflow.cli.Launcher" in line:
                self.launcher_cmd = line.rstrip().split("$> ")[-1]
                continue
            elif "[Task monitor] DEBUG n.processor.TaskPollingMonitor" in line:
                # something related to one of tasks
                time_data = line.split(">")[0].split("[Task monitor]")[0]
                line_data = line.rstrip().split(">")[1].split("[")[1].rstrip("]")
                fields = line_data.split("; ")
                # TODO: can be implemented much nicer
                task_id = None
                task_name = None
                task_status = None
                task_exit = None
                task_wd = None
                for field in fields:
                    key, value = field.split(": ")
                    if key == "id":
                        task_id = value
                    elif key == "name":
                        task_name = value
                    elif key == "status":
                        task_status = value
                    elif key == "exit":
                        task_exit = value
                    elif key == "workDir":
                        task_wd = value
                task_data = NFTaskHandler(task_id, task_name, task_status, task_exit, task_wd)
                task_cmd = task_data.get_cmd()
                self.command_to_tasks[task_cmd] = (task_data, time_data)
        f.close()

    # TODO: self.__repr__


if __name__ == '__main__':
    # TODO: to be removed after development
    log = NextflowLog(sys.argv[1])
    print(log.command_to_tasks)
