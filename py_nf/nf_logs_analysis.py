#!/usr/bin/env python3
"""Analyse nextflow logs."""
import sys
import os
from collections import defaultdict
import subprocess
from collections import defaultdict
import datetime
from .version import __version__

__author__ = "Bogdan Kirilenko"

NF_CLI_LAUNCHER_TAG = "nextflow.cli.Launcher"
NF_EXE_COMPLETE_TAG = "Goodbye"
NF_TASK_SUBMITTER_TAG = "[Task submitter]"
NF_TASK_MONITOR_TAG = "[Task monitor]"
NF_SESSION_TAG = "nextflow.Session"

# Q: maybe there is a more pythonic way?
NF_MON_TO_NUM = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}


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

    def __repr__(self):
        """Repr."""
        # TODO: __repr__ implementation
        return "NextflowLog: __repr__() is not yet implementer"


class NextflowTime:
    """Nextflow logs time reader."""
    def __init__(self, nf_project_dir):
        """Read NF logs and get time data."""
        self.nf_project_dir = nf_project_dir
        # TODO: handle multiple log files in the same dir
        # such as .nextflow.log.1, 2, etc
        self.nextflow_log_file = os.path.join(self.nf_project_dir, ".nextflow.log")
        if not os.path.isfile(self.nextflow_log_file):
            err_msg = (f"{self.nf_project_dir} is not a nextflow project dir"
                       f".nextflow.log file not found")
            raise ValueError(err_msg)
        # well, time to read the logs
        self.year = None
        self.__extract_year()
        self.start_time = None
        self.end_time = None
        self.job_to_data = defaultdict(dict)
        self.__extract_time_data()

    def __extract_year(self):
        """Extract year."""
        f = open(self.nextflow_log_file, "r")
        for line in f:
            if line.startswith("  Created:"):
                # workaround to get year =)
                # not specified in Mon-Day Hr-Min-Sec.Nanosec

                # TODO: consider the case if the jobs started at the end of a year
                # and finished on the 1st of January
                # we will get jobs running > 1 year =)
                _date_str = line.rstrip().split()[1]
                _, _, _yyyy = _date_str.split("-")
                self.year = int(_yyyy)
                break
        f.close()
        # sanity check
        if self.year is None:
            # was not specified:
            err_msg = ("Log file seems to be corrupted; "
                       "could not find a line specifying year.\n"
                       "The line startswith:\n  Created:\nis absent")
            raise ValueError(err_msg)


    def __extract_time_data(self):
        """Extract time-related data from logs."""
        f = open(self.nextflow_log_file, "r")
        job_to_start = {}
        job_to_end = {}
        job_to_stat = {}

        for line in f:
            line_tokens = line.rstrip().split()
            if line.startswith("  "):
                # Version, Runtime, etc data
                # starts with two spaces
                # no time data here
                continue
            _date_str = line_tokens[0]
            _time_str = line_tokens[1]
            
            if NF_CLI_LAUNCHER_TAG in line_tokens:
                self.start_time = self.__get_dt_obj(self.year, _date_str, _time_str)
                continue
            elif NF_EXE_COMPLETE_TAG in line_tokens:
                print()
                self.end_time = self.__get_dt_obj(self.year, _date_str, _time_str)
                continue
            elif NF_TASK_SUBMITTER_TAG in line and NF_SESSION_TAG in line_tokens:
                job_id = line_tokens[7][1:-1].replace('/', "")
                job_start_time = self.__get_dt_obj(self.year, _date_str, _time_str)
                job_to_start[job_id] = job_start_time
                continue
            elif NF_TASK_MONITOR_TAG in line:
                full_job_id = "".join(line_tokens[-1].split("/")[-2:]).rstrip("]")
                job_id = full_job_id[:8]
                job_end_time = self.__get_dt_obj(self.year, _date_str, _time_str)
                job_to_end[job_id] = job_end_time
                exit_stat_index = line_tokens.index("exit:") + 1
                exit_stat = int(line_tokens[exit_stat_index].rstrip(";"))
                job_to_stat[job_id] = exit_stat
        f.close()

        self.__save_job_stats_arr(job_to_start, job_to_end, job_to_stat)

    def __save_job_stats_arr(self, j_to_start, j_to_end, j_to_rc):
        """Save jobs data to self.job_to_data array."""
        id_keys = set(j_to_start.keys()).intersection(j_to_end.keys()).intersection(j_to_rc.keys())
        missing_j_to_s = [k for k in j_to_start.keys() if k not in id_keys]
        missing_j_to_e = [k for k in j_to_end.keys() if k not in id_keys]
        missing_j_to_r = [k for k in j_to_rc.keys() if k not in id_keys]
        if len(missing_j_to_s) > 0:
            raise ValueError(f"Cannot find start times for jobs:\n{missing_j_to_s}")
        elif len(missing_j_to_e) > 0:
            raise ValueError(f"Cannot find end times for jobs:\n{missing_j_to_e}")
        elif len(missing_j_to_r) > 0:
            raise ValueError(f"Cannot find return codes for jobs:\n{missing_j_to_r}")
        for id_key in id_keys:
            self.job_to_data[id_key]["start"] = j_to_start[id_key]
            self.job_to_data[id_key]["end"] = j_to_end[id_key]
            self.job_to_data[id_key]["rc"] = j_to_rc[id_key]
            self.job_to_data[id_key]["tot"] = j_to_end[id_key] - j_to_start[id_key]

    # TODO: think about proper function naming
    def total_runtime(self):
        """Print total joblist runtime."""
        tot_time = self.end_time - self.start_time
        return tot_time
    
    def __get_jobs_times(self, only_failed=False, only_success=False):
        """Get array of job-related timedeltas according to filters."""
        if only_failed is True and only_success is True:
            err_msg = ("NextflowTime: only_failed and only_success cannot be True\n"
                       "at the same time, please select only one (or none)")
            raise ValueError(err_msg)
        job_times = []
        job_ids = []
        for k, v in self.job_to_data.items():
            # just a few filters
            if only_failed and v["rc"] == 0:
                continue
            elif only_success and v["rc"] != 0:
                continue
            job_times.append(v["tot"])
            job_ids.append(k)
        # if no items: warning?
        return job_times, job_ids

    def total_cpu_time(self, only_failed=False, only_success=False):
        """Sum of all jobs runtime."""
        job_times, _ = self.__get_jobs_times(only_failed=only_failed, only_success=only_success)
        job_sum_time = sum(job_times, datetime.timedelta())
        return job_sum_time
    
    def average_job_runtime(self, only_failed=False, only_success=False):
        """Get average job runtime."""
        job_times, _ = self.__get_jobs_times(only_failed=only_failed, only_success=only_success)
        job_sum_time = sum(job_times, datetime.timedelta())
        ave_job_time = job_sum_time / len(job_times)
        return ave_job_time

    # def median_job_runtime

    def longest_job(self, only_failed=False, only_success=False):
        """Get the longest job.
        
        Returns tuple (datetime.timedelta, string)
        1) Runtime or the longest job
        2) Longest job id
        """
        job_times, job_ids = self.__get_jobs_times(only_failed=only_failed, only_success=only_success)
        if len(job_times) == 0:
            # TODO: warning message?
            return (None, None)
        sorted_by_len = sorted(zip(job_times, job_ids), key=lambda x: x[0], reverse=True)
        return (sorted_by_len[0])


    @staticmethod
    def __get_dt_obj(year, date_str, time_str):
        """Get python datetime object from string and time.
        
        Likely there is a better and less wordy solution.
        """
        mon_str, day_str = date_str.split("-")
        month = NF_MON_TO_NUM.get(mon_str)
        if month is None:  # is it even possible? Surely
            err_msg = f"Error! Unknown month symbol: {mon_str}"
            raise ValueError(err_msg)
        day = int(day_str)

        hour_str, min_str, sec_ms_str = time_str.split(":")
        hour = int(hour_str)
        minute = int(min_str)
        second_str, ms_str = sec_ms_str.split(".")
        second = int(second_str)
        milisecond = int(ms_str)

        date = datetime.date(year, month, day)
        time = datetime.time(hour, minute, second, milisecond)

        event_datetime = datetime.datetime.combine(date, time)
        return event_datetime



if __name__ == '__main__':
    # TODO: to be removed after development
    # log = NextflowLog(sys.argv[1])
    # print(log.command_to_tasks)
    test = NextflowTime(sys.argv[1])
    print(test.longest_job())
