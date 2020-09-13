# py_nf library

A draft. The software is not yet properly tested.

For those who writes computational workflows in python and needs nextflow only
to call job lists in parallel: on cluster or CPU.
The idea is the following: user just imports the py_nf library, sets some parameters such as
executor, amount of memory and so on and asks the library to execute some list of jobs.
Then library creates and runs nextflow script itself.
So there are just 3 commands from user's perspective.

Convenient if you create job lists for cluster with python and don't like to learn another
scripting language, or nextflow language is not powerful enough for you needs.
In this case the only thing you need from nextflow is pushing your jobs to scheduler.

## Installation

First of all install nextflow and make sure you can call it.
Please see [nextflow.io](https://nextflow.io) for details.

```shell script
curl -fsSL https://get.nextflow.io | bash
# OR
conda install -c bioconda nextflow
```

For now this is the only way to install this library, will be added
to pypi later.

```shell script
python3 setup.py bdist_wheel
pip3 install dist/py_nf-0.1.0-py3-none-any.whl
```

## Usage

Simple usage scenario:

```python
# import py_nf library:
from py_nf.py_nf import Nextflow

# initiate nextflow handler:
nf = Nextflow(executor="local", project_name="project", cpus=4)

# create joblist:
joblist = ["script.py in/1.txt out/1.txt",
           "script.py in/2.txt out/2.txt"
           "script.py in/3.txt out/3.txt"
           "script.py in/4.txt out/4.txt"
           "script.py in/5.txt out/5.txt"]

# execute jobs:
status = nf.execute(joblist)

if status == 0:
    # enjoy your results
    pass
else:
    # pipeline failed, do something!
    # do_some_cleanup()
    exit(1)
```

Important:

please use absolute pathways in your commands!

### Read more about nextflow executors

Nextflow supports a wide range of cluster schedulers, please read about them and
acceptable parameters [here](https://www.nextflow.io/docs/latest/executor.html).

### Nextflow class parameters

List of parameters you can initiate the Nextflow() object and what they mean.

### execute function

Input: list/tuple or other iterable of strings.
Each string is a separate shell script command, such as:

```shell script
python3 script.py in_dir/file_1.txt out_dir/file_1.txt --some_option 1 --other_option 2
```

Output: 0 or 1
- If 0: nextflow pipeline executed successfully.
- Otherwise, if 1: nextflow pipeline crashed.
Please have a look at logs.

Please note that if pipeline crashed, py_nf does not raise an error!
User should decide what to do in this case, for instance do some cleanup before or so.

The proper usage would be:

```python
status = nf.execute(job_list)
if status == 1:
    # pipeline failed, need to do some cleanup
    do_some_cleanup()
    sys.exit(1)
```

## Troubleshooting

Case 1, you see an error message like this:

```txt
Can't open cache DB: /lustre/projects/project-xxx/.nextflow/cache/a80d212d-5a68-42b0-a8a5-d92665bdc492/db

Nextflow needs to be executed in a shared file system that supports file locks.
Alternatively you can run it in a local directory and specify the shared work
directory by using by `-w` command line option.
```

That means your filesystem doesn't file locks: maybe it's lustre and your system
administrator disabled the locks.
The simplest way to override this is to pick some directory outside lustre filesystem and
call nextflow from there.
You can use "wd" parameter to do so:

```python
from py_nf.py_nf import Nextflow

some_dir = "/home/user/nextflow_stuff"
nf = Nextflow(wd=some_dir)
```
