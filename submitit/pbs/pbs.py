# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
#

import functools
import inspect
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import typing as tp
import uuid
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from ..core import core, job_environment, logger, utils






class PbsInfoWatcher(core.InfoWatcher):
    def _make_command(self) -> Optional[List[str]]:
        # asking for array id will return all status
        # on the other end, asking for each and every one of them individually takes a huge amount of time
        to_check = {x.split("_")[0] for x in self._registered - self._finished}
        if not to_check:
            return None
        command = ["qstat.bin","-f", "-Fjson"]
        for jid in to_check:
            command.extend(["-t", str(jid)])
        return command

    def get_state(self, job_id: str, mode: str = "standard") -> str:
        """Returns the state of the job
        State of finished jobs are cached (use watcher.clear() to remove all cache)

        Parameters
        ----------
        job_id: int
            id of the job on the cluster
        mode: str
            one of "force" (forces a call), "standard" (calls regularly) or "cache" (does not call)
        """
        info = self.get_info(job_id, mode=mode)
        
        #TODO CHECK THAT IT WORKS ON HPC
        if isinstance(info, type(None)):
            return info.get("jobs").get(job_id).get("job_state")
        else:
            return "UNKNOWN"            

    def read_info(self, string: Union[bytes, str]) -> Dict[str, Dict[str, str]]:
        """Reads the output of qstat and returns a dictionary containing main information"""
        if not isinstance(string, str):
            string = string.decode()
        all_stats = json.loads(string)
        return all_stats


    
class PbsJob(core.Job[core.R]):

    _cancel_command = "qdel"
    watcher = PbsInfoWatcher(delay_s=600)

    def _interrupt(self, timeout: bool = False) -> None:
        """Sends preemption or timeout signal to the job (for testing purpose)

        Parameter
        ---------
        timeout: bool
            not used
        """
        cmd = ["qdel", self.job_id]



class PbsParseException(Exception):
    pass


class PbsJobEnvironment(job_environment.JobEnvironment):
    _env = {
        "job_id": "JOB_ID",
        "num_tasks": "PBS_NOTDEFINED",
        "num_nodes":"PBS_NOTDEFINED", 
        "node": "PBS_NOTDEFINED",
        "nodes": "PBS_NOTDEFINED",
        "global_rank": "PBS_NOTDEFINED",
        "local_rank": "PBS_NOTDEFINED",
        "array_job_id": "PBS_NOTDEFINED",
        "array_task_id": "PBS_NOTDEFINED",
    }


class PbsExecutor(core.PicklingExecutor):
    """Pbs job executor
    This class is used to hold the parameters to run a job on pbs.
    In practice, it will create a batch file in the specified directory for each job,
    and pickle the task function and parameters. At completion, the job will also pickle
    the output. Logs are also dumped in the same directory.

    Parameters
    ----------
    folder: Path/str
        folder for storing job submission/output and logs.
    max_num_timeout: int
        Maximum number of time the job can be requeued after timeout (if
        the instance is derived from helpers.Checkpointable)

    Note
    ----
    - be aware that the log/output folder will be full of logs and pickled objects very fast,
      it may need cleaning.
    - the folder needs to point to a directory shared through the cluster. This is typically
      not the case for your tmp! If you try to use it, pbs will fail silently (since it
      will not even be able to log stderr.
    - use update_parameters to specify custom parameters (n_gpus etc...). If you
      input erroneous parameters, an error will print all parameters available for you.
    """

    job_class = PbsJob

    def __init__(self, folder: Union[Path, str], max_num_timeout: int = 3) -> None:
        super().__init__(folder, max_num_timeout)

    @classmethod
    def _equivalence_dict(cls) -> core.EquivalenceDict:
        return {
            "name": "job_name",
            "timeout_min": "time",
            "mem_gb": "mem",
            "nodes": "nodes",
            "cpus_per_task": "cpus_per_task",
            "gpus_per_node": "gpus_per_node",
            "tasks_per_node": "ntasks_per_node",
        }

    @classmethod
    def _valid_parameters(cls) -> Set[str]:
        """Parameters that can be set through update_parameters"""
        return set(_get_default_parameters())

    def _internal_process_submissions(
        self, delayed_submissions: tp.List[utils.DelayedSubmission]
    ) -> tp.List[core.Job[tp.Any]]:
        """Submits a task to the cluster.

        Parameters
        ----------
        fn: callable
            The function to compute
        *args: any positional argument for the function
        **kwargs: any named argument for the function

        Returns
        -------
        Job
            A Job instance, providing access to the job information,
            including the output of the function once it is computed.
        """
        eq_dict = self._equivalence_dict()
        timeout_min = self.parameters.get(eq_dict["timeout_min"] if eq_dict else "timeout_min", 5)
        jobs = []
        for idx, delayed in enumerate(delayed_submissions):
            #tmp_uuid = uuid.uuid4().hex
            paths = utils.JobPaths(folder=self.folder,job_id=str(idx)) #change later if we want something else than idx as job_id 
            pickle_path = paths.folder / f"submitted_pickle"  #utils.JobPaths.get_first_id_independent_folder(self.folder) / f"{tmp_uuid}.pkl"
            pickle_path.parent.mkdir(parents=True, exist_ok=True)
            delayed.set_timeout(timeout_min, self.max_num_timeout)
            delayed.dump(pickle_path)

            job = self._submit_command(command=self._submitit_command_str(job_paths=paths), job_paths=paths)
            job.paths.move_temporary_file(pickle_path, "submitted_pickle")
            jobs.append(job)
        return jobs

    def _submit_command(self, command: str, job_paths: utils.JobPaths) -> core.Job[tp.Any]:
        """Submits a command to the cluster
        It is recommended not to use this function since the Job instance assumes pickle
        files will be created at the end of the job, and hence it will not work correctly.
        You may use a CommandFunction as argument to the submit function instead. The only
        problem with this latter solution is that stdout is buffered, and you will therefore
        not be able to monitor the logs in real time.

        Parameters
        ----------
        command: str
            a command string
        job_paths: utils.JobPaths
            a jobpaths object that contains all the relevant information about a job to be submitted.

        Returns
        -------
        Job
            A Job instance, providing access to the crun job information.
            Since it has no output, some methods will not be efficient
        """
        tmp_uuid = uuid.uuid4().hex
        submission_file_path = job_paths.folder  / f"submission_file_{tmp_uuid}.sh"
        with submission_file_path.open("w") as f:
            f.write(self._make_submission_file_text(command, tmp_uuid, job_paths))
        job: Job[tp.Any] = self.job_class(folder=self.folder, job_id=job_paths.job_id)
        print(f"job folder: {job.paths.folder}")
        print(f"self folder: {self.folder}")
        job.paths.move_temporary_file(submission_file_path, "submission_file") # is that still necessary?            
        command_list = self._make_submission_command(job.paths.submission_file)

        # run
        output = utils.CommandFunction(command_list, verbose=False)()  # explicit errors
        
        #self._write_job_id(job.job_id, tmp_uuid)
        #self._set_job_permissions(job.paths.folder)
        return job



    def _submitit_command_str(self, job_paths: utils.JobPaths) -> str:
        #TODO look into this to see how we can change sys executable into something more singuliarity-based
        return " ".join(
            [shlex.quote(sys.executable), "-u -m submitit.core._submit", shlex.quote(str(job_paths.folder))]
        )

    def _make_submission_file_text(self, command: str, uid: str, job_paths: utils.JobPaths) -> str:
        return _make_jobfile_string(command=command, folder=self.folder, job_paths = job_paths, **self.parameters)

    def _num_tasks(self) -> int:
        print("HELLO")
        nodes: int = self.parameters.get("nodes", 1)
        tasks_per_node: int = max(1, self.parameters.get("ntasks_per_node", 1))
        return nodes * tasks_per_node

    def _make_submission_command(self, submission_file_path: Path) -> List[str]:
        #TODO CHANGE THIS
        return ["sh","-c",f"RANDOM=$$; echo $RANDOM; echo qsub {submission_file_path} >> launch_exp.sh"]

    @staticmethod
    def _get_job_id_from_submission_command(string: Union[bytes, str]) -> str:
        print(f"string: {string}")
        """Returns the job ID from the output of qsub string"""
        if not isinstance(string, str):
            string = string.decode()
        output = string #re.search(r"[0-9]+", string)
        if output is None:
            raise utils.FailedSubmissionError(
                f'Could not make sense of qsub output "{string}"\n'
                "Job instance will not be able to fetch status\n"
                "(you may however set the job job_id manually if needed)"
            )
        return output#.group(0)

    @classmethod
    def affinity(cls) -> int:
        return -1 if shutil.which("qsub") is None else 2


@functools.lru_cache()
def _get_default_parameters() -> Dict[str, Any]:
    """Parameters that can be set through update_parameters"""
    specs = inspect.getfullargspec(_make_jobfile_string)
    zipped = zip(specs.args[-len(specs.defaults) :], specs.defaults)  # type: ignore
    return {key: val for key, val in zipped if key not in {"command", "folder", "map_count"}}


# pylint: disable=too-many-arguments,unused-argument, too-many-locals
def _make_jobfile_string(
    command: str,
    folder: tp.Union[str, Path],
    job_paths: utils.JobPaths,
    job_name: str = "submitit",
    partition: tp.Optional[str] = None,
    time: int = 5,
    nodes: int = 1,
    ntasks_per_node: tp.Optional[int] = None,
    cpus_per_task: tp.Optional[int] = None,
    cpus_per_gpu: tp.Optional[int] = None,
    num_gpus: tp.Optional[int] = None,  # legacy
    gpus_per_node: tp.Optional[int] = None,
    gpus_per_task: tp.Optional[int] = None,
    qos: tp.Optional[str] = None,  # quality of service
    setup: tp.Optional[tp.List[str]] = None,
    mem: tp.Optional[str] = None,
    mem_per_gpu: tp.Optional[str] = None,
    mem_per_cpu: tp.Optional[str] = None,
    signal_delay_s: int = 90,
    comment: tp.Optional[str] = None,
    constraint: tp.Optional[str] = None,
    exclude: tp.Optional[str] = None,
    account: tp.Optional[str] = None,
    gres: tp.Optional[str] = None,
    exclusive: tp.Optional[tp.Union[bool, str]] = None,
    array_parallelism: int = 256,
    wckey: str = "submitit",
    stderr_to_stdout: bool = False,
    map_count: tp.Optional[int] = None,  # used internally
    additional_parameters: tp.Optional[tp.Dict[str, tp.Any]] = None,
    srun_args: tp.Optional[tp.Iterable[str]] = None,
) -> str:

#def _make_jobfile_string(
#    command: str,
#    folder: tp.Union[str, Path],
#    job_name: str = "submitit",
#    walltime: str ="01:29:00",
#    nodes: int = 1,
#    ncpus: int = 1,
#    mem: tp.Optional[str] = None,
#    ngpus: tp.Optional[int] = None,  
#    gpu_type: tp.Optional[int] = None,  
#    array_parallelism: int = 256,
#    stderr_to_stdout: bool = False,
#    additional_parameters: tp.Optional[tp.Dict[str, tp.Any]] = None,
#    qsub_args: tp.Optional[tp.Iterable[str]] = None,
#) -> str:
    """Creates the content of an PBS jobfile file with provided parameters
    TODO: Description needs to be updated

    Parameters
    ----------
    Below are the parameters that differ from pbs documentation:

    folder: str/Path
        folder where print logs and error logs will be written

    qsub_args: List[str]
        Add each argument in the list to the qsub call

    Raises
    ------
    ValueError
        In case an erroneous keyword argument is added, a list of all eligible parameters
        is printed, with their default values
    """

    # add necessary parameters
    stdout = str(job_paths.stdout).replace("%t", "0")
    stderr = str(job_paths.stderr).replace("%t", "0")
        

    print(f"STDOUT:{stdout}")
    # Job arrays will write files in the form  <ARRAY_ID>_<ARRAY_TASK_ID>_<TASK_ID>
    print("THIS IS THE COMMAND:")
    print(command)
    
    if num_gpus == 0 or num_gpus is None:
        gpu = ''
    else:
        gpu = f':ngpus={ngpus}:gpu_type={gpu_type}'

    if(array_parallelism == 1):
        array=''
    else:
        array=f'#PBS -J 1-{array_parallelism}'

    # now create
    template = ('#!/bin/bash \n'
                f'#PBS -N {job_name} \n'
                f'#PBS -o {stdout} \n'
                f'#PBS -e {stderr} \n'
                f'#PBS -j oe \n'
                f'#PBS -l walltime={time} \n'
                f'#PBS -l select={nodes}:ncpus={cpus_per_task}:mem={mem}{gpu} \n'
                f'{array} \n'
                f'export SUBMITIT_EXECUTOR=pbs \n'
                f'export JOB_ID={job_paths.job_id} \n'
                f'{command} \n'
                )
    
    return template




def _shlex_join(split_command: tp.List[str]) -> str:
    """Same as shlex.join, but that was only added in Python 3.8"""
    return " ".join(shlex.quote(arg) for arg in split_command)
