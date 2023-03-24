# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
#

import functools
import inspect
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
    # pylint: disable=abstract-method
    def register_job(self, job_id: str) -> None:
        pass

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

    #@classmethod
    #def _equivalence_dict(cls) -> core.EquivalenceDict:
    #    return {
    #        "name": "job_name",
    #        "timeout_min": "time",
    #        "mem_gb": "mem",
    #        "nodes": "nodes",
    #        "cpus_per_task": "cpus_per_task",
    #        "gpus_per_node": "gpus_per_node",
    #        "tasks_per_node": "ntasks_per_node",
    #    }

#    @classmethod
#    def _valid_parameters(cls) -> Set[str]:
#        """Parameters that can be set through update_parameters"""
#        return set(_get_default_parameters())
#
#    def _convert_parameters(self, params: Dict[str, Any]) -> Dict[str, Any]:
#        params = super()._convert_parameters(params)
#        # replace type in some cases
#        if "mem" in params:
#            params["mem"] = _convert_mem(params["mem"])
#        return params
#
#    def _internal_update_parameters(self, **kwargs: Any) -> None:
#        """Updates sbatch submission file parameters
#
#        Parameters
#        ----------
#        See pbs documentation for most parameters.
#        Most useful parameters are: time, mem, gpus_per_node, cpus_per_task, partition
#        Below are the parameters that differ from pbs documentation:
#
#        signal_delay_s: int
#            delay between the kill signal and the actual kill of the pbs job.
#        setup: list
#            a list of command to run in sbatch befure running qsub
#        array_parallelism: int
#            number of map tasks that will be executed in parallel
#
#        Raises
#        ------
#        ValueError
#            In case an erroneous keyword argument is added, a list of all eligible parameters
#            is printed, with their default values
#
#        Note
#        ----
#        Best practice (as far as Quip is concerned): cpus_per_task=2x (number of data workers + gpus_per_task)
#        You can use cpus_per_gpu=2 (requires using gpus_per_task and not gpus_per_node)
#        """
#        defaults = _get_default_parameters()
#        in_valid_parameters = sorted(set(kwargs) - set(defaults))
#        if in_valid_parameters:
#            string = "\n  - ".join(f"{x} (default: {repr(y)})" for x, y in sorted(defaults.items()))
#            raise ValueError(
#                f"Unavailable parameter(s): {in_valid_parameters}\nValid parameters are:\n  - {string}"
#            )
#        # check that new parameters are correct
#        _make_sbatch_string(command="nothing to do", folder=self.folder, **kwargs)
#        super()._internal_update_parameters(**kwargs)
#
#    def _internal_process_submissions(
#        self, delayed_submissions: tp.List[utils.DelayedSubmission]
#    ) -> tp.List[core.Job[tp.Any]]:
#        if len(delayed_submissions) == 1:
#            return super()._internal_process_submissions(delayed_submissions)
#        # array
#        folder = utils.JobPaths.get_first_id_independent_folder(self.folder)
#        folder.mkdir(parents=True, exist_ok=True)
#        timeout_min = self.parameters.get("time", 5)
#        pickle_paths = []
#        for d in delayed_submissions:
#            pickle_path = folder / f"{uuid.uuid4().hex}.pkl"
#            d.set_timeout(timeout_min, self.max_num_timeout)
#            d.dump(pickle_path)
#            pickle_paths.append(pickle_path)
#        n = len(delayed_submissions)
#        # Make a copy of the executor, since we don't want other jobs to be
#        # scheduled as arrays.
#        array_ex = PbsExecutor(self.folder, self.max_num_timeout)
#        array_ex.update_parameters(**self.parameters)
#        array_ex.parameters["map_count"] = n
#        self._throttle()
#
#        first_job: core.Job[tp.Any] = array_ex._submit_command(self._submitit_command_str)
#        tasks_ids = list(range(first_job.num_tasks))
#        jobs: List[core.Job[tp.Any]] = [
#            PbsJob(folder=self.folder, job_id=f"{first_job.job_id}_{a}", tasks=tasks_ids) for a in range(n)
#        ]
#        for job, pickle_path in zip(jobs, pickle_paths):
#            job.paths.move_temporary_file(pickle_path, "submitted_pickle")
#        return jobs
#
#    @property
#    def _submitit_command_str(self) -> str:
#        return " ".join(
#            [shlex.quote(sys.executable), "-u -m submitit.core._submit", shlex.quote(str(self.folder))]
#        )
#
    def _make_submission_file_text(self, command: str, uid: str) -> str:
        return _make_jobfile_string(command=command, folder=self.folder, **self.parameters)

#    def _num_tasks(self) -> int:
#        nodes: int = self.parameters.get("nodes", 1)
#        tasks_per_node: int = max(1, self.parameters.get("ntasks_per_node", 1))
#        return nodes * tasks_per_node
#
    def _make_submission_command(self, submission_file_path: Path) -> List[str]:
        return ["cat", str(submission_file_path)]

    @staticmethod
    def _get_job_id_from_submission_command(string: Union[bytes, str]) -> str:
        """Returns the job ID from the output of qsub string"""
        if not isinstance(string, str):
            string = string.decode()
        output = re.search(r"<id>[0-9]+", string)
        if output is None:
            raise utils.FailedSubmissionError(
                f'Could not make sense of qsub output "{string}"\n'
                "Job instance will not be able to fetch status\n"
                "(you may however set the job job_id manually if needed)"
            )
        return output.group("id")

    @classmethod
    def affinity(cls) -> int:
        return -1 if shutil.which("qsub") is None else 2


#@functools.lru_cache()
#def _get_default_parameters() -> Dict[str, Any]:
#    """Parameters that can be set through update_parameters"""
#    specs = inspect.getfullargspec(_make_sbatch_string)
#    zipped = zip(specs.args[-len(specs.defaults) :], specs.defaults)  # type: ignore
#    return {key: val for key, val in zipped if key not in {"command", "folder", "map_count"}}


# pylint: disable=too-many-arguments,unused-argument, too-many-locals
def _make_jobfile_string(
    command: str,
    folder: tp.Union[str, Path],
    job_name: str = "submitit",
    walltime: str ="01:29:00",
    nodes: int = 1,
    ncpus: int = 1,
    mem: tp.Optional[str] = None,
    ngpus: tp.Optional[int] = None,  
    gpu_type: tp.Optional[int] = None,  
    array_parallelism: int = 256,
    stderr_to_stdout: bool = False,
    additional_parameters: tp.Optional[tp.Dict[str, tp.Any]] = None,
    qsub_args: tp.Optional[tp.Iterable[str]] = None,
) -> str:
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
    paths = utils.JobPaths(folder=folder)
    stdout = str(paths.stdout)
    stderr = str(paths.stderr)
    # Job arrays will write files in the form  <ARRAY_ID>_<ARRAY_TASK_ID>_<TASK_ID>


    if ngpus == 0 or ngpus is None:
        gpu = ''
    else:
        gpu = f':ngpus={ngpus}:gpu_type={gpu_type}'

    if(array_parallelism == 1):
        array=''
    else:
        array=f'#PBS -J 1-{array_parallelism}'

    # now create
    template = ('#!/bin/bash'
                f'#PBS -N {job_name}'
                f'#PBS -o {stdout}'
                f'#PBS -e {stderr}'
                f'#PBS -j oe'
                f'#PBS -l walltime={walltime}'
                f'#PBS -l select={nodes}:ncpus={ncpus}:mem={mem}{gpu}'
                f'{array}'
                'export SUBMITIT_EXECUTOR=pbs'
                f'{command}'
                )
    
    return template




def _shlex_join(split_command: tp.List[str]) -> str:
    """Same as shlex.join, but that was only added in Python 3.8"""
    return " ".join(shlex.quote(arg) for arg in split_command)
