import os
import sys
import subprocess
from pathlib import Path

import fire
import pandas as pd

from constants import Constants


class System:
    def __init__(self, system, pandda_dir, data_dir):
        self.system: str = system
        self.pandda_dir: Path = pandda_dir
        self.data_dir: Path = data_dir

def execute(command: str):
    p = subprocess.Popen(command,
                         shell=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE
                         )

    stdout, stderr = p.communicate()

    print(f"stdout: {stdout}")
    print(f"stderr: {stderr}")


def try_make_dir(path: Path):
    if not path.exists():
        os.mkdir(str(path))


def chmod(path: Path):
    p = subprocess.Popen(
        f"chmod 777 {str(path)}",
        shell=True,
    )

    p.communicate()


def dispatch(system: System, out_dir: Path, phenix_setup, rhofit_setup, mode):
    system_id = system.system
    print(f"System id: {system.system}")

    # Create the Event dir
    system_dir = out_dir / system_id
    try_make_dir(system_dir)

    # Get path to python script
    batch_pandda_path = Path(__file__).parent / Constants.BATCH_PANDDA_SCRIPT

    # Get path to this python
    python = sys.executable

    executable_script = Constants.EXECUTABLE_BATCH_PANDDA.format(
        python=python,
        phenix_setup=phenix_setup,
        rhofit_setup=rhofit_setup,
        batch_pandda_script_path=batch_pandda_path,
        pandda_dir=str(system.pandda_dir),
        data_dir=str(system.data_dir),
        output_dir=str(system_dir),
    )
    executable_script_file = system_dir / Constants.EXECUTABLE_BATCH_PANDDA_SCRIPT_FILE.format(system_id=system_id)
    with open(executable_script_file, "w") as f:
        f.write(executable_script)

    chmod(executable_script_file)

    if mode == "condor":
        # Generate a job script file for a condor cluster
        executable_file = str(executable_script_file)
        log_file = system_dir / Constants.BATCH_PANDDA_LOG_FILE.format(system_id=system_id)
        output_file = system_dir / Constants.BATCH_PANDDA_OUTPUT_FILE.format(system_id=system_id)
        error_file = system_dir / Constants.BATCH_PANDDA_ERROR_FILE.format(system_id=system_id)
        request_memory = Constants.REQUEST_MEMORY
        job_script = Constants.JOB.format(
            executable_file=executable_file,
            log_file=log_file,
            output_file=output_file,
            error_file=error_file,
            request_memory=request_memory,
        )
        job_script_file = system_dir / Constants.BATCH_PANDDA_JOB_SCRIPT_FILE.format(system_id=system_id)
        with open(job_script_file, "w") as f:
            f.write(job_script)

        # Generate a shell command to submit the job to run the python script
        command = Constants.COMMAND.format(job_script_file=job_script_file)
        print(f"Command: {command}")

    elif mode == "qsub":
        # Generate a job script file for a condor cluster
        executable_file = str(executable_script_file)
        output_file = system_dir / Constants.BATCH_PANDDA_OUTPUT_FILE.format(system_id=system_id)
        error_file = system_dir / Constants.BATCH_PANDDA_ERROR_FILE.format(system_id=system_id)
        request_memory = Constants.REQUEST_MEMORY

        job_script = Constants.JOB_QSUB.format(
            output_file=output_file,
            error_file=error_file,
            h_vmem=request_memory,
            m_mem_free=request_memory,
            executable_file=executable_file,
        )
        job_script_file = system_dir / Constants.BATCH_PANDDA_JOB_SCRIPT_QSUB_FILE.format(system_id=system_id)
        with open(job_script_file, "w") as f:
            f.write(job_script)

        # Generate a shell command to submit the job to run the python script
        command = Constants.COMMAND_QSUB.format(job_script_file=job_script_file)
        print(f"Command: {command}")

    else:
        raise Exception("Invalid mode!")

    # Submit the job
    execute(command)


def get_system_list(pandda_dirs_path, data_dirs_path):
    system_list = []
    for system_path in pandda_dirs_path.glob("*"):
        if (system_path / Constants.PANDDA_ANALYSES_DIR / Constants.PANDDA_ANALYSE_EVENTS_FILE).exists():
            system_name = system_path.name
            system_pandda_dir = system_path
            system_data_dir = data_dirs_path / system_name

            system = System(
                system_name,
                system_pandda_dir,
                system_data_dir
            )

            system_list.append(system)

    return system_list


def main(pandda_dirs: str, data_dirs: str, output_dirs: str, phenix_setup, rhofit_setup, mode):
    # Format arguments
    pandda_dirs_path = Path(pandda_dirs).resolve().absolute()
    data_dirs_path = Path(data_dirs).resolve().absolute()
    output_dirs_path = Path(output_dirs).resolve().absolute()
    rhofit_setup = Path(rhofit_setup).resolve().absolute()
    phenix_setup = Path(phenix_setup).resolve().absolute()

    print(f"Pandda dirs path: {pandda_dirs_path}")
    print(f"Output dirs path: {output_dirs_path}")

    # Select which datasets to build
    system_list = get_system_list(pandda_dirs_path, data_dirs_path)
    print(f"Got {len(system_list)} events")

    for system in system_list:
        dispatch(system, output_dirs_path, phenix_setup, rhofit_setup, mode)


if __name__ == "__main__":
    fire.Fire(main)
