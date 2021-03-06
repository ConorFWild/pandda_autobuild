import os
import sys
import subprocess
from pathlib import Path

import fire
import pandas as pd

from constants import Constants


class Event:
    def __init__(self, dtag, event_idx, x, y, z, model_path, reflections_path, smiles_path, event_map_path):
        self.dtag = dtag
        self.event_idx = event_idx

        self.x = x
        self.y = y
        self.z = z

        self.model_path = model_path
        self.reflections_path = reflections_path
        self.smiles_path = smiles_path
        self.event_map_path = event_map_path


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


def dispatch(event: Event, out_dir: Path, phenix_setup, rhofit_setup, mode):
    event_id = f"{event.dtag}_{event.event_idx}"

    print(f"Event id: {event_id}")

    # Create the Event dir
    event_dir = out_dir / event_id
    try_make_dir(event_dir)

    # Write the script that will call python to autobuild the event
    model = event.model_path
    xmap = event.event_map_path
    mtz = event.reflections_path
    smiles = event.smiles_path

    # Get path to python script
    autobuild_script_path = Path(__file__).parent / Constants.AUTOBUILD_SCRIPT

    # Get path to this python
    python = sys.executable

    executable_script = Constants.EXECUTABLE.format(
        python=python,
        phenix_setup=phenix_setup,
        rhofit_setup=rhofit_setup,
        autobuild_script_path=autobuild_script_path,
        model=model,
        xmap=xmap,
        mtz=mtz,
        smiles=smiles,
        x=event.x,
        y=event.y,
        z=event.z,
        out_dir=str(event_dir),
        phenix_setup_arg=phenix_setup,
        rhofit_setup_arg=rhofit_setup
    )
    executable_script_file = event_dir / Constants.EXECUTABLE_SCRIPT_FILE.format(dtag=event.dtag,
                                                                                 event_idx=event.event_idx)
    with open(executable_script_file, "w") as f:
        f.write(executable_script)

    chmod(executable_script_file)

    if mode == "condor":
        # Generate a job script file for a condor cluster
        executable_file = str(executable_script_file)
        log_file = event_dir / Constants.LOG_FILE.format(event_id=event_id)
        output_file = event_dir / Constants.OUTPUT_FILE.format(event_id=event_id)
        error_file = event_dir / Constants.ERROR_FILE.format(event_id=event_id)
        request_memory = Constants.REQUEST_MEMORY
        job_script = Constants.JOB.format(
            executable_file=executable_file,
            log_file=log_file,
            output_file=output_file,
            error_file=error_file,
            request_memory=request_memory,
        )
        job_script_file = event_dir / Constants.JOB_SCRIPT_FILE.format(dtag=event.dtag,
                                                                       event_idx=event.event_idx)
        with open(job_script_file, "w") as f:
            f.write(job_script)

            # Generate a shell command to submit the job to run the python script
        command = Constants.COMMAND.format(job_script_file=job_script_file)
        print(f"Command: {command}")

    elif mode == "qsub":
        # Generate a job script file for a condor cluster
        executable_file = str(executable_script_file)
        output_file = event_dir / Constants.OUTPUT_FILE.format(event_id=event_id)
        error_file = event_dir / Constants.ERROR_FILE.format(event_id=event_id)
        request_memory = Constants.REQUEST_MEMORY

        job_script = Constants.JOB_QSUB.format(
            output_file=output_file,
            error_file=error_file,
            h_vmem=request_memory,
            m_mem_free=request_memory,
            executable_file=executable_file,
        )
        job_script_file = event_dir / Constants.BATCH_PANDDA_JOB_SCRIPT_QSUB_FILE.format(dtag=event.dtag,
                                                                       event_idx=event.event_idx)
        with open(job_script_file, "w") as f:
            f.write(job_script)

        # Generate a shell command to submit the job to run the python script
        command = Constants.COMMAND_QSUB.format(job_script_file=job_script_file)
        print(f"Command: {command}")

    else:
        raise Exception("Invalid mode!")



    # Submit the job
    execute(command)


def get_event_list(pandda_event_table, pandda_dir, data_dir, smiles_regex, pandda):
    event_list = []
    for index, row in pandda_event_table.iterrows():
        dtag = row["dtag"]
        event_idx = row["event_idx"]
        x = row["x"]
        y = row["y"]
        z = row["z"]
        model_path = pandda_dir / Constants.PANDDA_PROCESSED_DATASETS_DIR / dtag / Constants.PANDDA_PDB_FILE.format(
            dtag=dtag)
        reflections_path = pandda_dir / Constants.PANDDA_PROCESSED_DATASETS_DIR / dtag / Constants.PANDDA_MTZ_FILE.format(
            dtag=dtag)

        dataset_data_dir = data_dir / dtag
        try:
            smiles_path = next(dataset_data_dir.glob(f"{smiles_regex}"))
        except:
            smiles_path = None

        bdc = row["1-BDC"]
        if pandda==1:
            event_map_path = pandda_dir / Constants.PANDDA_PROCESSED_DATASETS_DIR / dtag / Constants.PANDDA_EVENT_MAP_FILE.format(
                dtag=dtag,
                event_idx=event_idx,
                bdc=bdc,
            )
        else:
            event_map_path = pandda_dir / Constants.PANDDA_PROCESSED_DATASETS_DIR / dtag / Constants.PANDDA_2_EVENT_MAP_FILE.format(
                dtag=dtag,
                event_idx=event_idx,
                bdc=bdc,
            )

        event = Event(
            dtag=dtag,
            event_idx=event_idx,
            x=x,
            y=y,
            z=z,
            model_path=model_path,
            reflections_path=reflections_path,
            smiles_path=smiles_path,
            event_map_path=event_map_path
        )

        if smiles_path:
            event_list.append(event)
        else:
            print(f"\tNo smiles present for dataset: {dtag} in folder {dataset_data_dir}")

    return event_list


def main(pandda_dir: str, data_dir: str, output_dir: str,
         phenix_setup, rhofit_setup, mode="condor", smiles_regex="*.smiles", pandda=1):
    # Format arguments
    pandda_dir_path = Path(pandda_dir).resolve().absolute()
    data_dir_path = Path(data_dir).resolve().absolute()
    output_dir_path = Path(output_dir).resolve().absolute()
    rhofit_setup = rhofit_setup
    phenix_setup = phenix_setup

    print(f"Pandda dir path path: {pandda_dir_path}")
    print(f"Data dirs path: {data_dir_path}")
    print(f"Output dir path: {output_dir_path}")

    print(f"phenix_setup: {phenix_setup}")
    print(f"rhofit_setup: {rhofit_setup}")
    print(f"mode: {mode}")

    # Load database
    event_table_path = pandda_dir_path / "analyses" / "pandda_analyse_events.csv"
    pandda_event_table = pd.read_csv(str(event_table_path))
    print(f"Found event table of length: {len(pandda_event_table)} at {event_table_path}")

    # Select which datasets to build
    event_list = get_event_list(pandda_event_table, pandda_dir_path, data_dir_path, smiles_regex, pandda)
    print(f"Got {len(event_list)} events")

    # Make output directory
    try_make_dir(output_dir_path)

    for event in event_list:
        dispatch(event, output_dir_path, phenix_setup, rhofit_setup, mode)


if __name__ == "__main__":
    fire.Fire(main)
