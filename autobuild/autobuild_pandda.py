import os
import subprocess
from pathlib import Path

import pandas as pd
import fire

from constants import Constants


def execute(command: str):
    p = subprocess.Popen(command,
                         shell=True,
                         )

    p.communicate()


def try_make_dir(path: Path):
    if not path.exists():
        os.mkdir(str(path))


def chmod(path: Path):
    p = subprocess.Popen(
        f"chmod 777 {str(path)}",
        shell=True,
    )

    p.communicate()


class Event:
    def __init__(self,
                 dtag,
                 event_idx,
                 model_path,
                 event_map_path,
                 reflections_path,
                 smiles_path,
                 x,
                 y,
                 z,
                 ):
        self.dtag = dtag
        self.event_idx = event_idx
        self.model_path = model_path
        self.event_map_path = event_map_path
        self.reflections_path = reflections_path
        self.smiles_path = smiles_path
        self.x = x
        self.y = y
        self.z = z


def dispatch(event: Event, out_dir: Path, mode: str):
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

    executable_script = Constants.EXECUTABLE.format(
        autobuild_script_path=str(Path(os.path.dirname(__file__)) / "autobuild.py"),
        model=model,
        xmap=xmap,
        mtz=mtz,
        smiles=smiles,
        x=event.x,
        y=event.y,
        z=event.z,
        out_dir=str(event_dir)
    )
    executable_script_file = event_dir / Constants.EXECUTABLE_SCRIPT_FILE.format(
        dtag=event.dtag,
        event_idx=event.event_idx,
    )
    with open(executable_script_file, "w") as f:
        f.write(executable_script)

    chmod(executable_script_file)

    # Generate a job script file for a condor cluster
    executable_file = str(executable_script_file)
    log_file = event_dir / Constants.LOG_FILE.format(event_id=event_id)
    output_file = event_dir / Constants.OUTPUT_FILE.format(event_id=event_id)
    error_file = event_dir / Constants.ERROR_FILE.format(event_id=event_id)
    request_memory = Constants.REQUEST_MEMORY

    if mode == "condor":
        job_script = Constants.JOB.format(
            executable_file=executable_file,
            log_file=log_file,
            output_file=output_file,
            error_file=error_file,
            request_memory=request_memory,
        )
        job_script_file = event_dir / Constants.JOB_SCRIPT_FILE.format(dtag=event.dtag,
                                                                       event_idx=event.event_idx)

        # Generate a shell command to submit the job to run the python script
        command = Constants.COMMAND.format(job_script_file=job_script_file)

    elif mode == "qsub":
        job_script = Constants.JOB_QSUB.format(
            executable_file=executable_file,
            output_file=output_file,
            error_file=error_file,
            request_memory=request_memory,
        )
        job_script_file = event_dir / Constants.JOB_SCRIPT_FILE_QSUB.format(dtag=event.dtag,
                                                                            event_idx=event.event_idx)
        chmod(job_script_file)

        # Generate a shell command to submit the job to run the python script
        command = Constants.COMMAND_QSUB.format(job_script_file=str(job_script_file))

    else:
        raise Exception("Not a valid cluster name!")

    with open(job_script_file, "w") as f:
        f.write(job_script)
    print(f"Command: {command}")

    # Submit the job
    execute(command)


# #####################
# # Autobuild
# #####################

def autobuild(pandda_dir: str, out_dir: str, mode: str = "qsub"):
    # Type all the input variables
    pandda_dir = Path(pandda_dir)
    pandda_analyses_dir = pandda_dir / Constants.PANDDA_ANALYSES_DIR
    pandda_processed_datasets_dir = pandda_dir / Constants.PANDDA_PROCESSED_DATASETS_DIR
    pandda_event_table_file = pandda_analyses_dir / Constants.PANDDA_ANALYSE_EVENTS_FILE
    out_dir = Path(out_dir)

    # Load the csv
    event_table = pd.read_csv(str(pandda_event_table_file))

    # Get the events
    event_list = []
    for idx, event_record_row in event_table.iterrows():
        dtag = event_record_row["dtag"]
        event_idx = event_record_row["event_idx"]
        bdc = event_record_row["1-BDC"]
        x = event_record_row["x"]
        y = event_record_row["y"]
        z = event_record_row["z"]

        event_dir = pandda_processed_datasets_dir / f"{dtag}"
        model_path = event_dir / Constants.PANDDA_PDB_FILE.format(dtag=dtag)
        reflections_path = event_dir / Constants.PANDDA_PDB_FILE.format(dtag=dtag)
        event_map_path = event_dir / Constants.PANDDA_EVENT_MAP_FILE.format(dtag=dtag, event_idx=event_idx, bdc=bdc)
        smiles_path_list = list(event_dir.glob("*.smiles"))
        if len(smiles_path_list) == 0:
            smiles_path = None
        else:
            smiles_path = smiles_path_list[0]

        if not smiles_path:
            print(f"No smiles for dtag: {dtag}")
            continue

        event = Event(
            dtag=dtag,
            event_idx=event_idx,
            model_path=model_path,
            event_map_path=event_map_path,
            reflections_path=reflections_path,
            smiles_path=smiles_path,
            x=z,
            y=y,
            z=z
        )
        event_list.append(event)

    for event in event_list:
        dispatch(event, out_dir, mode)


# #####################
# # main
# #####################

if __name__ == "__main__":
    fire.Fire(autobuild)
