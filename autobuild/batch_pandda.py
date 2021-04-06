import os
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


def dispatch(event: Event, out_dir: Path):
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

    executable_script = Constants.EXECUTABLE.format(model=model,
                                                    xmap=xmap,
                                                    mtz=mtz,
                                                    smiles=smiles,
                                                    x=event.x,
                                                    y=event.y,
                                                    z=event.z,
                                                    out_dir=str(event_dir)
                                                    )
    executable_script_file = event_dir / Constants.EXECUTABLE_SCRIPT_FILE.format(dtag=event.dtag,
                                                                                 event_idx=event.event_idx)
    with open(executable_script_file, "w") as f:
        f.write(executable_script)

    chmod(executable_script_file)

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

    # Submit the job
    execute(command)


def get_event_list(pandda_event_table, pandda_dir, data_dir):
    event_list = []
    for index, row in pandda_event_table.iterrows():
        dtag = row["dtag"]
        event_idx = row["event_idx"]
        x = row["x"]
        y = row["y"]
        z = row["z"]
        model_path = pandda_dir / Constants.PANDDA_PROCESSED_DATASETS_DIR / dtag / Constants.PANDDA_PDB_FILE.format(
            dtag=dtag)
        reflections_path = pandda_dir / Constants.PANDDA_PROCESSED_DATASETS_DIR / dtag / Constants.PANDDA_PDB_FILE.format(
            dtag=dtag)

        dataset_data_dir = data_dir / dtag
        try:
            smiles_path = dataset_data_dir.glob("*.smiles")
        except:
            smiles_path = None

        bdc = row["1-BDC"]
        event_map_path = pandda_dir / Constants.PANDDA_PROCESSED_DATASETS_DIR / dtag / Constants.PANDDA_EVENT_MAP_FILE.format(
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

    return event_list


def main(pandda_dir: str, data_dir: str, output_dir: str):
    # Format arguments
    pandda_dir_path = Path(pandda_dir)
    data_dir_path = Path(data_dir)
    output_dir_path = Path(output_dir)

    print(f"Database file path: {pandda_dir_path}")
    print(f"Database file path: {output_dir_path}")

    # Load database
    pandda_event_table = pd.read_csv(str(pandda_dir_path / "analyses" / "pandda_analyse_events.csv"))

    # Select which datasets to build
    event_list = get_event_list(pandda_event_table, pandda_dir_path, data_dir_path)
    print(f"Got {len(event_list)} events")

    for event in event_list:
        dispatch(event, output_dir_path)


if __name__ == "__main__":
    fire.Fire(main)
