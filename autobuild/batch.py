import os
import subprocess
from pathlib import Path

import fire

from constants import Constants
from xlib import database_sql


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


def dispatch(event: database_sql.Event, out_dir: Path):
    event_id = f"{event.dataset.dtag}_{event.event_idx}"

    print(f"Event id: {event_id}")

    # Create the Event dir
    event_dir = out_dir / event_id
    try_make_dir(event_dir)

    # Write the script that will call python to autobuild the event
    model = event.dataset.model.path
    xmap = event.event_map
    mtz = event.dataset.reflections.path
    smiles = event.dataset.smiles.path

    executable_script = Constants.EXECUTABLE.format(model=model,
                                                    xmap=xmap,
                                                    mtz=mtz,
                                                    smiles=smiles,
                                                    x=event.x,
                                                    y=event.y,
                                                    z=event.z,
                                                    out_dir=str(event_dir)
                                                    )
    executable_script_file = event_dir / Constants.EXECUTABLE_SCRIPT_FILE.format(dtag=event.dataset.dtag,
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
    job_script_file = event_dir / Constants.JOB_SCRIPT_FILE.format(dtag=event.dataset.dtag,
                                                                   event_idx=event.event_idx)
    with open(job_script_file, "w") as f:
        f.write(job_script)

    # Generate a shell command to submit the job to run the python script
    command = Constants.COMMAND.format(job_script_file=job_script_file)
    print(f"Command: {command}")

    # Submit the job
    execute(command)


def main(database_file: str, output_dir: str):
    # Format arguments
    database_file_path = Path(database_file)
    output_dir_path = Path(output_dir)

    print(f"Database file path: {database_file_path}")
    print(f"Database file path: {output_dir_path}")

    # Load database
    database: database_sql.Database = database_sql.Database(database_file_path)

    # Select which datasets to build
    event_query = database.session.query(database_sql.Event)
    reference_query = database.session.query(database_sql.ReferenceModel)
    # built_events = event_query.filter(database_sql.Event == )
    event_list = event_query.all()
    print(f"Got {len(event_list)} events")

    # Dispatch to run
    # map(
    #     lambda event: dispatch(event, output_dir_path),
    #     event_list,
    # )
    # for reference in reference_query.all():
    for event in event_list:

        reference = reference_query.filter(database_sql.ReferenceModel.dataset_id == event.dataset.id).all()
        if len(reference) == 0:
            print("No references: continuing")
            continue

        dispatch(event, output_dir_path)


if __name__ == "__main__":
    fire.Fire(main)
