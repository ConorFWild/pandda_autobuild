class Constants:
    EXECUTABLE_DEP = (
        "#!/bin/bash \n"
        ". /data/share-2/conor/anaconda3/etc/profile.d/conda.sh\n" 
        "conda activate lab\n"
        "source ~/.bashrc \n"
        "source /data/share-2/conor/xtal_software/ccp4-7.1/bin/ccp4.setup-sh \n"
        "source /data/share-2/conor/xtal_software/phenix/phenix-1.18.2-3874/phenix_env.sh \n"
        "source /data/share-2/conor/xtal_software/buster-2.10/setup.sh \n"
        "python /data/share-2/conor/pandda/pandda_scripts/autobuild/autobuild.py {model} {xmap} {mtz} {smiles} {x} {y} {z} {out_dir}"
    )

    EXECUTABLE = (
        "#!/bin/bash\n"
        "source {phenix_setup}\n"
        "source {rhofit_setup}\n"
        "{python} {autobuild_script_path} {model} {xmap} {mtz} {smiles} {x} {y} {z} {out_dir}"
    )

    EXECUTABLE_SCRIPT_FILE = "{dtag}_{event_idx}.sh"

    LOG_FILE = "{event_id}.log"
    OUTPUT_FILE = "{event_id}.out"
    ERROR_FILE = "{event_id}.err"
    REQUEST_MEMORY = "20"
    JOB = (
        "#################### \n"
        "# \n"
        "# Example 1                                   \n"
        "# Simple HTCondor submit description file \n"
        "#                          \n"
        "####################    \n"

        "Executable   = {executable_file} \n"
        "Log          = {log_file} \n"
        "Output = {output_file} \n"
        "Error = {error_file} \n"

        "request_memory = {request_memory} GB \n"

        "Queue"
    )

    JOB_SCRIPT_FILE = "{dtag}_{event_idx}.job"

    COMMAND = "condor_submit {job_script_file}"

    MASKED_PDB_FILE = "masked.pdb"

    TRUNCATED_EVENT_MAP_FILE = "truncated.ccp4"

    CUT_EVENT_MAP_FILE = "cut.ccp4"

    LIGAND_PREFIX = "ligand"
    LIGAND_CIF_FILE = "ligand.cif"
    ELBOW_COMMAND = "cd {out_dir}; phenix.elbow {smiles_file} --output=\"{prefix}\"; cd -"

    PANDDA_RHOFIT_SCRIPT_FILE = "/data/share-2/conor/pandda/pandda_scripts/pandda_rhofit.sh"
    RHOFIT_COMMAND = (
        "#!/bin/bash \n"
        "source ~/.bashrc \n"
        ". /data/share-2/conor/anaconda3/etc/profile.d/conda.sh\n" 
        "conda activate env_rdkit\n"  
        "source /data/share-2/conor/xtal_software/ccp4-7.1/bin/ccp4.setup-sh \n"
        "source /data/share-2/conor/xtal_software/phenix/phenix-1.18.2-3874/phenix_env.sh \n"
        "source /data/share-2/conor/xtal_software/buster-2.10/setup.sh \n"
        "{pandda_rhofit} -map {event_map} -mtz {mtz} -pdb {pdb} -cif {cif} -out {out_dir}"
    )

    JOB_QSUB = "qsub -P labxchem -q medium.q -o {output_file} -e {error_file} -l h_vmem={request_memory}G,m_mem_free={request_memory}G {executable_file}"
    JOB_SCRIPT_FILE_QSUB = "qsub_{dtag}_{event_idx}.sh"
    COMMAND_QSUB = "source {job_script_file}"

    PANDDA_EVENT_MAP_FILE = "{dtag}-event_{event_idx}_1-BDC_{bdc}_map.native.ccp4"
    PANDDA_PDB_FILE = "{dtag}-pandda-input.pdb"
    PANDDA_MTZ_FILE = "{dtag}-pandda-input.mtz"
    PANDDA_PROCESSED_DATASETS_DIR = "processed_datasets"
    PANDDA_ANALYSES_DIR = "analyses"
    PANDDA_ANALYSE_EVENTS_FILE = "pandda_analyse_events.csv"

    AUTOBUILD_SCRIPT = "autobuild.py"