# pandda_autobuild

## Installation:
 - git clone https://github.com/ConorFWild/pandda_autobuild.git
 - cd pandda_autobuild
 - pip install .

## Running a single dataset:
 - Make sure rhofit is on your path
 - python pandda_autobuild/autobuild/autobuild.py model xmap mtz smiles x y z out_dir

## Running an entire PanDDA:
 - Make sure rhofit is on your path
 - python pandda_autobuild/autobuild/autobuild_pandda.py pandda_event_table out_dir
