from __future__ import annotations

import os
import dataclasses
import subprocess
from pathlib import Path
import json

import fire
import numpy as np
import gemmi

from constants import Constants


def execute(command: str):
    p = subprocess.Popen(command,
                         shell=True,
                         )

    p.communicate()


@dataclasses.dataclass()
class Coord:
    x: float
    y: float
    z: float


# #####################
# # Truncate Model
# #####################

def get_pdb(pdb_file: Path) -> gemmi.Structure:
    structure: gemmi.Structure = gemmi.read_structure(str(pdb_file))
    return structure


def save_pdb_file(masked_pdb: gemmi.Structure, path: Path) -> Path:
    masked_pdb.write_minimal_pdb(str(path))

    return path


def get_masked_pdb(pdb: gemmi.Structure, coord: Coord, radius: float = 8.0) -> gemmi.Structure:
    event_centoid = gemmi.Position(
        coord.x,
        coord.y,
        coord.z,
    )

    new_structure = gemmi.Structure()

    for model_i, model in enumerate(pdb):
        new_model = gemmi.Model(model.name)
        new_structure.add_model(new_model, pos=-1)

        for chain_i, chain in enumerate(model):
            new_chain = gemmi.Chain(chain.name)
            new_structure[model_i].add_chain(new_chain, pos=-1)

            for residue_i, residue in enumerate(chain):
                new_residue = gemmi.Residue()
                new_residue.name = residue.name
                new_residue.seqid = residue.seqid
                new_residue.subchain = residue.subchain
                new_residue.label_seq = residue.label_seq
                new_residue.het_flag = residue.het_flag
                new_structure[model_i][chain_i].add_residue(new_residue, pos=-1)

                for atom_i, atom in enumerate(residue):
                    pos = atom.pos
                    if pos.dist(event_centoid) > radius:
                        new_structure[model_i][chain_i][residue_i].add_atom(atom, pos=-1)

    for model_i, model in enumerate(pdb):
        pdb.add_model(new_structure[model_i], pos=-1)
        del pdb[0]

    return pdb


def truncate_model(model_path: Path, coords: Coord, out_dir: Path):
    # Get pdb
    pdb: gemmi.Structure = get_pdb(model_path)

    # Truncate
    masked_pdb = get_masked_pdb(pdb, coords)

    # Write
    masked_pdb_file = save_pdb_file(masked_pdb,
                                    out_dir / Constants.MASKED_PDB_FILE,
                                    )

    # Return
    return masked_pdb_file


# #####################
# # Truncate event map
# #####################


def get_cut_out_event_map(event_map: gemmi.FloatGrid, coord: Coord, radius: float = 10.0) -> gemmi.FloatGrid:
    event_centroid = gemmi.Position(coord.x, coord.y, coord.z)

    xmap_array = np.array(event_map, copy=True)

    mask_grid = gemmi.Int8Grid(*xmap_array.shape)
    # print(f"Spacegroup: {mask_grid.spacegroup.xhm()}")
    # mask_grid.spacegroup = gemmi.find_spacegroup_by_name("P 21 21 21")  #  gemmi.find_spacegroup_by_name("P 1")#event_map.spacegroup
    mask_grid.spacegroup = gemmi.find_spacegroup_by_name("P 1")#event_map.spacegroup
    print(f"Spacegroup: {mask_grid.spacegroup.xhm()}")
    print(f"grid: {mask_grid}")
    mask_grid_array = np.array(mask_grid)
    print(f"Mask grid array: {mask_grid_array.shape}")
    print(f"Mask grid array: {mask_grid_array.size}")
    print(f"Mask grid array: {np.sum(np.isfinite(mask_grid_array))}")

    # print(f"Grid size: {mask_grid.size}")
    mask_grid.set_unit_cell(event_map.unit_cell)

    mask_grid.set_points_around(event_centroid,
                                radius=radius,
                                value=1,
                                )
    mask_grid.symmetrize_max()

    mask_array = np.array(mask_grid, copy=False, dtype=np.int8)

    new_grid = gemmi.FloatGrid(*xmap_array.shape)
    new_grid.spacegroup = event_map.spacegroup  # gemmi.find_spacegroup_by_name("P 1")
    new_grid.set_unit_cell(event_map.unit_cell)

    new_grid_array = np.array(new_grid, copy=False)

    new_grid_array[np.nonzero(mask_array)] = xmap_array[np.nonzero(mask_array)]
    new_grid.symmetrize_max()

    return new_grid


def get_event_map(event_map_file: Path) -> gemmi.FloatGrid:
    m = gemmi.read_ccp4_map(str(event_map_file))
    print(m.grid.spacegroup.xhm())

    # m.grid.spacegroup = gemmi.find_spacegroup_by_name("P 21 21 21")
    m.grid.spacegroup = gemmi.find_spacegroup_by_name("P 1")
    print(m.grid.spacegroup.xhm())

    m.setup()

    grid_array = np.array(m.grid, copy=True)

    print(m.grid.spacegroup.xhm())
    print(m.grid)
    print(grid_array.shape)

    new_grid = gemmi.FloatGrid(*grid_array.shape)
    new_grid.spacegroup = m.grid.spacegroup  # gemmi.find_spacegroup_by_name("P 1")
    new_grid.set_unit_cell(m.grid.unit_cell)

    new_grid_array = np.array(new_grid, copy=False)
    new_grid_array[:, :, :] = grid_array[:, :, :]

    return new_grid


def save_xmap_dep(cut_out_event_map,
                  path,
                  ):
    ccp4 = gemmi.Ccp4Map()
    ccp4.grid = cut_out_event_map

    ccp4.grid.spacegroup = gemmi.find_spacegroup_by_name("P 1")

    ccp4.update_ccp4_header(2, True)
    ccp4.write_ccp4_map(str(path))

    return path


def save_xmap(event_map,
              path,
              ):
    ccp4 = gemmi.Ccp4Map()
    ccp4.grid = event_map

    ccp4.grid.spacegroup = gemmi.find_spacegroup_by_name("P 1")

    ccp4.update_ccp4_header(2, True)
    ccp4.write_ccp4_map(str(path))

    return path


def truncate_xmap(xmap_path: Path, coords: Coord, out_dir: Path):
    event_map: gemmi.FloatGrid = get_event_map(xmap_path)

    # Cut out events:
    cut_out_event_map: gemmi.FloatGrid = get_cut_out_event_map(event_map, coords)

    # Save cut out event
    cut_out_event_map_file: Path = save_xmap(cut_out_event_map,
                                             out_dir / Constants.TRUNCATED_EVENT_MAP_FILE,
                                             )

    return cut_out_event_map_file


# #####################
# # Cut out map
# #####################
def get_ccp4_map(xmap_path):
    m = gemmi.read_ccp4_map(str(xmap_path))
    m.grid.spacegroup = gemmi.find_spacegroup_by_name("P 1")

    m.setup()

    return m


def get_bounding_box(event_map: gemmi.FloatGrid, coord: Coord, radius: float = 8.0,
                     margin: float = 5.0) -> gemmi.FloatGrid:
    event_centroid = gemmi.Position(coord.x, coord.y, coord.z)

    box_lower_bound = gemmi.Position(float(coord.x) - radius, float(coord.y) - radius, float(coord.z) - radius)
    box_upper_bound = gemmi.Position(float(coord.x) + radius, float(coord.y) + radius, float(coord.z) + radius)

    box_lower_bound_fractional = event_map.grid.unit_cell.fractionalize(box_lower_bound)
    box_upper_bound_fractional = event_map.grid.unit_cell.fractionalize(box_upper_bound)

    box = gemmi.FractionalBox()

    box.extend(box_lower_bound_fractional)
    box.extend(box_upper_bound_fractional)

    return box


def save_cut_xmap(event_ccp4,
                  bounding_box,
                  path,
                  ):
    event_ccp4.set_extent(bounding_box)
    event_ccp4.write_ccp4_map(str(path))

    return path


def cut_out_xmap(xmap_path: Path, coords: Coord, out_dir: Path):
    ccp4_map = get_ccp4_map(xmap_path)

    # Cut out events:
    bounding_box = get_bounding_box(ccp4_map, coords)

    # Save cut out event
    cut_out_event_map_file: Path = save_cut_xmap(ccp4_map,
                                                 bounding_box,
                                                 out_dir / Constants.CUT_EVENT_MAP_FILE,
                                                 )

    return cut_out_event_map_file


# #####################
# # Generate cif
# #####################

def get_elbow_command(smiles_file: Path, out_dir: Path) -> str:
    command = Constants.ELBOW_COMMAND.format(out_dir=str(out_dir),
                                             smiles_file=str(smiles_file),
                                             prefix=Constants.LIGAND_PREFIX, )
    return command


def generate_cif(smiles_path: Path, out_dir: Path):
    # Get the command to run elbow
    elbow_command = get_elbow_command(smiles_path, out_dir)

    # Run the command
    execute(elbow_command)

    return out_dir / Constants.LIGAND_CIF_FILE


# #####################
# # rhofit
# #####################


def rhofit(truncated_model_path: Path, truncated_xmap_path: Path, mtz_path: Path, cif_path: Path, out_dir: Path):
    # Make rhofit commands
    rhofit_command: str = Constants.RHOFIT_COMMAND.format(
        pandda_rhofit=Constants.PANDDA_RHOFIT_SCRIPT_FILE,
        event_map=str(truncated_xmap_path),
        mtz=str(mtz_path),
        pdb=str(truncated_model_path),
        cif=str(cif_path),
        out_dir=str(out_dir),
    )
    print(f"rhofit_command is: {rhofit_command}")

    # Execute job script
    execute(rhofit_command)


# #####################
# Rescore
# #####################

def score_structure(structure, xmap):
    unit_cell = xmap.unit_cell

    mask = gemmi.FloatGrid(
        xmap.nu, xmap.nv, xmap.nw
    )

    mask.set_unit_cell(unit_cell)
    mask.spacegroup = gemmi.find_spacegroup_by_name("P 1")

    for model in structure:
        for chain in model:
            for residue in chain:
                for atom_1 in residue:
                    if atom_1.element.name == "H":
                        continue
                    pos_1 = atom_1.pos

                    for atom_2 in residue:
                        if atom_2.element.name == "H":
                            continue
                        pos_2 = atom_2.pos
                        if pos_1.dist(pos_2) < 2.0:
                            new_pos = gemmi.Position(
                                (pos_1.x + pos_2.x) / 2,
                                (pos_1.y + pos_2.y) / 2,
                                (pos_1.z + pos_2.z) / 2,

                            )
                            mask.set_points_around(new_pos, 0.75, 1.0)

    mask_array = np.array(mask)

    xmap_array = np.array(xmap)

    truncated_xmap_mask = xmap_array > 1.25

    score = np.sum(truncated_xmap_mask * mask_array)

    return float(score)


def score_structure_path(path: Path, xmap):
    structure = gemmi.read_structure(str(path))
    score = score_structure(structure, xmap)

    return score


def score_builds(rhofit_dir: Path, xmap_path):
    scores = {}

    regex = "Hit_*.pdb"

    xmap = get_ccp4_map(xmap_path).grid

    for model_path in rhofit_dir.glob(regex):
        score = score_structure_path(model_path, xmap)
        scores[str(model_path)] = score

    return scores


def save_score_dictionary(score_dictionary, path):
    with open(str(path), "w") as f:
        json.dump(score_dictionary, f)


# #####################
# # Autobuild
# #####################

def autobuild(model: str, xmap: str, mtz: str, smiles: str, x: float, y: float, z: float, out_dir: str,):
    # Type all the input variables
    model_path = Path(model)
    xmap_path = Path(xmap)
    mtz_path = Path(mtz)
    smiles_path = Path(smiles)
    out_dir = Path(out_dir)
    coords = Coord(x, y, z)

    # Truncate the model
    truncated_model_path = truncate_model(model_path, coords, out_dir)
    print(f"\tTruncated model")

    # Truncate the ed map
    truncated_xmap_path = truncate_xmap(xmap_path, coords, out_dir)
    print(f"\tTruncated xmap")

    # Make cut out map
    cut_out_xmap(xmap_path, coords, out_dir)
    print(f"\tCut out xmap")

    # Generate the cif
    cif_path = generate_cif(smiles_path, out_dir)
    print(f"\tGenerated cif")

    # Call rhofit
    rhofit(truncated_model_path, truncated_xmap_path, mtz_path, cif_path, out_dir)
    print(f"\tRhofit")

    # Score rhofit builds
    score_dictionary = score_builds(out_dir / "rhofit", xmap_path)
    print(f"\tRescored")
    for path in sorted(score_dictionary, key=lambda _path: score_dictionary[_path]):
        print(f"\t\t{score_dictionary[path]}: {path}")

    # Write scores
    save_score_dictionary(score_dictionary, out_dir / "scores.json")
    print(f"\tSaved scores")

    # Remove the big map
    os.remove(str(truncated_xmap_path))



# #####################
# # main
# #####################

if __name__ == "__main__":
    fire.Fire(autobuild)
