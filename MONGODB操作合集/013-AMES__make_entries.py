from monty.serialization import loadfn
from monty.serialization import dumpfn
from pymatgen import Structure

from pymatgen.entries.computed_entries import ComputedStructureEntry, ComputedEntry
from pymatgen.ext.matproj import MPRester
from pymatgen.symmetry.analyzer import SpacegroupAnalyzer
from pymongo import MongoClient
import datetime
import glob

# leading keywords
keys_edft = ["decomposition", "above_hull"]



def insert(data):
    ret = db.TEST.insert_one(data)
    return ret


def prep():
    # "cses" will store all the entries created.
    cses = []

    # counters
    ct = 0

    # yaml files
    files = sorted(glob.glob("3d/*.yaml"))

    # loop over yaml files
    for ifile in files:
        ct += 1
        mmid = ifile.strip(".yaml")
        mm = loadfn(mmid + '.yaml')
        st = Structure.from_file(mmid + '.cif')
        print(ct, mmid, st.composition, st.composition.get_reduced_formula_and_factor())

        data = {}

        # Space Group
        sg = SpacegroupAnalyzer(st, 0.1)
        data["spacegroup"] = {"source": "spglib",
                              "symbol": sg.get_space_group_symbol(),
                              "number": sg.get_space_group_number(),
                              "point_group": sg.get_point_group_symbol(),
                              "crystal_system": sg.get_crystal_system(),
                              "hall": sg.get_hall()}

        # Number of elements
        data["system"] = {"nelem": st.ntypesp,
                          "reduced_formula": st.composition.reduced_formula}

        # parameters
        parameters = {"DFT": {"setup": {key: value
                                        for key, value in
                                        zip(["package", "functional", "pot_type"], mm["energy_dft"]["setup"])},
                              "cutoff": {key: value
                                         for key, value in zip(["ecutwfc", "unit"], mm["energy_dft"]["cutoff"])},
                              },
                      "GA": None,
                      "LMTO": None,
                      "local": mmid}

        # Keys
        keys = ["value", "unit", "per"]

        # Total energy
        try:
            data["total_energy"] = {key: value
                                    for key, value in zip(keys, mm["energy_dft"]["total_energy"])}
        except:
            data["total_energy"] = None

        # Formation energy
        try:
            data["formation_energy"] = {kkey: {key: value
                                               for key, value in zip(keys, mm["formation_energy"][kkey])}
                                        for kkey in keys_edft}
        except:
            data["formation_energy"] = {kkey: None
                                        for kkey in keys_edft}
        # Magnetic moment
        try:
            data["magnetic_moment"] = {"magnetic_ordering": mm["magnetic_moment"]["magnetic_ordering"],
                                       "magnetic_polarization": {key: value
                                                                 for key, value in zip(["value", "unit"],
                                                                                       mm["magnetic_moment"][
                                                                                           "magnetic_polarization"])},
                                       "total_magnetic_moment": {key: value
                                                                 for key, value in zip(["value", "unit", "per"],
                                                                                       mm["magnetic_moment"][
                                                                                           "total_magnetic_moment"])}
                                       }
        except:
            data["magnetic_moment"] = {key: None
                                       for key in
                                       ["magnetic_ordering", "magnetic_polarization", "total_magnetic_moment",
                                        "local_magnetic_moments"]}

        # Magnetic anisotropy
        keys = ["a-c", "b-c", "b-a", "d-a", "unit"]
        try:
            data["magnetic_anisotropy"] = {"energy": {key: value
                                                      for key, value in zip(keys, mm["magnetic_anisotropy"]["energy"])},
                                           "constant": {key: value
                                                        for key, value in
                                                        zip(keys, mm["magnetic_anisotropy"]["constant"])},
                                           "easy_axis": mm["magnetic_anisotropy"]["easy_axis"],
                                           "parameters": {"kappa": mm["magnetic_anisotropy"]["kappa"],
                                                          "kgrid": {key: value
                                                                    for key, value in zip(["kx", "ky", "kz"],
                                                                                          mm["magnetic_anisotropy"][
                                                                                              "kgrid"])}}
                                           }
        except:
            data["magnetic_anisotropy"] = {key: None
                                           for key in ["energy", "constant", "easy_axis", "parameters"]}

        # Critical temperature
        try:
            values = mm["critical_temperature"]["Curie_T"]
            keys = ["value", "unit"]
            data["critical_temperature"] = {"Curie_temperature": {key: value
                                                                  for key, value in zip(keys, values)},
                                            "Neel_temperature": None}
        except:
            data["critical_temperature"] = {"Curie_temperature": None,
                                            "Neel_temperature": None}

        # Notes and References
        try:
            data["notes"] = {"entry": {key: value
                                       for key, value in zip(["by", "method"], mm["notes"]["entry"])},
                             "mpid": mm["notes"]['mpid'],
                             "references": {key: value
                                            for key, value in zip(["paper", "doi", "url"], mm["notes"]["references"])},
                             "synthesis": {key: value
                                           for key, value in zip(["status", "url"], mm["notes"]["synthesis"])},
                             "updated": datetime.datetime.fromtimestamp(pathlib.Path(mmid + '.yaml').stat().st_mtime)
                             }
        except:
            data["notes"] = {key: None
                             for key in ["entry", "mpid", "references", "synthesis", "updated"]}

        # Site-specific data (read from '.dat')
        #   lmm: local magnetic moment
        #   soc: spin-orbit coupling strength
        #   ecc: exchange coupling constant J
        keys = ["lmm", "soc", "ecc"]
        with open(mmid + '.dat') as ssdata:
            i_site = 0
            for s in ssdata.readlines():
                ss = s.split()
                pos = [float(ss[i]) for i in range(1, 4)]
                values = [None, None, None]
                for i in [0, 1, 2]:
                    if ss[i + 4] != 'null':
                        values[i] = float(ss[i + 4])
                st.replace(i_site, ss[0], pos, properties={key: value
                                                           for key, value in zip(keys, values)})
                i_site += 1

        # LMTO
        if data["critical_temperature"]["Curie_temperature"]["value"]:
            try:
                # setup
                parameters['LMTO'] = mm['LMTO']['setup']
                # Pair-wise magnetic data (read from '.yaml')
                data['pairwise'] = {'J_ij': mm['LMTO']['J_ij']}
            except:
                parameters['LMTO'] = None
                data['pairwise'] = {'J_ij': None}
        else:
            parameters['LMTO'] = None
            data['pairwise'] = {'J_ij': None}


        # create a data entry
        cse = ComputedStructureEntry(structure=st,
                                     energy=mm["energy_dft"]["total_energy"][0],
                                     data=data,
                                     entry_id="MMD-" + str(ct),
                                     parameters=parameters)

        cses.append(cse.as_dict())

    # END loop
    return cses


if __name__ == "__main__":
    # setup the client
    client = "aaa"

    # use "test" database
    db = client.test

    # make entries
    ret = prep()

    # local copy
    print('... dumping to a.yaml')
    dumpfn(ret, "a.yaml")

    # insert new data
    ct = 0
    for data in ret:
        ct += 1
        entry = insert(data)
        print(ct, entry.inserted_id)
