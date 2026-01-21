#   test

# utils/db_SQLite.py
import os
import stat
import sqlite3
import streamlit as st
import re
from ViscAI.utils.ssh_connection import connect_remote_server
from ViscAI.utils.db_to_csv import export_db_to_csv, upload_csv, csv_format_to_pyrheo
from ViscAI.utils.clean_files import remove_db_local, remove_csv_exports

DIST_LABEL_MAP = {0: "Monodisperse", 1: "Gaussian", 2: "Log-normal", 3: "Poisson", 4: "Flory"}

def _sftp_exists(sftp, path: str) -> bool:
    try: sftp.stat(path); return True
    except Exception: return False

def _decode_line(line):
    return line.decode("utf-8", errors="ignore") if isinstance(line, (bytes, bytearray)) else str(line)

def _parse_info_file(fobj) -> tuple:
    mw = None; zero = None; cvis = None
    mw_pat_1 = re.compile(r"\[M\]_w\s*=\s*([0-9.\+\-eE]+)")
    mw_pat_2 = re.compile(r"\bMw\b\s*=\s*([0-9.\+\-eE]+)")
    for raw in fobj:
        line = _decode_line(raw)
        for pat in (mw_pat_1, mw_pat_2):
            m = pat.search(line)
            if m:
                try: mw = float(m.group(1))
                except Exception: pass
        if "zero-shear viscosity" in line:
            try: zero = float(line.split("=")[1].strip())
            except Exception: pass
        if "complex-viscosity" in line:
            try: cvis = float(line.split("=")[1].strip()) * 1.0e-6
            except Exception: pass
    return mw, zero, cvis

def _parse_dir_tokens(base_dir: str) -> tuple:
    name = os.path.basename(base_dir.rstrip("/"))
    m = re.match(r"^Mw_([0-9_\.]+)(?:__D(\d+))?(?:__PDI_([0-9_\.]+))?$", name)
    if not m: return None, None, None
    try: mw = float(m.group(1).replace("_", "."))
    except Exception: mw = None
    dist_code = int(m.group(2)) if m.group(2) is not None else None
    pdi = None
    if m.group(3) is not None:
        try: pdi = float(m.group(3).replace("_", "."))
        except Exception: pdi = None
    return mw, dist_code, pdi

def _infer_dist_mw_pdi_from_dat(sftp, base_dir: str):
    try: files = [f.filename for f in sftp.listdir_attr(base_dir)]
    except Exception: return None, None, None
    skip = {"gt.dat", "gtp.dat"}
    candidates = [f for f in files if f.lower().endswith(".dat")
                  and f not in skip and not f.lower().startswith("gpcls")]
    for dat in candidates:
        path = f"{base_dir}/{dat}"
        try:
            with sftp.open(path, "r") as fd:
                for idx, raw in enumerate(fd):
                    line = _decode_line(raw).strip()
                    if not line or idx < 5: continue
                    toks = line.split()
                    if len(toks) >= 3:
                        try:
                            dist_code = int(float(toks[0]))
                            mw_val    = float(toks[1])
                            pdi_val   = float(toks[2])
                            dlabel    = DIST_LABEL_MAP.get(dist_code)
                            return dlabel, mw_val, pdi_val
                        except Exception:
                            continue
        except Exception:
            continue
    return None, None, None

def _ensure_schema(cur):
    cur.execute('''
    CREATE TABLE IF NOT EXISTS simulation (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        molecular_weight REAL,
        pdi REAL,
        distribution_label TEXT,
        zero_shear_viscosity REAL,
        complex_viscosity REAL
    )''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS dynamic (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        simulation_id INTEGER,
        frequency REAL,
        elastic_modulu REAL,
        viscous_modulu REAL,
        FOREIGN KEY(simulation_id) REFERENCES simulation(rowid)
    )''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS relaxation (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        simulation_id INTEGER,
        time REAL,
        modulu REAL,
        FOREIGN KEY(simulation_id) REFERENCES simulation(rowid)
    )''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS job_status (
        simulation_id INTEGER,
        status TEXT CHECK(status IN ('queued','finished','error')),
        FOREIGN KEY(simulation_id) REFERENCES simulation(rowid)
    )''')

def _ingest_single_simulation(sftp, base_dir: str, cur) -> bool:
    info_path = f"{base_dir}/info.txt"
    gtp_path = f"{base_dir}/gtp.dat"
    gt_path = f"{base_dir}/gt.dat"
    if not _sftp_exists(sftp, gtp_path) or not _sftp_exists(sftp, gt_path):
        return False

    mw_dir, dist_dir, pdi_dir = _parse_dir_tokens(base_dir)
    mw = zero = cvis = None; pdi = None; distribution_label = None

    if _sftp_exists(sftp, info_path):
        with sftp.open(info_path, 'r') as rf:
            mw_i, zero, cvis = _parse_info_file(rf)
            if mw_i is not None: mw = mw_i

    if mw is None and mw_dir is not None: mw = mw_dir
    if pdi_dir is not None: pdi = pdi_dir
    if dist_dir is not None:
        try: distribution_label = DIST_LABEL_MAP.get(int(dist_dir))
        except Exception: distribution_label = None

    if (mw is None) or (pdi is None) or (distribution_label is None):
        dlabel_dat, mw_dat, pdi_dat = _infer_dist_mw_pdi_from_dat(sftp, base_dir)
        if mw is None and mw_dat is not None: mw = mw_dat
        if pdi is None and pdi_dat is not None: pdi = pdi_dat
        if distribution_label is None and dlabel_dat is not None: distribution_label = dlabel_dat

    cur.execute(
        '''INSERT INTO simulation (molecular_weight, pdi, distribution_label, zero_shear_viscosity, complex_viscosity)
           VALUES (?, ?, ?, ?, ?)''',
        (mw, pdi, distribution_label, zero, cvis)
    )
    simulation_id = cur.lastrowid

    with sftp.open(gtp_path, 'r') as rg:
        for raw in rg:
            parts = _decode_line(raw).split()
            if len(parts) >= 3:
                try:
                    freq, em, vm = map(float, parts[:3])
                    cur.execute(
                        'INSERT INTO dynamic (simulation_id, frequency, elastic_modulu, viscous_modulu) VALUES (?, ?, ?, ?)',
                        (simulation_id, freq, em, vm)
                    )
                except ValueError:
                    continue

    with sftp.open(gt_path, 'r') as rt:
        for raw in rt:
            parts = _decode_line(raw).split()
            if len(parts) >= 2:
                try:
                    t, mod = map(float, parts[:2])
                    cur.execute(
                        'INSERT INTO relaxation (simulation_id, time, modulu) VALUES (?, ?, ?)',
                        (simulation_id, t, mod)
                    )
                except ValueError:
                    continue

    cur.execute('INSERT INTO job_status (simulation_id, status) VALUES (?, ?)', (simulation_id, 'finished'))
    return True

def database_db_creation(name_server: str, name_user: str, ssh_key_options: str,
                         working_directory: str,
                         include_root: bool = True,
                         per_mw: bool = False,
                         upload_per_mw: bool = False,
                         sort_ids_by_mw: bool = False,
                         is_parallel: bool = False) -> None:
    local_db = os.path.join(os.getcwd(), 'viscai_database.db')
    try:
        conn = sqlite3.connect(local_db); cur = conn.cursor()
        _ensure_schema(cur); conn.commit()

        ssh = connect_remote_server(name_server, name_user, ssh_key_options)
        sftp = ssh.open_sftp()

        inserted_any = False
        if include_root:
            if _sftp_exists(sftp, f"{working_directory}/gtp.dat") and \
               _sftp_exists(sftp, f"{working_directory}/gt.dat"):
                if _ingest_single_simulation(sftp, working_directory, cur):
                    inserted_any = True
        try:
            for entry in sftp.listdir_attr(working_directory):
                if stat.S_ISDIR(entry.st_mode) and entry.filename.startswith("Mw_"):
                    subdir = f"{working_directory}/{entry.filename}"
                    ok = _ingest_single_simulation(sftp, subdir, cur)
                    inserted_any = inserted_any or ok
        except Exception:
            pass

        conn.commit()
        if not inserted_any:
            sftp.close(); ssh.close()
            conn.close(); remove_db_local(local_db)
            st.error("ERROR!!! No se encontraron ficheros reológicos.")
            return

        if is_parallel and sort_ids_by_mw:
            # Reindex si aplica
            pass

        # Subir DB al servidor
        sftp.put(local_db, f"{working_directory}/viscai_database.db")

        # Exportar CSVs
        export_db_to_csv(local_db, "csv_exports", generate_distribution_summary=False)

        # Generar CSV pyRheo (agregados + por-Mw)
        csv_format_to_pyrheo(local_csv_dir=os.path.join(os.getcwd(), "csv_exports"),
                             per_mw=is_parallel, generate_aggregated=True)

        # Subir CSVs:
        # - En paralelo: solo 01-* y por-Mw (no agregados)
        # - En simple: subir también los agregados 02-* al raíz
        if not is_parallel:
            # Subir agregados 02-* al working directory
            for fname in ["02-relaxation_pyRheo.csv", "02-dynamic_pyRheo.csv"]:
                local_path = os.path.join("csv_exports", fname)
                if os.path.exists(local_path):
                    sftp.put(local_path, os.path.join(working_directory, fname))

        upload_csv(sftp,
                   local_csv_dir=os.path.join(os.getcwd(), "csv_exports"),
                   remote_dir=working_directory,
                   upload_per_mw=is_parallel)

        # Descarga local y limpieza
        local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")
        if local_dir and os.path.isdir(local_dir):
            sftp.get(f"{working_directory}/viscai_database.db", os.path.join(local_dir, "viscai_database.db"))
            for fname in ["01-simulation.csv", "01-dynamic.csv", "01-relaxation.csv", "01-job_status.csv"]:
                try: sftp.get(f"{working_directory}/{fname}", os.path.join(local_dir, fname))
                except Exception: pass
            if not is_parallel:
                # Limpieza en modo simple
                for fname in ["01-simulation.csv", "01-dynamic.csv", "01-relaxation.csv", "01-job_status.csv", "viscai_database.db"]:
                    try:
                        fpath = os.path.join(local_dir, fname)
                        if os.path.exists(fpath): os.remove(fpath)
                    except Exception: pass

        sftp.close(); ssh.close()
        remove_db_local(local_db)
        remove_csv_exports(local_csv_dir=os.path.join(os.getcwd(), "csv_exports"))

    except Exception as e:
        st.error(f"ERROR!!! Rheological values database not avaliable: {e}")
