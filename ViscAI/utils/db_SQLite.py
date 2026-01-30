#   test

# utils/db_SQLite.py
import os
import stat
import re as _re
import sqlite3
from concurrent.futures import ProcessPoolExecutor
import streamlit as st
import re
from pathlib import Path
from ViscAI.utils.ssh_connection import connect_remote_server
from ViscAI.utils.db_to_csv import export_db_to_csv, upload_csv, csv_format_to_pyrheo
from ViscAI.utils.clean_files import remove_db_local, remove_csv_exports


DIST_LABEL_MAP = {0: "Monodisperse", 1: "Gaussian", 2: "Log-normal", 3: "Poisson", 4: "Flory"}

def _sftp_exists(sftp, path: str) -> bool:
    try: sftp.stat(path); return True
    except Exception: return False

def _decode_line(line):
    return line.decode("utf-8", errors="ignore") if isinstance(line, (bytes, bytearray)) else str(line)


# ****************************NUEVO CAMBIO **********
# Añadimos helpers para ingestión en modo local (sin sftp)
def _infer_dist_mw_pdi_from_dat_local(base_dir: str):
    """Versión local de _infer_dist_mw_pdi_from_dat usando ficheros locales."""
    try:
        files = os.listdir(base_dir)
    except Exception:
        return None, None, None
    skip = {"gt.dat", "gtp.dat"}
    candidates = [f for f in files if f.lower().endswith(".dat")
                  and f not in skip and not f.lower().startswith("gpcls")]
    for dat in candidates:
        path = os.path.join(base_dir, dat)
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as fd:
                for idx, raw in enumerate(fd):
                    line = raw.strip()
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

def _ingest_single_simulation_local(base_dir: str, cur) -> bool:
    """Ingesta de una simulación usando ficheros locales en lugar de SFTP."""
    info_path = os.path.join(base_dir, "info.txt")
    gtp_path = os.path.join(base_dir, "gtp.dat")
    gt_path = os.path.join(base_dir, "gt.dat")
    if not (os.path.exists(gtp_path) and os.path.exists(gt_path)):
        return False

    mw_dir, dist_dir, pdi_dir = _parse_dir_tokens(base_dir)
    mw = zero = cvis = None; pdi = None; distribution_label = None

    if os.path.exists(info_path):
        try:
            with open(info_path, 'r', encoding='utf-8', errors='ignore') as rf:
                mw_i, zero, cvis = _parse_info_file(rf)
                if mw_i is not None: mw = mw_i
        except Exception:
            pass

    if mw is None and mw_dir is not None: mw = mw_dir
    if pdi_dir is not None: pdi = pdi_dir
    if dist_dir is not None:
        try: distribution_label = DIST_LABEL_MAP.get(int(dist_dir))
        except Exception: distribution_label = None

    if (mw is None) or (pdi is None) or (distribution_label is None):
        dlabel_dat, mw_dat, pdi_dat = _infer_dist_mw_pdi_from_dat_local(base_dir)
        if mw is None and mw_dat is not None: mw = mw_dat
        if pdi is None and pdi_dat is not None: pdi = pdi_dat
        if distribution_label is None and dlabel_dat is not None: distribution_label = dlabel_dat

    cur.execute(
        '''INSERT INTO simulation (molecular_weight, pdi, distribution_label, zero_shear_viscosity, complex_viscosity)
           VALUES (?, ?, ?, ?, ?)''',
        (mw, pdi, distribution_label, zero, cvis)
    )
    simulation_id = cur.lastrowid

    # leer gtp.dat
    try:
        with open(gtp_path, 'r', encoding='utf-8', errors='ignore') as rg:
            for raw in rg:
                parts = raw.split()
                if len(parts) >= 3:
                    try:
                        freq, em, vm = map(float, parts[:3])
                        cur.execute(
                            'INSERT INTO dynamic (simulation_id, frequency, elastic_modulu, viscous_modulu) VALUES (?, ?, ?, ?)',
                            (simulation_id, freq, em, vm)
                        )
                    except ValueError:
                        continue
    except Exception:
        pass

    # leer gt.dat
    try:
        with open(gt_path, 'r', encoding='utf-8', errors='ignore') as rt:
            for raw in rt:
                parts = raw.split()
                if len(parts) >= 2:
                    try:
                        t, mod = map(float, parts[:2])
                        cur.execute(
                            'INSERT INTO relaxation (simulation_id, time, modulu) VALUES (?, ?, ?)',
                            (simulation_id, t, mod)
                        )
                    except ValueError:
                        continue
    except Exception:
        pass

    cur.execute('INSERT INTO job_status (simulation_id, status) VALUES (?, ?)', (simulation_id, 'finished'))
    return True
# ****************************NUEVO CAMBIO **********


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


#   TEST
# ****************************NUEVO CAMBIO **********
# ****************************NUEVO CAMBIO **********
def database_db_creation(name_server: str, name_user: str, ssh_key_options: str,
                         working_directory: str,
                         include_root: bool = True,
                         per_mw: bool = False,
                         upload_per_mw: bool = False,
                         sort_ids_by_mw: bool = False,
                         is_parallel: bool = False) -> None:
    """
    Ahora soporta modo local si `name_server` es falsy (None o "").
    - Si name_server es falsy: se asume que `working_directory` es un path LOCAL con subdirs Mw_*.
    - Si name_server tiene valor: comportamiento REMOTO (como antes).
    **************** NUEVO CAMBIO **********
    """
    local_db = os.path.join(os.getcwd(), 'viscai_database.db')
    try:
        conn = sqlite3.connect(local_db); cur = conn.cursor()
        _ensure_schema(cur); conn.commit()

        local_mode = not bool(name_server)  # True si name_server es None o "" (modo local-only)

        inserted_any = False

        if local_mode:
            # **************** NUEVO CAMBIO **********
            # Trabajar exclusivamente en local: recorrer working_directory en el FS local.
            try:
                # Si include_root: comprobar gtp/gt en la raíz local
                if include_root:
                    root_gtp = os.path.join(working_directory, "gtp.dat")
                    root_gt  = os.path.join(working_directory, "gt.dat")
                    if os.path.exists(root_gtp) and os.path.exists(root_gt):
                        if _ingest_single_simulation_local(working_directory, cur):
                            inserted_any = True

                # recorrer subdirectorios Mw_*
                for entry in os.listdir(working_directory):
                    full = os.path.join(working_directory, entry)
                    if os.path.isdir(full) and entry.startswith("Mw_"):
                        # Llamada SECUENCIAL a la función que requiere 'cur'
                        ok = _ingest_single_simulation_local(full, cur)
                        if ok:
                            inserted_any = True
            except Exception as e:
                conn.rollback()
                conn.close()
                if os.path.exists(local_db):
                    remove_db_local(local_db)
                raise

            conn.commit()
            if not inserted_any:
                # Nada insertado: borramos DB y salimos con error
                conn.close()
                if os.path.exists(local_db):
                    remove_db_local(local_db)
                st.error("ERROR!!! No se encontraron ficheros reológicos en modo local.")
                return

            # Reindex / ordering si aplica (se puede implementar si hace falta)
            if is_parallel and sort_ids_by_mw:
                # placeholder (opcional)
                pass

            # Ahora MOVEMOS/COPIAMOS la base de datos generada al directory destino (working_directory o input_file_002)
            target_local_dir = st.session_state.get("input_options", {}).get("input_file_002", "") or working_directory
            os.makedirs(target_local_dir, exist_ok=True)
            try:
                target_db = os.path.join(target_local_dir, "viscai_database.db")
                # Si ya existe, sobrescribir
                if os.path.exists(target_db):
                    os.remove(target_db)
                os.replace(local_db, target_db)
                # Update local_db path to new location for CSV export functions
                local_db = target_db
                conn = sqlite3.connect(local_db); cur = conn.cursor()
            except Exception:
                # si falló move, dejamos la DB en cwd y seguimos
                conn = sqlite3.connect(local_db); cur = conn.cursor()

            # Exportar CSVs (se generan en cwd/csv_exports)
            export_db_to_csv(local_db, "csv_exports", generate_distribution_summary=False)

            # Generar CSV pyRheo (agregados + por-Mw)
            csv_format_to_pyrheo(local_csv_dir=os.path.join(os.getcwd(), "csv_exports"),
                                 per_mw=is_parallel, generate_aggregated=True)

            # Copiar CSVs a target_local_dir
            try:
                csv_src = os.path.join(os.getcwd(), "csv_exports")
                if os.path.isdir(csv_src):
                    for f in os.listdir(csv_src):
                        srcf = os.path.join(csv_src, f)
                        dstf = os.path.join(target_local_dir, f)
                        try:
                            if os.path.exists(dstf): os.remove(dstf)
                            os.replace(srcf, dstf)
                        except Exception:
                            # fallback copy
                            with open(srcf, 'rb') as rf, open(dstf, 'wb') as wf:
                                wf.write(rf.read())
            except Exception:
                pass
            #   TEST

            # **************** NUEVO CAMBIO **********
            # Mover los CSV por-Mw (02-..._Mw_...) a sus subdirectorios locales.
            # Nota: anteriormente ya hemos trasladado todo desde csv_src -> target_local_dir,
            # por eso buscamos los ficheros en target_local_dir (no en csv_src, que estará vacío).


            try:
                # Buscamos en target_local_dir (no en csv_src, que ya fue vaciado por os.replace)
                for fname in os.listdir(target_local_dir):
                    m = _re.match(
                        r"^(02-(relaxation|dynamic)_pyRheo)_Mw_([^_]+(?:_.+)?)__D([^_]+)__PDI_(.+)\.csv$",
                        fname, _re.IGNORECASE
                    )
                    if not m:
                        continue

                    base_name = m.group(1)       # 02-relaxation_pyRheo | 02-dynamic_pyRheo
                    mw_token  = m.group(3)       # e.g. 20000_0
                    dist_token = m.group(4)      # e.g. 0, 2
                    pdi_token  = m.group(5)      # e.g. 1_5, 2_5

                    # Nombre del subdirectorio destino (igual que en remoto)
                    target_dir_name = f"Mw_{mw_token}__D{dist_token}__PDI_{pdi_token}"
                    dest_subdir = os.path.join(target_local_dir, target_dir_name)

                    # Crear subdirectorio si no existe (organización local razonable)
                    os.makedirs(dest_subdir, exist_ok=True)

                    # Ruta origen/destino (origen está en target_local_dir)
                    src_path = os.path.join(target_local_dir, fname)
                    # Renombrar el fichero dentro del subdir para nombre estándar: e.g. 02-dynamic_pyRheo.csv
                    dest_fname = f"{base_name}.csv"
                    dest_path = os.path.join(dest_subdir, dest_fname)

                    try:
                        # Si ya existe, sobreescribimos
                        if os.path.exists(dest_path):
                            os.remove(dest_path)
                        os.replace(src_path, dest_path)
                    except Exception:
                        # fallback copy si el replace falla (por permisos/filesystems distintos)
                        try:
                            with open(src_path, "rb") as rf, open(dest_path, "wb") as wf:
                                wf.write(rf.read())
                            os.remove(src_path)
                        except Exception as e:
                            st.warning(f"WARNING: no se pudo mover {fname} a {dest_subdir}: {e}")
            except Exception as e:
                st.warning(f"WARNING: error moviendo CSV por-Mw a subdirs: {e}")
            # **************** NUEVO CAMBIO **********


            # **************** NUEVO CAMBIO **********
            # Eliminar las copias agregadas (02-relaxation_pyRheo.csv / 02-dynamic_pyRheo.csv)
            # que quedan en la raíz local, ya que las versiones por-Mw fueron colocadas
            # dentro de los subdirectorios correspondientes.
            try:
                for root_csv in ("02-relaxation_pyRheo.csv", "02-dynamic_pyRheo.csv"):
                    candidate = os.path.join(target_local_dir, root_csv)
                    if os.path.exists(candidate):
                        try:
                            os.remove(candidate)
                        except Exception:
                            # intento alternativo: truncar si no se puede borrar directamente
                            try:
                                with open(candidate, "w"):
                                    pass
                            except Exception:
                                st.warning(f"WARNING: no se pudo eliminar {candidate}.")
            except Exception as e:
                st.warning(f"WARNING: error al limpiar CSV raíz: {e}")
            # **************** NUEVO CAMBIO **********





            # Limpieza local temporal
            try:
                if os.path.exists(os.path.join(os.getcwd(), "csv_exports")):
                    remove_csv_exports(local_csv_dir=os.path.join(os.getcwd(), "csv_exports"))
            except Exception:
                pass

            # Si llegamos aquí, cerramos la conexión DB
            try: conn.close()
            except Exception: pass

            return
        # **************** NUEVO CAMBIO **********

        # Si no es modo local, se mantiene la lógica REMOTA previa (con sftp).
        # (Código original a continuación, sin cambios funcionales)
        ssh = connect_remote_server(name_server, name_user, ssh_key_options)
        sftp = ssh.open_sftp()

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
        if not is_parallel:
            for fname in ["02-relaxation_pyRheo.csv", "02-dynamic_pyRheo.csv"]:
                local_path = os.path.join("csv_exports", fname)
                if os.path.exists(local_path):
                    sftp.put(local_path, os.path.join(working_directory, fname))

        upload_csv(sftp,
                   local_csv_dir=os.path.join(os.getcwd(), "csv_exports"),
                   remote_dir=working_directory,
                   upload_per_mw=is_parallel)

        # Descarga local y limpieza (remota original)
        local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")
        if local_dir and os.path.isdir(local_dir):
            sftp.get(f"{working_directory}/viscai_database.db", os.path.join(local_dir, "viscai_database.db"))
            for fname in ["01-simulation.csv", "01-dynamic.csv", "01-relaxation.csv", "01-job_status.csv"]:
                try: sftp.get(f"{working_directory}/{fname}", os.path.join(local_dir, fname))
                except Exception: pass
            if not is_parallel:
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
# ****************************NUEVO CAMBIO **********

