import sqlite3, os
import streamlit as st
from ViscAI.utils.rheology_utils import safe_logspace, float_array, resample_log_x
from pathlib import Path
from datetime import datetime


def database_inspection():
    local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")

    if not local_dir or not os.path.isdir(local_dir):
        raise RuntimeError(
            "ERROR: No se ha definido un directorio local válido en Program Options (input_file_002)."
        )
    
    DB_PATH = os.path.join(local_dir, "viscai_database.db")
    # ********************************* FIN CAMBIO PRINCIPAL *********************************

    BACKUP = DB_PATH + ".bak"
    if not os.path.exists(BACKUP):
        import shutil
        shutil.copy(DB_PATH, BACKUP)
        print("Backup creado:", BACKUP)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # listar tablas
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [r[0] for r in cur.fetchall()]
    print("Tablas:", tables)

    # esquema y muestras
    for t in tables:
        print("\n--- Tabla:", t, "---")
        cur.execute(f"PRAGMA table_info('{t}')")
        cols = cur.fetchall()
        print("Columnas:", cols)
        cur.execute(f"SELECT COUNT(*) FROM '{t}'")
        print("Filas:", cur.fetchone()[0])
        cur.execute(f"SELECT * FROM '{t}' LIMIT 5")
        rows = cur.fetchall()
        for row in rows:
            print(row)

    conn.close()


def preprocess_database():
    import sqlite3
    import pandas as pd
    import numpy as np
    import os
    import shutil

    local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")

    if not local_dir or not os.path.isdir(local_dir):
        raise RuntimeError(
            "ERROR: No se ha definido un directorio local válido en Program Options (input_file_002)."
        )

    DB_PATH = os.path.join(local_dir, "viscai_database.db")
    OUT_DIR = os.path.join(os.path.dirname(DB_PATH), "preprocessed")
    os.makedirs(OUT_DIR, exist_ok=True)

    # Ajusta según memoria disponible (filas por chunk)
    CHUNKSIZE = 100_000

    # 1) Backup
    BACKUP = DB_PATH + ".bak"
    if not os.path.exists(BACKUP):
        shutil.copy(DB_PATH, BACKUP)
        print("Backup creado:", BACKUP)

    # 2) Conexión y preparación (crea índices si no existen para acelerar)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # crear índices para acelerar filtrado por simulation_id (si no existen)
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_relaxation_sim ON relaxation(simulation_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dynamic_sim ON dynamic(simulation_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_simulation_id ON simulation(id);")
        conn.commit()
    except Exception as e:
        print("No se pudo crear índices (no crítico):", e)

    # 3) Cargar tablas pequeñas: simulation y job_status
    df_sim = pd.read_sql_query("SELECT * FROM simulation", conn)
    df_status = pd.read_sql_query("SELECT * FROM job_status", conn)

    # keep only finished simulations
    finished_ids = df_status.loc[df_status['status'] == 'finished', 'simulation_id'].unique().tolist()
    print("Simulations finished:", len(finished_ids))

    # 4) Limpieza robusta de df_sim
    # Coerce de columnas que deberían ser numéricas (si hay cadenas vacías -> NaN)
    numeric_cols = ['molecular_weight', 'pdi', 'zero_shear_viscosity', 'complex_viscosity']
    for c in numeric_cols:
        if c in df_sim.columns:
            df_sim[c] = pd.to_numeric(df_sim[c], errors='coerce')

    # Normalizar label de distribución (garantizar texto o 'unknown')
    if 'distribution_label' in df_sim.columns:
        df_sim['distribution_label'] = df_sim['distribution_label'].astype(str).replace({'nan': ''})
        df_sim.loc[df_sim['distribution_label'].str.strip() == '', 'distribution_label'] = 'unknown'
    else:
        df_sim['distribution_label'] = 'unknown'

    # Filtrar sólo finished_ids
    df_sim = df_sim[df_sim['id'].isin(finished_ids)].copy()
    print("Simulations kept (finished):", len(df_sim))

    # Aplicar validaciones: mw>0, pdi>0, zero_shear_viscosity (target) presente y >0
    # Si prefieres no exigir complex_viscosity, elimina de subset.
    required_positive = ['molecular_weight', 'pdi', 'zero_shear_viscosity']
    df_sim_before = len(df_sim)
    df_sim = df_sim.dropna(subset=required_positive)
    df_sim = df_sim[
        (df_sim['molecular_weight'] > 0) & (df_sim['pdi'] > 0) & (df_sim['zero_shear_viscosity'] > 0)].copy()
    print(f"Simulations removed by invalid numeric fields: {df_sim_before - len(df_sim)}")
    print("Simulation NA counts:\n", df_sim.isna().sum())

    # 4b) Guardar simulation_clean.csv
    sim_out = os.path.join(OUT_DIR, "simulation_clean.csv")
    df_sim.to_csv(sim_out, index=False)
    valid_sim_ids = set(df_sim['id'].tolist())
    print("simulation_clean saved:", sim_out)

    # 5) Procesar relaxation por chunks
    rel_in_query = "SELECT * FROM relaxation"
    rel_out = os.path.join(OUT_DIR, "relaxation_clean.csv")
    orphan_rel_out = os.path.join(OUT_DIR, "orphan_relaxation.csv")
    first_chunk = True
    orphan_rel_count = 0
    total_rel_in = 0
    total_rel_kept = 0

    for chunk in pd.read_sql_query(rel_in_query, conn, chunksize=CHUNKSIZE):
        total_rel_in += len(chunk)
        # numéricos seguros
        chunk['time'] = pd.to_numeric(chunk.get('time', pd.Series()), errors='coerce')
        # some DBs use 'modulu' or 'G_t'
        if 'modulu' in chunk.columns:
            chunk['modulu'] = pd.to_numeric(chunk['modulu'], errors='coerce')
        if 'G_t' in chunk.columns:
            chunk['G_t'] = pd.to_numeric(chunk['G_t'], errors='coerce')

        # valid rows: time>0 and module finite
        mask_valid = chunk['time'] > 0
        # prefer 'G_t' if exists, else 'modulu'
        valcol = 'G_t' if 'G_t' in chunk.columns else 'modulu' if 'modulu' in chunk.columns else None
        if valcol is None:
            # no hay columna esperada -> salta chunk
            continue
        mask_valid &= chunk[valcol].notna() & np.isfinite(chunk[valcol])
        chunk_valid = chunk.loc[mask_valid].copy()

        # keep only finished simulation ids
        mask_ref = chunk_valid['simulation_id'].isin(valid_sim_ids)
        orphan_rows = chunk_valid.loc[~mask_ref]
        if len(orphan_rows) > 0:
            orphan_rows.to_csv(orphan_rel_out, mode='a', index=False,
                               header=first_chunk and not os.path.exists(orphan_rel_out))
            orphan_rel_count += len(orphan_rows)
        chunk_final = chunk_valid.loc[mask_ref].copy()

        # rename column if desired
        chunk_final = chunk_final.rename(columns={valcol: 'G_t'})

        # append to CSV
        if not chunk_final.empty:
            chunk_final.to_csv(rel_out, mode='a', index=False, header=first_chunk)
            total_rel_kept += len(chunk_final)
            first_chunk = False

    print(f"Relaxation: read={total_rel_in}, kept={total_rel_kept}, orphans={orphan_rel_count}, out={rel_out}")

    # 6) Procesar dynamic por chunks
    dyn_in_query = "SELECT * FROM dynamic"
    dyn_out = os.path.join(OUT_DIR, "dynamic_clean.csv")
    orphan_dyn_out = os.path.join(OUT_DIR, "orphan_dynamic.csv")
    first_chunk = True
    orphan_dyn_count = 0
    total_dyn_in = 0
    total_dyn_kept = 0

    for chunk in pd.read_sql_query(dyn_in_query, conn, chunksize=CHUNKSIZE):
        total_dyn_in += len(chunk)
        # numéricos seguros
        chunk['frequency'] = pd.to_numeric(chunk.get('frequency', pd.Series()), errors='coerce')
        if 'elastic_modulu' in chunk.columns:
            chunk['elastic_modulu'] = pd.to_numeric(chunk['elastic_modulu'], errors='coerce')
        if 'viscous_modulu' in chunk.columns:
            chunk['viscous_modulu'] = pd.to_numeric(chunk['viscous_modulu'], errors='coerce')
        # valid rows
        mask_valid = chunk['frequency'] > 0
        mask_valid &= chunk['elastic_modulu'].notna() & np.isfinite(chunk['elastic_modulu'])
        mask_valid &= chunk['viscous_modulu'].notna() & np.isfinite(chunk['viscous_modulu'])
        chunk_valid = chunk.loc[mask_valid].copy()
        # keep only finished simulation ids
        mask_ref = chunk_valid['simulation_id'].isin(valid_sim_ids)
        orphan_rows = chunk_valid.loc[~mask_ref]
        if len(orphan_rows) > 0:
            orphan_rows.to_csv(orphan_dyn_out, mode='a', index=False,
                               header=first_chunk and not os.path.exists(orphan_dyn_out))
            orphan_dyn_count += len(orphan_rows)
        chunk_final = chunk_valid.loc[mask_ref].copy()
        # rename columns for clarity
        chunk_final = chunk_final.rename(columns={'elastic_modulu': 'G_prime', 'viscous_modulu': 'G_double_prime'})
        # append to CSV
        if not chunk_final.empty:
            chunk_final.to_csv(dyn_out, mode='a', index=False, header=first_chunk)
            total_dyn_kept += len(chunk_final)
            first_chunk = False

    print(f"Dynamic: read={total_dyn_in}, kept={total_dyn_kept}, orphans={orphan_dyn_count}, out={dyn_out}")

    conn.close()
    print("Preprocessing completo. CSVs en:", OUT_DIR)


    # Archivos pretratados esperados
    PRE_DIR = Path(OUT_DIR)
    clean_files = [
        PRE_DIR / "dynamic_clean.csv",
        PRE_DIR / "relaxation_clean.csv",
        PRE_DIR / "simulation_clean.csv",
    ]

    # Archivos raw a eliminar
    LOC_DIR = Path(local_dir)
    raw_files = [
        LOC_DIR / "01-dynamic.csv",
        LOC_DIR / "01-job_status.csv",
        LOC_DIR / "01-relaxation.csv",
        LOC_DIR / "01-simulation.csv",
    ]

    # Comprobación de seguridad
    if all(f.exists() for f in clean_files):
        print("✔ Preprocesado completo. Eliminando archivos raw...")
        for f in raw_files:
            if f.exists():
                f.unlink()
                print(f"  - Eliminado: {f.name}")
            else:
                print(f"  - No existe (skip): {f.name}")
    else:
        print("✖ ERROR: No se eliminaron los archivos raw (faltan CSVs preprocesados).")


def build_resampled_rheology_features():
    import os
    import numpy as np
    import pandas as pd

    local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")

    if not local_dir or not os.path.isdir(local_dir):
        raise RuntimeError(
            "ERROR: No se ha definido un directorio local válido en Program Options (input_file_002)."
        )


    # Ajusta según tu estructura
    PRE_DIR = os.path.join(local_dir, "preprocessed")
    SIM_CSV = os.path.join(PRE_DIR, "simulation_clean.csv")
    REL_CSV = os.path.join(PRE_DIR, "relaxation_clean.csv")
    DYN_CSV = os.path.join(PRE_DIR, "dynamic_clean.csv")

    OUT_DIR = PRE_DIR  # guarda los outputs aquí
    os.makedirs(OUT_DIR, exist_ok=True)

    # Parámetros remuestreo
    N_TIME = 100
    N_FREQ = 100
    TIME_MIN = 1e-12  # mínimos de seguridad si hay ceros o tiempos extremados
    TIME_MAX = 1e6
    FREQ_MIN = 1e-6
    FREQ_MAX = 1e6



    # ---------- cargar CSVs ----------
    df_sim = pd.read_csv(SIM_CSV)
    df_rel = pd.read_csv(REL_CSV)
    df_dyn = pd.read_csv(DYN_CSV)

    sim_ids = sorted(df_sim['id'].unique())
    print("Simulations to process:", len(sim_ids))

    # crear mallas fijas
    time_grid = safe_logspace(TIME_MIN, TIME_MAX, N_TIME)
    freq_grid = safe_logspace(FREQ_MIN, FREQ_MAX, N_FREQ)

    # arrays para guardar
    G_t_all = np.zeros((len(sim_ids), N_TIME), dtype=float)
    Gp_all = np.zeros((len(sim_ids), N_FREQ), dtype=float)
    Gpp_all = np.zeros((len(sim_ids), N_FREQ), dtype=float)

    features_rows = []

    for i, sid in enumerate(sim_ids):
        # meta
        row_sim = df_sim[df_sim['id'] == sid].iloc[0].to_dict()

        # RELAXATION -> tiempos y modulos
        sub_rel = df_rel[df_rel['simulation_id'] == sid]
        # intenta columnas esperadas: 'time' y 'G_t' o 'modulu'
        if 'time' not in sub_rel.columns:
            print(f"[WARN] 'time' column missing for sim {sid}, skipping")
            t = np.array([])
        else:
            t = float_array(sub_rel['time'].values)
        if 'G_t' in sub_rel.columns:
            G = float_array(sub_rel['G_t'].values)
        elif 'modulu' in sub_rel.columns:
            G = float_array(sub_rel['modulu'].values)
        else:
            G = np.array([])

        G_t_resampled = resample_log_x(t, G, time_grid)
        G_t_all[i, :] = G_t_resampled

        # DYNAMIC -> frequency, G' (G_prime), G'' (G_double_prime)
        sub_dyn = df_dyn[df_dyn['simulation_id'] == sid]
        if 'frequency' not in sub_dyn.columns:
            freq = np.array([])
        else:
            freq = float_array(sub_dyn['frequency'].values)

        if 'G_prime' in sub_dyn.columns and 'G_double_prime' in sub_dyn.columns:
            Gp = float_array(sub_dyn['G_prime'].values)
            Gpp = float_array(sub_dyn['G_double_prime'].values)
        else:
            Gp = float_array(sub_dyn.get('elastic_modulu', np.array([])))
            Gpp = float_array(sub_dyn.get('viscous_modulu', np.array([])))

        Gp_resampled = resample_log_x(freq, Gp, freq_grid)
        Gpp_resampled = resample_log_x(freq, Gpp, freq_grid)
        Gp_all[i, :] = Gp_resampled
        Gpp_all[i, :] = Gpp_resampled

        # features resumen (ejemplos)
        # usar trapz sobre log(x) como medida aproximada
        # si G_t_resampled contiene ceros todos, area=0
        try:
            area_Gt = np.trapz(G_t_resampled, np.log(time_grid + 1e-300))
        except Exception:
            area_Gt = 0.0
        max_Gt = float(np.nanmax(G_t_resampled)) if np.any(np.isfinite(G_t_resampled)) else 0.0
        mean_Gt = float(np.nanmean(G_t_resampled)) if np.any(np.isfinite(G_t_resampled)) else 0.0

        try:
            area_Gp = np.trapz(Gp_resampled, np.log(freq_grid + 1e-300))
        except Exception:
            area_Gp = 0.0
        max_Gp = float(np.nanmax(Gp_resampled)) if np.any(np.isfinite(Gp_resampled)) else 0.0
        mean_Gp = float(np.nanmean(Gp_resampled)) if np.any(np.isfinite(Gp_resampled)) else 0.0

        # característicos moleculares
        log_MW = np.log10(max(float(row_sim.get('molecular_weight', 1.0)), 1e-12))
        pdi = float(row_sim.get('pdi', np.nan))
        dist = row_sim.get('distribution_label', 'unknown')

        features_rows.append({
            'id': sid,
            'log_MW': log_MW,
            'pdi': pdi,
            'distribution': dist,
            'area_Gt': area_Gt,
            'max_Gt': max_Gt,
            'mean_Gt': mean_Gt,
            'area_Gp': area_Gp,
            'max_Gp': max_Gp,
            'mean_Gp': mean_Gp,
            'zero_shear_viscosity': float(row_sim.get('zero_shear_viscosity', np.nan)),
            'complex_viscosity': float(row_sim.get('complex_viscosity', np.nan)),
        })

    # crear DataFrame features
    df_feat = pd.DataFrame(features_rows).set_index('id')

    # codificar distribución (one-hot)
    df_dist = pd.get_dummies(df_feat['distribution'], prefix='dist')
    df_feat = pd.concat([df_feat.drop(columns=['distribution']), df_dist], axis=1)

    # Escalado Z-score (mean/std) para columnas numéricas en df_feat
    num_cols = df_feat.select_dtypes(include=[np.number]).columns.tolist()
    scaler = {}
    for c in num_cols:
        col = df_feat[c].values.astype(float)
        mu = np.nanmean(col)
        sigma = np.nanstd(col) if np.nanstd(col) > 0 else 1.0
        df_feat[c] = (col - mu) / sigma
        scaler[c] = (float(mu), float(sigma))

    # Guardar resultados
    feat_csv = os.path.join(OUT_DIR, "features.csv")
    npz_file = os.path.join(OUT_DIR, "resampled_data.npz")
    scaler_file = os.path.join(OUT_DIR, "features_scaler.npy")

    df_feat.to_csv(feat_csv)
    np.savez_compressed(npz_file,
                        sim_ids=np.array(sim_ids),
                        time_grid=time_grid,
                        freq_grid=freq_grid,
                        G_t_all=G_t_all,
                        Gp_all=Gp_all,
                        Gpp_all=Gpp_all)
    np.save(scaler_file, scaler)

    print("Guardado features:", feat_csv)
    print("Guardado resampled arrays:", npz_file)
    print("Guardado scaler:", scaler_file)
