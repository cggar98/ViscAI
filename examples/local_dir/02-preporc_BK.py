import sqlite3
import pandas as pd
import numpy as np
import os
import shutil
from typing import Iterable

DB_PATH = "/home/cgarcia/cgarcia_MOMENTUM/Programs/ViscAI/examples/local_dir/viscai_database.db"
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

# filter simulation table to those finished
df_sim = df_sim[df_sim['id'].isin(finished_ids)].copy()
print("Simulations kept (finished):", len(df_sim))

# basic checks on simulation
print("Simulation NA counts:\n", df_sim.isna().sum())

# 4) Export simulation_clean.csv
sim_out = os.path.join(OUT_DIR, "simulation_clean.csv")
# apply simple validation: mw>0 and pdi>0
df_sim = df_sim.dropna(subset=['molecular_weight', 'pdi'])
df_sim = df_sim[(df_sim['molecular_weight'] > 0) & (df_sim['pdi'] > 0)].copy()
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
    chunk['time'] = pd.to_numeric(chunk['time'], errors='coerce')
    chunk['modulu'] = pd.to_numeric(chunk['modulu'], errors='coerce')
    # valid rows
    mask_valid = chunk['time'] > 0
    mask_valid &= chunk['modulu'].notna() & np.isfinite(chunk['modulu'])
    chunk_valid = chunk.loc[mask_valid].copy()
    # keep only finished simulation ids
    mask_ref = chunk_valid['simulation_id'].isin(valid_sim_ids)
    orphan_rows = chunk_valid.loc[~mask_ref]
    if len(orphan_rows) > 0:
        orphan_rows.to_csv(orphan_rel_out, mode='a', index=False, header=first_chunk and not os.path.exists(orphan_rel_out))
        orphan_rel_count += len(orphan_rows)
    chunk_final = chunk_valid.loc[mask_ref].copy()
    # rename column if desired
    chunk_final = chunk_final.rename(columns={'modulu': 'G_t'})
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
    chunk['frequency'] = pd.to_numeric(chunk['frequency'], errors='coerce')
    chunk['elastic_modulu'] = pd.to_numeric(chunk['elastic_modulu'], errors='coerce')
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
        orphan_rows.to_csv(orphan_dyn_out, mode='a', index=False, header=first_chunk and not os.path.exists(orphan_dyn_out))
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

