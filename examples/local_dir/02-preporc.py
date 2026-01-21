import sqlite3
import pandas as pd
import numpy as np
import os
import shutil

DB_PATH = "/home/cgarcia/cgarcia_MOMENTUM/Programs/ViscAI/examples/local_dir/viscai_database.db"
OUT_DIR = os.path.join(os.path.dirname(DB_PATH), "preprocessed")
os.makedirs(OUT_DIR, exist_ok=True)

CHUNKSIZE = 100_000

# ------------------------------------------------------------------
# Backup
# ------------------------------------------------------------------
BACKUP = DB_PATH + ".bak"
if not os.path.exists(BACKUP):
    shutil.copy(DB_PATH, BACKUP)
    print("Backup creado:", BACKUP)

# ------------------------------------------------------------------
# DB connection + indices
# ------------------------------------------------------------------
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute("CREATE INDEX IF NOT EXISTS idx_relaxation_sim ON relaxation(simulation_id);")
cur.execute("CREATE INDEX IF NOT EXISTS idx_dynamic_sim ON dynamic(simulation_id);")
cur.execute("CREATE INDEX IF NOT EXISTS idx_simulation_id ON simulation(id);")
conn.commit()

# ------------------------------------------------------------------
# Load simulation + status
# ------------------------------------------------------------------
df_sim = pd.read_sql_query("SELECT * FROM simulation", conn)
df_status = pd.read_sql_query("SELECT * FROM job_status", conn)

finished_ids = df_status.loc[df_status['status'] == 'finished', 'simulation_id'].unique().tolist()
print("Simulations finished:", len(finished_ids))

df_sim = df_sim[df_sim['id'].isin(finished_ids)].copy()
print("Simulations kept (finished):", len(df_sim))

# basic validation
df_sim = df_sim.dropna(subset=['molecular_weight', 'pdi'])
df_sim = df_sim[(df_sim['molecular_weight'] > 0) & (df_sim['pdi'] > 0)].copy()

sim_out = os.path.join(OUT_DIR, "simulation_clean.csv")
df_sim.to_csv(sim_out, index=False)
print("simulation_clean saved:", sim_out)

valid_sim_ids = set(df_sim['id'].tolist())

# ==================================================================
# RELAXATION
# ==================================================================
rel_out = os.path.join(OUT_DIR, "relaxation_clean.csv")
orphan_rel_out = os.path.join(OUT_DIR, "orphan_relaxation.csv")

# ⛔ borrar salidas previas
for f in (rel_out, orphan_rel_out):
    if os.path.exists(f):
        os.remove(f)

write_header = True
total_in = total_kept = orphan_count = 0

for chunk in pd.read_sql_query("SELECT * FROM relaxation", conn, chunksize=CHUNKSIZE):
    total_in += len(chunk)

    chunk['time'] = pd.to_numeric(chunk['time'], errors='coerce')
    chunk['modulu'] = pd.to_numeric(chunk['modulu'], errors='coerce')

    mask = (chunk['time'] > 0) & np.isfinite(chunk['modulu'])
    chunk = chunk.loc[mask].copy()

    orphan = chunk.loc[~chunk['simulation_id'].isin(valid_sim_ids)]
    kept = chunk.loc[chunk['simulation_id'].isin(valid_sim_ids)]

    if not orphan.empty:
        orphan.to_csv(orphan_rel_out, mode="a", index=False,
                      header=not os.path.exists(orphan_rel_out))
        orphan_count += len(orphan)

    if not kept.empty:
        kept = kept.rename(columns={'modulu': 'G_t'})
        kept.to_csv(rel_out, mode="a", index=False, header=write_header)
        write_header = False
        total_kept += len(kept)

print(f"Relaxation: read={total_in}, kept={total_kept}, orphans={orphan_count}")

# ==================================================================
# DYNAMIC
# ==================================================================
dyn_out = os.path.join(OUT_DIR, "dynamic_clean.csv")
orphan_dyn_out = os.path.join(OUT_DIR, "orphan_dynamic.csv")

# ⛔ borrar salidas previas
for f in (dyn_out, orphan_dyn_out):
    if os.path.exists(f):
        os.remove(f)

write_header = True
total_in = total_kept = orphan_count = 0

for chunk in pd.read_sql_query("SELECT * FROM dynamic", conn, chunksize=CHUNKSIZE):
    total_in += len(chunk)

    chunk['frequency'] = pd.to_numeric(chunk['frequency'], errors='coerce')
    chunk['elastic_modulu'] = pd.to_numeric(chunk['elastic_modulu'], errors='coerce')
    chunk['viscous_modulu'] = pd.to_numeric(chunk['viscous_modulu'], errors='coerce')

    mask = (
        (chunk['frequency'] > 0) &
        np.isfinite(chunk['elastic_modulu']) &
        np.isfinite(chunk['viscous_modulu'])
    )
    chunk = chunk.loc[mask].copy()

    orphan = chunk.loc[~chunk['simulation_id'].isin(valid_sim_ids)]
    kept = chunk.loc[chunk['simulation_id'].isin(valid_sim_ids)]

    if not orphan.empty:
        orphan.to_csv(orphan_dyn_out, mode="a", index=False,
                      header=not os.path.exists(orphan_dyn_out))
        orphan_count += len(orphan)

    if not kept.empty:
        kept = kept.rename(columns={
            'elastic_modulu': 'G_prime',
            'viscous_modulu': 'G_double_prime'
        })
        kept.to_csv(dyn_out, mode="a", index=False, header=write_header)
        write_header = False
        total_kept += len(kept)

print(f"Dynamic: read={total_in}, kept={total_kept}, orphans={orphan_count}")

conn.close()
print("Preprocessing completo. CSVs en:", OUT_DIR)

