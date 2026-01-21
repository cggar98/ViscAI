
# utils/db_to_csv.py
import streamlit as st
import sqlite3
import csv
import os
import pandas as pd
import re

# --- NUEVO: mapa de etiqueta -> código de distribución ---
DIST_CODE_MAP = {
    "Monodisperse": 0,
    "Gaussian": 1,
    "Log-normal": 2,
    "Poisson": 3,
    "Flory": 4
}

def export_db_to_csv(db_path: str, output_dir: str, generate_distribution_summary: bool = True):
    os.makedirs(output_dir, exist_ok=True)
    conn = sqlite3.connect(db_path)

    # 01-simulation.csv (orden: id, mw, pdi, dist_label, eta0, eta*)
    df_sim = pd.read_sql_query(
        "SELECT id, molecular_weight, pdi, distribution_label, zero_shear_viscosity, complex_viscosity "
        "FROM simulation ORDER BY molecular_weight ASC, id ASC",
        conn
    )
    df_sim.to_csv(os.path.join(output_dir, "01-simulation.csv"), index=False)

    # 01-dynamic.csv
    df_dyn = pd.read_sql_query(
        "SELECT id, simulation_id, frequency, elastic_modulu, viscous_modulu "
        "FROM dynamic ORDER BY simulation_id ASC, frequency ASC",
        conn
    )
    df_dyn.to_csv(os.path.join(output_dir, "01-dynamic.csv"), index=False)

    # 01-relaxation.csv
    df_rel = pd.read_sql_query(
        "SELECT id, simulation_id, time, modulu "
        "FROM relaxation ORDER BY simulation_id ASC, time ASC",
        conn
    )
    df_rel.to_csv(os.path.join(output_dir, "01-relaxation.csv"), index=False)

    # 01-job_status.csv
    df_js = pd.read_sql_query(
        "SELECT simulation_id, status FROM job_status ORDER BY simulation_id ASC",
        conn
    )
    df_js.to_csv(os.path.join(output_dir, "01-job_status.csv"), index=False)

    # 03-viscosity_by_distribution.csv (solo si se solicita)
    if generate_distribution_summary:
        try:
            agg = (
                df_sim.dropna(subset=["distribution_label"])
                     .groupby(["distribution_label"], as_index=False)
                     .agg({
                         "zero_shear_viscosity": "mean",
                         "complex_viscosity": "mean"
                     })
            )
            agg.rename(columns={
                "zero_shear_viscosity": "zero_shear_viscosity_mean",
                "complex_viscosity": "complex_viscosity_mean"
            }, inplace=True)
            agg.to_csv(os.path.join(output_dir, "03-viscosity_by_distribution.csv"), index=False)
        except Exception:
            # No interrumpir si no hay datos suficientes
            pass

    conn.close()


def csv_format_to_pyrheo(local_csv_dir: str,
                         per_mw: bool = False,
                         generate_aggregated: bool = True) -> None:
    """
    Convierte 01-relaxation.csv y 01-dynamic.csv a formato pyRheo.
    - Si `generate_aggregated=True`, genera 02-*_pyRheo.csv (agregados, en raíz local).
    - Si `per_mw=True`, además genera ficheros por simulación (Mw+D+PDI):
      02-relaxation_pyRheo_Mw_<mw_token>__D<dist_code>__PDI_<pdi_token>.csv
      02-dynamic_pyRheo_Mw_<mw_token>__D<dist_code>__PDI_<pdi_token>.csv
    """
    if not os.path.isdir(local_csv_dir):
        st.warning("WARNING!!! CSV files are not in the pyRheo format.")
        return

    # --- Índice por simulación: sid -> (mw_token, dist_code_token, pdi_token) ---
    sim_path = os.path.join(local_csv_dir, "01-simulation.csv")
    sim_index = {}  # sid -> dict(tokens)
    if os.path.exists(sim_path):
        df_sim = pd.read_csv(sim_path)
        for _, row in df_sim.iterrows():
            sid = int(row["id"])
            mw = row.get("molecular_weight", None)
            pdi = row.get("pdi", None)
            dlabel = row.get("distribution_label", None)

            mw_token = str(float(mw)).replace(".", "_") if pd.notna(mw) else f"sim_{sid}"
            # pdi puede venir como float; tokenizamos con '_' por compatibilidad de nombres
            pdi_token = (str(float(pdi)).replace(".", "_")) if pd.notna(pdi) else "NA"

            # code desde label; si falta, NA (no subiremos si no hay destino)
            if pd.notna(dlabel) and dlabel in DIST_CODE_MAP:
                dist_code_token = str(DIST_CODE_MAP[dlabel])
            else:
                dist_code_token = "NA"

            sim_index[sid] = {
                "mw_token": mw_token,
                "dist_token": dist_code_token,
                "pdi_token": pdi_token
            }

    # --- 01-relaxation.csv -> 02-relaxation_pyRheo*.csv ---
    relaxation_csv = os.path.join(local_csv_dir, "01-relaxation.csv")
    if os.path.exists(relaxation_csv):
        df_relax = pd.read_csv(relaxation_csv)

        # Agregado (opcional) en raíz local
        if generate_aggregated:
            df_r = df_relax.copy()
            for col in ['id', 'simulation_id']:
                if col in df_r.columns:
                    df_r.drop(columns=[col], inplace=True)
            df_r = df_r.rename(columns={"time": "Time", "modulu": "Relaxation Modulus"})
            df_r.to_csv(os.path.join(local_csv_dir, "02-relaxation_pyRheo.csv"), index=False)

        # Por simulación (Mw + D + PDI)
        if per_mw and "simulation_id" in df_relax.columns:
            for sid, grp in df_relax.groupby("simulation_id"):
                sid = int(sid)
                df_mw = grp.copy()
                for col in ['id', 'simulation_id']:
                    if col in df_mw.columns:
                        df_mw.drop(columns=[col], inplace=True)
                df_mw = df_mw.rename(columns={"time": "Time", "modulu": "Relaxation Modulus"})

                tokens = sim_index.get(sid, None)
                if not tokens:
                    # No hay info suficiente para nombrar; saltamos
                    continue

                mw_token = tokens["mw_token"]
                dist_token = tokens["dist_token"]
                pdi_token = tokens["pdi_token"]

                out_mw = os.path.join(
                    local_csv_dir,
                    f"02-relaxation_pyRheo_Mw_{mw_token}__D{dist_token}__PDI_{pdi_token}.csv"
                )
                df_mw.to_csv(out_mw, index=False)

    # --- 01-dynamic.csv -> 02-dynamic_pyRheo*.csv ---
    dynamic_csv = os.path.join(local_csv_dir, "01-dynamic.csv")
    if os.path.exists(dynamic_csv):
        df_dyn = pd.read_csv(dynamic_csv)

        # Agregado (opcional) en raíz local
        if generate_aggregated:
            df_d = df_dyn.copy()
            for col in ['id', 'simulation_id']:
                if col in df_d.columns:
                    df_d.drop(columns=[col], inplace=True)
            df_d = df_d.rename(columns={
                "frequency": "Angular Frequency",
                "elastic_modulu": "Storage Modulus",
                "viscous_modulu": "Loss Modulus"
            })
            df_d.to_csv(os.path.join(local_csv_dir, "02-dynamic_pyRheo.csv"), index=False)

        # Por simulación (Mw + D + PDI)
        if per_mw and "simulation_id" in df_dyn.columns:
            for sid, grp in df_dyn.groupby("simulation_id"):
                sid = int(sid)
                df_mw = grp.copy()
                for col in ['id', 'simulation_id']:
                    if col in df_mw.columns:
                        df_mw.drop(columns=[col], inplace=True)
                df_mw = df_mw.rename(columns={
                    "frequency": "Angular Frequency",
                    "elastic_modulu": "Storage Modulus",
                    "viscous_modulu": "Loss Modulus"
                })

                tokens = sim_index.get(sid, None)
                if not tokens:
                    continue

                mw_token = tokens["mw_token"]
                dist_token = tokens["dist_token"]
                pdi_token = tokens["pdi_token"]

                out_mw = os.path.join(
                    local_csv_dir,
                    f"02-dynamic_pyRheo_Mw_{mw_token}__D{dist_token}__PDI_{pdi_token}.csv"
                )
                df_mw.to_csv(out_mw, index=False)
    else:
        st.warning("WARNING!!! CSV files are not in the pyRheo format.")


def upload_csv(sftp, local_csv_dir: str, remote_dir: str,
               upload_per_mw: bool = False) -> None:
    """
    Subida de .csv:
    - Subir SOLO 01-*.csv al raíz (y 03- si existe y se desea).
    - Si `upload_per_mw=True`: subir por simulación al subdirectorio EXACTO
      'Mw_<mw_token>__D<dist_token>__PDI_<pdi_token>'.
      **No** crear subdirectorios nuevos si no existen.
    """
    try:
        if os.path.isdir(local_csv_dir):
            # Agregados al raíz -> solo 01-*.csv (y 03- si existe)
            for fname in [
                "01-simulation.csv", "01-dynamic.csv", "01-relaxation.csv",
                "01-job_status.csv",
                "03-viscosity_by_distribution.csv"  # puede no existir
            ]:
                local_path = os.path.join(local_csv_dir, fname)
                if os.path.exists(local_path):
                    sftp.put(local_path, os.path.join(remote_dir, fname))

            # Subida por simulación (Mw + D + PDI)
            if upload_per_mw:
                # Remoto: entradas existentes en el working directory
                try:
                    remote_entries = sftp.listdir(remote_dir)
                except Exception:
                    remote_entries = []

                for fname in os.listdir(local_csv_dir):
                    # Nuevo patrón de nombres:
                    # 02-<type>_pyRheo_Mw_<mw>__D<dist>__PDI_<pdi>.csv
                    m = re.match(
                        r"^(02-(relaxation|dynamic)_pyRheo)_Mw_([^_]+(?:_.+)?)__D([^_]+)__PDI_(.+)\.csv$",
                        fname, re.IGNORECASE
                    )
                    if not m:
                        continue

                    base_name = m.group(1)      # 02-relaxation_pyRheo | 02-dynamic_pyRheo
                    mw_token  = m.group(3)      # e.g. 20000_0
                    dist_token = m.group(4)     # e.g. 0, 4
                    pdi_token  = m.group(5)     # e.g. 1_5, 2_5

                    # Subdirectorio DESTINO exacto
                    target_dir_name = f"Mw_{mw_token}__D{dist_token}__PDI_{pdi_token}"
                    if target_dir_name not in remote_entries:
                        # No existe: no crear, saltar
                        continue

                    remote_subdir = os.path.join(remote_dir, target_dir_name)
                    # Verificar accesibilidad
                    try:
                        sftp.stat(remote_subdir)
                    except Exception:
                        continue

                    local_path = os.path.join(local_csv_dir, fname)
                    remote_path = os.path.join(remote_subdir, f"{base_name}.csv")
                    try:
                        sftp.put(local_path, remote_path)
                    except Exception as e:
                        st.warning(f"WARNING!!! No se pudo subir {fname} a {remote_subdir}: {e}")

    except Exception:
        st.error(e)
