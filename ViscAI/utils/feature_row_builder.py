import os
import streamlit as st
import pandas as pd


# --- Aux: construir una fila 'x' compatible para un id dado ---
def build_row_for_id(sid):
    local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")

    if not local_dir or not os.path.isdir(local_dir):
        raise RuntimeError(
            "ERROR: No se ha definido un directorio local válido en Program Options (input_file_002)."
        )
    PRE_DIR = os.path.join(local_dir, "preprocessed")
    FEATURES_CSV = os.path.join(PRE_DIR, "features.csv")
    XTRAIN_CSV = os.path.join(PRE_DIR, "X_train.csv")
    df_feat = pd.read_csv(FEATURES_CSV, index_col=None)

    # buscar fila en features.csv por id
    row = df_feat[df_feat['id'] == sid]
    if row.empty:
        raise KeyError(f"id {sid} no encontrado en {FEATURES_CSV}")
    row = row.iloc[0].to_dict()

    df_xtrain_hdr = pd.read_csv(XTRAIN_CSV, nrows=0)  # solo header -> columnas usadas en X_train.csv

    # convertir a DataFrame con todas las columnas esperadas
    all_cols = [c for c in df_xtrain_hdr.columns if c != "id"]
    df_row = pd.DataFrame([{c: row.get(c, 0.0) for c in all_cols}])
    # asegurar tipos float
    df_row = df_row.astype(float)
    return df_row
