import os, numpy as np, pandas as pd
import streamlit as st
from sklearn.model_selection import train_test_split
from ViscAI.utils.rheology_utils import load_npy


def prepare_rheology_dataset():

    local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")

    if not local_dir or not os.path.isdir(local_dir):
        raise RuntimeError(
            "ERROR: No se ha definido un directorio local válido en Program Options (input_file_002)."
        )

    # Ajusta según tu estructura
    PRE_DIR = os.path.join(local_dir, "preprocessed")
    FEATURES_CSV = os.path.join(PRE_DIR, "features.csv")

    df = pd.read_csv(FEATURES_CSV, index_col=0)  # index = id

    # --- Selecciona target (elige uno de los parametros reologicos) ---
    target_col = "zero_shear_viscosity"  # <-- ya has elegido A)

    # convert y clean
    df[target_col] = pd.to_numeric(df[target_col], errors='coerce')
    df = df.dropna(subset=[target_col])
    # target = log10(target) (recomendado)
    df['y'] = np.log10(df[target_col].replace(0, np.nan))
    df = df.dropna(subset=['y'])

    # X: todas las columnas excepto target y 'y'
    X = df.drop(columns=[target_col, 'y'])
    y = df['y'].values

    # reproducibilidad y split 70/15/15
    RND = 42
    X_train, X_tmp, y_train, y_tmp = train_test_split(X, y, test_size=0.30, random_state=RND)
    X_val, X_test, y_val, y_test = train_test_split(X_tmp, y_tmp, test_size=0.5, random_state=RND)

    OUT = PRE_DIR
    X_train.to_csv(os.path.join(OUT, "X_train.csv"), index=True)
    X_val.to_csv(os.path.join(OUT, "X_val.csv"), index=True)
    X_test.to_csv(os.path.join(OUT, "X_test.csv"), index=True)
    np.save(os.path.join(OUT, "y_train.npy"), y_train)
    np.save(os.path.join(OUT, "y_val.npy"), y_val)
    np.save(os.path.join(OUT, "y_test.npy"), y_test)

    print("Split saved: ", OUT)
    print("Sizes:", len(y_train), len(y_val), len(y_test))


def validate_splits():

    local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")

    if not local_dir or not os.path.isdir(local_dir):
        raise RuntimeError(
            "ERROR: No se ha definido un directorio local válido en Program Options (input_file_002)."
        )

    # Ajusta según tu estructura
    PRE_DIR = os.path.join(local_dir, "preprocessed")
    FEATURES_CSV = os.path.join(PRE_DIR, "features.csv")

    # carga features
    df = pd.read_csv(FEATURES_CSV, index_col=0)

    # reproduce el filtrado que hiciste
    target_col = "zero_shear_viscosity"  # si elegiste otro, cámbialo
    df[target_col] = pd.to_numeric(df[target_col], errors='coerce')
    df_filtered = df.dropna(subset=[target_col]).copy()
    df_filtered['y'] = np.log10(df_filtered[target_col].replace(0, np.nan))
    df_filtered = df_filtered.dropna(subset=['y'])

    # cuenta filas tras filtrar
    n_filtered = len(df_filtered)
    print("Filas en features.csv tras filtrar target y log:", n_filtered)

    y_train = load_npy(os.path.join(PRE_DIR, "y_train.npy"))
    y_val = load_npy(os.path.join(PRE_DIR, "y_val.npy"))
    y_test = load_npy(os.path.join(PRE_DIR, "y_test.npy"))

    n_splits_sum = len(y_train) + len(y_val) + len(y_test)
    print("Suma tamaños splits (train+val+test):", n_splits_sum)
    print("Desajuste (splits - filtered):", n_splits_sum - n_filtered)

    # comprobación final
    if n_splits_sum == n_filtered:
        print("OK: la suma de los splits coincide con las filas filtradas.")
    else:
        print("ERROR: no coinciden. Revisa el filtrado/saved files.")
