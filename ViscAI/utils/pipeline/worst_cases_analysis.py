import joblib
from sklearn.neighbors import NearestNeighbors
import streamlit as st
import numpy as np
import os
import pandas as pd
from ViscAI.utils.rheology_utils import safe_minmax, plot_Gt, plot_GpGpp
from ViscAI.utils.feature_row_builder import build_row_for_id


def save_worst_cases():
    local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")

    if not local_dir or not os.path.isdir(local_dir):
        raise RuntimeError(
            "ERROR: No se ha definido un directorio local válido en Program Options (input_file_002)."
        )

    # Ajusta según tu estructura
    PRE_DIR = os.path.join(local_dir, "preprocessed")
    OUT = os.path.join(PRE_DIR, "model_output")
    preds = np.load(os.path.join(OUT, "predictions_rf.npz"))
    y_test = preds["y_test"]; y_test_pred = preds["y_test_pred"]
    ids = pd.read_csv(os.path.join(PRE_DIR, "X_test.csv"), index_col=0).index
    df = pd.DataFrame({"id": ids, "y_true_log": y_test, "y_pred_log": y_test_pred})
    df["y_true_lin"] = 10 ** df.y_true_log
    df["y_pred_lin"] = 10 ** df.y_pred_log
    df["abs_err_lin"] = (df.y_true_lin - df.y_pred_lin).abs()
    worst = df.sort_values("abs_err_lin", ascending=False).head(5)
    worst.to_csv(os.path.join(OUT, "worst_cases.csv"), index=False)
    print("Worst cases saved:", os.path.join(OUT, "worst_cases.csv"))


def plot_worst_cases():
    """
    11-inspect_worst_cases.py
    Script para visualizar las curvas (G(t), G'(w), G''(w)) de los "worst cases"
    guardados en model_output/worst_cases.csv usando los arrays remuestreados.
    Versión corregida: safe_minmax acepta numpy arrays y pandas Series.
    """

    # ----------------------- Ajusta si hace falta -----------------------
    # Si ejecutas el script desde el directorio del proyecto, el script
    # intenta localizar automáticamente la carpeta 'preprocessed'.
    local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")

    if not local_dir or not os.path.isdir(local_dir):
        raise RuntimeError(
            "ERROR: No se ha definido un directorio local válido en Program Options (input_file_002)."
        )

    # Ajusta según tu estructura
    PRE_DIR = os.path.join(local_dir, "preprocessed")

    MODEL_OUT = os.path.join(PRE_DIR, "model_output")
    WORST_CSV = os.path.join(MODEL_OUT, "worst_cases.csv")
    RESAMPLED_NPZ = os.path.join(PRE_DIR, "resampled_data.npz")
    OUTDIR = MODEL_OUT  # guardamos gráficos en model_output
    os.makedirs(OUTDIR, exist_ok=True)

    # ----------------------- Carga de ficheros -----------------------
    if not os.path.exists(WORST_CSV):
        raise FileNotFoundError(f"No se encuentra '{WORST_CSV}'. Ajusta PRE_DIR.")

    if not os.path.exists(RESAMPLED_NPZ):
        raise FileNotFoundError(f"No se encuentra '{RESAMPLED_NPZ}'. Ajusta PRE_DIR.")

    df_worst = pd.read_csv(WORST_CSV)
    npz = np.load(RESAMPLED_NPZ, allow_pickle=True)
    sim_ids = npz["sim_ids"]
    time_grid = npz["time_grid"]
    freq_grid = npz["freq_grid"]
    G_t_all = npz["G_t_all"]
    Gp_all = npz["Gp_all"]
    Gpp_all = npz["Gpp_all"]

    # ----------------------- Procesado de cada worst case -----------------------
    # El CSV worst_cases.csv debe tener columna 'id' con el id de simulation (coincide con sim_ids)
    if "id" not in df_worst.columns:
        raise ValueError("worst_cases.csv no contiene la columna 'id'")

    for idx, row in df_worst.iterrows():
        sim_id = int(row["id"])
        # localizar índice en sim_ids (sim_ids puede ser tipo float o int)
        matches = np.where(sim_ids == sim_id)[0]
        if len(matches) == 0:
            print(f"[WARN] sim_id {sim_id} no encontrado en resampled_data.npz -> salto")
            continue
        sim_idx = int(matches[0])

        # extraer arrays resampleados
        Gt = np.asarray(G_t_all[sim_idx, :], dtype=float)
        Gp = np.asarray(Gp_all[sim_idx, :], dtype=float)
        Gpp = np.asarray(Gpp_all[sim_idx, :], dtype=float)

        # limpiar NaNs / Infs por seguridad
        Gt = np.nan_to_num(Gt, nan=0.0, posinf=np.finfo(float).max, neginf=0.0)
        Gp = np.nan_to_num(Gp, nan=0.0, posinf=np.finfo(float).max, neginf=0.0)
        Gpp = np.nan_to_num(Gpp, nan=0.0, posinf=np.finfo(float).max, neginf=0.0)

        # obtener rangos seguros
        tmin, tmax = safe_minmax(time_grid)
        fmin, fmax = safe_minmax(freq_grid)
        title_gt = f"Sim {sim_id} - worst#{idx + 1} - G(t)"
        title_gp = f"Sim {sim_id} - worst#{idx + 1} - G'(w), G''(w)"

        out_gt = os.path.join(OUTDIR, f"worst_{sim_id}_Gt.png")
        out_gp = os.path.join(OUTDIR, f"worst_{sim_id}_GpGpp.png")

        # plot (si no hay datos, se crean imágenes con ejes vacíos)
        try:
            plot_Gt(time_grid, Gt, title_gt, out_gt)
            plot_GpGpp(freq_grid, Gp, Gpp, title_gp, out_gp)
            print(f"Saved plots for sim {sim_id}: {out_gt}, {out_gp}")
        except Exception as e:
            print(f"[ERROR] plotting sim {sim_id}: {e}")

    print("Done. Plots stored in:", OUTDIR)


def check_worst_cases_ranges():
    local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")

    if not local_dir or not os.path.isdir(local_dir):
        raise RuntimeError(
            "ERROR: No se ha definido un directorio local válido en Program Options (input_file_002)."
        )

    # Ajusta según tu estructura
    PRE_DIR = os.path.join(local_dir, "preprocessed")
    MO = os.path.join(PRE_DIR, "model_output")
    FEAT = os.path.join(PRE_DIR, "features.csv")
    SPLIT_DIR = os.path.join(PRE_DIR)  # donde están X_train.csv, etc.
    WORST = os.path.join(MO, "worst_cases.csv")

    df_feat = pd.read_csv(FEAT, index_col=0)
    df_train = pd.read_csv(os.path.join(SPLIT_DIR, "X_train.csv"), index_col=0)
    df_worst = pd.read_csv(WORST)

    # features numéricas que usamos
    num_cols = ['log_MW', 'pdi', 'area_Gt', 'max_Gt', 'mean_Gt', 'area_Gp', 'max_Gp', 'mean_Gp', 'complex_viscosity']
    # ranges from training
    ranges = {}
    for c in num_cols:
        col = df_train[c].astype(float)
        ranges[c] = (float(col.min()), float(col.max()))

    print("Train ranges (min,max):")
    for k, v in ranges.items():
        print(f" {k}: {v[0]:.6g} .. {v[1]:.6g}")

    # check each worst case
    rows = []
    for _, r in df_worst.iterrows():
        sid = int(r['id'])
        # get feature row in df_feat (index is id)
        if sid not in df_feat.index:
            rows.append((sid, "NOT_IN_FEATURES", None))
            continue
        fr = df_feat.loc[sid]
        outside = []
        for c in num_cols:
            val = float(fr[c])
            mn, mx = ranges[c]
            if val < mn or val > mx:
                outside.append((c, val, mn, mx))
        rows.append((sid, len(outside), outside))

    # report
    for sid, nout, outside in rows:
        print(f"\nSim {sid} -> {nout} features fuera de rango")
        if nout > 0:
            for c, val, mn, mx in outside:
                print(f"  - {c}: {val:.6g} (train min {mn:.6g}, max {mx:.6g})")

    # group counts (distribution)
    if 'distribution' in df_feat.columns:
        # reconstruct distribution column if missing in features.csv then join with simulation_clean.csv
        pass
    else:
        print("\n(Nota: no se encontró columna 'distribution' en features.csv — si quieres, miro simulation_clean.csv)")


def check_worst_cases_local_density():
    local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")

    if not local_dir or not os.path.isdir(local_dir):
        raise RuntimeError(
            "ERROR: No se ha definido un directorio local válido en Program Options (input_file_002)."
        )

    # Ajusta según tu estructura
    PRE_DIR = os.path.join(local_dir, "preprocessed")
    MO = os.path.join(PRE_DIR, "model_output")
    X_train = pd.read_csv(os.path.join(PRE_DIR, "X_train.csv"), index_col=0)
    worst = pd.read_csv(os.path.join(MO, "worst_cases.csv"))

    # columnas numéricas usadas como features (mismas que en entrenamiento)
    feat_cols = ['log_MW', 'pdi', 'area_Gt', 'max_Gt', 'mean_Gt', 'area_Gp', 'max_Gp', 'mean_Gp', 'complex_viscosity']
    X = X_train[feat_cols].astype(float).values
    nbrs = NearestNeighbors(n_neighbors=5, metric='euclidean').fit(X)

    print("K=5 nearest neighbors distances (mean) for worst cases:")
    for _, r in worst.iterrows():
        sid = int(r['id'])
        # busca fila en features.csv (index id)
        try:
            row = pd.read_csv(os.path.join(PRE_DIR, "features.csv"), index_col=0).loc[sid]
        except Exception as e:
            print(f"Sim {sid}: not found in features.csv")
            continue
        x = row[feat_cols].astype(float).values.reshape(1, -1)
        dists, idxs = nbrs.kneighbors(x)
        print(f"Sim {sid}: mean_dist={float(dists.mean()):.6g}, dists={dists.flatten().tolist()}")


def rf_uncertainty_for_worst_cases():
    local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")

    if not local_dir or not os.path.isdir(local_dir):
        raise RuntimeError(
            "ERROR: No se ha definido un directorio local válido en Program Options (input_file_002)."
        )

    # Ajusta según tu estructura
    PRE_DIR = os.path.join(local_dir, "preprocessed")
    MO = os.path.join(PRE_DIR, "model_output")
    MODEL_PATH = os.path.join(MO, "rf_baseline.joblib")
    FEATURES_CSV = os.path.join(PRE_DIR, "features.csv")
    XTRAIN_CSV = os.path.join(PRE_DIR, "X_train.csv")
    WORST_CSV = os.path.join(MO, "worst_cases.csv")

    # --- Cargar objetos ---
    rf = joblib.load(MODEL_PATH)
    df_feat = pd.read_csv(FEATURES_CSV, index_col=None)
    df_xtrain_hdr = pd.read_csv(XTRAIN_CSV, nrows=0)  # solo header -> columnas usadas en X_train.csv
    df_worst = pd.read_csv(WORST_CSV)

    # --- Determinar columnas de features que el RF espera ---
    # En tus CSVs el X_train tiene columna 'id' al principio; aseguramos quitarla
    all_cols = [c for c in df_xtrain_hdr.columns if c != "id"]
    n_expected = len(all_cols)
    print(f"RandomForest espera {n_expected} features. Columnas: {all_cols}")

    # --- Para cada worst case: calcular predicción por árbol (mean, std) ---
    out = []
    for idx, rw in df_worst.iterrows():
        sid = int(rw['id'])
        try:
            df_row = build_row_for_id(sid)
        except KeyError as e:
            print(e);
            continue

        X = df_row.values  # shape (1, n_features)

        # sanity check
        if X.shape[1] != n_expected:
            raise RuntimeError(f"Dim mismatch: X has {X.shape[1]} features, expected {n_expected}")

        # predecir por árbol
        preds = np.array([est.predict(X)[0] for est in rf.estimators_])
        mean_pred = float(preds.mean())
        std_pred = float(preds.std(ddof=0))
        out.append((sid, mean_pred, std_pred, preds))

        print(f"Sim {sid}: mean_pred={mean_pred:.6f}, std_pred={std_pred:.6f}, n_trees={len(preds)}")

    # --- (Opcional) guardar resumen ---
    summary_df = pd.DataFrame([{"id": a, "mean_pred": b, "std_pred": c} for a, b, c, _ in out])
    summary_csv = os.path.join(os.path.dirname(MODEL_PATH), "rf_per_tree_uncertainty_worst_summary.csv")
    summary_df.to_csv(summary_csv, index=False)
    print("Resumen guardado en:", summary_csv)

    # --- (Opcional) guardar preds individuales (pesado) ---
    # np.savez(os.path.join(os.path.dirname(MODEL_PATH), "rf_per_tree_preds_worst.npz"),
    #          data={str(a):d for a,_,_,d in out})
