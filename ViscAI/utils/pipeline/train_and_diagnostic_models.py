import shap
from sklearn.inspection import permutation_importance
from sklearn.metrics import mean_squared_error, mean_absolute_error
from joblib import load
import os
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st
from sklearn.ensemble import RandomForestRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.model_selection import GridSearchCV
from ViscAI.utils.rheology_utils import metrics_report, mae, rmse, ci
import joblib
import warnings


def train_baseline_models():
    warnings.filterwarnings("ignore")

    # --- Ajusta rutas ---
    local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")

    if not local_dir or not os.path.isdir(local_dir):
        raise RuntimeError(
            "ERROR: No se ha definido un directorio local válido en Program Options (input_file_002)."
        )

    # Ajusta según tu estructura
    PRE_DIR = os.path.join(local_dir, "preprocessed")
    X_TRAIN = os.path.join(PRE_DIR, "X_train.csv")
    X_VAL = os.path.join(PRE_DIR, "X_val.csv")
    X_TEST = os.path.join(PRE_DIR, "X_test.csv")
    Y_TRAIN = os.path.join(PRE_DIR, "y_train.npy")
    Y_VAL = os.path.join(PRE_DIR, "y_val.npy")
    Y_TEST = os.path.join(PRE_DIR, "y_test.npy")

    OUT_DIR = os.path.join(PRE_DIR, "model_output")
    os.makedirs(OUT_DIR, exist_ok=True)

    # --- Cargar datos ---
    X_train = pd.read_csv(X_TRAIN, index_col=0)
    X_val = pd.read_csv(X_VAL, index_col=0)
    X_test = pd.read_csv(X_TEST, index_col=0)
    y_train = np.load(Y_TRAIN, allow_pickle=True)
    y_val = np.load(Y_VAL, allow_pickle=True)
    y_test = np.load(Y_TEST, allow_pickle=True)

    # --- Baseline 1: RandomForestRegressor (fast, robust) ---
    print("Training RandomForest baseline...")
    rf = RandomForestRegressor(n_estimators=200, max_depth=10, random_state=42, n_jobs=-1)
    rf.fit(X_train, y_train)
    y_val_pred = rf.predict(X_val)
    val_metrics = metrics_report(y_val, y_val_pred)
    print("Validation metrics (RF):", val_metrics)

    # Optional: quick grid search on a tiny grid (uncomment if you want tuning; can be slow)
    DO_GRID = False
    if DO_GRID:
        print("Running GridSearchCV (this may take a while)...")
        param_grid = {
            "n_estimators": [100, 200],
            "max_depth": [8, 12],
            "min_samples_leaf": [1, 3]
        }
        gs = GridSearchCV(RandomForestRegressor(random_state=42, n_jobs=-1),
                          param_grid, cv=3, scoring="neg_mean_squared_error", verbose=1)
        gs.fit(X_train, y_train)
        rf = gs.best_estimator_
        print("Best RF params:", gs.best_params_)
        y_val_pred = rf.predict(X_val)
        val_metrics = metrics_report(y_val, y_val_pred)
        print("Validation metrics (RF tuned):", val_metrics)

    # Save RF model and validation metrics
    joblib.dump(rf, os.path.join(OUT_DIR, "rf_baseline.joblib"))
    with open(os.path.join(OUT_DIR, "rf_val_metrics.json"), "w") as f:
        json.dump(val_metrics, f, indent=2)

    # --- Evaluate on test set ---
    y_test_pred = rf.predict(X_test)
    test_metrics = metrics_report(y_test, y_test_pred)
    print("Test metrics (RF):", test_metrics)
    with open(os.path.join(OUT_DIR, "rf_test_metrics.json"), "w") as f:
        json.dump(test_metrics, f, indent=2)

    # --- Baseline 2: Simple MLP (requires scaled features; your features are Z-scored already) ---
    print("Training MLP baseline (quick)...")
    mlp = MLPRegressor(hidden_layer_sizes=(128, 64), max_iter=1000, random_state=42)
    mlp.fit(X_train, y_train)
    y_val_mlp = mlp.predict(X_val)
    mlp_val_metrics = metrics_report(y_val, y_val_mlp)
    print("Validation metrics (MLP):", mlp_val_metrics)

    # Save MLP model and metrics
    joblib.dump(mlp, os.path.join(OUT_DIR, "mlp_baseline.joblib"))
    with open(os.path.join(OUT_DIR, "mlp_val_metrics.json"), "w") as f:
        json.dump(mlp_val_metrics, f, indent=2)

    # Evaluate MLP on test
    y_test_mlp = mlp.predict(X_test)
    mlp_test_metrics = metrics_report(y_test, y_test_mlp)
    print("Test metrics (MLP):", mlp_test_metrics)
    with open(os.path.join(OUT_DIR, "mlp_test_metrics.json"), "w") as f:
        json.dump(mlp_test_metrics, f, indent=2)

    # --- Save predictions (log and linear) for analysis ---
    np.savez_compressed(os.path.join(OUT_DIR, "predictions_rf.npz"),
                        y_test=y_test, y_test_pred=y_test_pred,
                        y_test_lin=10 ** y_test, y_test_pred_lin=10 ** y_test_pred)
    np.savez_compressed(os.path.join(OUT_DIR, "predictions_mlp.npz"),
                        y_test=y_test, y_test_pred=y_test_mlp,
                        y_test_lin=10 ** y_test, y_test_pred_lin=10 ** y_test_mlp)

    print("Training + evaluation complete. Outputs in:", OUT_DIR)


def model_diagnostics():
    local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")

    if not local_dir or not os.path.isdir(local_dir):
        raise RuntimeError(
            "ERROR: No se ha definido un directorio local válido en Program Options (input_file_002)."
        )

    # Ajusta según tu estructura
    PRE_DIR = os.path.join(local_dir, "preprocessed")
    OUT = os.path.join(PRE_DIR, "model_output")
    os.makedirs(OUT, exist_ok=True)

    # Load data
    X_test = pd.read_csv(os.path.join(PRE_DIR, "X_test.csv"), index_col=0)
    y_test = np.load(os.path.join(PRE_DIR, "y_test.npy"), allow_pickle=True)

    # Load RF model + predictions
    rf = joblib.load(os.path.join(OUT, "rf_baseline.joblib"))
    preds = np.load(os.path.join(OUT, "predictions_rf.npz"))
    y_test_pred = preds["y_test_pred"]
    # back to linear
    y_test_lin = preds["y_test_lin"]
    y_test_pred_lin = preds["y_test_pred_lin"]

    # Parity plot (log)
    plt.figure(figsize=(5, 5))
    plt.scatter(y_test, y_test_pred, s=30, alpha=0.8)
    mn = min(np.min(y_test), np.min(y_test_pred))
    mx = max(np.max(y_test), np.max(y_test_pred))
    plt.plot([mn, mx], [mn, mx], 'k--')
    plt.xlabel("y_true (log10)")
    plt.ylabel("y_pred (log10)")
    plt.title("Parity plot (log)")
    plt.tight_layout()
    plt.savefig(os.path.join(OUT, "parity_log.png"), dpi=150)
    plt.close()

    # Residuals (log)
    res = y_test_pred - y_test
    plt.figure(figsize=(6, 4))
    plt.hist(res, bins=30)
    plt.xlabel("Residual (pred - true) [log10]")
    plt.title("Residuals histogram (log)")
    plt.tight_layout()
    plt.savefig(os.path.join(OUT, "residuals_hist_log.png"), dpi=150)
    plt.close()

    # Parity (linear) - use small jitter if needed
    plt.figure(figsize=(5, 5))
    plt.scatter(y_test_lin, y_test_pred_lin, s=30, alpha=0.8)
    mn = min(y_test_lin.min(), y_test_pred_lin.min())
    mx = max(y_test_lin.max(), y_test_pred_lin.max())
    plt.plot([mn, mx], [mn, mx], 'k--')
    plt.xscale('log');
    plt.yscale('log')
    plt.xlabel("y_true (linear)")
    plt.ylabel("y_pred (linear)")
    plt.title("Parity plot (linear, log-log)")
    plt.tight_layout()
    plt.savefig(os.path.join(OUT, "parity_lin_loglog.png"), dpi=150)
    plt.close()

    # Feature importances (RF)
    if hasattr(rf, "feature_importances_"):
        fi = pd.Series(rf.feature_importances_, index=X_test.columns).sort_values(ascending=False)
        fi.head(20).to_csv(os.path.join(OUT, "rf_feature_importances.csv"))
        plt.figure(figsize=(6, 4))
        fi.head(20).plot(kind='bar')
        plt.title("Top 20 feature importances (RF)")
        plt.tight_layout()
        plt.savefig(os.path.join(OUT, "rf_feature_importances.png"), dpi=150)
        plt.close()

    # Error by PDI and distribution (grouped)
    # Necesitamos features originales para agrupar
    X_all = pd.read_csv(os.path.join(PRE_DIR, "X_test.csv"), index_col=0)
    # if distribution one-hot, find column names
    dist_cols = [c for c in X_all.columns if c.startswith("dist_")]
    group_col = None
    if dist_cols:
        # reconstruct distribution label
        dist_series = X_all[dist_cols].idxmax(axis=1).str.replace("dist_", "")
        group_col = dist_series
    else:
        group_col = X_all['pdi'] if 'pdi' in X_all.columns else None

    df_err = pd.DataFrame({
        "y_true_log": y_test,
        "y_pred_log": y_test_pred,
        "y_true_lin": y_test_lin,
        "y_pred_lin": y_test_pred_lin
    }, index=X_all.index)

    if group_col is not None:
        df_err['group'] = group_col.values
        # compute MAE per group in linear space
        summary = df_err.assign(abs_err_lin=(df_err.y_pred_lin - df_err.y_true_lin).abs()).groupby('group').agg(
            mae_lin=('abs_err_lin', 'mean'),
            count=('abs_err_lin', 'size')
        ).sort_values('mae_lin', ascending=False)
        summary.to_csv(os.path.join(OUT, "error_by_group.csv"))

    print("Diagnostics generated in:", OUT)


def bootstrap_metric():
    local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")

    if not local_dir or not os.path.isdir(local_dir):
        raise RuntimeError(
            "ERROR: No se ha definido un directorio local válido en Program Options (input_file_002)."
        )

    # Ajusta según tu estructura
    PRE_DIR = os.path.join(local_dir, "preprocessed")
    OUT = os.path.join(PRE_DIR, "model_output")
    rf = load(os.path.join(OUT, "rf_baseline.joblib"))

    # carga test
    y_test = np.load(os.path.join(PRE_DIR, "y_test.npy"), allow_pickle=True)
    preds = np.load(os.path.join(OUT, "predictions_rf.npz"))
    y_pred = preds["y_test_pred"]  # log-space

    R = 2000  # repeticiones bootstrap
    rng = np.random.default_rng(42)
    rmse_log_samples = []
    mae_log_samples = []
    rmse_lin_samples = []
    mae_lin_samples = []

    y_test_lin = 10 ** y_test
    y_pred_lin = 10 ** y_pred

    n = len(y_test)
    for _ in range(R):
        idx = rng.integers(0, n, n)  # bootstrap indices
        yt = y_test[idx];
        yp = y_pred[idx]
        rmse_log_samples.append(rmse(yt, yp))
        mae_log_samples.append(mae(yt, yp))
        rmse_lin_samples.append(rmse(10 ** yt, 10 ** yp))
        mae_lin_samples.append(mae(10 ** yt, 10 ** yp))

    out = {
        "rmse_log_ci": ci(rmse_log_samples),
        "mae_log_ci": ci(mae_log_samples),
        "rmse_lin_ci": ci(rmse_lin_samples),
        "mae_lin_ci": ci(mae_lin_samples)
    }

    with open(os.path.join(OUT, "bootstrap_metrics_ci.json"), "w") as f:
        json.dump(out, f, indent=2)

    print("Bootstrap done. Results saved:", os.path.join(OUT, "bootstrap_metrics_ci.json"))


def compute_permutation_importance():
    local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")

    if not local_dir or not os.path.isdir(local_dir):
        raise RuntimeError(
            "ERROR: No se ha definido un directorio local válido en Program Options (input_file_002)."
        )

    # Ajusta según tu estructura
    PRE_DIR = os.path.join(local_dir, "preprocessed")
    OUT = os.path.join(PRE_DIR, "model_output")
    rf = load(os.path.join(OUT, "rf_baseline.joblib"))

    X_test = pd.read_csv(os.path.join(PRE_DIR, "X_test.csv"), index_col=0)
    y_test = np.load(os.path.join(PRE_DIR, "y_test.npy"), allow_pickle=True)

    res = permutation_importance(rf, X_test, y_test, n_repeats=30, random_state=42, n_jobs=-1,
                                 scoring='neg_mean_squared_error')

    imp_df = pd.DataFrame({
        "feature": X_test.columns,
        "importance_mean": res.importances_mean,
        "importance_std": res.importances_std
    }).sort_values("importance_mean", ascending=False)

    imp_df.to_csv(os.path.join(OUT, "permutation_importance.csv"), index=False)
    print("Permutation importance saved:", os.path.join(OUT, "permutation_importance.csv"))


def save_shap_summary():
    local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")

    if not local_dir or not os.path.isdir(local_dir):
        raise RuntimeError(
            "ERROR: No se ha definido un directorio local válido en Program Options (input_file_002)."
        )

    # Ajusta según tu estructura
    PRE_DIR = os.path.join(local_dir, "preprocessed")
    OUT = os.path.join(PRE_DIR, "model_output")

    # Cargar modelo una sola vez
    rf = joblib.load(os.path.join(OUT, "rf_baseline.joblib"))
    model = rf  # <-- aquí usamos la referencia

    X = pd.read_csv(os.path.join(PRE_DIR, "X_test.csv"), index_col=0)
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X)
    shap.summary_plot(shap_values, X, show=False)

    # Guardar la figura
    plt.savefig(os.path.join(OUT, "shap_summary.png"), bbox_inches="tight", dpi=300)
    plt.close()
