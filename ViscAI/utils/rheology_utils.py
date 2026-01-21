import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score


# ---------- helpers ----------
def safe_logspace(minv, maxv, n):
    return np.logspace(np.log10(max(minv, 1e-300)), np.log10(maxv), num=n)

def float_array(x):
    """Convierte cualquier iterable a ndarray de float, coercing con NaN para invalidos."""
    try:
        arr = np.asarray(x, dtype=float)
    except Exception:
        # fallback: use pandas to_numeric
        arr = pd.to_numeric(pd.Series(x), errors='coerce').to_numpy(dtype=float)
    return arr

def resample_log_x(x, y, x_new):
    """
    Interpola y(x) para nuevos puntos x_new.
    - convierte x,y a float arrays
    - filtra por x>0 y valores finitos
    - interpola lineal en log10(x) vs y (y en escala lineal)
    - rellena NaNs con 0
    """
    x = float_array(x)
    y = float_array(y)

    # validar
    mask = np.isfinite(x) & np.isfinite(y) & (x > 0)
    if mask.sum() < 2:
        # no hay suficientes puntos válidos: devolver ceros
        return np.zeros_like(x_new, dtype=float)

    xs = x[mask]
    ys = y[mask]

    # ordenar por xs (por si vienen desordenados)
    order = np.argsort(xs)
    xs = xs[order]
    ys = ys[order]

    # log-scale interpolation on x axis
    logx = np.log10(xs)
    logx_new = np.log10(np.where(x_new > 0, x_new, 1e-300))

    # interp (left/right -> fill with nan)
    y_new = np.interp(logx_new, logx, ys, left=np.nan, right=np.nan)

    # replace NaNs: prefer nearest valid value, otherwise 0
    if np.all(np.isnan(y_new)):
        return np.zeros_like(y_new, dtype=float)
    # fill edge NaNs with nearest valid using forward/backward fill
    nan_idx = np.isnan(y_new)
    if nan_idx.any():
        # fill forward/backward using nearest neighbor approach
        valid_idx = np.where(~np.isnan(y_new))[0]
        for idx in np.where(nan_idx)[0]:
            # find nearest valid index
            nearest = valid_idx[np.argmin(np.abs(valid_idx - idx))]
            y_new[idx] = y_new[nearest]
    # still any NaN -> set 0
    y_new = np.where(np.isnan(y_new), 0.0, y_new)
    return y_new


def load_npy(path):
    # LOad saved splits
    return np.load(path, allow_pickle=True)


def metrics_report(y_true_log, y_pred_log):
    """Return dict with metrics in log-space and original-space (back-transform)."""
    # log-space metrics (y are log10(target))
    mse_log = mean_squared_error(y_true_log, y_pred_log)
    rmse_log = mse_log ** 0.5
    mae_log = mean_absolute_error(y_true_log, y_pred_log)
    r2 = r2_score(y_true_log, y_pred_log)

    # back to linear space
    y_true_lin = 10 ** y_true_log
    y_pred_lin = 10 ** y_pred_log
    mse_lin = mean_squared_error(y_true_lin, y_pred_lin)
    rmse_lin = mse_lin ** 0.5
    mae_lin = mean_absolute_error(y_true_lin, y_pred_lin)

    return {
        "rmse_log": float(rmse_log),
        "mae_log": float(mae_log),
        "r2_log": float(r2),
        "rmse_lin": float(rmse_lin),
        "mae_lin": float(mae_lin)
    }


def rmse(a, b): return np.sqrt(mean_squared_error(a, b))


def mae(a, b): return mean_absolute_error(a, b)


def ci(arr, alpha=0.05):
    lo = np.percentile(arr, 100 * alpha / 2)
    hi = np.percentile(arr, 100 * (1 - alpha / 2))
    return float(lo), float(hi), float(np.mean(arr))


# ----------------------- Helpers robustos -----------------------
def safe_minmax(series):
    """
    Acepta: list, pandas Series, numpy array.
    Devuelve (min, max) como floats, o (None, None) si no hay datos válidos.
    """
    # for safety convert into pandas Series first
    s = pd.Series(series)
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return None, None
    return float(s.min()), float(s.max())

def plot_Gt(time_grid, G_t, title, outpath):
    plt.figure(figsize=(6, 4))
    plt.loglog(time_grid, np.maximum(G_t, 1e-300))  # evita ceros negativos en log
    plt.xlabel("time (s)")
    plt.ylabel("G(t)")
    plt.title(title)
    plt.grid(True, which="both", ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()

def plot_GpGpp(freq_grid, Gp, Gpp, title, outpath):
    plt.figure(figsize=(6, 4))
    plt.loglog(freq_grid, np.maximum(Gp, 1e-300), label="G' (elastic)")
    plt.loglog(freq_grid, np.maximum(Gpp, 1e-300), label="G'' (viscous)")
    plt.xlabel("frequency (rad/s)")
    plt.ylabel("G' / G''")
    plt.title(title)
    plt.legend()
    plt.grid(True, which="both", ls=":", alpha=0.6)
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()
