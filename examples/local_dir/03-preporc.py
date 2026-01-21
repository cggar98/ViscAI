import os
import numpy as np
import pandas as pd

# Ajusta según tu estructura
PRE_DIR = "/home/cgarcia/cgarcia_MOMENTUM/Programs/ViscAI/examples/local_dir/preprocessed"
SIM_CSV = os.path.join(PRE_DIR, "simulation_clean.csv")
REL_CSV = os.path.join(PRE_DIR, "relaxation_clean.csv")
DYN_CSV = os.path.join(PRE_DIR, "dynamic_clean.csv")

OUT_DIR = PRE_DIR  # guarda los outputs aquí
os.makedirs(OUT_DIR, exist_ok=True)

# Parámetros remuestreo
N_TIME = 100
N_FREQ = 100
TIME_MIN = 1e-12   # mínimos de seguridad si hay ceros o tiempos extremados
TIME_MAX = 1e6
FREQ_MIN = 1e-6
FREQ_MAX = 1e6

# ---------- helpers ----------
def safe_logspace(minv, maxv, n):
    return np.logspace(np.log10(max(minv, 1e-300)), np.log10(maxv), num=n)

def to_float_array(x):
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
    x = to_float_array(x)
    y = to_float_array(y)

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
Gp_all  = np.zeros((len(sim_ids), N_FREQ), dtype=float)
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
        t = to_float_array(sub_rel['time'].values)
    if 'G_t' in sub_rel.columns:
        G = to_float_array(sub_rel['G_t'].values)
    elif 'modulu' in sub_rel.columns:
        G = to_float_array(sub_rel['modulu'].values)
    else:
        G = np.array([])

    G_t_resampled = resample_log_x(t, G, time_grid)
    G_t_all[i, :] = G_t_resampled

    # DYNAMIC -> frequency, G' (G_prime), G'' (G_double_prime)
    sub_dyn = df_dyn[df_dyn['simulation_id'] == sid]
    if 'frequency' not in sub_dyn.columns:
        freq = np.array([])
    else:
        freq = to_float_array(sub_dyn['frequency'].values)

    if 'G_prime' in sub_dyn.columns and 'G_double_prime' in sub_dyn.columns:
        Gp = to_float_array(sub_dyn['G_prime'].values)
        Gpp = to_float_array(sub_dyn['G_double_prime'].values)
    else:
        Gp = to_float_array(sub_dyn.get('elastic_modulu', np.array([])))
        Gpp = to_float_array(sub_dyn.get('viscous_modulu', np.array([])))

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

