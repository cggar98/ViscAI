# 04-train_test_fixed.py
import os
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

PRE_DIR = "/home/cgarcia/cgarcia_MOMENTUM/Programs/ViscAI/examples/local_dir/preprocessed"
FEATURES_CSV = os.path.join(PRE_DIR, "features.csv")

# Ajustes
target_col = "zero_shear_viscosity"   # <- tu target elegido
USE_LOG_EPS = True    # si True usa log10(x + eps) para evitar perder ceros
EPS = 1e-12
RND = 42

# Cargar
if not os.path.exists(FEATURES_CSV):
    raise SystemExit(f"No existe {FEATURES_CSV}")

df = pd.read_csv(FEATURES_CSV, index_col=0)
print("features.csv shape:", df.shape)
print("columns:", df.columns.tolist())

if target_col not in df.columns:
    raise SystemExit(f"ERROR: columna target '{target_col}' no encontrada en features.csv")

# asegurarse numérico
df[target_col] = pd.to_numeric(df[target_col], errors='coerce')
non_null_count = df[target_col].notna().sum()
print(f"Valores no nulos en '{target_col}': {non_null_count} / {len(df)}")

# mostrar primeras filas para diagnóstico
print("\nPrimeras filas (target y primeras 5 columnas):")
cols_show = [target_col] + df.columns.drop(target_col).tolist()[:5]
print(df[cols_show].head(10))

# construir y
if USE_LOG_EPS:
    # evita perder 0's; si prefieres eliminar ceros, cambia USE_LOG_EPS = False
    df['y'] = np.log10(df[target_col].fillna(0.0) + EPS)
else:
    df['y'] = np.log10(df[target_col].replace(0, np.nan))
    df = df.dropna(subset=['y'])

n = len(df)
print("Filas disponibles tras procesar target:", n)

if n == 0:
    raise SystemExit("No hay muestras válidas para entrenar. Revisa target_col y los valores del target (¿ceros? ¿NaN?).")

# preparar X,y
X = df.drop(columns=[target_col, 'y'])
y = df['y'].values

# Splits robustos según tamaño
if n == 1:
    print("Solo 1 muestra: la coloco en TRAIN, no hay val/test.")
    X_train = X.copy(); y_train = y.copy()
    X_val = X_test = pd.DataFrame(columns=X.columns); y_val = y_test = np.array([])
elif n == 2:
    print("2 muestras: split 50/50 (1 train, 1 test), sin validation.")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.5, random_state=RND)
    X_val, y_val = pd.DataFrame(columns=X.columns), np.array([])
else:
    # n >= 3: usar 70/15/15
    X_train, X_tmp, y_train, y_tmp = train_test_split(X, y, test_size=0.30, random_state=RND)
    X_val, X_test, y_val, y_test = train_test_split(X_tmp, y_tmp, test_size=0.5, random_state=RND)

OUT = PRE_DIR
X_train.to_csv(os.path.join(OUT, "X_train.csv"), index=True)
X_val.to_csv(os.path.join(OUT, "X_val.csv"), index=True)
X_test.to_csv(os.path.join(OUT, "X_test.csv"), index=True)
np.save(os.path.join(OUT, "y_train.npy"), y_train)
np.save(os.path.join(OUT, "y_val.npy"), y_val)
np.save(os.path.join(OUT, "y_test.npy"), y_test)

print("Saved splits to:", OUT)
print("Sizes -> train:", len(y_train), "val:", len(y_val), "test:", len(y_test))

