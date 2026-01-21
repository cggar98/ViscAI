import os, numpy as np, pandas as pd
from sklearn.model_selection import train_test_split

PRE_DIR = "/home/cgarcia/cgarcia_MOMENTUM/Programs/ViscAI/examples/local_dir/preprocessed"
FEATURES_CSV = os.path.join(PRE_DIR, "features.csv")

df = pd.read_csv(FEATURES_CSV, index_col=0)  # index = id

# --- Selecciona target (elige uno) ---
target_col = "zero_shear_viscosity"   # <-- ya has elegido A)

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

