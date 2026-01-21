import sqlite3, csv, os

DB_PATH = "/home/cgarcia/cgarcia_MOMENTUM/Programs/ViscAI/examples/local_dir/viscai_database.db"   # <--- ajustar
BACKUP = DB_PATH + ".bak"
if not os.path.exists(BACKUP):
    import shutil
    shutil.copy(DB_PATH, BACKUP)
    print("Backup creado:", BACKUP)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# listar tablas
cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [r[0] for r in cur.fetchall()]
print("Tablas:", tables)

# esquema y muestras
for t in tables:
    print("\n--- Tabla:", t, "---")
    cur.execute(f"PRAGMA table_info('{t}')")
    cols = cur.fetchall()
    print("Columnas:", cols)
    cur.execute(f"SELECT COUNT(*) FROM '{t}'")
    print("Filas:", cur.fetchone()[0])
    cur.execute(f"SELECT * FROM '{t}' LIMIT 5")
    rows = cur.fetchall()
    for row in rows:
        print(row)

conn.close()

