
# utils/gnu_creations.py
import os
import streamlit as st
from ViscAI.utils.ssh_connection import connect_remote_server

def gnu_modulus_generation(name_server: str,
                           name_user: str,
                           ssh_key_options: str,
                           working_directory: str) -> None:
    """
    Generate 'modulus.gnu' with G(t), G'(ω) and G''(ω) graphics
    """
    # modulus.gnu script content
    modulus_script = """\
reset
##############################################
set term qt 1 enhanced dashed size 500,400 font "Arial,10"
set encoding iso_8859_1
set encoding utf8
f1="./gt.dat"
# 'gt.dat' figure
set xlabel "t(s)"
set ylabel "G(t) (Pa)"
set logscale xy
# set xrange [1e-3:1e3]
set format x "10^{%L}"
p f1 u 1:2 w l ls 1 lc rgb "black" lw 2.0 title "G(t)",
##############################################
set term qt 2 enhanced dashed size 500,400 font "Arial,10"
set encoding iso_8859_1
set encoding utf8
f1="./gtp.dat"
# 'gtp.dat' figure
set xlabel "ω (s^{-1})"
set ylabel "G (Pa)"
set logscale xy
set format x "10^{%L}"
set format y "10^{%L}"
set key top left
p f1 u 1:2 w l ls 1 lc rgb "red" lw 2.0 title "G'(ω)", f1 u 1:3 w l ls 1 lc rgb "blue" lw 2.0 title "G''(ω)"
"""
    # Create 'modulus.gnu' locally
    local_path = os.path.join(os.getcwd(), "modulus.gnu")
    try:
        with open(local_path, "w") as f:
            f.write(modulus_script)
    except Exception as e:
        st.error(f"ERROR!!! 'modulus.gnu' not created locally: {e}")
        return

    # Upload 'modulus.gnu' to remote server
    try:
        ssh = connect_remote_server(name_server, name_user, ssh_key_options)
        sftp = ssh.open_sftp()
        remote_path = os.path.join(working_directory, "modulus.gnu")
        sftp.put(local_path, remote_path)
        sftp.close()
        ssh.close()
    except Exception as e:
        st.error(f"ERROR!!! Upload of 'modulus.gnu' failed: {e}")
    finally:
        # Remove local copy
        if os.path.exists(local_path):
            os.remove(local_path)


def gnu_gpclssys_generation(name_server: str, name_user: str, ssh_key_options: str,
                            working_directory: str) -> bool:
    """
    If CalcGPCLS = yes in remote bob.rc parameters, generate:
    - a gpclssys.gnu that plots the chosen gpcls*.dat (or gpclssys.dat).
    Returns True if gpclssys.gnu was created and uploaded, False otherwise.
    """
    try:
        ssh = connect_remote_server(name_server, name_user, ssh_key_options)
        sftp = ssh.open_sftp()

        # Read remote bob.rc (decodificar si viene en bytes)
        try:
            with sftp.open(f"{working_directory}/bob.rc", 'r') as f:
                bobrc_raw = f.read()
            if isinstance(bobrc_raw, bytes):
                bobrc = bobrc_raw.decode('utf-8', errors='ignore')
            else:
                bobrc = bobrc_raw
        except Exception:
            # No hay bob.rc -> nada que hacer
            try: sftp.close(); ssh.close()
            except Exception: pass
            st.info(f"No bob.rc found in {working_directory} -> skipping gpclssys.gnu")
            return False

        # Aceptar 'CalcGPCLS = yes' o 'CalcGPCLS=yes' (tolerante a espacios)
        if 'calcgpcls' not in bobrc.lower() or ('yes' not in bobrc.lower() and '1' not in bobrc.lower()):
            # No está activado
            try: sftp.close(); ssh.close()
            except Exception: pass
            return False

        # Determinar .dat a usar
        files = sftp.listdir(working_directory)
        if 'gpclssys.dat' in files:
            dat_name = 'gpclssys.dat'
        else:
            gpcls = [fn for fn in files if fn.lower().startswith('gpcls') and fn.lower().endswith('.dat')]
            if not gpcls:
                st.info(f"No gpcls*.dat in {working_directory} -> skipping gpclssys.gnu")
                try: sftp.close(); ssh.close()
                except Exception: pass
                return False
            dat_name = sorted(gpcls)[0]

        # Construir contenido .gnu (texto)
        gnu_content = f"""\
reset
##############################################
set term qt 1 enhanced dashed size 500,400 font "Arial,10"
set encoding iso_8859_1
set encoding utf8
f1="./{dat_name}"
# Plot P[log(M)] vs Mass
set logscale x
set xlabel "M (g/mol)"
set ylabel "P [log(M)]"
p f1 using 1:2 with points pointtype 7 pointsize 1 title "P [log(M)]"
# Plot nbr vs Mass
set term qt 2 enhanced dashed size 500,400 font "Arial,10"
set logscale x
set xlabel "M (g/mol)"
set ylabel "n_{{br}} / 500 monomer"
p f1 using 1:3 with points pointtype 7 pointsize 1 title "n_{{br}}"
# Plot radius of gyration vs Mass
set term qt 3 enhanced dashed size 500,400 font "Arial,10"
set logscale x
set xlabel "M (g/mol)"
set ylabel "g = (Rg_br / Rg_lin)^2"
p f1 using 1:4 with points pointtype 7 pointsize 1 title "g"
"""

        # Guardar .gnu en temporal (texto, codificado correctamente)
        import tempfile
        local_gnu = os.path.join(tempfile.gettempdir(), f"gpclssys_{os.path.basename(working_directory)}.gnu")
        with open(local_gnu, "w", encoding="utf-8") as fg:
            fg.write(gnu_content)

        # Subir al remoto (sftp.put espera rutas, así que strings están bien)
        try:
            sftp.put(local_gnu, f"{working_directory}/gpclssys.gnu")
        except Exception as e:
            st.warning(f"Failed to upload gpclssys.gnu to {working_directory}: {e}")
            try: os.remove(local_gnu)
            except Exception: pass
            try: sftp.close(); ssh.close()
            except Exception: pass
            return False

        # Limpieza local
        try: os.remove(local_gnu)
        except Exception:
            pass

        sftp.close()
        ssh.close()
        return True

    except Exception as e:
        # Capturar cualquier otro error y cerrrar conexiones
        try:
            sftp.close()
        except Exception:
            pass
        try:
            ssh.close()
        except Exception:
            pass
        st.error(f"ERROR during the creation of gpclssys.gnu!!!: {e}")
        return False



# =============================================================================
# ***NEWWW*** NUEVA VERSIÓN: xtics explícitos + rotación + sin mxtics
# =============================================================================
def gnu_viscosity_vs_mw_generation(name_server: str,
                                   name_user: str,
                                   ssh_key_options: str,
                                   working_directory: str) -> None:
    """
    Genera 'viscosity_mw.gnu' que lee './01-simulation.csv' en el servidor remoto y dibuja:
    - η0 (zero-shear viscosity) vs Mw
    - η* (complex viscosity) vs Mw
    Además, construye 'set xtics (...)' con los Mw del CSV para que se muestren como números
    en el eje X (log), rota las etiquetas y elimina mxtics en X para mejorar legibilidad.
    """
    try:
        # Creamos el .gnu localmente
        local_gnu = os.path.join(os.getcwd(), "viscosity_mw.gnu")

        # ***NEWWW*** leer '01-simulation.csv' del servidor y preparar xtics
        ssh = connect_remote_server(name_server, name_user, ssh_key_options)
        sftp = ssh.open_sftp()
        remote_csv = os.path.join(working_directory, "01-simulation.csv")
        mw_values = []
        try:
            with sftp.open(remote_csv, "r") as rf:
                header = rf.readline()  # saltar cabecera
                for raw in rf:
                    line = raw.decode("utf-8", errors="ignore").strip()
                    if not line:
                        continue
                    parts = line.split(",")
                    if len(parts) >= 2:
                        try:
                            mw = float(parts[1])
                            if mw > 0:
                                mw_values.append(mw)
                        except Exception:
                            pass
        except Exception:
            mw_values = []

        # deduplicar y ordenar
        mw_values = sorted(set(mw_values))

        # Construimos la cadena de xtics
        xtics_line = ""
        xrange_line = ""
        if mw_values:
            xtics_elems = []
            for mw in mw_values:
                label = f"{int(mw)}" if float(mw).is_integer() else f"{mw:g}"
                xtics_elems.append(f"\"{label}\" {mw:g}")
            xtics_line = "set xtics (" + ", ".join(xtics_elems) + ")"
            # rotar y traer al frente; quitar mxtics en X
            xtics_line += "\nset xtics rotate by 45 font \",9\" front"
            xtics_line += "\nunset mxtics"
            # (opcional) acotar rango
            xmin = mw_values[0]
            xmax = mw_values[-1]
            xrange_line = f"set xrange [{xmin:g}:{xmax:g}]"
        sftp.close()
        ssh.close()

        # Contenido del script Gnuplot
        gnu_content = f"""\
reset
##############################################
# Gráfica de viscosidades vs peso molecular a partir de CSV
set term qt enhanced dashed size 900,500 font "Arial,10"  # ***NEWWW*** más ancho para etiquetas
set encoding utf8
set datafile separator ","
set key top left
set xlabel "Peso molecular, Mw (g/mol)"
set ylabel "Viscosidad (Pa·s)"
# Escalas y formato del eje X
set logscale xy
set format x "%g"
{ (xrange_line if xrange_line else "# (sin xrange explícito)") }
{ (xtics_line if xtics_line else "# (sin xtics explícitos: se mostrarán solo potencias de 10)") }
# Archivo CSV en el directorio de trabajo remoto (subido por ViscAI)
f = "./01-simulation.csv"
# Saltamos la cabecera con 'every ::1' y trazamos:
# Columnas: id, molecular_weight (2), pdi (3), distribution_label (4), zero_shear_viscosity (5), complex_viscosity (6)
plot f every ::1 using 2:5 with linespoints lw 2 pt 7 lc rgb "red" title "η0 (Pa·s)", \\
     f every ::1 using 2:6 with linespoints lw 2 pt 7 lc rgb "blue" title "η* (Pa·s)"
"""
        # Guardamos el script local y lo subimos al servidor
        with open(local_gnu, "w") as fg:
            fg.write(gnu_content)
        ssh = connect_remote_server(name_server, name_user, ssh_key_options)
        sftp = ssh.open_sftp()
        remote_gnu = os.path.join(working_directory, "viscosity_mw.gnu")
        sftp.put(local_gnu, remote_gnu)
        # Limpieza local
        try:
            os.remove(local_gnu)
        except Exception:
            pass
        sftp.close()
        ssh.close()
    except Exception as e:
        st.error(f"ERROR!!! No se pudo crear/subir 'viscosity_mw.gnu': {e}")


# =============================================================================
# ***NEWWW*** Script único con tres ventanas (actualizado Ventana 3)
def gnu_viscosity_summary_generation(name_server: str,
                                     name_user: str,
                                     ssh_key_options: str,
                                     working_directory: str) -> None:
    """
    Crea 'viscosity_summary.gnu' en local y lo sube al 'working_directory'.
    Ventana 1: η0 y η* vs Mw (log-log)
    Ventana 2: η0 y η* vs PDI (x lineal, y log)
    Ventana 3: Barras por tipo de distribución (medias), usando '03-viscosity_by_distribution.csv'
               (columnas: distribution_label, zero_shear_viscosity_mean, complex_viscosity_mean)
    """
    try:
        local_gnu = os.path.join(os.getcwd(), "viscosity_summary.gnu")
        gnu_content = """\
reset
set encoding utf8
set datafile separator ","

# --- Ventana 1: viscosidad vs Mw (log-log) ---
set term qt 1 enhanced size 800,480 font "Arial,10"
set key top left
set xlabel "Peso molecular, Mw (g/mol)"
set ylabel "Viscosidad (Pa·s)"
set logscale xy
set format x "%g"
set mxtics 10
f1 = "./01-simulation.csv"
# Columnas (nuevo orden): id, mw(2), pdi(3), dist_label(4), eta0(5), eta*(6)
plot f1 using 2:5 with linespoints lw 2 pt 7 lc rgb "red" title "η0 (Pa·s)", \\
     f1 using 2:6 with linespoints lw 2 pt 7 lc rgb "blue" title "η* (Pa·s)"

# --- Ventana 2: viscosidad vs PDI (y log, x lineal) ---
set term qt 2 enhanced size 800,480 font "Arial,10"
set key top left
set xlabel "PDI"
set ylabel "Viscosidad (Pa·s)"
unset logscale x
set logscale y
set grid ytics ls 1
f2 = "./01-simulation.csv"
plot f2 using 3:5 with points pt 7 lc rgb "red" title "η0 (Pa·s)", \\
     f2 using 3:6 with points pt 7 lc rgb "blue" title "η* (Pa·s)"

# --- Ventana 3: barras por distribución (medias) ---
set term qt 3 enhanced size 800,480 font "Arial,10"
set key outside
set style data histograms
set style fill solid 0.7 border -1
set boxwidth 0.8
set xlabel "Distribución"
set ylabel "Viscosidad media (Pa·s)"
unset logscale x
set logscale y
set grid ytics ls 1
# Nuevo CSV (3 columnas): distribution_label, zero_shear_viscosity_mean, complex_viscosity_mean
f3 = "./03-viscosity_by_distribution.csv"
plot f3 using 2:xtic(1) title "η0 media", \\
     f3 using 3 title "η* media"
"""
        with open(local_gnu, "w") as fg:
            fg.write(gnu_content)
        ssh = connect_remote_server(name_server, name_user, ssh_key_options)
        sftp = ssh.open_sftp()
        sftp.put(local_gnu, os.path.join(working_directory, "viscosity_summary.gnu"))
        try:
            os.remove(local_gnu)
        except Exception:
            pass
        sftp.close()
        ssh.close()
    except Exception as e:
        st.error(e)
