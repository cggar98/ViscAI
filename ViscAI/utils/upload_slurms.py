# File: ViscAI/utils/upload_slurms.py
# ****************************CAMBIO*************
# Versión actualizada: correcciones SBATCH nodes/ntasks y evitar '-c polyconf.dat' indeseado.
# ****************************CAMBIO*************

import os
import stat
import tempfile
from typing import List, Tuple, Optional

import streamlit as st

from ViscAI.utils.ssh_connection import connect_remote_server

# ---------------------------
# Helpers internos
# ---------------------------

def _ensure_remote_dir(sftp, remote_path: str):
    parts = remote_path.strip("/").split("/")
    cur = ""
    for p in parts:
        cur = f"{cur}/{p}" if cur else f"/{p}"
        try:
            sftp.stat(cur)
        except IOError:
            try:
                sftp.mkdir(cur)
            except Exception:
                pass

def _write_remote_file(sftp, remote_path: str, content: str, mode: int = 0o750):
    parent = os.path.dirname(remote_path)
    try:
        _ensure_remote_dir(sftp, parent)
    except Exception:
        pass
    try:
        with sftp.open(remote_path, "w") as rf:
            rf.write(content)
        try:
            sftp.chmod(remote_path, mode)
        except Exception:
            pass
        return True, ""
    except Exception as e:
        return False, str(e)

def _find_dat_in_remote_dir(sftp, remote_dir: str, base_name: Optional[str] = None) -> Optional[str]:
    """
    Busca un fichero .dat plausible en remote_dir.
    Preferencias:
      1) fichero que contenga 'MW' y 'D' y 'PDI' en el nombre
      2) fichero que contenga base_name (si se pasa)
      3) el primer .dat encontrado
    Devuelve el nombre relativo (basename) o None.
    """
    try:
        files = sftp.listdir(remote_dir)
    except Exception:
        return None
    dats = [f for f in files if f.lower().endswith(".dat")]
    if not dats:
        return None
    # Prefer those having MW and D and PDI pattern
    for d in dats:
        low = d.lower()
        if "mw_" in low and "d" in low and "pdi" in low:
            return d
    if base_name:
        for d in dats:
            if base_name in d:
                return d
    return dats[0]

# ****************************CAMBIO*************
# Función pública exportada: ahora crea slurm.sh únicamente en subdirs combinados (D & PDI)
def _slurm_submit_multiple_mw(
    name_server: str,
    name_user: str,
    ssh_key_options: str,
    working_dir: str,
    mw_list: List[float],
    input_file: Optional[str] = None,
    polymer_file: Optional[str] = None,
    submit: bool = False,
    partition: Optional[str] = None,
    nodes: Optional[int] = None,
    cpus_per_task: Optional[int] = None,
    mem_per_cpu_mb: Optional[int] = None,
    job_name_prefix: str = "BoBjob"
) -> List[Tuple[str, str, str]]:
    """
    Crea (y opcionalmente encola) scripts SLURM 'slurm.sh' solo en subdirectorios
    del tipo 'Mw_<...>__D<...>__PDI_<...>' ya existentes en working_dir.
    """
    results: List[Tuple[str, str, str]] = []

    bob_remote_fullpath = st.session_state.get("bob_remote_fullpath")

    if not bob_remote_fullpath:
        results.append(("GLOBAL", "ERROR", "BoB executable path not set"))
        return results

    # obtener flags globales (batch / generate_polymers) desde session_state
    batch_flag = "-b" if st.session_state.get("batch_mode", False) else ""
    genpoly_flag = "-p" if st.session_state.get("generate_polymers", False) else ""

    # ****************************CAMBIO*************
    # Prioridad: argumentos explícitos > session_state > valores por defecto
    if partition is None:
        partition = st.session_state.get("slurm_partition", "")
    if nodes is None:
        try:
            nodes = int(st.session_state.get("slurm_nodes", 1))
        except Exception:
            nodes = 1
    if cpus_per_task is None:
        try:
            cpus_per_task = int(st.session_state.get("slurm_cpus_per_task", 1))
        except Exception:
            cpus_per_task = 1
    if mem_per_cpu_mb is None:
        try:
            mem_raw = st.session_state.get("slurm_mem_per_cpu", 2048)
            if isinstance(mem_raw, str):
                # admite formatos tipo '2048M' o '2G'
                if mem_raw.upper().endswith("M"):
                    mem_per_cpu_mb = int(mem_raw[:-1])
                elif mem_raw.upper().endswith("G"):
                    mem_per_cpu_mb = int(float(mem_raw[:-1]) * 1024)
                else:
                    mem_per_cpu_mb = int(mem_raw)
            else:
                mem_per_cpu_mb = int(mem_raw)
        except Exception:
            mem_per_cpu_mb = 2048
    # ****************************CAMBIO*************

    # connect to server
    try:
        ssh = connect_remote_server(name_server, name_user, ssh_key_options)
        sftp = ssh.open_sftp()
    except Exception as e:
        results.append(("GLOBAL", "ERROR", f"SSH connect failed: {e}"))
        return results

    try:
        # listar subdirectorios remotos y filtrar los que contengan D__PDI
        try:
            remote_entries = sftp.listdir(working_dir)
        except Exception:
            remote_entries = []

        # ****************************CAMBIO*************
        # Sólo procesamos subdirectorios que correspondan a combinaciones
        # Ejemplo de nombre esperado: Mw_10000_0__D1__PDI_1_5
        combo_subdirs = [d for d in remote_entries
                         if d.startswith("Mw_") and "__D" in d and "__PDI" in d]

        # Si el usuario pasó mw_list: limitamos por Mw presentes en la rejilla solicitada
        if mw_list:
            mw_prefixes = set(f"Mw_{str(mw).replace('.', '_')}" for mw in mw_list)
            combo_subdirs = [d for d in combo_subdirs if any(d.startswith(prefix) for prefix in mw_prefixes)]
        # ****************************CAMBIO*************

        for sd in combo_subdirs:
            remote_subdir = os.path.join(working_dir, sd)
            sd_name = sd  # nombre del subdir: incluye D and PDI

            # localizar dat en el subdir
            base_input_name = os.path.splitext(os.path.basename(input_file or ""))[0] if input_file else None
            dat_basename = _find_dat_in_remote_dir(sftp, remote_subdir, base_input_name)

            if dat_basename:
                # construir jobname y logs basado en dat_basename (sin extensión)
                dat_base_noext = os.path.splitext(dat_basename)[0]
                job_basename = dat_base_noext
            else:
                # fallback: usar sd como jobname
                job_basename = sd_name

            # construir bob command: usar dat_basename si lo encontramos, sino fallback al input_file basename
            bob_cmd_parts = ["${BOBEXE}"]
            if dat_basename:
                bob_cmd_parts += ["-i", dat_basename]
            elif input_file:
                bob_cmd_parts += ["-i", os.path.basename(input_file)]

            # ****************************CAMBIO*************
            # NO añadir '-c' basándose en archivos generados como 'polyconf.dat' automáticos.
            # Solo añadimos '-c <poly>' si el usuario pasó `polymer_file` (program options) y
            # su basename aparece en el subdirectorio remoto.
            poly_name_in_dir = None
            try:
                files = sftp.listdir(remote_subdir)
            except Exception:
                files = []
            if polymer_file:
                polymer_basename = os.path.basename(polymer_file)
                if polymer_basename in files:
                    poly_name_in_dir = polymer_basename
            # ****************************CAMBIO*************

            if poly_name_in_dir:
                bob_cmd_parts += ["-c", poly_name_in_dir]

            # añadir flags -b/-p si están activadas
            if batch_flag:
                bob_cmd_parts.append(batch_flag)
            if genpoly_flag:
                bob_cmd_parts.append(genpoly_flag)

            bob_cmd = " ".join(bob_cmd_parts)

            # ****************************CAMBIO*************
            # Contenido del script: siguiendo tus requisitos
            # - Incluir los #SBATCH con valores nodes / ntasks tomados de arriba
            # - NO incluir '#SBATCH --time'
            # - NO redirigir la ejecución a un .log (dejamos SLURM manejar stdout/stderr)
            script_lines = [
                "#!/bin/bash",
                f"#SBATCH --partition={partition}" if partition else "#SBATCH --partition=",
                f"#SBATCH -N {nodes}",
                f"#SBATCH -n {cpus_per_task}",
                f"#SBATCH --mem-per-cpu={mem_per_cpu_mb}M",
                f"#SBATCH --job-name={job_basename}\n",
                "echo \"Job ${SLURM_JOB_ID} started: `date`\"\n",
                "WK=`pwd`\n",
                "# Move to simulation directory",
                f"cd {remote_subdir}\n",
                "# BoB remote fullpath",
                f"BOBEXE={bob_remote_fullpath}\n",
                "# BoB execution",
                f"{bob_cmd}\n",
                "echo \"Job ${SLURM_JOB_ID} ended: `date`\"\n",
                "echo \"Job Done\""
            ]
            # ****************************CAMBIO*************
            script_text = "\n".join(script_lines)

            # Escribir slurm.sh remoto
            remote_script_path = os.path.join(remote_subdir, "slurm.sh")
            ok, msg = _write_remote_file(sftp, remote_script_path, script_text, mode=0o750)
            if not ok:
                results.append((sd_name, "ERROR", f"Failed to write slurm.sh: {msg}"))
                continue

            # si se pidió submit, hacemos sbatch
            if submit:
                try:
                    stdin, stdout, stderr = ssh.exec_command(f"cd {remote_subdir} && sbatch {os.path.basename(remote_script_path)}")
                    rc = stdout.channel.recv_exit_status()
                    out = stdout.read().decode().strip()
                    err = stderr.read().decode().strip()
                    if rc == 0:
                        results.append((sd_name, "OK", f"sbatch output: {out or 'submitted'}"))
                    else:
                        results.append((sd_name, "ERROR", f"sbatch failed: {err or out}"))
                except Exception as e:
                    results.append((sd_name, "ERROR", f"sbatch exception: {e}"))
            else:
                results.append((sd_name, "OK", f"slurm.sh created at {remote_script_path}"))

    finally:
        try:
            sftp.close()
        except Exception:
            pass
        try:
            ssh.close()
        except Exception:
            pass

    return results
# ****************************CAMBIO*************

# EOF
