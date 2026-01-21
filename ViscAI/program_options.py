#   TEST

# ViscAI/program_options.py
import streamlit as st
import os
import stat
import tempfile

from ViscAI.slurm_adapter import SlurmAdapter
from ViscAI.ViscAI_exec import build_viscai_command, execute_remote_process
from ViscAI.utils.db_SQLite import database_db_creation
from ViscAI.utils.gnu_creations import gnu_modulus_generation, gnu_gpclssys_generation
from ViscAI.utils.clean_files import clean_remote_directory
from ViscAI.utils.bob_rc_transfer import bob_rc_transfering
from ViscAI.utils.ssh_connection import connect_remote_server
from ViscAI.utils.get_conda_path import get_conda_sh_path
from ViscAI.utils.pipeline.database_preprocessed import database_inspection, preprocess_database, build_resampled_rheology_features
from ViscAI.utils.pipeline.training_preparation import prepare_rheology_dataset, validate_splits
from ViscAI.utils.pipeline.train_and_diagnostic_models import (train_baseline_models, model_diagnostics,
                                                               bootstrap_metric, compute_permutation_importance,
                                                               save_shap_summary)
from ViscAI.utils.pipeline.worst_cases_analysis import save_worst_cases, plot_worst_cases, check_worst_cases_ranges, check_worst_cases_local_density, rf_uncertainty_for_worst_cases

import time


# ***NEWWW*** helper común para reescritura (dist, Mw, PDI)
def _rewrite_input_with_mw_dist_pdi(input_file: str, mw_value: float,
                                    dist_code: int | None, pdi_val: float | None) -> list[str]:
    """
    Busca la primera línea candidata tras la cabecera (idx>=5)
    con >=3 tokens, e interpreta como:
    tokens[0] = dist_code
    tokens[1] = Mw
    tokens[2] = PDI
    Sustituye lo que corresponda y devuelve el nuevo contenido.
    """
    with open(input_file, "r") as f:
        lines = f.readlines()
    out = []
    touched = 0
    for idx, ln in enumerate(lines):
        if idx >= 5:
            toks = ln.split()
            if len(toks) >= 3:
                try:
                    _ = int(float(toks[0]))  # dist code
                    _ = float(toks[1])       # Mw
                    _ = float(toks[2])       # PDI
                    # Candidata
                    if dist_code is not None:
                        toks[0] = str(int(dist_code))
                    toks[1] = f"{float(mw_value):.6f}"
                    if pdi_val is not None:
                        toks[2] = f"{float(pdi_val):.6f}"
                    ln = " ".join(toks) + "\n"
                    touched += 1
                    out.append(ln)
                    continue
                except Exception:
                    pass
        out.append(ln)
    if touched == 0:
        st.warning(f"[Mw={mw_value}] WARNING: no se encontró línea (dist, Mw, PDI) candidata en el input.")
    return out

# ***NEWWW*** FIN helper

# ***NEWWW*** NUEVA FUNCIÓN: ejecuciones por rejilla Mw × PDI × Distribución
def viscai_paramgrid_run(
    name_server, name_user, ssh_key_options,
    working_directory, virtualenv_path,
    input_file, polymer_file,
    mw_list: list[float],
    dist_codes: list[int] | None,
    pdi_list: list[float] | None
):
    """
    Ejecuta todas las combinaciones de Mw × PDI × Distribución.
    - Si dist_codes == [] o None: no cambia el dist_code (se usa el del .dat)
    - Si pdi_list == [] o None: no cambia el PDI (se usa el del .dat)
    Subdirectorio remoto por combinación:
    Mw_<mw>__D<dist>__PDI_<pdi_token>
    """
    results = []
    input_filename = os.path.basename(input_file)
    base_name, ext = os.path.splitext(input_filename)

    ssh = connect_remote_server(name_server, name_user, ssh_key_options)
    sftp = ssh.open_sftp()
    conda_sh = get_conda_sh_path(ssh) or "~/.bashrc"

    use_slurm = bool(st.session_state.get("slurm_partition", "").strip())
    batch_flag = " -b" if st.session_state.get("batch_mode", False) else ""
    genpoly_flag = " -p" if st.session_state.get("generate_polymers", False) else ""

    # Normalizar dimensiones para producto cartesiano
    dist_opts = dist_codes if (dist_codes and len(dist_codes) > 0) else [None]
    pdi_opts  = pdi_list  if (pdi_list  and len(pdi_list)  > 0) else [None]

    # util: activar conda
    def _activate_cmd() -> str:
        return (
    f"source '{conda_sh}'\n"
    f"eval \"$(conda shell.bash hook)\"\n"
    f"conda activate '{virtualenv_path}'")



    #   TEST

    for mw in mw_list:
        for dist_code in dist_opts:
            for pdi_val in pdi_opts:
                # Subdirectorio único por combinación
                pdi_token  = "NA" if (pdi_val  is None) else str(pdi_val).replace(".", "_")
                dist_token = "NA" if (dist_code is None) else str(int(dist_code))
                subdir = f"{working_directory}/Mw_{str(mw).replace('.', '_')}__D{dist_token}__PDI_{pdi_token}"
                try:
                    ssh.exec_command(f"mkdir -p '{subdir}'")
                except Exception:
                    pass

                # Crear input modificado y subirlo
                try:
                    lines = _rewrite_input_with_mw_dist_pdi(input_file, float(mw), dist_code, pdi_val)
                    new_filename = f"{base_name}_MW_{str(mw).replace('.', '_')}_D{dist_token}_PDI_{pdi_token}{ext}"
                    local_tmp = os.path.join(tempfile.gettempdir(), new_filename)
                    with open(local_tmp, "w") as f:
                        f.writelines(lines)
                    remote_input = f"{subdir}/{new_filename}"
                    sftp.put(local_tmp, remote_input)
                    os.remove(local_tmp)
                except Exception as e:
                    results.append((mw, dist_code, pdi_val, f"Error creando/subiendo input: {e}"))
                    continue

                # Subir polymer file si aplica
                polymer_filename = None
                if polymer_file:
                    try:
                        polymer_filename = os.path.basename(polymer_file)
                        sftp.put(polymer_file, f"{subdir}/{polymer_filename}")
                    except Exception as e:
                        results.append((mw, dist_code, pdi_val, f"Error subiendo polymer file: {e}"))
                        polymer_filename = None

                # bob.rc
                try:
                    if st.session_state.get("configure_rc_toggle", False):
                        custom_bobrc = st.session_state.get("bobrc_file", "")
                        if custom_bobrc and os.path.exists(custom_bobrc):
                            sftp.put(custom_bobrc, f"{subdir}/bob.rc")
                        else:
                            st.warning(f"[Mw={mw}] No se encontró 'bob.rc' local -> usando el por defecto.")
                            ssh.exec_command(
                                "wget -q https://sourceforge.net/projects/bob-rheology/files/"
                                "bob-rheology/bob2.5/bob.rc "
                                f"-O '{subdir}/bob.rc'"
                            )
                    else:
                        ssh.exec_command(
                            "wget -q https://sourceforge.net/projects/bob-rheology/files/"
                            "bob-rheology/bob2.5/bob.rc "
                            f"-O '{subdir}/bob.rc'"
                        )
                except Exception as e:
                    results.append((mw, dist_code, pdi_val, f"Error preparando bob.rc: {e}"))
                    continue

                # TESTETSTEST

                # Comando bob2p5
                bob_cmd = f"bob2p5 -i {os.path.basename(remote_input)}"
                if polymer_filename:
                    bob_cmd += f" -c {polymer_filename}"
                bob_cmd += batch_flag + genpoly_flag

                try:
                    if use_slurm:
                        # Parametría Slurm
                        job_name = st.session_state.get("slurm_job_name", "BoBjob")
                        partition = st.session_state.get("slurm_partition", "")
                        nodes = int(st.session_state.get("slurm_nodes", 1))
                        cpus = int(st.session_state.get("slurm_cpus_per_task", 1))
                        mem = st.session_state.get("slurm_mem_per_cpu", "1G")
                        activate_cmd = _activate_cmd()
                        script = f"""#!/bin/bash -l
#SBATCH --partition={partition}
#SBATCH --nodes={nodes}
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem-per-cpu={mem}
#SBATCH --job-name={job_name}_MW{str(mw).replace('.', '_')}_D{dist_token}_PDI{pdi_token}
#SBATCH --output={subdir}/slurm-%j.out
#SBATCH --error={subdir}/slurm-%j.err

set -euxo pipefail

echo "===== SLURM DEBUG START ====="
echo "HOST: $(hostname)"
echo "USER: $(whoami)"
echo "SHELL: $SHELL"
echo "PWD (before cd): $(pwd)"
date

echo "=== LS ROOT ==="
ls -la

echo "=== CD to subdir ==="
cd "{subdir}"
pwd
ls -la

echo "=== CONDA INIT ==="
source "{conda_sh}"
eval "$(conda shell.bash hook)"
conda activate "{virtualenv_path}"

echo "=== ENV CHECK ==="
which python || true
python --version || true
which bob2p5 || true
echo "PATH=$PATH"

echo "=== INPUT FILES ==="
ls -la *.dat || true
ls -la

echo "=== RUN bob2p5 ==="
{bob_cmd}

echo "===== SLURM DEBUG END ====="
date
"""

                        local_sh = tempfile.NamedTemporaryFile(delete=False,
                            suffix=f"_MW{str(mw).replace('.', '_')}_D{dist_token}_PDI{pdi_token}.sh")
                        local_sh.write(script.encode("utf-8"))
                        local_sh.close()
                        remote_sh = f"{subdir}/run_MW{str(mw).replace('.', '_')}_D{dist_token}_PDI{pdi_token}.sh"
                        sftp.put(local_sh.name, remote_sh)
                        os.remove(local_sh.name)

                        try:
                            # instantiate adapter (create a small local DB filename; ajusta si lo quieres en otro lugar)
                            dbname_local = os.path.join(os.getcwd(), "viscai_slurm_jobs.db")
                            slurm = SlurmAdapter(name_server, dbname_local, name_user, ssh_key_options)

                            # upload (you may already have done sftp.put earlier; using adapter.upload is fine)
                            slurm.upload(local_sh.name, remote_sh)
                            # ensure executable
                            slurm.chmod(remote_sh, 0o750)

                            # prepare job name and submit
                            job_name_full = f"{job_name}_MW{str(mw).replace('.', '_')}_D{dist_token}_PDI{pdi_token}"
                            out_sbatch, err_sbatch = slurm.submit_script(
                                remotedir=subdir,
                                script_name=os.path.basename(remote_sh),
                                partition=partition,
                                nodes=nodes,
                                cpus_per_task=cpus,
                                mem_per_cpu=mem,
                                job_name=job_name_full
                            )
                            if err_sbatch:
                                results.append((mw, dist_code, pdi_val, f"Slurm ERROR: {err_sbatch}"))
                            else:
                                results.append((mw, dist_code, pdi_val, f"Enqueued: {out_sbatch.strip()}"))

                            slurm.close()
                        except Exception as e:
                            results.append((mw, dist_code, pdi_val, f"Slurm Adapter ERROR: {e}"))

                    else:
                        # Ejecución directa
                        activate_cmd = _activate_cmd()
                        full_cmd = f"bash -lc \"cd '{subdir}' && {activate_cmd} && {bob_cmd}\""

                        #####################   TEST    #####################
                        # after successful execution (non-slurm)
                        stdin, stdout, stderr = ssh.exec_command(full_cmd)
                        # nueva comprobación robusta:
                        exit_code = stdout.channel.recv_exit_status()  # espera a que termine
                        out_exec = stdout.read().decode().strip()
                        err_exec = stderr.read().decode().strip()

                        if exit_code == 0:
                            # comprobaremos gt + gtp primero
                            if not _wait_for_remote_files(sftp, subdir, ["gt.dat", "gtp.dat"], retries=6, delay=1.0):
                                results.append((mw, dist_code, pdi_val, "WARNING: outputs (gt/gtp) not found after execution; skipping .gnu creation"))
                            else:
                                # si BoB produce gpcls cuando CalcGPCLS=yes, espera gpcls*.dat
                                if st.session_state.get("configure_rc_toggle", False) or True:  # si tu bob.rc tiene CalcGPCLS=yes
                                    if _wait_for_remote_files(sftp, subdir, ["gpcls*"], retries=6, delay=1.0):
                                        # ya está gpcls1.dat (o gpclssys.dat) -> gg
                                        try:
                                            gnu_gpclssys_generation(name_server, name_user, ssh_key_options, subdir)
                                        except Exception as e:
                                            results.append((mw, dist_code, pdi_val, f"Advertencia creando gpclssys.gnu: {e}"))
                                    else:
                                        # no hay gpcls*.dat -> no intentamos gpclssys.gnu
                                        results.append((mw, dist_code, pdi_val, "No gpcls*.dat found -> skipping gpclssys.gnu"))
                                # siempre intentamos modulus.gnu si gt/gtp existen
                                try:
                                    gnu_modulus_generation(name_server, name_user, ssh_key_options, subdir)
                                except Exception as e:
                                    results.append((mw, dist_code, pdi_val, f"Advertencia creando modulus.gnu: {e}"))

                        else:
                            results.append((mw, dist_code, pdi_val, f"Exec ERROR (code {exit_code}): {err_exec or out_exec}"))


                        #####################   TEST    #####################

                except Exception as e:
                    results.append((mw, dist_code, pdi_val, f"ERROR general en ejecución: {e}"))

    # Cierre de conexiones iniciales
    try: sftp.close()
    except Exception: pass
    try: ssh.close()
    except Exception: pass

    # Generación DB y CSV (no-Slurm): recoger todos los subdirs que empiezan por Mw_
    try:
        use_slurm = bool(st.session_state.get("slurm_partition", "").strip())
        if not use_slurm:
            # Generación de DB + CSV por-Mw en remoto (paralelo)
            database_db_creation(
                name_server, name_user, ssh_key_options, working_directory,
                include_root=False,      # ignorar raíz en paralelo
                per_mw=True,             # generar CSV por-Mw
                upload_per_mw=True,      # subir por-Mw a cada subdir Mw_*
                sort_ids_by_mw=True,     # reindex IDs por Mw ascendente
                is_parallel=True         # bandera de modo paralelo
            )

            results.append(("DB", f"OK -> {working_directory}/viscai_database.db"))

            # --- NUEVO: descargar subdirectorios Mw_* al directorio local ---
            local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")
            if local_dir and os.path.isdir(local_dir):
                # Conectar y listar subdirs remotos
                ssh = connect_remote_server(name_server, name_user, ssh_key_options)
                sftp = ssh.open_sftp()
                try:
                    remote_subdirs = sftp.listdir(working_directory)
                except Exception:
                    remote_subdirs = []
                mw_subdirs = [d for d in remote_subdirs if d.startswith("Mw_")]
                for sd in mw_subdirs:
                    remote_sd = f"{working_directory}/{sd}"
                    local_target = os.path.join(local_dir, sd)
                    try:
                        _download_tree(sftp, remote_sd, local_target)  # usa la utilidad definida más abajo
                        results.append((sd, f"Downloaded DIR -> {local_target}"))
                    except Exception as e:
                        results.append((sd, f"Download DIR ERROR -> {e}"))
                try:
                    sftp.close()
                except Exception:
                    pass
                try:
                    ssh.close()
                except Exception:
                    pass
            else:
                results.append(("DIR-LOCAL", "Directorio local no definido -> outputs permanecen en remoto"))


            ############## PREPROCESSING AND ML ##############
            database_inspection()
            st.info("DONE preproecessed part 1!!!")
            preprocess_database()
            st.info("DONE preproecessed part 2!!!")
            build_resampled_rheology_features()
            st.info("DONE preproecessed part 3!!!")

            prepare_rheology_dataset()
            st.info("DONE training data part 1!!!")
            validate_splits()
            st.info("DONE training data part 2!!!")

            train_baseline_models()
            st.info("DONE models part 1!!!")
            model_diagnostics()
            st.info("DONE models part 2!!!")
            bootstrap_metric()
            st.info("DONE models part 3!!!")
            compute_permutation_importance()
            st.info("DONE models part 4!!!")
            save_worst_cases()
            st.info("DONE worst cases part 1!!!")
            save_shap_summary()
            st.info("DONE models part 5!!!")

            plot_worst_cases()
            st.info("DONE worst cases part 2!!!")
            check_worst_cases_ranges()
            st.info("DONE worst cases part 3!!!")
            check_worst_cases_local_density()
            st.info("DONE worst cases part 4!!!")
            rf_uncertainty_for_worst_cases()
            st.info("DONE worst cases part 5!!!")

        else:
            # Con Slurm: intentar descarga masiva (puede fallar si los jobs no han terminado)
            local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")
            if local_dir and os.path.isdir(local_dir):
                summary = collect_mw_dirs(name_server, name_user, ssh_key_options, working_directory, local_dir)
                results.append(("COLLECT", summary))
            else:
                results.append(("COLLECT", "Directorio local no definido"))
    except Exception as e:
        results.append(("DB", f"ERROR generando/descargando DB y subdirectorios: {e}"))

    return results

def reset_bob_options():
    st.session_state.input_options = {}
    st.session_state.batch_mode = False
    st.session_state.generate_polymers = False

def bob_check_arguments(script_lines):
    """Check BoB arguments"""
    valid_arguments = ["-i", "-c", "-b", "-p"]
    for line in script_lines:
        if "bob2p5" in line:
            parts = line.split()
            for part in parts:
                if part.startswith('-') and part not in valid_arguments:
                    st.error(f"ERROR!!! Not recognized argument: '{part}'")
                    return True
    return False

def viscai_single_run(
    name_server, name_user, ssh_key_options, working_directory,
    virtualenv_path, input_file, polymer_file,
    batch_mode, generate_polymers
):
    # 1) Limpia el directorio remoto
    clean_remote_directory(name_server, name_user, ssh_key_options, working_directory)

    # 2) Transfiere/descarga bob.rc
    bobrc_transfered = bob_rc_transfering(
        name_server, name_user, ssh_key_options,
        working_directory, virtualenv_path, input_file
    )
    if not bobrc_transfered:
        st.stop()

    # 3) Construye comando BoB
    viscai_command = build_viscai_command(
        input_file,
        polymer_file,
        batch_mode,
        generate_polymers
    )
    if not viscai_command:
        return

    # 4) Ejecuta BoB (sube ficheros + ejecuta)
    output, error = execute_remote_process(
        name_server,
        name_user,
        ssh_key_options,
        working_directory,
        virtualenv_path,
        viscai_command,
        input_file,
        polymer_configuration=polymer_file if polymer_file else None
    )
    if error:
        st.error(f"ERROR!!! Program execution failed: {error}")
    else:
        st.success("JOB DONE!!!")

    # 5) .gnu
    gnu_modulus_generation(name_server, name_user, ssh_key_options, working_directory)
    gnu_gpclssys_generation(name_server, name_user, ssh_key_options, working_directory)

    # 6) Genera DB + CSV (flujo actual)
    database_db_creation(name_server, name_user, ssh_key_options, working_directory, is_parallel=False)

def _sync_pyrheo_per_mw_to_local(
    name_server: str, name_user: str, ssh_key_options: str,
    working_directory: str, local_dir: str
) -> None:
    """
    Descarga exclusivamente los ficheros pyRheo por-Mw desde cada subdirectorio remoto 'Mw_*'
    al subdirectorio local correspondiente, manteniendo los nombres:
    - 02-relaxation_pyRheo.csv
    - 02-dynamic_pyRheo.csv
    No borra ni sobreescribe otros outputs locales.
    """
    if not local_dir or not os.path.isdir(local_dir):
        st.info("Directorio local no definido: no se sincronizan CSV por-Mw.")
        return
    try:
        ssh = connect_remote_server(name_server, name_user, ssh_key_options)
        sftp = ssh.open_sftp()
        # Enumerar subdirectorios Mw_* en remoto
        subdirs = [d for d in sftp.listdir(working_directory) if d.startswith("Mw_")]
        for sd in subdirs:
            remote_sd = f"{working_directory}/{sd}"
            local_sd = os.path.join(local_dir, sd)
            os.makedirs(local_sd, exist_ok=True)
            # Dos ficheros pyRheo por-Mw esperados dentro del subdir
            per_mw_files = ["02-relaxation_pyRheo.csv", "02-dynamic_pyRheo.csv"]
            for fname in per_mw_files:
                rpath = f"{remote_sd}/{fname}"
                lpath = os.path.join(local_sd, fname)
                try:
                    sftp.get(rpath, lpath)
                except Exception:
                    # Si aún no existe en remoto (p. ej., job no ha terminado), ignoramos y seguimos
                    pass
        sftp.close()
        ssh.close()
        st.success(f"Sincronizados pyRheo por-Mw en: {local_dir}")
    except Exception as e:
        st.warning(f"Sincronización de pyRheo por-Mw fallida: {e}")

def _cleanup_root_pyrheo_csv(
    name_server: str, name_user: str, ssh_key_options: str,
    working_directory: str, local_dir: str
) -> None:
    """
    Elimina los ficheros agregados del raíz:
    - <working_directory>/02-relaxation_pyRheo.csv
    - <working_directory>/02-dynamic_pyRheo.csv
    - <local_dir>/02-relaxation_pyRheo.csv
    - <local_dir>/02-dynamic_pyRheo.csv
    SOLO si ya existen los por-Mw en TODOS los subdirectorios 'Mw_*' del remoto y en local:
    - Mw_*/02-relaxation_pyRheo.csv
    - Mw_*/02-dynamic_pyRheo.csv
    - <local_dir>/Mw_*/02-...
    Esto evita borrar agregados si aún no se han generado/sincronizado.
    """
    try:
        # Validación de local_dir
        local_ok = bool(local_dir and os.path.isdir(local_dir))
        # Conexión remota
        ssh = connect_remote_server(name_server, name_user, ssh_key_options)
        sftp = ssh.open_sftp()
        # Enumerar subdirs Mw_* en remoto
        remote_mw_subdirs = [d for d in sftp.listdir(working_directory) if d.startswith("Mw_")]
        if not remote_mw_subdirs:
            # No hay subdirectorios -> no borramos agregados
            sftp.close(); ssh.close()
            return
        # Comprobar que en remoto todos los Mw_* tienen los 2 ficheros por-Mw
        for sd in remote_mw_subdirs:
            base = f"{working_directory}/{sd}"
            for fname in ("02-relaxation_pyRheo.csv", "02-dynamic_pyRheo.csv"):
                try:
                    sftp.stat(f"{base}/{fname}")
                except Exception:
                    # Falta al menos uno: abortar limpieza
                    sftp.close(); ssh.close()
                    return
        # Comprobar que en local, si se definió, todos los Mw_* tienen los 2 ficheros por-Mw
        if local_ok:
            for sd in remote_mw_subdirs:
                local_sd = os.path.join(local_dir, sd)
                # Si el subdir local no existe, abortamos limpieza
                if not os.path.isdir(local_sd):
                    sftp.close(); ssh.close()
                    return
                for fname in ("02-relaxation_pyRheo.csv", "02-dynamic_pyRheo.csv"):
                    if not os.path.exists(os.path.join(local_sd, fname)):
                        sftp.close(); ssh.close()
                        return
        # Si hemos llegado aquí, es seguro borrar los agregados del raíz remoto
        for fname in ("02-relaxation_pyRheo.csv", "02-dynamic_pyRheo.csv"):
            rpath = f"{working_directory}/{fname}"
            try:
                sftp.remove(rpath)
            except Exception:
                pass  # si no existe, ignoramos
        sftp.close(); ssh.close()
        # Borrar agregados del raíz local (si aplica)
        if local_ok:
            for fname in ("02-relaxation_pyRheo.csv", "02-dynamic_pyRheo.csv"):
                lpath = os.path.join(local_dir, fname)
                try:
                    if os.path.exists(lpath):
                        os.remove(lpath)
                except Exception:
                    pass
        st.success("Limpieza realizada: ficheros agregados 02-*_pyRheo.csv eliminados del raíz (remoto y local).")
    except Exception as e:
        st.warning(f"Limpieza de agregados fallida: {e}")

# --- Utilidad: descarga recursiva de un directorio remoto a local ---
def _download_tree(sftp, remote_path: str, local_path: str):
    os.makedirs(local_path, exist_ok=True)
    for entry in sftp.listdir_attr(remote_path):
        rname = entry.filename
        rpath = remote_path.rstrip('/') + '/' + rname
        lpath = os.path.join(local_path, rname)
        if stat.S_ISDIR(entry.st_mode):
            _download_tree(sftp, rpath, lpath)
        else:
            sftp.get(rpath, lpath)

def viscai_multiple_run(
    name_server, name_user, ssh_key_options,
    working_directory, virtualenv_path,
    input_file, polymer_file, mw_list
):
    """
    Lanza simulaciones (una por Mw en 'mw_list') en subdirs 'Mw_<valor>' y
    genera un 'ViscAI_output.tar.gz' dentro de cada subdirectorio en el servidor.
    - Si hay Slurm (partition en session_state), encola un job por Mw y empaqueta
      al final del job. La descarga del subdirectorio completo se hará con 'collect_mw_dirs'.
    - Si NO hay Slurm, ejecuta por SSH, empaqueta y DESCARGA el subdirectorio remoto completo
      al directorio local (en lugar de traer sólo el .tar.gz).
    Devuelve: lista de tuplas (mw, estado).
    """
    # ... (contenido original sin cambios, ya gestiona descarga recursiva en no-Slurm)
    pass  # mantener tu versión previa si ya la tienes

def collect_mw_dirs(
    name_server, name_user, ssh_key_options,
    working_directory, local_dir
):
    """
    Recolecta/descarga de forma RECURSIVA todos los subdirectorios 'Mw_*'
    desde el servidor al 'local_dir'. Útil en escenarios con Slurm.
    Devuelve: lista de tuplas (subdir, estado).
    """
    summary = []
    if not local_dir or not os.path.isdir(local_dir):
        return [("LOCAL_DIR", "ERROR: local directory not defined or missing")]
    ssh = connect_remote_server(name_server, name_user, ssh_key_options)
    sftp = ssh.open_sftp()
    try:
        subdirs = sftp.listdir(working_directory)
        subdirs = [d for d in subdirs if d.startswith("Mw_")]
        if not subdirs:
            summary.append(("Mw_*", "No subdirectories found"))
        for sd in subdirs:
            remote_sd = f"{working_directory}/{sd}"
            local_target = os.path.join(local_dir, sd)
            try:
                _download_tree(sftp, remote_sd, local_target)
                summary.append((sd, f"Downloaded DIR -> {local_target}"))
            except Exception as e:
                summary.append((sd, f"Download DIR ERROR: {e}"))
    finally:
        try: sftp.close()
        except Exception: pass
        try: ssh.close()
        except Exception: pass
    return summary

def _es_linea_de_mw(tokens: list[str]) -> bool:
    """
    Auxiliar original por si la importas en otro sitio.
    """
    try:
        int(tokens[0]); float(tokens[1]); float(tokens[2])
        return True
    except Exception as e:
        st.error(f"ERROR: {e}")


def _wait_for_remote_files(sftp, remote_dir: str, patterns: list[str], retries: int = 5, delay: float = 1.0) -> bool:
    """
    Espera hasta `retries` veces a que existan *alguno* de los patrones dados en remote_dir.
    patterns puede contener nombres exactos (e.g. 'gt.dat') o prefijos como 'gpcls' (detecta gpcls*.dat).
    Devuelve True si encuentra al menos lo esperado, False en caso contrario.
    """
    for _ in range(retries):
        try:
            files = sftp.listdir(remote_dir)
        except Exception:
            files = []
        ok = True
        for p in patterns:
            if p.endswith('*'):  # prefijo con asterisco tipo 'gpcls*'
                prefix = p[:-1]
                if not any(fn.lower().startswith(prefix.lower()) for fn in files):
                    ok = False; break
            else:
                if p not in files:
                    ok = False; break
        if ok:
            return True
        time.sleep(delay)
    return False
