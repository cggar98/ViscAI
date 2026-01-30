#   TEST

# ViscAI/program_options.py
import streamlit as st
import os
import stat
import tempfile
from pathlib import Path

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
from ViscAI.utils.upload_slurms import _slurm_submit_multiple_mw

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

    batch_flag = " -b" if st.session_state.get("batch_mode", False) else ""
    genpoly_flag = " -p" if st.session_state.get("generate_polymers", False) else ""

    # Normalizar dimensiones para producto cartesiano
    dist_opts = dist_codes if (dist_codes and len(dist_codes) > 0) else [None]
    pdi_opts  = pdi_list  if (pdi_list  and len(pdi_list)  > 0) else [None]

    # Bandera para crear/upload/submit full_send.sh y slurm submit solo UNA vez
    full_send_created = False
    slurm_created_results = None

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


    # **************** NUEVO CAMBIO **********
    # Track created subdirectories (para debug)
    created_subdirs = []
    # dentro del bucle donde defines `subdir` y creas mkdir -p:
    # después de ssh.exec_command(f"mkdir -p '{subdir}'") añade:
    # created_subdirs.append(subdir)

    # Si no lo añadiste durante la creación, recabar ahora:
    try:
        remote_entries = sftp.listdir(working_directory)
        created_subdirs = [os.path.join(working_directory, d) for d in remote_entries if d.startswith("Mw_")]
        results.append(("CREATED_SUBDIRS_COUNT", len(created_subdirs)))
        # opcional: mostrar los primeros N
        results.append(("CREATED_SUBDIRS_SAMPLE", created_subdirs[:10]))
    except Exception:
        results.append(("CREATED_SUBDIRS_ERROR", "Could not list remote working_directory"))
    # **************** NUEVO CAMBIO **********



    # ----------------- FUERA DEL BUCLE: crear/ subir / submit slurm scripts UNA VEZ -----------------
    # ****************************NUEVO CAMBIO*************
    try:
        # Llamada única para crear/colocar todos los slurm.sh (opción B)
        slurm_created_results = _slurm_submit_multiple_mw(
            name_server=name_server,
            name_user=name_user,
            ssh_key_options=ssh_key_options,
            working_dir=working_directory,
            mw_list=mw_list,
            input_file=input_file,
            polymer_file=polymer_file,
        )
        if slurm_created_results:
            for r in slurm_created_results:
                results.append(("SLURM", str(r)))
        results.append(("SLURM_SUBMIT", "DONE"))
        st.success("SLURM scripts created/submitted (ver resumen).")
    except Exception as e:
        results.append(("SLURM", f"ERROR calling external slurm submit: {e}"))
        st.warning(f"SLURM submit failed: {e}")
    # ****************************NUEVO CAMBIO*************

    # ----------------- Crear / subir / enviar full_send.sh UNA SOLA VEZ -----------------
    # ****************************NUEVO CAMBIO*************
    try:
        #   test
        # contenido del full_send.sh con corrección wc -l

        # **************** NUEVO CAMBIO **********
        full_send_content = """#!/bin/bash
#SBATCH --partition=all
#SBATCH -N 1
#SBATCH -n 1
#SBATCH --mem-per-cpu=1024M
#SBATCH --job-name=FULL_SEND

# Config
WK="$(pwd)"
MAXJOBSINSLURM=60    # configurable: ajustar según política del cluster
SLEEP_WHEN_BUSY=60   # segundos a esperar cuando se alcanza el límite

# Use user's jobs only (safer) - cuenta solo los jobs del usuario que ejecuta el script
# Si quieres otro usuario, asigna USERNAME aquí
USERNAME="${USER}"

# Number of total jobs (number of Mw*/ dirs)
DIRBOB=( $(ls -d Mw*/ 2>/dev/null) )
TOTALJOBS=${#DIRBOB[@]}

if [[ ${TOTALJOBS} -eq 0 ]]; then
    echo "No Mw_* directories found in ${WK}" >&2
    exit 1
fi

if [[ ! -e "${WK}/jobs.txt" ]]; then
    : > "${WK}/jobs.txt"
fi

index=0
while [ ${index} -lt ${TOTALJOBS} ]; do
    # Actualiza número de jobs del usuario
    NJOBS=$(squeue -h -u "${USERNAME}" | wc -l)

    current="${DIRBOB[$index]}"
    if [[ ${NJOBS} -lt ${MAXJOBSINSLURM} ]]; then
       echo "Submitting ${current} (NJOBS=${NJOBS}) at $(date)" >> "${WK}/full_send.log"
       cd "${current}" || { echo "cd failed to ${current}" >> "${WK}/full_send.log"; break; }
       sbatch slurm.sh 1>tmp_submit.txt 2>tmp_submit.err
       rc=$?
       jobid=$(awk '{print $NF}' tmp_submit.txt 2>/dev/null || true)
       if [[ ${rc} -eq 0 && -n "${jobid}" ]]; then
           echo "${jobid} ${current}" >> "${WK}/jobs.txt"
           echo "OK submit ${jobid} for ${current}" >> "${WK}/full_send.log"
       else
           echo "SUBMIT-ERROR rc=${rc} out=$(cat tmp_submit.txt 2>/dev/null) err=$(cat tmp_submit.err 2>/dev/null)" >> "${WK}/full_send.log"
       fi
       rm -f tmp_submit.txt tmp_submit.err
       index=$((index+1))
       cd "${WK}"
       echo "NEW $(date) --> JOBSEND: ${index}, TOTALJOBS: ${TOTALJOBS}, ${current}" >> "${WK}/full_send.log"
    else
       echo "$(date) WAIT: NJOBS=${NJOBS} >= MAXJOBSINSLURM=${MAXJOBSINSLURM}" >> "${WK}/full_send.log"
       sleep ${SLEEP_WHEN_BUSY}
    fi
done

echo "Jobs submission loop finished at $(date)" >> "${WK}/full_send.log"
echo "Jobs Done!!!!!"
    """
# **************** NUEVO CAMBIO **********





        # Guardar localmente
        local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")
        if local_dir and os.path.isdir(local_dir):
            local_full_send = os.path.join(local_dir, "full_send.sh")
        else:
            local_full_send = os.path.join(tempfile.gettempdir(), "full_send.sh")

        with open(local_full_send, "w") as f:
            f.write(full_send_content)
        try:
            os.chmod(local_full_send, 0o750)
        except Exception:
            pass
        results.append(("FULL_SEND_LOCAL", f"Created {local_full_send}"))

        # Subir al remoto
        remote_full_send = os.path.join(working_directory, "full_send.sh")
        try:
            sftp.put(local_full_send, remote_full_send)
            try:
                sftp.chmod(remote_full_send, 0o750)
            except Exception:
                pass
            results.append(("FULL_SEND_REMOTE", f"Uploaded to {remote_full_send}"))
            st.success(f"'full_send.sh' creado localmente y subido a: {remote_full_send}")
        except Exception as e:
            results.append(("FULL_SEND_UPLOAD_ERROR", str(e)))
            st.warning(f"No se pudo subir full_send.sh al remoto: {e}")
    except Exception as e:
        results.append(("FULL_SEND_ERROR", str(e)))
        st.warning(f"Error creando/subiendo full_send.sh: {e}")
        # ****************************NUEVO CAMBIO*************

    # ----------------- Enviar full_send.sh UNA VEZ -----------------
    # ****************************NUEVO CAMBIO*************
    try:
        cmd = f"cd '{working_directory}' && sbatch full_send.sh"
        stdin, stdout, stderr = ssh.exec_command(cmd)
        exit_code = stdout.channel.recv_exit_status()
        out_text = stdout.read().decode().strip()
        err_text = stderr.read().decode().strip()

        if exit_code == 0:
            results.append(("FULL_SEND_SUBMIT_OK", out_text or err_text or "sbatch returned 0"))
            st.success(f"'full_send.sh' enviado con sbatch: {out_text or err_text}")
        else:
            results.append(("FULL_SEND_SUBMIT_ERROR", f"rc={exit_code}, out={out_text}, err={err_text}"))
            st.warning(f"sbatch fallo: rc={exit_code}, err={err_text}")
    except Exception as e:
        results.append(("FULL_SEND_SUBMIT_EXCEPTION", str(e)))
        st.warning(f"Error ejecutando sbatch full_send.sh en remoto: {e}")
    # ****************************NUEVO CAMBIO*************

    # ********************** Esperar a que los jobs enviados por full_send.sh terminen ****************
    # ****************************NUEVO CAMBIO*************
    try:
        remote_jobs_txt = os.path.join(working_directory, "jobs.txt")
        # espera a que exista jobs.txt (timeout configurable)
        wait_for_jobsfile_secs = int(st.session_state.get("fullsend_wait_jobsfile_secs", 600))  # default 10 min
        poll_interval = float(st.session_state.get("fullsend_poll_interval_secs", 5.0))  # default 5 s
        waited = 0.0
        jobsfile_found = False
        while waited < wait_for_jobsfile_secs:
            try:
                sftp.stat(remote_jobs_txt)
                jobsfile_found = True
                break
            except Exception:
                time.sleep(poll_interval)
                waited += poll_interval

        if not jobsfile_found:
            results.append(
                ("FULL_SEND_JOBSFILE_TIMEOUT", f"No se encontró {remote_jobs_txt} tras {wait_for_jobsfile_secs}s"))
            st.warning(
                f"No se encontró {remote_jobs_txt} en remoto tras {wait_for_jobsfile_secs}s; procediendo a descarga (puede faltar output generado por jobs).")
        else:
            # Leer job ids desde jobs.txt
            jobids = []
            try:
                with sftp.open(remote_jobs_txt, "r") as jf:
                    raw = jf.read().decode() if isinstance(jf.read(), bytes) else jf.read()
                    # Note: above .read() was used twice to be robust; if your sftp.open returns text, adapt.
                # (Mejor reabrir para leer correctamente)
                with sftp.open(remote_jobs_txt, "r") as jf2:
                    lines = jf2.readlines()
                for ln in lines:
                    toks = ln.strip().split()
                    if len(toks) >= 1:
                        jobids.append(toks[0])
            except Exception as e:
                # Si no pudimos leer jobs.txt, lo notificamos y seguiremos intentando vía squeue por patrón
                results.append(("FULL_SEND_JOBS_READ_ERR", str(e)))
                jobids = []

            # Si tenemos jobids: poll hasta que ninguno esté en squeue
            if jobids:
                joblist = ",".join(jobids)
                st.info(f"Esperando a que terminen {len(jobids)} jobs: {joblist}")
                max_wait_for_jobs_secs = int(st.session_state.get("fullsend_wait_jobs_secs", 7200))  # default 2h
                waited = 0.0
                still_running = True
                while waited < max_wait_for_jobs_secs and still_running:
                    try:
                        # Pregunta a squeue cuántos de esos jobids siguen en cola
                        stdin2, stdout2, stderr2 = ssh.exec_command(f"squeue -h -j {joblist} | wc -l")
                        cnt_text = stdout2.read().decode().strip()
                        cnt = int(cnt_text) if cnt_text.isdigit() else 0
                    except Exception:
                        # En caso de error con squeue, asumimos que aún puede haber jobs; dormir y reintentar
                        cnt = 1

                    if cnt == 0:
                        still_running = False
                        break
                    else:
                        # esperar y volver a comprobar
                        time.sleep(max(5.0, poll_interval))
                        waited += max(5.0, poll_interval)
                if still_running:
                    results.append(("FULL_SEND_JOBS_TIMEOUT",
                                    f"Algunos jobs siguen en cola tras {max_wait_for_jobs_secs}s; descargando lo disponible."))
                    st.warning(
                        f"Algunos jobs siguen en cola tras {max_wait_for_jobs_secs}s; procediendo a descargar lo que ya exista.")
                else:
                    results.append(("FULL_SEND_JOBS_DONE", f"Todos los jobs ({len(jobids)}) han terminado."))
                    st.success(f"Todos los jobs enviados por full_send.sh han terminado. Procediendo a descarga.")
            else:
                # Si no hay jobids, intentamos una espera razonable para dar tiempo a que se generen salidas
                fallback_wait = int(st.session_state.get("fullsend_fallback_wait_secs", 60))
                st.info(f"No se han leído jobids de jobs.txt; esperando otros {fallback_wait}s antes de descargar.")
                time.sleep(fallback_wait)

    except Exception as e:
        results.append(("FULL_SEND_WAIT_EXCEPTION", str(e)))
        st.warning(f"Error durante espera/consulta de jobs: {e}")
    # ****************************NUEVO CAMBIO*************



    # Cierre de conexiones iniciales
    try: sftp.close()
    except Exception: pass
    try: ssh.close()
    except Exception: pass


    ##########################################################

    #   DESCARGAR WORKING SIRECORY A LOCAL
    # ****************************NUEVO CAMBIO*************
    try:
        local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")
        if local_dir and os.path.isdir(local_dir):
            ssh_dl = connect_remote_server(name_server, name_user, ssh_key_options)
            sftp_dl = ssh_dl.open_sftp()
            try:
                entries = sftp_dl.listdir_attr(working_directory)
            except Exception:
                entries = []

            if not entries:
                results.append(("COLLECT_ALL", "No entries found in remote working_directory"))
            else:
                try:
                    for entry in entries:
                        rname = entry.filename
                        rpath = working_directory.rstrip("/") + "/" + rname
                        lpath = os.path.join(local_dir, rname)
                        if stat.S_ISDIR(entry.st_mode):
                            _download_tree(sftp_dl, rpath, lpath)
                        else:
                            os.makedirs(os.path.dirname(lpath) or local_dir, exist_ok=True)
                            sftp_dl.get(rpath, lpath)
                    results.append(("COLLECT_ALL", f"Downloaded all content of {working_directory} to {local_dir}"))
                    st.success(f"Todos los ficheros de '{working_directory}' descargados a '{local_dir}'")
                except Exception as e:
                    results.append(("COLLECT_ALL_ERROR", str(e)))
                    st.warning(f"Error descargando todo el working_directory: {e}")
            try:
                sftp_dl.close()
            except Exception:
                pass
            try:
                ssh_dl.close()
            except Exception:
                pass
        else:
            results.append(("COLLECT_ALL", "Local directory not defined - skip full collect"))
    except Exception as e:
        results.append(("COLLECT_ALL_EXCEPTION", str(e)))
        st.warning(f"Error general descargando working_directory: {e}")


    # # **************************** CAMBIO ****************************
    # local_dir = Path(
    #     st.session_state["input_options"]["input_file_002"]
    # )
    # # **************************** CAMBIO ****************************
    #
    # if not local_dir.exists():
    #     raise RuntimeError("El directorio local no existe")
    #
    # print(f"[INFO] Usando datos locales en: {local_dir}")
    #
    # # **************************** CAMBIO ****************************
    # db_path = database_db_creation(
    #     name_server=None,  # <- FUERZA MODO LOCAL
    #     name_user=None,
    #     ssh_key_options=None,
    #     working_directory=local_dir,
    #     include_root=False,
    #     per_mw=True,
    #     upload_per_mw=False,
    #     sort_ids_by_mw=True,
    #     is_parallel=True
    # )
    # # **************************** CAMBIO ****************************
    #
    # print(f"[OK] Base de datos lista: {db_path}")
    #
    # return db_path

        # ****************************NUEVO CAMBIO************************

    # ****************************NUEVO CAMBIO************************

    #
    # ############## PREPROCESSING AND ML ##############
    # database_inspection()
    # st.info("DONE preproecessed part 1!!!")
    # preprocess_database()
    # st.info("DONE preproecessed part 2!!!")
    # build_resampled_rheology_features()
    # st.info("DONE preproecessed part 3!!!")
    #
    # prepare_rheology_dataset()
    # st.info("DONE training data part 1!!!")
    # validate_splits()
    # st.info("DONE training data part 2!!!")
    #
    # train_baseline_models()
    # st.info("DONE models part 1!!!")
    # model_diagnostics()
    # st.info("DONE models part 2!!!")
    # bootstrap_metric()
    # st.info("DONE models part 3!!!")
    # compute_permutation_importance()
    # st.info("DONE models part 4!!!")
    # save_worst_cases()
    # st.info("DONE worst cases part 1!!!")
    # save_shap_summary()
    # st.info("DONE models part 5!!!")
    #
    # plot_worst_cases()
    # st.info("DONE worst cases part 2!!!")
    # check_worst_cases_ranges()
    # st.info("DONE worst cases part 3!!!")
    # check_worst_cases_local_density()
    # st.info("DONE worst cases part 4!!!")
    # rf_uncertainty_for_worst_cases()
    # st.info("DONE worst cases part 5!!!")

    ##########################################################
    # **************** NUEVO CAMBIO **********
    expected_combinations = len(mw_list) * len(dist_opts) * len(pdi_opts)
    results.append(("EXPECTED_COMBINATIONS", expected_combinations))
    # **************** NUEVO CAMBIO **********

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
