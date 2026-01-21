# program_options_tab.py
import os
import re
import stat as statmod
import streamlit as st
import paramiko
from typing import List, Tuple

# ******NEWWW******* importar el nuevo lanzador de combinaciones
from ViscAI.program_options import viscai_paramgrid_run
# ******NEWWW******* FIN import


from ViscAI.utils.program_update_events import handle_button_click
from ViscAI.utils.parameters_customer import input_file_parameters
from ViscAI.utils.parse_args_mult_sim import _parse_mw_list

from ViscAI.program_options import viscai_single_run
from ViscAI.server_options import validate_server_connection
from ViscAI.utils.get_conda_path import get_conda_sh_path
# Reutilizamos el builder del script (definido en server_options_tab)


class ProgramoptionsScreen:
    def __init__(self):
        self._input_files = [
            "**Upload input file parameters (DAT)**",
            "Upload polymer configuration file (DAT)",
            "**Select local directory (FOLDER)**"
        ]
        self._input_options = None
        self._batch_mode = None
        self._generate_polymers = None
        self._mw_selection = None

    # -------------------- Utilidades SSH/SFTP -------------------- #

    def _connect_remote(self, host: str, user: str, key_path: str) -> Tuple[paramiko.SSHClient, paramiko.SFTPClient]:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=host, username=user, key_filename=key_path)
        sftp = ssh.open_sftp()
        return ssh, sftp

    def _remote_exec(self, ssh: paramiko.SSHClient, cmd: str) -> Tuple[int, str, str]:
        wrapped = f'bash -lc \'{cmd}\''
        stdin, stdout, stderr = ssh.exec_command(wrapped)
        out = stdout.read().decode("utf-8", errors="ignore").strip()
        err = stderr.read().decode("utf-8", errors="ignore").strip()
        rc = stdout.channel.recv_exit_status()
        return rc, out, err

    def _ensure_remote_dir(self, sftp: paramiko.SFTPClient, remote_path: str):
        parts = remote_path.strip("/").split("/")
        cur = ""
        for p in parts:
            cur = f"{cur}/{p}" if cur else f"/{p}"
            try:
                sftp.stat(cur)
            except IOError:
                sftp.mkdir(cur)

    def _find_or_create_mw_subdir(self, sftp: paramiko.SFTPClient, working_dir: str, mw: int) -> str:
        try:
            entries = sftp.listdir(working_dir)
        except Exception:
            entries = []
        pattern = re.compile(rf".*\b{mw}\b.*", re.IGNORECASE)
        for e in entries:
            remote_subdir = os.path.join(working_dir, e)
            try:
                st_ = sftp.stat(remote_subdir)
                if statmod.S_ISDIR(st_.st_mode) and pattern.match(e):
                    return remote_subdir
            except Exception:
                continue
        new_subdir = os.path.join(working_dir, f"Mw_{mw}_gmol")
        self._ensure_remote_dir(sftp, new_subdir)
        return new_subdir

    def _upload_if_provided(self, sftp: paramiko.SFTPClient, local_path: str, remote_dir: str) -> str:
        if local_path and os.path.isfile(local_path):
            remote_path = os.path.join(remote_dir, os.path.basename(local_path))
            sftp.put(local_path, remote_path)
            return os.path.basename(local_path)
        return ""

    def _resolve_conda_sh_path(self, ssh: paramiko.SSHClient) -> str:
        """
        1) Si ya quedó resuelto en session_state, úsalo.
        2) Llama a get_conda_sh_path(ssh).
        3) Fallbacks remotos probando rutas comunes con [[ -f ]].
        """
        # 1) ¿Guardado de una ejecución previa?
        cached = st.session_state.get("conda_sh_path")
        if isinstance(cached, str) and cached.strip():
            return cached

        # 2) Resolver con tu función real que requiere 'ssh'
        try:
            path = get_conda_sh_path(ssh)  # <-- AHORA SÍ pasamos 'ssh'
            if isinstance(path, str) and path.strip():
                st.session_state["conda_sh_path"] = path
                return path
        except Exception as e:
            st.warning(f"No se pudo resolver get_conda_sh_path(ssh): {e}. Probando rutas estándar.")

        # 3) Fallbacks remotos comprobados
        candidates = [
            "$HOME/miniconda3/etc/profile.d/conda.sh",
            "$HOME/anaconda3/etc/profile.d/conda.sh",
            "/opt/miniconda/etc/profile.d/conda.sh",
            "/opt/conda/etc/profile.d/conda.sh",
        ]
        for c in candidates:
            rc, out, _ = self._remote_exec(ssh, f'[[ -f {c} ]] && echo OK || echo NO')
            if rc == 0 and "OK" in out:
                st.session_state["conda_sh_path"] = c
                return c

        raise RuntimeError("No se encuentra conda.sh en el servidor remoto. Ajusta tu .bashrc o la función get_conda_sh_path(ssh).")

    def _build_bob2p5_cmd(self, input_basename: str, polymer_basename: str) -> str:
        parts = ["bob2p5"]
        if input_basename:
            parts += ["-i", input_basename]
        if polymer_basename:
            parts += ["-c", polymer_basename]
        parts += ["-b", "-p"]
        return " ".join(parts)

    # -------------------- UI Program Options -------------------- #

    def show_screen(self):
        st.write("Fields in **bold** and with '*' are required")

        # Estado de inputs
        self._input_options = st.session_state.get("input_options", {}) or {}
        st.markdown("---")

        # Modo de entrada del DAT
        mode = st.radio(
            "**Select the way to enter input file parameters**",
            ["Upload existing file", "Edit the input file parameters"],
            horizontal=True,
            key="dat_mode"
        )

        if mode == "Upload existing file":
            dat_key = "input_file_000"
            self._input_options.setdefault(dat_key, "")
            st.text_input("**Upload input file parameters (DAT)**",
                          self._input_options[dat_key],
                          key="upload_input_dat_text")

            col1, col2 = st.columns(2)
            if col1.button("Browse file", key="browse_upload_input_dat"):
                handle_button_click("**Upload input file parameters (DAT)**",
                                    dat_key, "browse", "Program options")
            if self._input_options[dat_key] and col2.button("Remove file", key="remove_upload_input_dat"):
                handle_button_click(None, dat_key, "remove", "Program options")

            file_path = self._input_options[dat_key]
            if file_path and os.path.exists(file_path):
                try:
                    content = open(file_path, 'r').read()
                    st.text_area("Input file parameters content:", value=content, height=300)
                except Exception:
                    st.error("ERROR!!! Could not read the input file parameters.")
            st.markdown("---")
        else:
            input_file_parameters()
            file_generated = st.session_state.get("generated_input_dat")
            local_dir = self._input_options.get("input_file_002", "")
            if file_generated and local_dir and os.path.isdir(local_dir):
                save_path = os.path.join(local_dir, "generated_input.dat")
                with open(save_path, "w") as f:
                    f.write(file_generated)
                self._input_options["dat key"] = save_path
                st.session_state["input_options"] = self._input_options

        # Selección de ficheros adicionales
        for index, option in enumerate(self._input_files[1:], start=1):
            input_key = f"input_file_{index:03d}"
            self._input_options.setdefault(input_key, "")
            st.text_input(option, self._input_options[input_key], key=f"{input_key}_input_text")
            if self._input_options[input_key]:
                st.markdown(f"<span style='color:yellow;'>*{os.path.basename(self._input_options[input_key])}*</span>", unsafe_allow_html=True)
            col1, col2 = st.columns(2)
            with col1:
                if "FOLDER" in option:
                    if st.button("Browse directory", key=f"browse_{input_key}"):
                        handle_button_click(option, input_key, "browse_directory", "Program options")
                else:
                    if st.button("Browse file", key=f"browse_{input_key}"):
                        handle_button_click(option, input_key, "browse", "Program options")
            with col2:
                if self._input_options[input_key] and st.button("Remove", key=f"remove_{input_key}"):
                    handle_button_click(option, input_key, "remove", "Program options")

            st.session_state["input_options"] = self._input_options

        # Flags
        self._batch_mode = st.toggle("Use default filenames", value=st.session_state.get("batch_mode", False), key="batch_mode")
        self._generate_polymers = st.toggle("Generate the polymers and quit before trying to relax them", value=st.session_state.get("generate_polymers", False), key="generate_polymers")

        input_file = self._input_options.get("dat key", self._input_options.get("input_file_000", ""))
        polymer_file = self._input_options.get("input_file_001", "")

        # =================== ******NEWWW******* TOGGLE raíz para simulaciones paralelas ===================
        st.markdown("---")
        parallel_toggle = st.toggle("Multiple simulations mode",
                                    value=st.session_state.get("parallel_toggle", False),
                                    key="parallel_toggle")
        # ******NEWWW******* bloque de opciones paralelas (solo visible si toggle activo)
        mw_list = []
        dist_codes_sel = []
        pdi_list = []
        if parallel_toggle:
            # --- DISTRIBUCIONES (multiselect) ---
            DIST_LABELS = ["Monodisperse", "Gaussian", "Log-normal", "Poisson", "Flory"]
            DIST_CODE_MAP = {"Monodisperse": 0, "Gaussian": 1, "Log-normal": 2, "Poisson": 3, "Flory": 4}
            dist_labels_sel = st.multiselect(
                "Select distribution types",
                DIST_LABELS,
                default=st.session_state.get("dist_multi_sel_labels", [])
            )
            st.session_state["dist_multi_sel_labels"] = dist_labels_sel
            dist_codes_sel = [DIST_CODE_MAP[lbl] for lbl in dist_labels_sel]
            st.session_state["dist_multi_sel_codes"] = dist_codes_sel

            # --- PDI múltiples ---
            pdi_text = st.text_input(
                "Enter polydispersities",
                placeholder="Ex.: 1.0, 2.0, 2.5",
                value=st.session_state.get("pdi_text_multi", ""),
                key="pdi_text_multi"
            )

            def _parse_pdi_list(s):
                vals = []
                for t in (s or "").replace("\n", ",").replace(";", ",").split(","):
                    t = t.strip().replace(" ", "")
                    if not t:
                        continue
                    try:
                        vals.append(float(t))
                    except Exception:
                        pass
                return vals

            pdi_list = _parse_pdi_list(pdi_text)
            st.session_state["pdi_list_multi"] = pdi_list

            # --- MW múltiples ---
            mw_text = st.text_area(
                "Enter molecular weights (g/mol)",
                placeholder="Ex.: 10000, 250000, 500000",
                key="mw_list_text",
                height=100,
            )
            mw_list = _parse_mw_list(mw_text)
        # =================== ******NEWWW******* FIN toggle raíz ===================

        # RUN
        col_run, _ = st.columns(2)
        with col_run:
            if st.button("RUN"):
                with st.spinner("The program is running. Please wait..."):
                    st.session_state["run_pressed"] = True

                    if not input_file:
                        st.error("ERROR!!! Please, upload input file parameters (DAT)")
                        return
                    elif not self._input_options.get("input_file_002"):
                        st.error("ERROR!!! Local directory not defined")
                        return

                    connected_server = validate_server_connection()
                    if not connected_server:
                        st.error("ERROR!!! Server connection failed")
                        return

                    if parallel_toggle:
                        if not mw_list and not pdi_list and not dist_codes_sel:
                            st.error("ERROR!!! Please fill at least one of the fields in the 'Multiple simulations mode'")
                            return


                    # ******NEWWW******* modo paralelo si toggle activo; si no, modo simple
                    if st.session_state.get("parallel_toggle", False) and mw_list:
                        dist_codes = st.session_state.get("dist_multi_sel_codes", [])
                        pdis = st.session_state.get("pdi_list_multi", [])
                        results = viscai_paramgrid_run(
                            connected_server["name_server"],
                            connected_server["name_user"],
                            connected_server["ssh_key_options"],
                            connected_server["working_directory"],
                            connected_server["path_virtualenv"],
                            input_file,
                            polymer_file,
                            mw_list,
                            dist_codes,  # puede ser []
                            pdis  # puede ser []
                        )

                        #   TEST
                        # Guardamos info en session_state para output tab
                        st.session_state["multi_sim_results"] = {
                            "results": results,
                            "local_dir": self._input_options.get("input_file_002", "")
                        }

                        st.success("Simulaciones por Mw × PDI × Distribución encoladas/ejecutadas.")
                        if results:
                            st.write("Resumen:", results)

                    else:
                        viscai_single_run(
                            connected_server["name_server"],
                            connected_server["name_user"],
                            connected_server["ssh_key_options"],
                            connected_server["working_directory"],
                            connected_server["path_virtualenv"],
                            input_file,
                            polymer_file,
                            st.session_state.get("batch_mode", False),
                            st.session_state.get("generate_polymers", False)
                        )



    # -------------------- SLURM submit -------------------- #

    def _slurm_submit_multiple_mw(self, connected_server: dict, mw_list: List[int],
                                  local_input_file: str, local_polymer_file: str) -> List[Tuple[str, str, str]]:
        name_server = connected_server["name_server"]
        name_user = connected_server["name_user"]
        ssh_key_options = connected_server["ssh_key_options"]
        working_dir = connected_server["working_directory"]
        venv_path = connected_server["path_virtualenv"]

        partition = st.session_state.get("selected_partition", "")
        nodes = int(st.session_state.get("slurm_nodes", 1))
        cpus_per_task = int(st.session_state.get("slurm_cpus_per_task", 1))
        mem_per_cpu_mb = int(st.session_state.get("slurm_mem_per_cpu", 2048))
        job_name = st.session_state.get("slurm_job_name", "run_simulation_I")

        results: List[Tuple[str, str, str]] = []
        ssh, sftp = None, None
        try:
            ssh, sftp = self._connect_remote(name_server, name_user, ssh_key_options)

            # Resolver conda.sh AHORA con ssh
            conda_sh_path = self._resolve_conda_sh_path(ssh)

            # Comprobar sbatch remoto
            rc, out, _ = self._remote_exec(ssh, "which sbatch || command -v sbatch || echo NO")
            if rc != 0 or ("NO" in out and "sbatch" not in out):
                return [("GLOBAL", "ERROR", "No se encontró 'sbatch' en el servidor remoto (PATH).")]

            for idx, mw in enumerate(mw_list, start=1):
                remote_subdir = self._find_or_create_mw_subdir(sftp, working_dir, mw)
                subdir_name = os.path.basename(remote_subdir)

                # Subir ficheros si existen
                input_basename = self._upload_if_provided(sftp, local_input_file, remote_subdir)
                polymer_basename = self._upload_if_provided(sftp, local_polymer_file, remote_subdir)

                bob2p5_cmd = self._build_bob2p5_cmd(input_basename, polymer_basename)
                suffix = f"simulation_{idx}"  # o usa `subdir_name` si lo prefieres


                remote_script_path = os.path.join(remote_subdir, "submit_slurm.sh")
                self._ensure_remote_dir(sftp, remote_subdir)
                sftp.chmod(remote_script_path, 0o750)

                rc, out, err = self._remote_exec(ssh, f"cd {remote_subdir} && sbatch submit_slurm.sh")
                if rc == 0 and out:
                    results.append((subdir_name, "OK", out.strip()))
                else:
                    results.append((subdir_name, "ERROR", (err or out or "Fallo desconocido al enviar sbatch.").strip()))
        except Exception as e:
            results.append(("GLOBAL", "ERROR", str(e)))
        finally:
            try:
                if sftp:
                    sftp.close()
                if ssh:
                    ssh.close()
            except Exception:
                pass

        return results
