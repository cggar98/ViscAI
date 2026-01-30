import os
import re
import paramiko
import stat as statmod
import streamlit as st
from typing import Tuple
from ViscAI.ViscAI_gui.dat_help_generator import parse_inp_dat, generate_help_text
from ViscAI.utils.db_SQLite import database_db_creation
from ViscAI.utils.program_update_events import handle_button_click
from ViscAI.utils.parameters_customer import input_file_parameters
from ViscAI.utils.parse_args_mult_sim import _parse_mw_list, _parse_pdi_list
from ViscAI.utils.get_conda_path import get_conda_sh_path
from ViscAI.program_options import viscai_paramgrid_run, viscai_single_run
from ViscAI.server_options import validate_server_connection


class ProgramoptionsScreen:
    def __init__(self):
        self._input_files = [
            "**Upload input file parameters (DAT)***",
            "Upload polymer configuration file (DAT)",
            "**Select local directory (FOLDER)***"
        ]
        self._input_options = None
        self._batch_mode = None
        self._generate_polymers = None
        self._multiple_sim = None
        self._dist_labels_sel = None
        self._pdi_selection = None
        self._mw_selection = None

    # -------------------- UI Program Options -------------------- #

    def show_screen(self):
        st.write("Fields in **bold** and with '*' are required")

        self._input_options = st.session_state.get("input_options", {}) or {}
        st.markdown("---")

        col_files, col_toggles = st.columns(2)

        # -------------- File upload -------------- #
        with col_files:
            mode = st.radio(
                "**Select the way to enter input file parameters***",
                ["Upload existing file", "Edit the input file parameters"],
                horizontal=True,
                key="dat_mode"
            )

            if mode == "Upload existing file":
                dat_key = "input_file_000"
                help_line = ("**LINE 1**\n\n"
                             "First entry: maximum number of polymers\n\n"
                             "Second entry: maximum number of segments\n\n"
                             "**LINE 2**\n\n"
                             "Dynamic dilation exponent α\n\n"
                             "**LINE 3**\n\n"
                             "Fine tuning parameter\n\n"
                             "**LINE 4**\n\n"
                             "First entry: monomer mass (g/mol)\n\n"
                             "Second entry: number of monomers in one entangled segment\n\n"
                             "Third entry: density (g/cm³)\n\n"
                             "**LINE 5**\n\n"
                             "First entry: entanglement time (s)\n\n"
                             "Second entry: temperature (K)\n\n"
                             "**LINE 6**\n\n"
                             "Number of components or species\n\n")
                self._input_options.setdefault(dat_key, "")

                col1, col2 = st.columns(2)
                if col1.button("Browse file", key="browse_upload_input_dat"):
                    handle_button_click("**Upload input file parameters (DAT)***",
                                        dat_key, "browse", "Program options")
                if self._input_options[dat_key] and col2.button("Remove file", key="remove_upload_input_dat"):
                    handle_button_click(None, dat_key, "remove", "Program options")

                file_path = self._input_options[dat_key]
                if file_path and os.path.exists(file_path):
                    content = open(file_path).read()
                    lines = [l.strip() for l in content.splitlines() if l.strip()]
                    try:
                        parsed = parse_inp_dat(lines)
                        help_dynamic = generate_help_text(parsed)
                    except Exception as e:
                        help_dynamic = f"Could not parse file: {e}"
                else:
                    help_dynamic = help_line
                st.text_input("**Upload input file parameters (DAT)***",
                              self._input_options[dat_key],
                              key="upload_input_dat_text")
                try:
                    content = open(file_path, 'r').read()
                    st.text_area("Input file parameters content:", value=content, height=300, help=help_dynamic)
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

        # -------------- Toggle buttons -------------- #
        with col_toggles:
            self._batch_mode = st.toggle("Use default filenames", value=st.session_state.get("batch_mode", False), key="batch_mode")
            self._generate_polymers = st.toggle("Generate the polymers and quit before trying to relax them", value=st.session_state.get("generate_polymers", False), key="generate_polymers")

            input_file = self._input_options.get("dat key", self._input_options.get("input_file_000", ""))
            polymer_file = self._input_options.get("input_file_001", "")


            st.markdown("---")
            # -------------- Multiple Simulation mode -------------- #
            self._multiple_sim = st.toggle("Multiple simulations mode",
                                        value=st.session_state.get("multiple_sim", False),
                                        key="multiple_sim")
            mw_list = [] # Molecular weight
            dist_codes_sel = [] # Distribution types
            pdi_list = [] # PDI
            if self._multiple_sim:
                # -------------- Distribution type selection -------------- #
                dist_labels = ["Monodisperse", "Gaussian", "Log-normal", "Poisson", "Flory"]
                dist_code_map = {"Monodisperse": 0, "Gaussian": 1, "Log-normal": 2, "Poisson": 3, "Flory": 4}
                self._dist_labels_sel = st.multiselect(
                    "Select distribution types",
                    dist_labels,
                    default=st.session_state.get("dist_multi_sel_labels", [])
                )
                st.session_state["dist_multi_sel_labels"] = self._dist_labels_sel
                dist_codes_sel = [dist_code_map[lbl] for lbl in self._dist_labels_sel]
                st.session_state["dist_multi_sel_codes"] = dist_codes_sel

                # -------------- PDI selection -------------- #
                self._pdi_selection = st.text_input(
                    "Enter polydispersities",
                    placeholder="Ex.: 1.0, 2.0, 2.5",
                    value=st.session_state.get("pdi_selection_multi", ""),
                    key="pdi_selection_multi"
                )

                pdi_list = _parse_pdi_list(self._pdi_selection)
                st.session_state["pdi_list_multi"] = pdi_list

                # -------------- Mw selection -------------- #
                self._mw_selection = st.text_area(
                    "Enter molecular weights (g/mol)",
                    placeholder="Ex.: 10000, 250000, 500000",
                    key="mw_list_text",
                    height=100,
                )
                mw_list = _parse_mw_list(self._mw_selection)
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

                    if self._multiple_sim:
                        if not mw_list and not pdi_list and not dist_codes_sel:
                            st.error("ERROR!!! Please fill at least one of the fields in the 'Multiple simulations mode'")
                            return

                    if st.session_state.get("multiple_sim", False) and mw_list:
                        dist_codes = st.session_state.get("dist_multi_sel_codes", [])
                        pdis = st.session_state.get("pdi_list_multi", [])
                        results = viscai_paramgrid_run(
                            connected_server["name_server"],
                            connected_server["name_user"],
                            connected_server["ssh_key_options"],
                            connected_server["working_directory"],
                            connected_server["bob_remote_fullpath"],
                            input_file,
                            polymer_file,
                            mw_list,
                            dist_codes,
                            pdis
                        )

                        # Guardamos info en session_state para output tab
                        st.session_state["multi_sim_results"] = {
                            "results": results,
                            "local_dir": self._input_options.get("input_file_002", "")
                        }

                        # Marcamos que la ejecución paralela ha terminado para que se muestre el botón GENERATE...
                        st.session_state["parallel_run_finished"] = True


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
        # =================== ******NEWWW******* FIN toggle raíz ===================


        # ****************************CAMBIO*************
        # ****************************CAMBIO*************
        # Si la ejecución paralela terminó, mostramos el botón GENERATE RHEOLOGICAL DATABASE
        # en la columna de la derecha (col_gen).
        if st.session_state.get("parallel_run_finished", False):
            # **************** NUEVO CAMBIO **********
            # Ejecutar generación de DB reológica en local al pulsar el botón
            if st.button("GENERATE RHEOLOGICAL DATABASE", key="generate_rheo_db"):
                local_dir = self._input_options.get("input_file_002", "")
                if not local_dir or not os.path.isdir(local_dir):
                    st.error("ERROR!!! Local directory not defined or does not exist.")
                else:
                    st.session_state["generate_rheo_pressed"] = True
                    # Llamada sin conexión al servidor: forzamos modo local pasando name_server=None
                    with st.spinner("Generating rheological database locally..."):
                        try:
                            db_path = database_db_creation(
                                name_server=None,          # modo LOCAL
                                name_user=None,
                                ssh_key_options=None,
                                working_directory=local_dir,
                                include_root=False,
                                per_mw=True,
                                upload_per_mw=False,
                                sort_ids_by_mw=True,
                                is_parallel=True
                            )
                            # database_db_creation devuelve path (o None / lanza excepción)
                            st.success(f"Rheological database generated: {db_path}")
                            st.session_state["rheo_db_path"] = str(db_path)
                        except Exception as e:
                            st.error(f"ERROR generating rheological DB: {e}")
            # **************** NUEVO CAMBIO **********
