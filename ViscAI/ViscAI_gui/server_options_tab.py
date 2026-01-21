# server_options_tab.py
# submit_bob2p5_slurm.py
import json
import tkinter as tk
from tkinter import filedialog
import os
import streamlit as st

# ✅ imports que faltaban

from ViscAI.server_options import (
    check_username_and_name_server,
    verify_virtualenv_path,
    verify_working_directory,
    ensure_json_extension,
)


# ============================ Pantalla de opciones de servidor ============================ #

class ServerScreen:
    def __init__(self):
        self._name_server = None
        self._name_user = None
        self._ssh_key_options = None
        self._path_virtualenv = None
        self._working_directory = None
        self._json_filename = None
        self._input_placeholder = None
        self._use_queueing_system = None
        self._script_uploaded = None

    def show_screen(self):
        st.markdown("## Load server options", unsafe_allow_html=True)
        self._input_placeholder = st.empty()
        self._input_placeholder.text_input("Load file:", key="json_filepath")

        browse_load_file = st.button("Browse", key="browse_load")
        if browse_load_file:
            root = tk.Tk()
            root.withdraw()
            root.call('wm', 'attributes', '.', '-topmost', True)
            wkdir = os.getcwd()
            filename = filedialog.askopenfilename(
                initialdir=wkdir,
                parent=root,
                title="Select a file containing server options",
                filetypes=[("JSON files", "*.json")]
            )
            root.destroy()
            if filename:
                json_filepath = filename
                self._input_placeholder.text_input("Load file:", key="json_input", value=json_filepath)
                file_name = os.path.basename(json_filepath)
                st.markdown(f"*{file_name}*", unsafe_allow_html=True)

        # Cargar JSON si se ha seleccionado
        if st.session_state.get("json_input"):
            json_filepath = st.session_state["json_input"]
            try:
                with open(json_filepath, "r") as f:
                    server_options = json.load(f)
                if server_options:
                    self._name_server = server_options.get("Name Server*", "")
                    self._name_user = server_options.get("Username*", "")
                    self._ssh_key_options = server_options.get("Key SSH file path*", "")
                    self._path_virtualenv = server_options.get("Virtual environment path*", "")
                    self._working_directory = server_options.get("Working directory*", "")

                    st.session_state.server_options = {
                        "Name Server*": self._name_server,
                        "Username*": self._name_user,
                        "Key SSH file path*": self._ssh_key_options,
                        "Virtual environment path*": self._path_virtualenv,
                        "Working directory*": self._working_directory
                    }
                    st.session_state["ssh_key_options"] = self._ssh_key_options
            except Exception as e:
                st.error(f"ERROR!!! Error loading file: {str(e)}")

        # Formulario
        st.markdown("## Configure server options", unsafe_allow_html=True)
        st.write("Fields in **bold** and with '*' are required")

        server_options = st.session_state.get("server_options", {
            "Name Server*": "",
            "Username*": "",
            "Key SSH file path*": "",
            "Virtual environment path*": "",
            "Working directory*": ""
        })

        self._name_server = st.text_input("**Name Server***", server_options.get("Name Server*", ""), key="name_server")
        self._name_user = st.text_input("**Username***", server_options.get("Username*", ""))

        ssh_key_temp = st.session_state.get("ssh_key_temp", "")
        self._ssh_key_options = st.text_input("**Key SSH file path***", value=ssh_key_temp, key="ssh_key_options")
        browse_ssh_file = st.button("Browse SSH Key", key="browse_ssh")
        if browse_ssh_file:
            root = tk.Tk()
            root.withdraw()
            root.call('wm', 'attributes', '.', '-topmost', True)
            ssh_filename = filedialog.askopenfilename(title="Select SSH Key file", filetypes=[("SSH Key files", "*")])
            root.destroy()
            if ssh_filename:
                st.session_state["ssh_key_temp"] = ssh_filename
                st.rerun()

        self._path_virtualenv = st.text_input("**Virtual environment path***", server_options.get("Virtual environment path*", ""))
        self._working_directory = st.text_input("**Working directory***", server_options.get("Working directory*", ""))

        # Persistir
        st.session_state["server_options"] = {
            "Name Server*": self._name_server,
            "Username*": self._name_user,
            "Key SSH file path*": self._ssh_key_options,
            "Virtual environment path*": self._path_virtualenv,
            "Working directory*": self._working_directory
        }

        # Validaciones básicas de conexión
        valid = True
        if not self._name_server or not self._name_user or not self._ssh_key_options or not self._path_virtualenv or not self._working_directory:
            st.error("ERROR!!! Please enter all required fields")
            valid = False
            return

        if not os.path.exists(self._ssh_key_options):
            st.error(f"ERROR!!! SSH key path '{self._ssh_key_options}' does not exist")
            valid = False
            return

        if not check_username_and_name_server(self._name_server, self._name_user, self._ssh_key_options):
            st.error(f"ERROR!!! Invalid server or username")
            valid = False
            return

        if not verify_virtualenv_path(self._name_server, self._name_user, self._ssh_key_options, self._path_virtualenv):
            st.error(f"ERROR!!! Virtual environment path '{self._path_virtualenv}' does not exist on remote server")
            valid = False
            return

        if not verify_working_directory(self._name_server, self._name_user, self._ssh_key_options, self._working_directory):
            st.error(f"ERROR!!! Working directory '{self._working_directory}' does not exist")
            valid = False
            return
        else:
            st.success("Connected server successfully")

        st.markdown("## Save server options", unsafe_allow_html=True)
        self._json_filename = st.text_input("Filename:", key="json_filename")
        if self._json_filename:
            if self._json_filename.strip():
                self._json_filename = ensure_json_extension(self._json_filename.strip())
                json_payload = json.dumps({
                    "Name Server*": self._name_server,
                    "Username*": self._name_user,
                    "Key SSH file path*": self._ssh_key_options,
                    "Virtual environment path*": self._path_virtualenv,
                    "Working directory*": self._working_directory
                }, indent=2).encode("utf-8")
                st.download_button(label="Save", data=json_payload, file_name=self._json_filename, mime="application/json")

        #########


               # =================== SLURM OPTIONS ===================
        st.markdown("---")
        use_slurm = st.toggle(
            "Usar sistema de colas SLURM",
            value=bool(st.session_state.get("slurm_partition", "")),
            key="use_slurm_toggle"
        )

        if use_slurm:
            st.subheader("Configuración SLURM")

            st.session_state["slurm_partition"] = st.text_input(
                "Partición",
                value=st.session_state.get("slurm_partition", "")
            )

            st.session_state["slurm_nodes"] = st.number_input(
                "Número de nodos",
                min_value=1,
                value=int(st.session_state.get("slurm_nodes", 1))
            )

            st.session_state["slurm_cpus_per_task"] = st.number_input(
                "CPUs por tarea",
                min_value=1,
                value=int(st.session_state.get("slurm_cpus_per_task", 1))
            )

            st.session_state["slurm_mem_per_cpu"] = st.text_input(
                "Memoria por CPU",
                value=str(st.session_state.get("slurm_mem_per_cpu", "1G")),
                help="Ejemplos: 1024M, 2G"
            )

            st.session_state["slurm_job_name"] = st.text_input(
                "Prefijo del nombre del job",
                value=st.session_state.get("slurm_job_name", "BoBjob")
            )

        else:
            # Si SLURM se desactiva, limpiamos la partición (clave)
            st.session_state["slurm_partition"] = ""
