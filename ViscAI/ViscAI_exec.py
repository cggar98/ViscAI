import os
import streamlit as st
from ViscAI.utils.inp_files_transfer import upload_file_to_server
from ViscAI.utils.ssh_connection import connect_remote_server
from ViscAI.utils.get_conda_path import get_conda_sh_path


def build_viscai_command(input_file, polymer_file, batch_mode, generate_polymers):
    # ViscAI command construction
    if not input_file:
        st.error("ERROR!!! Please, upload input file parameters (DAT)")
        return None

    command = f"bob2p5 -i {os.path.basename(input_file)}"
    if batch_mode:
        command += " -b"
    if polymer_file:
        command += f" -c {os.path.basename(polymer_file)}"
    if generate_polymers:
        command += " -p"

    return command


def execute_remote_process(name_server, username, ssh_key_options, working_directory, virtualenv_path, command,
                           input_file,
                           polymer_configuration=None):
    try:
        ssh = connect_remote_server(name_server, username, ssh_key_options)

        # Upload mandatory input file
        remote_input_file = f"{working_directory}/{os.path.basename(input_file)}"
        upload_file_to_server(ssh, input_file, remote_input_file)

        # Upload polymer configuration file (optional)
        if polymer_configuration:
            remote_polymer_file = f"{working_directory}/{os.path.basename(polymer_configuration)}"
            upload_file_to_server(ssh, polymer_configuration, remote_polymer_file)

        # Find conda.sh path within remote server
        conda_sh_path = get_conda_sh_path(ssh)

        # Conda environment activation
        if conda_sh_path:
            activate_virtualenv = f"source {conda_sh_path} && conda activate {virtualenv_path}"
        else:
            st.error(
                "ERROR!!! conda.sh path not found."
                "Please, check conda.sh path in '~/.bashrc' file."
            )
            st.stop() # MEJORARLO

        # Full command ViscAI execution
        if activate_virtualenv:
            full_command = f"cd {working_directory} && {activate_virtualenv} && {command}"
        else:
            full_command = f"cd {working_directory} && {command}"

        stdin, stdout, stderr = ssh.exec_command(full_command)
        output = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')
        ssh.close()
        return output, error

    except Exception as e:
        return None, str(e)
