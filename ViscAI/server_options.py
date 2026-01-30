import streamlit as st
import json
import paramiko
import os


def ensure_json_extension(json_filename):
    if not json_filename.endswith('.json'):
        json_filename += '.json'
    return json_filename


def save_options_to_json(name_server, name_user,
                         ssh_key_options, bob_remote_fullpath,
                         working_directory, json_filename):
    if not json_filename:
        return None

    options = {
        "Name Server*": "{}".format(name_server),
        "Username*": "{}".format(name_user),
        "Key SSH file path*": "{}".format(ssh_key_options),
        "BoB remote fullpath*": "{}".format(bob_remote_fullpath),
        "Working directory*": "{}".format(working_directory)
    }

    json_string = json.dumps(options, indent=4)
    return json_string


def validate_server_connection():
    server_options = st.session_state.get("server_options", {})
    name_server = server_options.get("Name Server*", "")
    name_user = server_options.get("Username*", "")
    ssh_key_options = server_options.get("Key SSH file path*", "")
    bob_remote_fullpath = server_options.get("BoB remote fullpath*", "")
    working_directory = server_options.get("Working directory*", "")

    server_connected = (
        check_username_and_name_server(name_server, name_user, ssh_key_options) and
        verify_bob_remote_fullpath(name_server, name_user, ssh_key_options, bob_remote_fullpath) and
        verify_working_directory(name_server, name_user, ssh_key_options, working_directory)
    )

    if not server_connected:
        st.error("ERROR!!! Server connection failed")
        st.warning("Configure 'Server options' tab")
        return None
    return {
        "name_server": name_server,
        "name_user": name_user,
        "ssh_key_options": ssh_key_options,
        "bob_remote_fullpath": bob_remote_fullpath,
        "working_directory": working_directory
    }


def check_username_and_name_server(name_server, name_user, ssh_key_options):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(name_server, username=name_user, key_filename=ssh_key_options, timeout=0.5)
        ssh.close()
        return True  # Succesfull conection
    except paramiko.AuthenticationException:
        return False
    except (paramiko.SSHException, paramiko.ssh_exception.NoValidConnectionsError) as e:
        return False
    except Exception as e:  # Failed conection
        return False


def verify_bob_remote_fullpath(name_server, name_user, ssh_key_options, bob_remote_fullpath):
    """Verifies if the bob executable exists and is executable on the remote server."""

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(name_server, username=name_user, key_filename=ssh_key_options)

    cmd = f"[ -f '{bob_remote_fullpath}' ] && [ -x '{bob_remote_fullpath}' ] && echo exists || echo not_exists"
    stdin, stdout, stderr = ssh.exec_command(cmd)

    result = stdout.read().decode().strip()
    ssh.close()

    return result == "exists"


def verify_working_directory(name_server, name_user, ssh_key_options, working_directory):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(name_server, username=name_user, key_filename=ssh_key_options)

    stdin, stdout, stderr = ssh.exec_command(f"if [ -d '{working_directory}' ];"
                                             f" then echo 'exists'; else echo 'not exists'; fi")
    result = stdout.read().decode().strip()
    return result == "exists"


def generate_command(tab):
    if "script_content" not in st.session_state:
        st.session_state.script_content = ""
    if tab == "Program options":
        lines_to_keep = []
        script_lines = st.session_state.script_content.splitlines()
        for line in script_lines:
            line = line.strip()
            if line.startswith("#!/bin/bash -l") or line.startswith("#SBATCH") or \
                    line.startswith("WD=`pwd`") or "module load" in line or "source" in line or "sleep" in line or "mamba" in line or "conda" in line:
                lines_to_keep.append(line)

        # Generates 'BoB' commands with currently options
        input_file = os.path.basename(st.session_state.get("input_options", {}).get("input_file_000", ""))
        polymer_configuration = os.path.basename(st.session_state.get("input_options", {}).get("input_file_001", ""))

        # Dynamic command construction
        command_parts = [f"-i {input_file}" if input_file else "",
                         f"-c {polymer_configuration}" if polymer_configuration else ""]

        if st.session_state.get("batch_mode", False):
            command_parts.append("-b")
        if st.session_state.get("generate_polymers", False):
            command_parts.append("-p")

        # Join command parts
        command = "bob2p5 " + " ".join(part for part in command_parts if part)

        # Keep script interface
        interface_script = "\n".join(lines_to_keep) + f"\n\n{command}\n"

        # Update interface script to show it
        st.session_state["script_content"] = interface_script
        return interface_script


def update_inputs_from_text_area():
    script_content = st.session_state.script_content

    # Add #SBATCH options line
    additional_options = []

    for line in script_content.splitlines():
        line = line.strip()
        if line.startswith("#SBATCH"):
            additional_options.append(line)

    st.session_state.sbatch_options = "\n".join(additional_options)

    if "module load" in script_content:
        st.session_state.load_modules = script_content.split("module load")[1].split("\n")[0].strip()

    if "source" in script_content:
        st.session_state.source_env = script_content.split("source")[1].split("\n")[0].strip()

    if "mamba" in script_content:
        st.session_state.mamba_env = script_content.split("mamba")[1].split("\n")[0].strip()

    if "conda" in script_content:
        st.session_state.conda_env = script_content.split("conda")[1].split("\n")[0].strip()

    if "bob2p5" in script_content:
        input_file_line = next(
            (line for line in script_content.splitlines() if "bob2p5" in line and "-i" in line), None)
        if input_file_line:
            st.session_state["input_options"]["input_file_000"] = input_file_line.split("-i ")[1].split()[0].strip()
        else:
            st.session_state["input_options"]["input_file_000"] = ""

        polymer_configuration_line = next(
            (line for line in script_content.splitlines() if "bob2p5" in line and "-c" in line), None)
        if polymer_configuration_line:
            st.session_state["input_options"]["input_file_001"] = polymer_configuration_line.split("-c ")[1].split()[0].strip()
        else:
            st.session_state["input_options"]["input_file_001"] = ""

        batch_mode_line = next(
            (line for line in script_content.splitlines() if "bob2p5" in line and "-b" in line), None)
        if batch_mode_line:
            st.session_state["batch_mode"] = True
        else:
            st.session_state["batch_mode"] = False

        generate_polymers_line = next(
            (line for line in script_content.splitlines() if "bob2p5" in line and "-p" in line), None)
        if generate_polymers_line:
            st.session_state["generate_polymers"] = True
        else:
            st.session_state["generate_polymers"] = False
