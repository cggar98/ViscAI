import streamlit as st
import os
import shutil
import paramiko


def clean_remote_directory(name_server, name_user, ssh_key_options, working_directory):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(name_server, username=name_user, key_filename=ssh_key_options)

        clean_wd = f"rm -rf {working_directory}/*"
        stdin, stdout, stderr = ssh.exec_command(clean_wd)
        clean_error = stderr.read().decode().strip()

        ssh.close()

        if clean_error:
            st.warning(f"WARNING!!! Could not clean remote directory: {clean_error}")
    except Exception as e:
        st.error(f"ERROR!!! Failed to clean remote directory: {str(e)}")


def remove_db_local(local_db):
    # Remove DB local
    if os.path.exists(local_db):
        os.remove(local_db)


def remove_csv_exports(local_csv_dir):
    # Remove 'csv_exports' local directory
    shutil.rmtree(local_csv_dir)
