import streamlit as st
import tempfile
import subprocess
import tarfile
import paramiko
import os
from PIL import Image


def download_file_from_server(name_server, username, ssh_key_options, remote_path):
    local_temp_path = tempfile.NamedTemporaryFile(delete=False).name
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(name_server, username=username, key_filename=ssh_key_options)
        sftp = ssh.open_sftp()
        sftp.get(remote_path, local_temp_path)
        sftp.close()
        ssh.close()
        return local_temp_path
    except Exception as e:
        st.error(f"ERROR!!! File download failed: {str(e)}")
        return None


def tar_output_files(name_server, username, ssh_key_options, remote_directory):
    """
    Save output files in a 'tar.gz' file
    """
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(name_server, username=username, key_filename=ssh_key_options)
        sftp = ssh.open_sftp()
        file_list = sftp.listdir(remote_directory)

        with tempfile.TemporaryDirectory() as temp_dir:
            # Download each remote file in the temporal directory
            for file in file_list:
                remote_file = os.path.join(remote_directory, file)
                local_file = os.path.join(temp_dir, file)
                sftp.get(remote_file, local_file)

            # Create 'tar.gz' file
            tar_path = os.path.join(temp_dir, "ViscAI_output.tar.gz")
            with tarfile.open(tar_path, "w:gz") as tar:
                for file in file_list:
                    local_file = os.path.join(temp_dir, file)
                    tar.add(local_file, arcname=file)

            # Read 'tar.gz' file with binary mode
            with open(tar_path, "rb") as f:
                tar_data = f.read()
        sftp.close()
        ssh.close()
        return tar_data
    except Exception as e:
        st.error(f"ERROR!!! BoB output files not downloaded: {str(e)}")
        return None


def list_remote_files(name_server, username, ssh_key_options, remote_directory):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(name_server, username=username, key_filename=ssh_key_options)
        sftp = ssh.open_sftp()
        files = sftp.listdir(remote_directory)
        sftp.close()
        ssh.close()
        return files
    except Exception as e:
        st.error(f"ERROR!!! Listing remote files failed: {str(e)}")
        return []


def convert_agr_to_png(local_agr_path):
    png_file = local_agr_path + ".png"
    command = ["xmgrace", "-hardcopy", "-printfile", png_file, local_agr_path]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"ERROR!!! {local_agr_path} > PNG conversion failed: {result.stderr}")

    # Open 'png' images
    try:
        image = Image.open(png_file)
        rotated_image = image.rotate(-90, expand=True)
        rotated_png_file = png_file.replace(".png", "_rotated.png")
        rotated_image.save(rotated_png_file)
        return rotated_png_file
    except Exception as e:
        raise Exception(f"ERROR!!! Image rotation failed: {str(e)}")
