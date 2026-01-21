import os


def upload_file_to_server(ssh, local_path, remote_path):
    sftp = ssh.open_sftp()
    sftp.put(local_path, remote_path)
    sftp.close()


def upload_input_files(ssh, input_file, polymer_configuration, working_directory):
    """
    Sube los archivos de entrada al servidor remoto usando SFTP.
    """
    sftp = ssh.open_sftp()

    # Subir archivo de entrada obligatorio
    remote_input_file = os.path.join(working_directory, os.path.basename(input_file))
    sftp.put(input_file, remote_input_file)

    # Subir archivo de configuración de polímero si existe
    if polymer_configuration:
        remote_polymer_file = os.path.join(working_directory, os.path.basename(polymer_configuration))
        sftp.put(polymer_configuration, remote_polymer_file)

    sftp.close()

