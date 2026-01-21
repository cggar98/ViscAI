import paramiko


def connect_remote_server(name_server, username, ssh_key_options):
    # SSH connection
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(name_server, username=username, key_filename=ssh_key_options)
    return ssh
