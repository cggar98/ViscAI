# ViscAI/slurm_adapter.py
from typing import Optional
from ViscAI.utils.ServerSlurm import ServerSlurmBasic # ajustar import si el archivo está en otro paquete/path
import logging

class SlurmAdapter:
    """
    Small wrapper around ServerSlurmBasic to:
    - open connection
    - provide sftp upload/put
    - submit sbatch scripts with common options
    - close connection
    """

    def __init__(self, nameserver: str, databasename: str, username: str, key_file: str,
                 encrypted_pass: Optional[str] = None, logger: Optional[logging.Logger] = None):
        # databasename: local DB filename for job tracking (created by ServerSlurmBasic/utils.DBjobs)
        self._srv = ServerSlurmBasic(nameserver, databasename, username, key_file, encrypted_pass, logger=logger)
        self._client = self._srv.connection()  # opens paramiko client
        self._sftp = None

    def ftp(self):
        if self._sftp is None:
            self._sftp = self._srv.ftp_connect()
        return self._sftp

    def upload(self, local_path: str, remote_path: str):
        sftp = self.ftp()
        sftp.put(local_path, remote_path)

    def chmod(self, remote_path: str, mode: int = 0o750):
        # run chmod via execute_cmd (ServerSlurmBasic.execute_cmd uses the same ssh client)
        cmd = f"chmod {oct(mode)[2:]} '{remote_path}'"
        self._srv.execute_cmd(cmd)

    def submit_script(self, remotedir: str, script_name: str,
                      partition: str, nodes: int = 1, cpus_per_task: int = 1,
                      mem_per_cpu: str = "1G", job_name: str = "BoBjob",
                      time_limit: Optional[str] = None, nodelist: Optional[str] = None) -> tuple[str,str]:
        """
        Submit the given script (assumes script_name already uploaded to remotedir).
        Returns (stdout, stderr) from sbatch invocation.
        """
        sbatch_cmd = f"cd '{remotedir}' && sbatch --partition={partition} --nodes={nodes} --cpus-per-task={cpus_per_task} --mem-per-cpu={mem_per_cpu}"
        if time_limit:
            sbatch_cmd += f" --time={time_limit}"
        if nodelist:
            sbatch_cmd += f" --nodelist={nodelist}"
        sbatch_cmd += f" --job-name={job_name} '{script_name}'"
        out, err = self._srv.execute_cmd(sbatch_cmd)
        return out, err

    def close(self):
        try:
            if self._sftp:
                self._sftp.close()
        except Exception:
            pass
        try:
            self._srv.close_connection()
        except Exception:
            pass
