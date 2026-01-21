import paramiko
import os
import datetime
from socket import gaierror

from ViscAI.utils.Server import ServerBasic
from collections import defaultdict



"""
Some functions are inspired or directly copy from slurmqueen project

(https://github.com/Kasekopf/SlurmQueen)

"""


class ServerSlurmBasic(ServerBasic):

    # ===========================================================================================
    def __init__(self, nameserver,  databasename, username, key_file, encrypted_pass=None, logger=None):

        """
        Initialize a server based on SLURM queue system

        Args:
            nameserver (str): The name of the server (i.e: trueno.csic.es or localhost).
            databasename (str):  File name of the database.
            username (str): User name in the server
            key_file (str): Name of the key file generated with "ssh-keygen -t rsa". \
            The passphrase must be empty and the public part must be installed in the server \
            (see `web <https://serverpilot.io/docs/how-to-use-ssh-public-key-authentication/>`_)
            encrypted_pass (str): File containing the encrypted password usign key_file.pub


        """

        super().__init__(nameserver, username=username,
                         keyfile=key_file, dbname=databasename)

        self._logger = logger
        self._encrypted_pass = encrypted_pass

        if self._db_base is not None:
            self._db_base.create_table_qmjobs()

    # ===========================================================================================
    def connection(self, logger=None):
        """
        Open a connection to this server. Throws an exception if the connection fails.

        Returns:
            A `SSHClient`_ connection to this server

        .. _SSHClient: http://docs.paramiko.org/en/stable/api/client.html
        """

        if not self.is_connected():
            self._client = paramiko.SSHClient()
            self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            key = paramiko.RSAKey.from_private_key_file(self._key_file)

            try:
                self._client.connect(self._nameserver, username=self._username, pkey=key)
            except gaierror:
                self._client = None
                raise Exception("**GECOS: Unable to connect to server " + self._nameserver)
            except paramiko.ssh_exception.NoValidConnectionsError:
                self._client = None
                raise Exception("**GECOS: Unable to connect to server " + self._nameserver)

            fmt = "%a %d/%m/%Y, %H:%M:%S"
            m = "\t#QM# Connected to " + self._nameserver + " at {}\n".format(datetime.datetime.now().strftime(fmt))
            print(m) if logger is None else logger.info(m)

        return self._client

    # ===========================================================================================
    def is_connected(self):
        """
        Check if we are actively connected to the server.

        Returns:
            bool: True if we have a connection open to the server, and False otherwise.

        """
        transport = self._client.get_transport() if self._client else None
        return transport and transport.is_active()

    # ===========================================================================================
    def close_connection(self):
        """
        Check if we are actively connected to the server.

        Returns:
            bool: True if we have a connection open to the server, and False otherwise.

        """
        self._client.close()

    # ===========================================================================================
    def ftp_connect(self):

        """
        Open an sftp connection to this server. Throws an exception if the connection fails.

        Returns:
             An `sftp`_ connection to this server.

        .. _sftp: http://docs.paramiko.org/en/stable/api/client.html
        """
        return self.connection().open_sftp()

    # ===========================================================================================
    def execute_cmd(self, command, other_input=None, timeout=10):
        """
        Execute a command (from the WORKING directory) on this server.
        Throws an exception if the connection fails.

        Args:
            command (str): The name to execute.
            other_input (str): Text to be written to stdin.
            timeout (int): A timeout to use when executing the command, in seconds.

        Returns:
            tuple: All text written to stdout by the command
            as a tuple of strings (stdout, stderr).

        Examples:
            A typical use of the method.

            >>> remote_dir = "/"
            >>> zipfilename = "file.zip"
            >>> cmd2 = 'cd %s; rm -f %s'%(remote_dir,zipfilename)
            >>> stdout, stderr = self.execute_cmd(cmd2)

        """

        stdin, stdout, stderr = self._client.exec_command(command, timeout=timeout)  # Non-blocking call
        exit_status = stdout.channel.recv_exit_status()   # Blocking call

        if exit_status != 0:
            print("Command {} cannot be run in server {}".format(command, self._nameserver))

        if other_input is not None:
            stdin.write(other_input)
            stdin.flush()

        return stdout.read().decode("utf8"), stderr.read().decode("utf8")

    # ===========================================================================================
    def send_input_files_to_server(self, listfiles, localdir, remotedir, maxjobsslurm):

        # FTP listfiles *********************************
        server_sftp = self.ftp_connect()

        # Send all files in listfiles from localdir to remotedir
        for ifile_path in listfiles:
            try:

                base = os.path.splitext(ifile_path)[0]               # /home/user/IsoP_rdkit_03_012_gaussian
                fullpath_com = base + ".com"                         # /home/user/IsoP_rdkit_03_012_gaussian.com
                fullpath_sh = base + ".sh"                           # /home/user/IsoP_rdkit_03_012_gaussian.sh
                inputfilename_com = os.path.split(fullpath_com)[-1]  # IsoP_rdkit_03_012_gaussian.com
                inputfilename_sh = os.path.split(fullpath_sh)[-1]    # IsoP_rdkit_03_012_gaussian.sh
                name_job = base.split("/")[-1]                       # IsoP_rdkit_03_012_gaussian

                out_com_path = os.path.join(remotedir, inputfilename_com)
                out_sh_path = os.path.join(remotedir, inputfilename_sh)

                server_sftp.put(fullpath_com, out_com_path)
                server_sftp.put(fullpath_sh, out_sh_path)

                sql_insert = "INSERT INTO qm_jobs VALUES ({0:d}, '{1:s}', 'INSERVER', " \
                             "'NULL', 'NULL', 'NULL', 'NULL')".format(self._db_index, name_job,)
                self._db_base.insert_data(sql_insert)
                self._db_index += 1

            except FileNotFoundError:
                base = os.path.splitext(ifile_path)[0]
                m = "\nEither file {} does not exist in localhost\n".format(fullpath_com)
                m += "Or directory {} does not exist in the remote server\n".format(remotedir)
                print(m) if self._logger is None else self._logger.info(m)
                exit()

        # Create script full_send.sh
        utils.generate_bashscript_send_slurm(localdir, maxjobsslurm=maxjobsslurm)
        source = os.path.join(localdir, "full_send.sh")
        target = os.path.join(remotedir, "full_send.sh")
        server_sftp.put(source, target)

        # Close FTP and update database
        server_sftp.close()
        self._db_base.commit_db()

    # ===========================================================================================
    def extract_energy_calculations(self, localdir, remotedir, qm_engine="gaussian"):

        """
        Create a bash file to get all energy values from logs in the remote_dir.
        Files summary_energy.txt and summary.txt are created (¿¿¿SUSTITUIR POR info.txt???)


        Args:
            localdir (str): Path to store files in the local server.
            remotedir (str): Path to store files in the remote server.
            qm_engine (str): Name of the QM package (example: gaussian)

        """

        scriptname = "check_remote_dir.sh"

        # Generate the bash-script to extract the energy from qm calculations in slurm
        utils.generate_bashscript_check_jobs(qm_engine, localdir, inputname=scriptname)

        # Transfer the script to the remote machine
        # noinspection PyBroadException
        try:
            server_sftp = self.ftp_connect()
            localfile = os.path.join(localdir, scriptname)
            remotefile = os.path.join(remotedir, scriptname)
            server_sftp.put(localfile, remotefile)
            server_sftp.close()
        except Exception:
            pass

        # Run the script --> Extract energy to a file named: summary_energy.txt
        # noinspection PyBroadException
        try:
            cmd1 = 'cd %s; bash %s' % (remotedir, scriptname)
            self.execute_cmd(cmd1)
        except Exception:
            pass

    # ===========================================================================================
    def get_energy_from_calculations(self, out_localdir, remotedir):

        """
        Get the summary files and update the database with the energies from log files.
        This is for SLURM servers

        Args:
            out_localdir (str): Path to store files in the local server.
            remotedir (str): Path to store files in the remote server.

        """

        energyfilename = "summary_energy.txt"
        summaryfilename = "summary.txt"
        dict_energy = defaultdict()

        # Get summary files from FTP ======================
        server_sftp = self.ftp_connect()

        try:
            source = os.path.join(remotedir, energyfilename)
            target = os.path.join(out_localdir, energyfilename)
            server_sftp.get(source, target)
        except FileNotFoundError:
            m = "summary_energy.txt not found in the remote server!!!\n"
            m += " or local {} is not accesible!!".format(out_localdir)
            print(m) if self._logger is None else self._logger.info(m)
            pass

        try:
            source = os.path.join(remotedir, summaryfilename)
            target = os.path.join(out_localdir, summaryfilename)
            server_sftp.get(source, target)
        except FileNotFoundError:
            m = "summary.txt not found in the remote server!!!\n"
            m += " or local {} is not accesible!!".format(out_localdir)
            print(m) if self._logger is None else self._logger.info(m)
            pass

        server_sftp.close()

        # Update database
        source = os.path.join(out_localdir, energyfilename)
        if os.path.exists(source):
            with open(source, 'r') as fin:
                lines = fin.readlines()
                for item in lines:
                    try:
                        name_item, pid_slurm, energy_h, energy_rel, time_s = item.split()
                    except ValueError:
                        continue
                    self._db_base.update_data_row("qm_jobs", "pid_slurm", pid_slurm, "name_job", name_item)
                    self._db_base.update_data_row("qm_jobs", "energy", energy_h, "name_job", name_item)
                    self._db_base.update_data_row("qm_jobs", "cpu_time", time_s, "name_job", name_item)
                    dict_energy[name_item] = float(energy_h)

        source = os.path.join(out_localdir, summaryfilename)
        if os.path.exists(source):
            with open(source, 'r') as f:
                lines = f.readlines()
                for item in lines:
                    pid_slurm, name_item, state = item.split()
                    self._db_base.update_data_row("qm_jobs", "status_job", state, "name_job", name_item)

        self._db_base.commit_db()
        return dict_energy

    # ===========================================================================================
    def send_qm_remote_calc(self, remotedir, partitionmaster=None, nodemaster=None, jobname="g16m",
                            timelimit=None, memlimit=None):

        if nodemaster is None:
            if timelimit is None:
                cmd1 = "cd %s; sbatch --partition=%s --job-name=%s full_send.sh;"\
                     % (remotedir, partitionmaster, jobname)
            else:
                cmd1 = "cd %s; sbatch --partition=%s --job-name=%s --time=%s --mem=%s full_send.sh;"\
                     % (remotedir, partitionmaster, jobname, timelimit, memlimit)
        else:
            if timelimit is None:
                cmd1 = "cd %s; sbatch --partition=%s --nodelist=%s --job-name=%s  full_send.sh;"\
                     % (remotedir, partitionmaster, nodemaster, jobname)
            else:
                cmd1 = "cd %s; sbatch --partition=%s --nodelist=%s --job-name=%s --time=%s --mem=%s full_send.sh;"\
                     % (remotedir, partitionmaster, nodemaster, jobname, timelimit, memlimit)

        self.execute_cmd(cmd1)

    # ===========================================================================================
    def get_output_files_from_server(self, outdir_local, remotedir, pattern=".log"):

        # Open ftp server
        server_sftp = self.ftp_connect()

        # Get name of the completed jobs
        sql_query = "SELECT name_job FROM qm_jobs WHERE status_job LIKE 'COMPLETED'"
        p = self._db_base.query_data(sql_query).fetchall()
        sql_query = "SELECT name_job FROM qm_jobs"
        q = self._db_base.query_data(sql_query).fetchall()
        sql_query = "SELECT name_job FROM qm_jobs WHERE status_job LIKE 'FAILED'"
        r = self._db_base.query_data(sql_query).fetchall()

        m = "\t\tNumber of logs completed from server : {}\n".format(len(p))
        m += "\t\tNumber of logs failed in server      : {}\n".format(len(r))
        m += "\t\tNumber of logs in server             : {}\n".format(len(q))

        print(m) if self._logger is None else self._logger.info(m)

        completed_jobs = 0
        for item in p:
            name_log = item[0]+pattern
            fullpath = os.path.join(outdir_local, name_log)
            completed_jobs += 1
            if os.path.exists(fullpath):
                continue
            else:
                print("Transfering: {}, {} of {}".format(name_log, completed_jobs, len(p)))
            source = os.path.join(remotedir, name_log)
            target = os.path.join(outdir_local, name_log)
            server_sftp.get(source, target)

        server_sftp.close()

        return completed_jobs

    # ===========================================================================================
    def server_check_qm_jobs(self, localdir, remotedir, outdir_local, exec_rmsddock,
                             energy_threshold=99999.0, cutoff_rmsd=1.0):

        # Create a directory to put results
        if not os.path.isdir(outdir_local):
            os.mkdir(outdir_local)

        # Is the calculation already finnished?. Check for done file in the remote dir.
        try:
            server_sftp = self.ftp_connect()
            remotefile = os.path.join(remotedir, 'done')
            localfile = os.path.join(localdir, 'done')
            if os.path.exists(localfile):
                return -1

            # Check done in remote file after all operations
            server_sftp.stat(remotefile)

            # Get energy from calculations
            e_dict = self.get_energy_from_calculations(outdir_local, remotedir)

            # Get logs structure
            completed = self.get_output_files_from_server(outdir_local, remotedir, pattern=".log")
            # Get xyz file from gaussian log optimization
            utils.get_optimized_coordinates(outdir_local)

            # Allign and clusterize mol2 molecules
            cluster_dict, deltaE_dict, rmsd_dict, rmsd_incluster = \
                utils.cluster_optimized_coordinates(e_dict, outdir_local, exec_rmsddock,
                                                    energy_threshold=energy_threshold, cutoff=cutoff_rmsd)

            # Update database
            self._db_base.add_column("qm_jobs", "DeltaE", "FLOAT")
            self._db_base.add_column("qm_jobs", "RMSD", "FLOAT")
            self._db_base.add_column("qm_jobs", "RMSD_INCLUSTER", "FLOAT")
            self._db_base.add_column("qm_jobs", "Cluster", "INTEGER")

            for item in deltaE_dict:
                self._db_base.update_data_row("qm_jobs", "DeltaE", round(deltaE_dict[item], 2), "name_job", item)
                self._db_base.update_data_row("qm_jobs", "RMSD", round(rmsd_dict[item], 3), "name_job", item)
                self._db_base.update_data_row("qm_jobs", "RMSD_INCLUSTER",
                                              round(rmsd_incluster[item], 3), "name_job", item)

            for key, value in cluster_dict.items():
                for ifile in value['files']:
                    item = ifile.split("_allign.")[0]
                    self._db_base.update_data_row("qm_jobs", "Cluster", key, "name_job", item)

            self._db_base.commit_db()

            # Create done in localdir file after all operations
            server_sftp.get(remotefile, localfile)
            server_sftp.close()

            return -2

        except FileNotFoundError:

            # Execute a bash script in the remote server to check the status of the jobs
            self.extract_energy_calculations(localdir, remotedir)
            # Get energy from calculations
            e_dict = self.get_energy_from_calculations(outdir_local, remotedir)
            # Get logs structure
            completed = self.get_output_files_from_server(outdir_local, remotedir, pattern=".log")
            # Get xyz file from gaussian log optimization
            utils.get_optimized_coordinates(outdir_local)

            try:
                server_sftp.stat(remotefile)
                server_sftp.get(remotefile, localfile)
                server_sftp.close()

                # Allign and clusterize mol2 molecules
                cluster_dict, deltaE_dict, rmsd_dict, rmsd_dict_incluster = \
                    utils.cluster_optimized_coordinates(e_dict,
                                                        outdir_local,
                                                        exec_rmsddock,
                                                        energy_threshold=energy_threshold,
                                                        cutoff=cutoff_rmsd)

                # Update database
                self._db_base.add_column("qm_jobs", "DeltaE", "FLOAT")
                self._db_base.add_column("qm_jobs", "RMSD", "FLOAT")
                self._db_base.add_column("qm_jobs", "RMSD_INCLUSTER", "FLOAT")
                self._db_base.add_column("qm_jobs", "Cluster", "INTEGER")

                for item in deltaE_dict:
                    self._db_base.update_data_row("qm_jobs", "DeltaE", round(deltaE_dict[item], 2), "name_job", item)
                    self._db_base.update_data_row("qm_jobs", "RMSD", round(rmsd_dict[item], 3), "name_job", item)
                    self._db_base.update_data_row("qm_jobs", "RMSD_INCLUSTER",
                                                  round(rmsd_dict_incluster[item], 3), "name_job", item)

                for key, value in cluster_dict.items():
                    for ifile in value['files']:
                        item = ifile.split("_allign")[0]
                        self._db_base.update_data_row("qm_jobs", "Cluster", key, "name_job", item)

                self._db_base.commit_db()
                return -2

            except FileNotFoundError:
                return completed
