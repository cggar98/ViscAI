import os
import streamlit as st
from ViscAI.utils.inp_files_transfer import upload_file_to_server
from ViscAI.utils.ssh_connection import connect_remote_server
from ViscAI.ViscAI_exec import execute_remote_process


def bob_rc_transfering(name_server, name_user, ssh_key_options, working_directory, virtualenv_path, input_file):
    """
    How bob_rc parameters transfer to remote server
    """
    if st.session_state.get("configure_rc_toggle", False):
        transfer_mode = st.session_state.get("bobrc_mode", "Edit the parameters of bob.rc")

        if transfer_mode == "Upload existing file":
            rc_custom_path = st.session_state.get("bobrc_file", "")
        else:
            rc_content = st.session_state.get("bobrc_custom_content", "")
            rc_custom_path = os.path.join(os.getcwd(), "custom_bob.rc")
            try:
                with open(rc_custom_path, 'w') as f:
                    f.write(rc_content)
            except Exception as e:
                st.error(f"ERROR!!! Could not write custom bob.rc: {e}")
                return False

        if not rc_custom_path or not os.path.exists(rc_custom_path):
            st.error("ERROR!!! No bob.rc to upload.")
            return False

        try:
            ssh = connect_remote_server(name_server, name_user, ssh_key_options)
            remote_rc = os.path.join(working_directory, 'bob.rc')
            upload_file_to_server(ssh, rc_custom_path, remote_rc)
            ssh.close()

            # Remove 'custom_bob.rc' from local
            if os.path.exists(rc_custom_path) and os.path.basename(rc_custom_path) == "custom_bob.rc":
                try:
                    os.remove(rc_custom_path)
                except Exception as e:
                    st.warning(f"WARNING!!! Could not delete temporary bob.rc file: {e}")
            return True
        except Exception as e:
            st.error(f"ERROR!!! Upload custom bob.rc failed: {e}")
            return False

    else:
        # Download default bob.rc parameters
        remote_rc_path = os.path.join(working_directory, 'bob.rc')
        download_command = (
            f"wget -q https://sourceforge.net/projects/bob-rheology/files/bob-rheology/bob2.5/bob.rc "
            f"-O {remote_rc_path}"
        )

        output_dl, error_dl = execute_remote_process(
            name_server, name_user, ssh_key_options,
            working_directory, virtualenv_path,
            download_command, input_file, polymer_configuration=None
        )

        if error_dl:
            st.error(f"ERROR!!! 'bob.rc' file download failed: {error_dl}")
            return False
        else:
            st.info("'bob.rc' parameters configured by default")
            return True
