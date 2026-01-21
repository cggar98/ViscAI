import streamlit as st
import os
import base64
import matplotlib.pyplot as plt
from ViscAI.program_output import (download_file_from_server,
                                                tar_output_files,
                                                list_remote_files,
                                                convert_agr_to_png)

class ProgramoutputScreen:

    #   TEST
    def show_screen(self):
        st.header("ViscAI output")

        # You must press 'RUN' button before
        if not st.session_state.get("run_pressed", False):
            return

        # Get server options from 'st.session_state'
        server_options = st.session_state.get("server_options", {})
        name_server = server_options.get("Name Server*", "")
        name_user = server_options.get("Username*", "")
        ssh_key_options = server_options.get("Key SSH file path*", "")
        working_directory = server_options.get("Working directory*", "")

        if not all([name_server, name_user, ssh_key_options, working_directory]):
            st.error("ERROR!!! Connection data is missing")
            st.warning("Please, configure 'Server options' tab")
            return

        ########################    TEST output    #########################
        # Replace the previous single-info.txt handling with this block

        # Get listing of the working directory (files + subdirs)
        remote_entries = list_remote_files(name_server, name_user, ssh_key_options, working_directory)


        ########################################################
        # Listado de subdirectorios Mw_* (suponiendo remote_entries obtenido antes)
        mw_subdirs = [e for e in remote_entries if str(e).startswith("Mw_")]

        if mw_subdirs:
            for idx, sd in enumerate(sorted(mw_subdirs)):
                remote_info_path = os.path.join(working_directory, sd, "info.txt")
                local_info_path = download_file_from_server(name_server, name_user, ssh_key_options, remote_info_path)
                with st.expander(f"{sd}"):
                    if local_info_path and os.path.exists(local_info_path):
                        try:
                            with open(local_info_path, "r", encoding="utf-8", errors="ignore") as f:
                                content = f.read()
                            # clave única para evitar colisiones en Streamlit
                            safe_key = f"info_{idx}_{sd}"
                            st.text_area("info.txt", value=content, height=200, key=safe_key)
                        except Exception as e:
                            st.error(f"ERROR!!! Leyendo info.txt en {sd}: {e}")
                        finally:
                            try:
                                os.remove(local_info_path)
                            except Exception:
                                pass
                    else:
                        st.warning(f"'info.txt' no encontrado en: {sd}")


                        ##################################################
        else:
            # Fallback: behaviour for single-run (root info.txt), keep previous behaviour
            remote_file_path = os.path.join(working_directory, "info.txt")
            local_file_path = download_file_from_server(name_server, name_user, ssh_key_options, remote_file_path)
            if local_file_path and os.path.exists(local_file_path):
                try:
                    with open(local_file_path, "r", encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                    st.text_area("'info.txt' content", value=content, height=300)
                except Exception as e:
                    st.error(f"ERROR!!! File reading failed: {str(e)}")
                finally:
                    try:
                        os.remove(local_file_path)
                    except Exception:
                        pass
            else:
                st.error("ERROR!!! 'info.txt' file not found or downloaded")

            ########################    TEST    #########################







        # Link for download output files
        #   TEST

        # Mensaje de éxito para Multiple Simulations Mode
        multi_sim = st.session_state.get("multi_sim_results")
        if not multi_sim:
            tar_data = tar_output_files(name_server, name_user, ssh_key_options, working_directory)
            if tar_data:
                # 1) Intentamos grabar directamente en el directorio local elegido
                local_dir = st.session_state.get("input_options", {}).get("input_file_002", "")
                if local_dir and os.path.isdir(local_dir):
                    target = os.path.join(local_dir, "ViscAI_output.tar.gz")

                try:

                    with open(target, "wb") as f:
                        f.write(tar_data)
                    st.success(f"Output saved to local directory: `{target}`")
                except Exception as e:
                    st.error(f"Saving to `{local_dir}` failed: {e}")
                # 2) Siempre ofrecemos también el enlace de descarga por si el usuario prefiere
                b64 = base64.b64encode(tar_data).decode()
                href = f'<a href="data:application/gzip;base64,{b64}" download="ViscAI_output.tar.gz">Download ViscAI output files</a>'
                st.markdown(href, unsafe_allow_html=True)
        else:
            local_dir = multi_sim.get("local_dir", "")
            if local_dir and os.path.isdir(local_dir):
                st.success(
                    f"✅ Los resultados de las simulaciones múltiples se han guardado en el directorio local: `{local_dir}`")





        # tar_data = tar_output_files(name_server, name_user, ssh_key_options, working_directory)
        # if tar_data:
        #     b64 = base64.b64encode(tar_data).decode()
        #     href = f'<a href="data:application/gzip;base64,{b64}" download="ViscAI_output.tar.gz">Download BoB output files</a>'
        #     st.markdown(href, unsafe_allow_html=True)

        # Graphics from '.agr' output files
        remote_files = list_remote_files(name_server, name_user, ssh_key_options, working_directory)
        agr_files = [f for f in remote_files if f.lower().endswith(".agr")]

        if agr_files:
            for agr_file in agr_files:
                #   TEST
                if agr_file.lower() == "gt.agr":
                    st.markdown("**Relaxation modulus G(t)**")
                elif agr_file.lower() == "gtp.agr":
                    st.markdown("**Elastic modulus G'(ω) and viscous modulus G''(ω)**")
                # elif GPCLS GRAPHICS!!!!!

                remote_agr_path = os.path.join(working_directory, agr_file)
                local_agr_path = download_file_from_server(name_server, name_user, ssh_key_options,
                                                                remote_agr_path)
                if local_agr_path and os.path.exists(local_agr_path):
                    try:
                        # Convert '.agr' > PNG by xmgrace
                        png_path = convert_agr_to_png(local_agr_path)
                        if os.path.exists(png_path):
                            st.image(png_path, caption=f"'{agr_file}' plot") #  CONTINUE HERE
                            # Download image
                            with open(png_path, "rb") as img_file:
                                btn = st.download_button(
                                    label="Descargar imagen",
                                    data=img_file,
                                    file_name=agr_file.replace(".agr", ".png"), # DAT si se usa GPCLS
                                    mime="image/png"
                                )
                        else:
                            st.error(f"ERROR!!! Image not generated to {agr_file}.")
                    except Exception as e:
                        st.error(f"ERROR!!! {agr_file} > image conversion failed: {str(e)}")
                    finally:
                        os.remove(local_agr_path)
                        if os.path.exists(png_path):
                            os.remove(png_path)
                else:
                    st.error(f"{agr_file} file not downloaded.")
        #   TEST
        # —————— GPCLS plots ——————
        # buscamos el .dat correspondiente
        gpcls_files = [f for f in remote_files if f.lower().startswith("gpcls") and f.lower().endswith(".dat")]
        if gpcls_files:
            # si hay gpclssys.dat lo usamos, si no tomamos el primero gpclsX.dat
            dat_name = "gpclssys.dat" if "gpclssys.dat" in gpcls_files else gpcls_files[0]
            st.markdown("## GPCLS plots")
            # lo descargamos
            remote_dat = os.path.join(working_directory, dat_name)
            local_dat = download_file_from_server(name_server, name_user, ssh_key_options, remote_dat)
            if local_dat and os.path.exists(local_dat):
                try:
                    # cargamos datos: columnas [M, P(logM), n_br, g]
                    import numpy as np
                    data = np.loadtxt(local_dat)
                    M = data[:, 0]
                    P = data[:, 1]
                    nbr = data[:, 2]
                    g = data[:, 3]

                    # 1) P[log(M)] vs M
                    fig1, ax1 = plt.subplots()
                    ax1.set_xscale("log")
                    ax1.plot(M, P, marker="o", linestyle="", markersize=4, color="blue")
                    ax1.set_xlabel("M (g/mol)")
                    ax1.set_ylabel("P [log(M)]")
                    st.pyplot(fig1)

                    # 2) n_br vs M
                    fig2, ax2 = plt.subplots()
                    ax2.set_xscale("log")
                    ax2.plot(M, nbr, marker="o", linestyle="", markersize=4, color="red")
                    ax2.set_xlabel("M (g/mol)")
                    ax2.set_ylabel(r"$n_{br}$ / 500 monomer")
                    st.pyplot(fig2)

                    # 3) g vs M
                    fig3, ax3 = plt.subplots()
                    ax3.set_xscale("log")
                    ax3.plot(M, g, marker="o", linestyle="", markersize=4, color="green")
                    ax3.set_xlabel("M (g/mol)")
                    ax3.set_ylabel(r"$g = \left(\frac{R_{g}^{br}}{R_{g}^{lin}}\right)^2$")
                    st.pyplot(fig3)

                except Exception as e:
                    st.error(f"ERROR!!! Falló al generar GPCLS plots: {e}")
                finally:
                    os.remove(local_dat)
            else:
                st.error(f"ERROR!!! No se pudo descargar {dat_name}")
