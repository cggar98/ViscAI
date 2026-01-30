import streamlit as st


def info_link_program():
    col_link, col_manual = st.columns([2, 2])

    #   ============================    Program link   ============================    #
    with col_link:
        with st.expander("ViscAI LINK", expanded=False):
            st.markdown(
                "https://github.com/cggar98/ViscAI")

    #   ============================    BoB manual   ============================    #
    with col_manual:
        with st.expander("Download BoB manual", expanded=False):
            st.markdown("> [BoB 2.3](https://sourceforge.net/projects/bob-rheology/files/bob-rheology/bob2.3/bob2.3.pdf/download)")
            st.markdown("> [BoB 2.5](https://sourceforge.net/projects/bob-rheology/files/bob-rheology/bob2.5/bob2p5.pdf/download)")

        #   ============================    INFO program   ============================    #
    with st.expander("INFO", expanded=False):
        st.markdown("""
    **ViscAI** is a tool for extracting rheological data and using
     AI to predict viscoelastic properties of new formulations.

     It is powered by **BoB (Branch-on-Branch)**, a rheology engine which models
     the linear viscoelastic response of complex polymer architectures based on tube theory.
     """)
