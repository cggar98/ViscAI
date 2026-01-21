import streamlit as st


def info_link_program():
    col_info, col_link = st.columns([3, 2])

    #   ============================    INFO program   ============================    #
    with col_info:
        with st.expander("INFO", expanded=False):
            st.markdown("""
        **ViscAI** is a tool for extracting rheological data and using
         AI to predict viscoelastic properties of new formulations.

         It is powered by **BoB (Branch-on-Branch)**, a rheology engine which models
         the linear viscoelastic response of complex polymer architectures based on tube theory.
         """)

    #   ============================    Program link   ============================    #
    with col_link:
        with st.expander("ViscAI LINK", expanded=False):
            st.markdown(
                "https://github.com/cggar98/ViscAI")
