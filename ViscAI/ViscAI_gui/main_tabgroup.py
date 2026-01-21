import streamlit as st
from ViscAI.ViscAI_gui.program_options_tab import ProgramoptionsScreen
from ViscAI.ViscAI_gui.bobrc_parameters_tab import BoBparametersScreen
from ViscAI.ViscAI_gui.server_options_tab import ServerScreen
from ViscAI.ViscAI_gui.program_output_tab import ProgramoutputScreen


def tabgroup_layout():
    viscai_tabs = st.tabs(["Program options", "BoB.rc parameters", "Server options", "Program output"])

    # Run selected tab
    st.session_state.bob_tabs = viscai_tabs

    with viscai_tabs[0]:
        program_options_obj = ProgramoptionsScreen()
        program_options_obj.show_screen()

    with viscai_tabs[1]:
        bob_parameters_obj = BoBparametersScreen()
        bob_parameters_obj.show_screen()

    with viscai_tabs[2]:
        server_options_obj = ServerScreen()
        server_options_obj.show_screen()

    with viscai_tabs[3]:
        program_output_obj = ProgramoutputScreen()
        program_output_obj.show_screen()
