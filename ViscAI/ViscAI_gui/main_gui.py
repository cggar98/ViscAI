import streamlit as st
from ViscAI.ViscAI_gui.info_link import info_link_program
from ViscAI.ViscAI_gui.main_tabgroup import tabgroup_layout
from ViscAI.ViscAI_gui.icon import logo_config


#   ============================    Title configuration   ============================    #
def main_gui_app():
    st.markdown("""
    <style>
    @keyframes move {
        0% { transform: translateX(-100%); }
        100% { transform: translateX(100%); }
    }

    .moving-title {
        width: 100%;
        overflow: hidden;
        white-space: nowrap;
        box-sizing: border-box;
        animation: move 15s linear infinite;
    }
    </style>
    <div class="moving-title">
        <h1>Welcome to ViscAI</h1>
    </div>
    """, unsafe_allow_html=True)

#   ============================    Info & link ViscAI   ============================    #
    info_link_program()

#   ============================    Tabs configuration   ============================    #
    tabgroup_layout()

#   ============================    Init program and logo configuration   ============================    #
if __name__ == '__main__':
    logo_config()
    main_gui_app()
