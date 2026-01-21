import streamlit as st
import os
from PIL import Image


def logo_config():
    # Currently directory of main_gui
    base_dir = os.path.dirname(os.path.abspath(__file__))
    icon_path = os.path.join(base_dir, 'biophym_logo.ico')

    if os.path.exists(icon_path):
        icon = Image.open(icon_path)
        st.set_page_config(
            page_title='ViscAI',
            page_icon=icon,
            layout='centered',
            initial_sidebar_state='auto',
            # Items to redirect to other pages
            menu_items={
                'About': '**ViscAI (https://github.com/cggar98/ViscAI)**'
            }
        )
    else:
        print(f"Icon not found in: {icon_path}")
