import streamlit as st
from tkinter import Tk, filedialog


def handle_button_click(option, input_key, action, tab_group):
    if action == "browse":
        file_selection_options(option, input_key, tab_group)
    elif action == "browse_directory":
        directory_selection_options(option, input_key, tab_group)
    elif action == "remove":
        if tab_group == "Program options":
            st.session_state["input_options"][input_key] = ""
            st.rerun()
        if tab_group == "BoB.rc parameters":
            st.session_state[input_key] = ""
            st.rerun()


def file_selection_options(option, input_key, tab_group):
    # Setting Tkinter hidden window
    root = Tk()
    root.withdraw()
    root.call('wm', 'attributes', '.', '-topmost', True)

    # Definition filetypes based on tab group
    filetypes = [("DAT files", "*.dat")] if tab_group == "Program options" else [("RC files", "*.rc")]
    title = "Select an input file" if tab_group == "Program options" else "Select '.rc' file"

    # Open file selection dialogu
    input_filename = filedialog.askopenfilename(parent=root, title=title, filetypes=filetypes)
    root.destroy()

    # Save the selected path in the session state
    if input_filename:
        target = st.session_state.setdefault("input_options", {}) if tab_group == "Program options" else st.session_state
        target[input_key] = input_filename
        st.rerun()


def directory_selection_options(option, input_key, bob_tabs):
    # Initialize hidden Tkinter root window
    root = Tk()
    root.withdraw()
    root.call('wm', 'attributes', '.', '-topmost', True)

    # Open the directory selection dialog
    input_directory = filedialog.askdirectory(parent=root,
                                              title="Select a directory")
    # Close Tkinter window
    root.destroy()

    # Save selected directory in session state.
    if input_directory:
        st.session_state.setdefault("input_options", {})[input_key] = input_directory
        st.rerun()
