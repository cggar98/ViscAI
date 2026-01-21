import streamlit as st
import os
from ViscAI.utils.program_update_events import handle_button_click


class BoBparametersScreen:
    def __init__(self):
        self._input_files = [
            "**Upload input file parameters (DAT)***",
            "Upload polymer configuration file (DAT)"
        ]

        self._input_options = None
        self._batch_mode = None
        self._generate_polymers = None

        # server_screen = ServerScreen()
        # self._server_valid = server_screen.show_screen()
        #
        # self._name_server = server_screen._name_server
        # self._name_user = server_screen._name_user
        # self._ssh_key_options = server_screen._ssh_key_options
        # self._path_virtualenv = server_screen._path_virtualenv
        # self._working_directory = server_screen._working_directory
        # self._script_uploaded = server_screen._script_uploaded
        # self._use_queuing_system = server_screen._use_queuing_system

    def show_screen(self):
        # st.header("Configuration 'bob.rc'")
        # Toggle to let user choose custom configuration
        configure_rc = st.toggle(
            "Customize 'bob.rc' parameters manually",
            value=False,
            key="configure_rc_toggle")

        if not configure_rc:
            st.info("BoB will use default 'bob.rc' parameters.")
            return

        st.markdown("---")

        mode = st.radio(
            "**Select the way to enter bob.rc parameters***",
            ["Upload existing file", "Edit 'bob.rc' parameters"],
            horizontal=True,
            key="bobrc_mode"
        )

        # ---------------------- UPLOADING bob.rc FILE ---------------------- #
        if mode == "Upload existing file":
            # Text input shows selected path
            st.text_input(
                "**Upload bob.rc file***",
                st.session_state.get("bobrc_file", ""),
                key="upload_bobrc_text"
            )
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Browse file", key="browse_bobrc"):  # use shared callback
                    handle_button_click(None, "bobrc_file", "browse", "BoB.rc parameters")
            with col2:
                if st.session_state.get("bobrc_file") and st.button("Remove file", key="remove_bobrc"):
                    handle_button_click(None, "bobrc_file", "remove", "BoB.rc parameters")

            # Show 'bob.rc' file content
            path = st.session_state.get("bobrc_file")
            if path and os.path.exists(path):
                try:
                    content = open(path, 'r').read()
                    st.text_area("bob.rc content:", value=content, height=300)
                except Exception:
                    st.error("ERROR!!! 'bob.rc' file not read.")
            return

        # ---------------------- EDITING bob.rc PARAMETERS ---------------------- #

        st.subheader("General Settings")
        with st.expander("Parameters", expanded=False):
            gen_poly_only_option = st.toggle("Stop after generating polymers")
            gen_poly_only = "yes" if gen_poly_only_option else "no"

            out_mode_selection = st.selectbox("Out mode", ["Headerless ascii file",
                                                           "Xmgrace plot",
                                                           "Ascii file with reptate header"], index=0)
            if out_mode_selection == "Headerless ascii file":
                out_mode = 0
            elif out_mode_selection == "Xmgrace plot":
                out_mode = 1
            else:
                out_mode = 2

        st.subheader("Relaxation Scheme")
        with st.expander("Parameters", expanded=False):
            psquare = st.number_input("p2 for branch-point friction", value=0.0250)
            alpha = st.number_input("Tube dilation exponent", value=1.0)
            tstart = st.number_input("Time at which to start integration", value=1.0e-4, format="%.6e")
            dtmult = st.number_input("Ratio of successive integration times", value=1.0050)

        st.subheader("Slowly Relaxing Branched Material")
        with st.expander("Parameters", expanded=False):
            slave_phi_option = st.toggle(
             "Make the fraction of unrelaxed material the same as that of the supertube"
             )
            slave_phi = "yes" if slave_phi_option else "no"

        st.subheader("Frecuency limits for G', G''")
        with st.expander("Parameters", expanded=False):
            freq_min = st.number_input("Minimum", value=0.003)
            freq_max = st.number_input("Maximum", value=1.0e8, format="%.1e")
            freq_interval = st.number_input("Ratio of successive frequencies of G’/G” data", value=1.1)

        st.subheader("Nonlinear Calculation")
        with st.expander("Parameters", expanded=False):
            prio_mode = st.text_input("Sets relaxation priority mode", value="entangled")
            stretch_bin_width = st.number_input("Bin width for stretch classification", value=1.25)
            num_nlin_stretch = st.number_input("Number of stretch bins for nonlinear averaging", value=20)
            nlin_av_dt = st.number_input("Time step multiplier for nonlinear averaging", value=1.10)

        st.subheader("Maxwell Modes")
        with st.expander("Parameters", expanded=False):
            defined_maxwell_option = st.toggle("Define Maxwell modes")
            defined_maxwell = "yes" if defined_maxwell_option else "no"

            maxwell_interval = st.number_input("Maxwell Interval", value=2.0)
            nlin_av_interval = st.number_input("Time spacing factor for nonlinear averaging", value=1.02)

        st.subheader("GPCLS")
        with st.expander("Parameters", expanded=False):
            calc_gpcls_option = st.toggle("Calculate molar mass distribution, branching "
                                          "and ideal g-factor")
            calc_gpcls = "yes" if calc_gpcls_option else "no"

            gpc_num_bin = st.number_input("Number of bins for GPC-LS histogram", value=50)

        st.subheader("Advanced Parameters")
        with st.expander("Parameters", expanded=False):
            pref_mode_selection = st.selectbox("Prefactor Mode", ["Same as outermost arm",
                                                                  "Use the effective armlength",
                                                                  "Include full effective friction"], index=1)
            if pref_mode_selection == "Same as outermost arm":
                pref_mode = 0
            elif pref_mode_selection == "Use the effective armlength":
                pref_mode = 1
            else:
                pref_mode = 2

            rept_scheme_selection = st.selectbox("Selects reptation scheme for relaxation", ["Reptation in thin tube",
                                                                                   "Reptation in current tube",
                                                                                   "Tube diameter from ReptAmount-long linear polymer",
                                                                                   "Tube diameter from ReptAmount fraction of current polymer"], index=0)
            if rept_scheme_selection == "Reptation in thin tube":
                rept_scheme = 1
            elif rept_scheme_selection == "Reptation in current tube":
                rept_scheme = 2
            elif rept_scheme_selection == "Tube diameter from ReptAmount-long linear polymer":
                rept_scheme = 3
            else:
                rept_scheme = 4

            ret_lim = st.number_input("Distance from branch point where side arms can freely retract", value=0.0)

            help_rept_amount = ("Used when you select one of these reptation scheme options:\n"
                                "- Tube diameter from ReptAmount-long linear polymer\n"
                                "- Tube diameter from ReptAmount fraction of current polymer")
            rept_amount = st.number_input("Rept Amount", value=1.0, help=help_rept_amount)

        st.subheader("Boolean and Flow Parameters")
        with st.expander("Parameters", expanded=False):
            nlin_prep_option = st.toggle("Prepare for nonlinear simulation")
            nlin_prep = "yes" if nlin_prep_option else "no"

            calc_nlin_option = st.toggle("Enable nonlinear rheology calculation")
            calc_nlin = "yes" if calc_nlin_option else "no"

            flow_time = st.number_input("Flow time applied in nonlinear simulation", value=10.0)
            flow_priority_option = st.toggle("Prioritize flow")
            flow_priority = "yes" if flow_priority_option else "no"

        # Update 'bob.rc' file parameters

        file_content = (
            "----------------------------------------------------------------\n"
            "*** rc file bob.rc                                          ****\n"
            "*** an equal sign in the line denotes option                ****\n"
            "*** without the equal sign things are treated as comments   ****\n"
            "----------------------------------------------------------------\n\n"
            "*** For nonlinear calculation\n"
            "* Step 1: CalcNlin -> no, NlinPrep-> yes, FlowTime-> time, FlowPriority-> no\n"
            "* Step 2:             yes            yes             time                 yes\n\n"
            "**** For linear rheology\n"
            "* CalcNlin -> no, NlinPrep-> no, FlowTime-> time, FlowPriority-> no\n\n"
            f"GenPolyOnly = {gen_poly_only}\n"
            f"OutMode={out_mode}\n\n"
            "** Relaxation scheme **\n"
            f"PSquare={psquare}\n"
            f"Alpha={alpha}\n"
            f"TStart={tstart}\n"
            f"DtMult={dtmult}\n\n"
            "** For small fraction of slowly relaxing branched material**\n"
            f"SlavePhiToPhiST={slave_phi}\n\n"
            "** Limits on frequency for G', G'' **\n"
            f"FreqMin = {freq_min}\n"
            f"FreqMax = {freq_max}\n"
            f"FreqInterval = {freq_interval}\n\n"
            "** Nonlinear calculation **\n"
            f"PrioMode={prio_mode}\n"
            f"StretchBinWidth={stretch_bin_width}\n"
            f"NumNlinStretch={num_nlin_stretch}\n"
            f"NlinAvDt={nlin_av_dt}\n\n"
            "** Maxwell modes **\n"
            f"DefinedMaxwellmodes={defined_maxwell}\n"
            f"MaxwellInterval={maxwell_interval}\n"
            f"NlinAvInterval={nlin_av_interval}\n\n\n"
            f"CalcGPCLS = {calc_gpcls}\n"
            f"GPCNumBin={gpc_num_bin}\n\n"
            "** Better don't change this **\n"
            f"PrefMode={pref_mode}\n"
            f"ReptScheme={rept_scheme}\n"
            f"RetLim={ret_lim}\n"
            f"ReptAmount={rept_amount}\n"
            f"NlinPrep={nlin_prep}\n"
            f"CalcNlin={calc_nlin}\n"
            f"FlowTime={flow_time}\n"
            f"FlowPriority={flow_priority}"
        )

        st.markdown("---")
        st.subheader("'bob.rc' parameters content")
        st.text_area("Edit the 'bob.rc' parameters below:", value=file_content, height=300, key="bobrc_custom_content")
