import streamlit as st


# Polymer type labels
POLY_TYPE_LABELS = [
    "Linear",
    "Star",
    "Asymmetric star",
    "H",
    "Combs with Poisson distributed number of arms",
    "Combs with fixed number of side arms",
    "Coupled combs (2 comb backbones attached) (at some random position)",
    "Cayley tree (3 arm star)",
    "Cayley tree (linear)",
    "Cayley tree (4 arm star)",
    "Metallocene catalyzed polyethylene (MPE)",
    "MPE (weight averaged)",
    "Gelation ensemble",
    "Polymer prototype",
    "From configuration file"
]

# Polymer type codes
POLY_TYPE_CODES = [0, 1, 2, 3, 4, 5, 6, 10, 11, 12, 20, 21, 25, 40, 60]

# Distribution types
DISTRIBUTION_TYPES = ["Monodisperse", "Gaussian", "Log-normal", "Poisson", "Flory"]


def input_file_parameters():
    st.subheader("Configuration input file")
    with st.expander("Parameters", expanded=False):

        # ---------------------- Line 1 parameters ---------------------- #
        max_polymers = st.number_input("Maximum number of polymers", step=1, min_value=0)
        max_segments = st.number_input("Maximum number of segments",step=1, min_value=0)

        # ---------------------- Line 2 parameter ---------------------- #
        alpha = st.number_input("Dynamic dilation exponent α", value=1.0, step=0.1)

        # ---------------------- Line 3 parameter ---------------------- #
        tuning_option = st.selectbox(
            "Fine tuning parameter",
            ["Stored values", "Enter custom value"],
            index=0
        )
        if tuning_option == "Stored values":
            fine_tune = 1
        else:
            fine_tune = st.number_input("Enter custom fine tuning value", step=1)

        # ---------------------- Line 4 parameter ---------------------- #
        monomer_mass = st.number_input("Monomer mass (g/mol)", step=0.001, min_value=0.000)
        monomers_entangled = st.number_input("Number of monomers in one entangled segment", step=0.001, min_value=0.000)
        density = st.number_input("Density (g/cm³)", step=0.001, min_value=0.000)

        # ---------------------- Line 5 parameter ---------------------- #
        tau_e = st.number_input("Entanglement time (s)", step=0.00000000001, min_value=0.00000000000)
        temperature = st.number_input("Temperature (K)", step=0.01, min_value=0.00)

    # ---------------------- Component settings ---------------------- #

    st.subheader("Component settings")
    with st.expander("Parameters", expanded=False):
        num_components = st.number_input("Number of components or species", min_value=1, step=1)

    # Starting to save edited parameters within input file
    file_content = (
        f"{max_polymers} {max_segments}\n"
        f"{alpha}\n"
        f"{fine_tune}\n"
        f"{monomer_mass} {monomers_entangled} {density}\n"
        f"{tau_e} {temperature}\n"
        f"{num_components}\n"
    )

    #   ============================    PARAMETERS OF EACH COMPONENT   ============================    #
    for comp_idx in range(1, num_components + 1):
        st.subheader(f"Component {comp_idx}")
        with st.expander("Parameters", expanded=False):
            weight_fraction = st.number_input(
                f"Weight fraction", min_value=0.0, max_value=1.0,
                step=0.1, key=f"comp_{comp_idx}_weight_fraction"
            )
            num_polymers = st.number_input(
                f"Number of polymers", min_value=0, step=1,
                key=f"comp_{comp_idx}_num_polymers"
            )

            # Selection polymer type
            polymer_type = select_polymer_type(comp_idx)

            #   ==============    LINEAR   ==============    #
            if polymer_type == 0:
                dist_type = st.selectbox(
                    f"Distribution type",
                    DISTRIBUTION_TYPES,
                    key=f"comp_{comp_idx}_dist_type"
                )
                if dist_type == "Monodisperse":
                    distribution_type = 0
                elif dist_type == "Gaussian":
                    distribution_type = 1
                elif dist_type == "Log-normal":
                    distribution_type = 2
                elif dist_type == "Poisson":
                    distribution_type = 3
                else:
                    distribution_type = 4

                mw = st.number_input("Molecular weight (g/mol)", min_value=0.0, step=0.1, key=f"comp_{comp_idx}_mw")
                pdi = st.number_input("Polydispersity index (PDI)", min_value=0.0, step=0.1, key=f"comp_{comp_idx}_pdi")
                file_content += f"{weight_fraction}\n{num_polymers} {polymer_type}\n{distribution_type} {mw} {pdi}\n"

            #   ==============    STAR   ==============    #
            if polymer_type == 1:
                dist_type = st.selectbox(
                    f"Distribution type",
                    DISTRIBUTION_TYPES,
                    key=f"comp_{comp_idx}_dist_type"
                )
                if dist_type == "Monodisperse":
                    distribution_type = 0
                elif dist_type == "Gaussian":
                    distribution_type = 1
                elif dist_type == "Log-normal":
                    distribution_type = 2
                elif dist_type == "Poisson":
                    distribution_type = 3
                else:
                    distribution_type = 4

                mw_seg = st.number_input("Segment molecular weight (g/mol)", min_value=0.0, step=1.0, key=f"comp_{comp_idx}_seg_mw")
                pdi_seg = st.number_input("Polydispersity index (PDI)", min_value=0.0, step=0.1, key=f"comp_{comp_idx}_seg_pdi")
                n_arms = st.number_input("Number of arms", min_value=1, step=1,
                                          key=f"comp_{comp_idx}_arms_n")
                file_content += f"{weight_fraction}\n{num_polymers} {polymer_type}\n{distribution_type} {mw_seg} {pdi_seg}\n{n_arms}\n"

            #   ==============    ASYMMETRIC STAR   ==============    #
            if polymer_type == 2:
                symmetric_dist_type = st.selectbox(
                    f"Distribution type for symmetric arms",
                    DISTRIBUTION_TYPES,
                    key=f"comp_{comp_idx}_symmetric_dist_type"
                )
                if symmetric_dist_type == "Monodisperse":
                    symmetric_distribution_type = 0
                elif symmetric_dist_type == "Gaussian":
                    symmetric_distribution_type = 1
                elif symmetric_dist_type == "Log-normal":
                    symmetric_distribution_type = 2
                elif symmetric_dist_type == "Poisson":
                    symmetric_distribution_type = 3
                else:
                    symmetric_distribution_type = 4

                asymmetric_dist_type = st.selectbox(
                    f"Distribution type for asymmetric arms",
                    DISTRIBUTION_TYPES,
                    key=f"comp_{comp_idx}_asymmetric_dist_type"
                )
                if asymmetric_dist_type == "Monodisperse":
                    asymmetric_distribution_type = 0
                elif asymmetric_dist_type == "Gaussian":
                    asymmetric_distribution_type = 1
                elif asymmetric_dist_type == "Log-normal":
                    asymmetric_distribution_type = 2
                elif asymmetric_dist_type == "Poisson":
                    asymmetric_distribution_type = 3
                else:
                    asymmetric_distribution_type = 4

                mw_symmetric = st.number_input("Symmetric arms molecular weight (g/mol)", min_value=0.0, step=1.0, key=f"comp_{comp_idx}_symmetric_mw")
                mw_asymmetric = st.number_input("Asymmetric arm molecular weight (g/mol)", min_value=0.0, step=1.0,
                                               key=f"comp_{comp_idx}_asymmetric_mw")
                pdi_symmetric = st.number_input("Symmetric arms polydispersity index (PDI)", min_value=0.0, step=0.1, key=f"comp_{comp_idx}_symmetric_pdi")
                pdi_asymmetric = st.number_input("Asymmetric arm polydispersity index (PDI)", min_value=0.0, step=0.1,
                                                key=f"comp_{comp_idx}_asymmetric_pdi")
                file_content += f"{weight_fraction}\n{num_polymers} {polymer_type}\n{symmetric_distribution_type} {mw_symmetric} {pdi_symmetric}\n{asymmetric_distribution_type} {mw_asymmetric} {pdi_asymmetric}\n"

            #   ==============    H   ==============    #
            if polymer_type == 3:
                side_arm_dist_type = st.selectbox(
                    f"Distribution type for side arms",
                    DISTRIBUTION_TYPES,
                    key=f"comp_{comp_idx}_side_arm_dist_type"
                )
                if side_arm_dist_type == "Monodisperse":
                    side_arm_distribution_type = 0
                elif side_arm_dist_type == "Gaussian":
                    side_arm_distribution_type = 1
                elif side_arm_dist_type == "Log-normal":
                    side_arm_distribution_type = 2
                elif side_arm_dist_type == "Poisson":
                    side_arm_distribution_type = 3
                else:
                    side_arm_distribution_type = 4

                crossbar_dist_type = st.selectbox(
                    f"Distribution type for crossbar",
                    DISTRIBUTION_TYPES,
                    key=f"comp_{comp_idx}_crossbar_dist_type"
                )
                if crossbar_dist_type == "Monodisperse":
                    crossbar_distribution_type = 0
                elif crossbar_dist_type == "Gaussian":
                    crossbar_distribution_type = 1
                elif crossbar_dist_type == "Log-normal":
                    crossbar_distribution_type = 2
                elif crossbar_dist_type == "Poisson":
                    crossbar_distribution_type = 3
                else:
                    crossbar_distribution_type = 4

                mw_side_arms = st.number_input("Side arms molecular weight (g/mol)", min_value=0.0, step=1.0,
                                               key=f"comp_{comp_idx}_side_arms_mw")
                mw_crossbar = st.number_input("Crossbar molecular weight (g/mol)", min_value=0.0, step=1.0,
                                                key=f"comp_{comp_idx}_crossbar_mw")
                pdi_side_arms = st.number_input("Side arms polydispersity index (PDI)", min_value=0.0, step=0.1,
                                                key=f"comp_{comp_idx}_side_arms_pdi")
                pdi_crossbar = st.number_input("Crossbar polydispersity index (PDI)", min_value=0.0, step=0.1,
                                                 key=f"comp_{comp_idx}_crossbar_pdi")
                file_content += f"{weight_fraction}\n{num_polymers} {polymer_type}\n{side_arm_distribution_type} {mw_side_arms} {pdi_side_arms}\n{crossbar_distribution_type} {mw_crossbar} {pdi_crossbar}\n"

            #   ==============    COMBS POISSON NUMBER OF ARMS (4) & COUPLED COMBS (6)   ==============    #
            if polymer_type in [4, 6]:
                bb_dist_type = st.selectbox(
                    f"Distribution type for backbone",
                    DISTRIBUTION_TYPES,
                    key=f"comp_{comp_idx}_bb_dist_type"
                )
                if bb_dist_type == "Monodisperse":
                    bb_distribution_type = 0
                elif bb_dist_type == "Gaussian":
                    bb_distribution_type = 1
                elif bb_dist_type == "Log-normal":
                    bb_distribution_type = 2
                elif bb_dist_type == "Poisson":
                    bb_distribution_type = 3
                else:
                    bb_distribution_type = 4

                sarms_dist_type = st.selectbox(
                    f"Distribution type for side arms",
                    DISTRIBUTION_TYPES,
                    key=f"comp_{comp_idx}_sarm_dist_type"
                )
                if sarms_dist_type == "Monodisperse":
                    sarms_distribution_type = 0
                elif sarms_dist_type == "Gaussian":
                    sarms_distribution_type = 1
                elif sarms_dist_type == "Log-normal":
                    sarms_distribution_type = 2
                elif sarms_dist_type == "Poisson":
                    sarms_distribution_type = 3
                else:
                    sarms_distribution_type = 4

                mw_bb = st.number_input("Backbone molecular weight (g/mol)", min_value=0.0, step=1.0, key=f"comp_{comp_idx}_bb_mw")
                mw_sarm = st.number_input("Side arms molecular weight (g/mol)", min_value=0.0, step=1.0, key=f"comp_{comp_idx}_sarm_mw")
                pdi_bb = st.number_input("Backbone polydispersity index (PDI)", min_value=0.0, step=0.1, key=f"comp_{comp_idx}_bb_pdi")
                pdi_sarm = st.number_input("Side arms polydispersity index (PDI)", min_value=0.0, step=0.1, key=f"comp_{comp_idx}_sarm_pdi")
                average_n_arm = st.number_input("Average number of arms", min_value=0.0, step=0.1,
                                            key=f"comp_{comp_idx}_average_n_arm")
                file_content += f"{weight_fraction}\n{num_polymers} {polymer_type}\n{bb_distribution_type} {mw_bb} {pdi_bb}\n{sarms_distribution_type} {mw_sarm} {pdi_sarm}\n{average_n_arm}\n"

                #   ==============    COMBS WITH FIXED NUMBER OF SIDE ARMS   ==============    #
            if polymer_type == 5:
                bb_dist_type = st.selectbox(
                    f"Distribution type for backbone",
                    DISTRIBUTION_TYPES,
                    key=f"comp_{comp_idx}_bb_dist_type"
                )
                if bb_dist_type == "Monodisperse":
                    bb_distribution_type = 0
                elif bb_dist_type == "Gaussian":
                    bb_distribution_type = 1
                elif bb_dist_type == "Log-normal":
                    bb_distribution_type = 2
                elif bb_dist_type == "Poisson":
                    bb_distribution_type = 3
                else:
                    bb_distribution_type = 4

                sarms_dist_type = st.selectbox(
                    f"Distribution type for side arms",
                    DISTRIBUTION_TYPES,
                    key=f"comp_{comp_idx}_sarm_dist_type"
                )
                if sarms_dist_type == "Monodisperse":
                    sarms_distribution_type = 0
                elif sarms_dist_type == "Gaussian":
                    sarms_distribution_type = 1
                elif sarms_dist_type == "Log-normal":
                    sarms_distribution_type = 2
                elif sarms_dist_type == "Poisson":
                    sarms_distribution_type = 3
                else:
                    sarms_distribution_type = 4

                mw_bb = st.number_input("Backbone molecular weight (g/mol)", min_value=0.0, step=1.0,
                                        key=f"comp_{comp_idx}_bb_mw")
                mw_sarm = st.number_input("Side arms molecular weight (g/mol)", min_value=0.0, step=1.0,
                                          key=f"comp_{comp_idx}_sarm_mw")
                pdi_bb = st.number_input("Backbone polydispersity index (PDI)", min_value=0.0, step=0.1,
                                         key=f"comp_{comp_idx}_bb_pdi")
                pdi_sarm = st.number_input("Side arms polydispersity index (PDI)", min_value=0.0, step=0.1,
                                           key=f"comp_{comp_idx}_sarm_pdi")
                integer_n_arm = st.number_input("Integer number of arms", min_value=0, step=1,
                                                key=f"comp_{comp_idx}_integer_n_arm")
                file_content += f"{weight_fraction}\n{num_polymers} {polymer_type}\n{bb_distribution_type} {mw_bb} {pdi_bb}\n{sarms_distribution_type} {mw_sarm} {pdi_sarm}\n{integer_n_arm}\n"

            #   ==============    CAYLEY TREE (3 and 4 ARM STAR & LINEAR)   ==============    #
            if polymer_type in [10, 11, 12]:
                n_generations = st.number_input(
                    "Number of generations",
                    min_value=1, step=1,
                    key=f"comp_{comp_idx}_n_generations"
                )
                file_content += f"{weight_fraction}\n{num_polymers} {polymer_type}\n{n_generations - 1}\n"
                for gen in range(n_generations):

                    dist_gen = st.selectbox(
                        f"Distribution type for generation {gen}",
                        DISTRIBUTION_TYPES,
                        key=f"comp_{comp_idx}_gen{gen}_dist"
                    )

                    if dist_gen == "Monodisperse":
                        distribution_gen = 0
                    elif dist_gen == "Gaussian":
                        distribution_gen = 1
                    elif dist_gen == "Log-normal":
                        distribution_gen = 2
                    elif dist_gen == "Poisson":
                        distribution_gen = 3
                    else:
                        distribution_gen = 4

                    mw_gen = st.number_input(
                        f"Generation {gen} molecular weight (g/mol)", min_value=0.0, step=1.0,
                        key=f"comp_{comp_idx}_gen{gen}_mw"
                    )
                    pdi_gen = st.number_input(
                        f"Generation {gen} polydispersity index (PDI)", min_value=0.0, step=0.1,
                        key=f"comp_{comp_idx}_gen{gen}_pdi"
                    )
                    file_content += f"{distribution_gen} {mw_gen} {pdi_gen}\n"

            #   ==============    MPE & MPE wt averaged   ==============    #
            if polymer_type in [20, 21]:
                mw_mpe = st.number_input("MPE molecular weight (g/mol)", min_value=0.0, step=1.0, key=f"comp_{comp_idx}_mpe_mw")
                average_bm = st.number_input("Average branches per molecule", min_value=0.0, step=0.001,
                                     key=f"comp_{comp_idx}_bm_average")
                file_content += f"{weight_fraction}\n{num_polymers} {polymer_type}\n{mw_mpe} {average_bm}\n"

            #   ==============    GELATION ENSEMBLE   ==============    #
            if polymer_type == 25:
                molar_mass = st.number_input("Segment molar mass (g/mol)", min_value=0.0, step=1.0, key=f"comp_{comp_idx}_mass_molar")
                branch_probability = st.number_input("Branch probability", min_value=0.0, max_value=1.0, step=0.01,
                                    key=f"comp_{comp_idx}_probability_branch")
                file_content += f"{weight_fraction}\n{num_polymers} {polymer_type}\n{molar_mass} {branch_probability}\n"

            #   ==============    POLYMER PROTOTYPE   ==============    #
            if polymer_type == 40:
                # Upload prototype file
                proto_upload = st.file_uploader(
                    "Upload your prototype file (poly.proto)",
                    type=["proto", "txt"],
                    key=f"comp_{comp_idx}_proto"
                )
                # Save uploaded file as "poly.proto"
                if proto_upload is not None:
                    with open("poly.proto", "wb") as f:
                        f.write(proto_upload.getbuffer())
                # Add parameters in the input file
                file_content += f"{weight_fraction}\n"
                file_content += f"{num_polymers} 40\n"

            #   ==============    FROM FILE CONFIGURATION   ==============    #
            if polymer_type == 60:
                cfg_file = st.text_input(
                    "Configuration file for pre-generated polymer (no spaces)",
                    placeholder="e.g. poly1.dat",
                    key=f"comp_{comp_idx}_cfg60"
                )
                file_content += f"{weight_fraction}\n"
                file_content += f"{num_polymers} 60\n"
                file_content += f"{cfg_file.strip()}\n"

    st.session_state["generated_input_dat"] = file_content
    st.markdown("---")


def select_polymer_type(comp_idx):
    label = st.selectbox(
        "Polymer type",
        POLY_TYPE_LABELS,
        key=f"comp_{comp_idx}_poly_type"
    )

    return POLY_TYPE_CODES[POLY_TYPE_LABELS.index(label)]
