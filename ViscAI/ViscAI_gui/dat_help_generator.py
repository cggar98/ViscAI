
import re


def parse_inp_dat(lines):
    """
    Parser robusto: consume tokens independientemente de saltos de línea.
    Admite que cada dato esté en una línea o en varias.
    """
    # Tokenización: elimina comentarios al estilo '# ...' o '; ...' si existieran
    clean = []
    for ln in lines:
        ln = ln.split("#", 1)[0].strip()
        if not ln:
            continue
        clean.append(ln)

    tokens = []
    for ln in clean:
        tokens.extend(ln.split())

    def next_int():
        return int(tokens.pop(0))

    def next_float():
        return float(tokens.pop(0))

    result = {}

    # Línea 1
    result["max_polymers"] = next_int()
    result["max_segments"] = next_int()

    # Línea 2
    result["alpha"] = next_float()

    # Línea 3
    result["fine_tune"] = next_int()

    # Línea 4
    result["monomer_mass"] = next_float()
    result["ne"] = next_float()
    result["density"] = next_float()

    # Línea 5
    result["tau_e"] = next_float()
    result["temperature"] = next_float()

    # Línea 6
    num_components = next_int()
    result["num_components"] = num_components
    result["components"] = []

    # Componentes
    for comp_idx in range(num_components):
        comp = {}

        # weight fraction
        comp["weight_fraction"] = next_float()

        # num_polymers y poly_type
        comp["num_polymers"] = next_int()
        comp["poly_type"] = next_int()

        p = comp["poly_type"]
        comp["specific_lines"] = []

        # ---- Dependiendo del tipo ----
        if p == 0:  # Linear: dist Mw PDI
            comp["specific_lines"].append({
                "distribution": next_int(),
                "Mw": next_float(),
                "PDI": next_float()
            })

        elif p == 1:  # Star: dist Mw PDI + arms
            comp["specific_lines"].append({
                "distribution": next_int(),
                "Mw": next_float(),
                "PDI": next_float()
            })
            comp["specific_lines"].append({
                "arms": next_int()
            })

        elif p == 2:  # Asymmetric star: (dist Mw PDI) x2
            comp["specific_lines"].append({
                "sym_distribution": next_int(),
                "sym_Mw": next_float(),
                "sym_PDI": next_float()
            })
            comp["specific_lines"].append({
                "asym_distribution": next_int(),
                "asym_Mw": next_float(),
                "asym_PDI": next_float()
            })

        elif p == 3:  # H polymer: (dist Mw PDI) x2
            comp["specific_lines"].append({
                "side_distribution": next_int(),
                "side_Mw": next_float(),
                "side_PDI": next_float()
            })
            comp["specific_lines"].append({
                "cross_distribution": next_int(),
                "cross_Mw": next_float(),
                "cross_PDI": next_float()
            })

        # (puedes ir completando el resto p==4,5,6,10,11,12,20,21,25,60, etc.)
        else:
            # Si todavía no lo soportas, al menos no petes:
            comp["specific_lines"].append({"warning": f"poly_type {p} not implemented in parser yet"})

        result["components"].append(comp)

    return result



def generate_help_text(parsed):
    """
    Genera un help_line dinámico basado en la estructura parseada.
    """
    H = []

    # Las primeras 6 líneas son siempre las mismas
    H.append("**LINE 1**\nMaximum number of polymers | Maximum number of segments")
    H.append("**LINE 2**\nDynamic dilation exponent α")
    H.append("**LINE 3**\nFine tuning parameter (1 = stored values)")
    H.append("**LINE 4**\nMonomer mass (g/mol) | Monomers per entangled segment | Density (g/cm³)")
    H.append("**LINE 5**\nEntanglement time (s) | Temperature (K)")
    H.append("**LINE 6**\nNumber of components")

    # Ahora los componentes
    index_line = 7

    for i, comp in enumerate(parsed["components"], start=1):
        H.append(f"**LINE {index_line}**\nComponent {i}: weight fraction")
        index_line += 1

        H.append(f"**LINE {index_line}**\nNum polymers | Polymer type")
        index_line += 1

        p = comp["poly_type"]

        ### Aquí generamos la ayuda según el tipo ###
        if p == 0:
            H.append(f"**LINE {index_line}**\nDistribution type | Mw (g/mol) | PDI")
            index_line += 1

        elif p == 1:
            H.append(f"**LINE {index_line}**\nDistribution | Mw (arm) | PDI")
            index_line += 1
            H.append(f"**LINE {index_line}**\nNumber of arms")
            index_line += 1

        elif p == 2:
            H.append(f"**LINE {index_line}** (Symmetric arms)\nDistribution | Mw | PDI")
            index_line += 1
            H.append(f"**LINE {index_line}** (Asymmetric arm)\nDistribution | Mw | PDI")
            index_line += 1

        elif p == 3:
            H.append(f"**LINE {index_line}** (Side arms)\nDistribution | Mw | PDI")
            index_line += 1
            H.append(f"**LINE {index_line}** (Crossbar)\nDistribution | Mw | PDI")
            index_line += 1

        # Complete the rest of the polymers type

    return "\n\n".join(H)
