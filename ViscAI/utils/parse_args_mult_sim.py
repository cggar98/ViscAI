# utils/parse_args_mult_sim.py
def _parse_mw_list(text: str) -> list[float]:
    """
    Convierte un texto tipo '100000, 250000, 500000' en lista de float.
    - Admite comas, espacios y saltos de línea.
    - Filtra entradas no numéricas y <= 0.
    - Deduplica manteniendo orden.
    """
    if not text:
        return []
    tokens = [t.strip() for t in text.replace("\n", ",").split(",")]
    out, seen = [], set()
    for tok in tokens:
        if not tok:
            continue
        try:
            v = float(tok)
            if v > 0 and v not in seen:
                out.append(v)
                seen.add(v)
        except ValueError:
            continue
    return out


def _parse_pdi_list(s):
    vals = []
    for t in (s or "").replace("\n", ",").replace(";", ",").split(","):
        t = t.strip().replace(" ", "")
        if not t:
            continue
        try:
            vals.append(float(t))
        except Exception:
            pass
    return vals
