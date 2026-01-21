import sys
import os
from pathlib import Path

ENV_FLAG = "VISCAI_STREAMLIT_LAUNCHED"

# =============================================================================
def main_app():

    # Not infinite loop
    if os.environ.get(ENV_FLAG) == "1":
        return

    os.environ[ENV_FLAG] = "1"

    app_path = Path(__file__).resolve().parent.parent / "ViscAI/ViscAI_gui/main_gui.py"

    os.execv(
        sys.executable,
        [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            str(app_path),
        ]
    )

# =============================================================================
def main():
    main_app()