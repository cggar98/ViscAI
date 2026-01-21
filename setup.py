import datetime
from setuptools import setup, find_packages

# Read version from version.py
version = {}
try:
    with open("version.py") as f:
        exec(f.read(), version)
except FileNotFoundError:
    version["__version__"] = "N/A"

# ================= MAIN =====================
fnamelog = "install.log"
now = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")

with open(fnamelog, "w") as f:
    f.write("Starting ViscAI installation ({})\n".format(now))

use_cython=False
with open(fnamelog, "a") as f:
    f.write("\t\t Use cython: {}\n".format(use_cython))

setup(
    name="ViscAI",
    version=version["__version__"],
    author="Carlos Garcia",
    author_email="cggarcia098@gmail.com",
    description="A software tool designed to extract rheological property data from materials and "
                "apply artificial intelligence techniques to predict the viscoelastic behavior "
                "of new formulations.",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/cggar98/ViscAI.git@refactor_1",
    license="GPL-3.0-or-later",
    packages=find_packages(),
    include_package_data=False,
    install_requires=[
        "streamlit==1.38.0",
        "paramiko",
        "numpy",
        "pandas",
        "matplotlib",
        "pillow",
        "grace",
        "scikit-learn"
     ],
    python_requires=">=3.10",
    entry_points={
        "console_scripts": [
             "ViscAI_gui_cmd = ViscAI.ViscAI_gui_cmd:main_app",
        ]
    },
)

with open(fnamelog, "a") as f:
    f.write("End topology installation ({})\n".format(now))