# ViscAI Installation

This document contains:
- The local installation of **ViscAI** software (GUI).
- The installation & creation of **conda virtual environment**
- The installation of **DB Browser for SQLite**
- The installation for **BoB 2.5 remote execution**.

---

## 1) Clone the ViscAI repository in your local machine
    ```bash
    git clone https://github.com/cggar98/ViscAI.git
    cd ViscAI
    ```

---

## 2) If you not have *conda* within your remote server, install it
We recommend *Miniconda* or *Anaconda* to manage virtual environments

### Linux / macOS (Miniconda)
1. Download the installer from de official Miniconda website for your architecture (https://www.anaconda.com/docs/getting-started/miniconda/install)
2. Install and load the environment:
    ```bash
     bash Miniconda3-latest-Linux-x86_64.sh	# Linux x86_64
     source ~/.bashrc	# restarting your terminal
    ```
3. Verify the installation
    ```bash
    conda --version
    ```

### Windows

1. Download the graphical installer for Miniconda/Anaconda.
2. Run the installer with proper permissions and, once finished, open *Anaconda Prompt*.
3. Check the installation:
    ```bat
    conda --version
    ```

---

## 3) Create & activate a conda environment on local and remote server with **Python >=3.10**.
    ```bash
    conda create -n <virtualenv_name> python=3.10
    conda activate <virtualenv_name>
    ```

---

## 4) Install in your local machine ViscAI dependencies
1. Python dependencies.
Install all required Python packages from the conda-forge channel:
    ```bash
    conda install -c conda-forge \
        streamlit \
        paramiko \
        numpy \
        pandas \
        matplotlib \
        pillow \
    ```
2. System dependencies.
Some features require system-level packages that are not Python libraries.
    ```bash
    conda install -c conda-forge \
        tk \
        grace \
    ```

**NOTE**: These dependencies can be installed in a single step using **conda** and the provided **requirements.txt** file.
    ```bash
    conda create -n <virtualenv_name> -c conda-forge --file requirements.txt
    ```

3. Verify installation.
    ```bash
    streamlit --version
    python -c "import streamlit, paramiko, numpy, pandas, matplotlib; print('All ViscAI dependencies installed')
    ```

---

## 5) Within the local machine, install **DB Browser for SQLite (>=3.12.1 version)**
Used to see the database and export tables to CSV.

- **Windows/macOS**: use the graphical installer from the official website (https://sqlitebrowser.org/).
- **Linux (Debian/Ubuntu)**:
    ```bash
    sudo apt update
    sudo apt install sqlitebrowser
    ```

---

## 6) Install **BoB 2.5 (Branch-on-Branch)** on remote servers
Download and install following the official instructions:

- Dowload page: https://sourceforge.net/projects/bob-rheology/files/bob-rheology/bob2.5/

**NOTES**
- Check if the BoB executable is in the user PATH.
- Check libraries/compilers required by BoB for your system.

---

## 7) Open the ViscAI GUI in your local machine
With the conda virtual environment activated and from the cloned repository:
    ```bash
    cd ViscAI
    streamlit run ViscAI_gui/main_gui.py
    ```

---
