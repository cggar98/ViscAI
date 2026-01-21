import re


def get_conda_sh_path(ssh) -> str | None:
    """
    Find 'conda.sh' path within ~/.bashrc.
    """
    try:
        # Find 'conda.sh' with 'egrep' command
        cmd = r"egrep -r 'conda\.sh' ~/.bashrc 2>/dev/null || true"
        stdin, stdout, stderr = ssh.exec_command(cmd)
        output = stdout.read().decode().strip()

        if not output:
            return None

        # Get conda path
        pattern = re.compile(r"""
            (?:^|[;\s])
            (?:source|\.)\s*
            ["']?
            (?P<path>[^"' ]*?/etc/profile\.d/conda\.sh)
            ["']?
        """, re.VERBOSE)

        # Output lines analysis
        for line in output.splitlines():
            m = pattern.search(line)
            if m:
                return m.group("path")

        # Last attempt if parsing failed.
        fallback = re.search(r"(/[^\"'\s]*?/etc/profile\.d/conda\.sh)", output)
        if fallback:
            return fallback.group(1)

        return None
    except Exception:
        return None
