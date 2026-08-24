"""macOS integration: keep the machine awake during a download, chime when one finishes."""

import platform
import subprocess


def stay_awake():
    """Prevent macOS idle sleep for the life of the process — a dropped screen
    puts the whole system (and its network) to sleep mid-download otherwise.
    Native `caffeinate`, no code of our own needed to implement it."""
    if platform.system() != "Darwin":
        return None
    return subprocess.Popen(["caffeinate", "-i"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def mdns_name():
    """The .local name the phone can actually resolve. Deliberately not
    socket.gethostname() — behind a router that hands out DHCP hostnames that
    returns something like <uuid>.fritz.box, while the Bonjour name the phone
    needs lives in scutil."""
    if platform.system() != "Darwin":
        return None
    r = subprocess.run(["scutil", "--get", "LocalHostName"],
                       capture_output=True, text=True, check=False)
    return r.stdout.strip() or None


def reveal(path):
    """Open a folder in Finder — `open` is the native way, no scripting needed."""
    if platform.system() == "Darwin":
        subprocess.Popen(["open", str(path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def ding():
    if platform.system() == "Darwin":
        subprocess.Popen(["afplay", "/System/Library/Sounds/Glass.aiff"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
