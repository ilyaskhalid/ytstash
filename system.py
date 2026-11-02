"""macOS integration: keep the machine awake during a download, chime when one
finishes, read a link off the clipboard, and open, reveal or trash a finished file.

Everything here shells out to something the OS already ships — caffeinate, pbpaste,
open, afplay, Finder — rather than pulling in a library to wrap it."""

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


def clipboard():
    """Whatever is on the Mac clipboard, as text.

    Read here rather than with navigator.clipboard in the page: the browser API needs
    a permission prompt plus a user gesture, and this is a local app talking to the
    machine it is running on. The caller is responsible for never returning anything
    that isn't a YouTube link — the rest of your clipboard is none of the page's
    business."""
    if platform.system() != "Darwin":
        return None
    r = subprocess.run(["pbpaste"], capture_output=True, text=True, check=False)
    return r.stdout


def open_file(path):
    """Hand the file to whatever the Mac opens it with — Infuse or VLC for .mkv."""
    if platform.system() == "Darwin":
        subprocess.Popen(["open", str(path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def reveal_file(path):
    """Finder, with the file itself selected. -R is the difference between landing in
    a folder of 200 videos and landing on the one you asked about."""
    if platform.system() == "Darwin":
        subprocess.Popen(["open", "-R", str(path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def trash(path) -> bool:
    """Move to Trash, never unlink. A mis-click on a 4GB download you cannot get back
    on hotel wifi should cost one Cmd-Z in Finder, not the file.

    The path goes through argv rather than into the AppleScript text, so a title with
    a quote in it can't close the string and run as script."""
    if platform.system() != "Darwin":
        return False
    r = subprocess.run(
        ["osascript", "-e", "on run argv",
         "-e", 'tell application "Finder" to delete POSIX file (item 1 of argv)',
         "-e", "end run", str(path)],
        capture_output=True, text=True, check=False)
    return r.returncode == 0
