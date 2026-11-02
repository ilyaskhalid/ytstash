"""Phone-side downloader — one hop, YouTube straight onto the iPhone.

Runs in a-Shell (free on the App Store). Deliberately a single file with no
imports from this repo: AirDrop just this one file over, no venv, no folder.

    pip install -U yt-dlp          # once, inside a-Shell
    python phone.py <url> [...]

Files land in ~/Documents, which a-Shell publishes to the Files app under
"On My iPhone -> a-Shell", so Infuse and VLC can open them from there.

Two things decide whether this works at all, both verified on the Mac first:

1. cookies.txt. YouTube bot-checks anonymous requests — without cookies most
   videos fail outright with "Sign in to confirm you're not a bot". a-Shell
   can't read Safari's cookies, so you export them once on the Mac and copy
   the file over. See the README section this script prints on failure.
2. ffmpeg. 1080p arrives as separate video and audio streams that must be
   merged. No ffmpeg means no merge, so we fall back to a single progressive
   file, which YouTube caps well below 1080p.
"""

import os
import shutil
import sys

from yt_dlp import YoutubeDL

# H.264/AAC — the only combo iOS hardware-decodes. Same reasoning as the Mac app:
# VP9/AV1 software-decodes on iPhone, which means a hot phone and a flat battery.
MERGE_FMT = "bv*[vcodec^=avc1][height<=1080]+ba[acodec^=mp4a]/b[ext=mp4]/b"
# Without ffmpeg the selector must land on one already-muxed file. "No +" is not
# enough: YouTube's muxed 720p/1080p entries are HLS (protocol m3u8_native), and
# yt-dlp repackages those with a FixupM3u8 pass that itself shells out to ffmpeg —
# so they download but come out broken. protocol^=http excludes HLS and leaves the
# genuinely progressive formats, which YouTube caps at 360p.
FLAT_FMT = ("b[ext=mp4][protocol^=http][vcodec^=avc1]"
            "/b[ext=mp4][protocol^=http]/b[protocol^=http]")

OUT = os.path.expanduser("~/Documents")
COOKIES = os.path.join(OUT, "cookies.txt")

HELP = """
No cookies.txt found, and YouTube almost certainly refused because of it.

On the Mac, once:
    uv run python -c "import yt_dlp,sys; yt_dlp.YoutubeDL({'cookiesfrombrowser':('chrome',),'cookiefile':'cookies.txt'}).cookiejar.save()"
then AirDrop the cookies.txt it writes into a-Shell's Documents folder.

That file is a live Google login — anyone holding it is signed in as you.
Keep it off shared folders, and consider using a throwaway Google account.
Cookies expire every few weeks; re-export when downloads start failing.
"""


def has_ffmpeg() -> bool:
    """a-Shell ships ffmpeg as a WASM module you drop in ~/Documents/bin. It is
    callable as `ffmpeg` but isn't a POSIX executable, so which() alone misses it."""
    if shutil.which("ffmpeg"):
        return True
    return any(os.path.exists(os.path.expanduser(p))
               for p in ("~/Documents/bin/ffmpeg.wasm", "~/bin/ffmpeg.wasm"))


def build(fmt: str) -> dict:
    o = {
        "format": fmt,
        "outtmpl": os.path.join(OUT, "%(title)s [%(id)s].%(ext)s"),
        # Lower than the Mac's 8: a phone on wifi gains nothing from more, and
        # yt-dlp holds every fragment in flight in memory.
        "concurrent_fragment_downloads": 4,
        # Screen-off on iOS suspends the app; resume rather than restart.
        "retries": float("inf"),
        "fragment_retries": float("inf"),
        # ponytail: no "remote_components" here — the ejs solver wants a JS
        # runtime that iOS has no way to provide. yt-dlp's built-in interpreter
        # handles it, just slower.
    }
    if os.path.exists(COOKIES):
        o["cookiefile"] = COOKIES
    return o


def main(urls: list[str]) -> int:
    if not urls:
        print(__doc__)
        return 2
    merge = has_ffmpeg()
    print("1080p (ffmpeg found)" if merge else "no ffmpeg — progressive only, low res")
    if not os.path.exists(COOKIES):
        print("warning: no cookies.txt, expect YouTube to refuse")
    try:
        with YoutubeDL(build(MERGE_FMT if merge else FLAT_FMT)) as y:
            y.download(urls)
    except Exception as e:  # noqa: BLE001 — print it, whatever it is
        print(f"\nfailed: {str(e).strip().splitlines()[-1]}")
        if "Sign in to confirm" in str(e) and not os.path.exists(COOKIES):
            print(HELP)
        return 1
    print(f"\nsaved to {OUT} — open Files > On My iPhone > a-Shell")
    return 0


def self_check() -> None:
    # The whole point of FLAT_FMT is that it needs no ffmpeg: no merge, and no HLS
    # (which would drag in the FixupM3u8 pass and silently produce a broken file).
    assert "+" not in FLAT_FMT, "progressive selector must not request a merge"
    assert FLAT_FMT.count("protocol^=http") == FLAT_FMT.count("/") + 1, \
        "every FLAT_FMT fallback must exclude HLS, or it needs ffmpeg after all"
    assert "+" in MERGE_FMT and "avc1" in MERGE_FMT and "mp4a" in MERGE_FMT
    # Cookies must be opt-in by presence, never fabricated.
    assert "cookiefile" not in build(FLAT_FMT) or os.path.exists(COOKIES)
    assert build(FLAT_FMT)["format"] == FLAT_FMT
    assert "%(id)s" in build(FLAT_FMT)["outtmpl"], "id in filename for dedupe"
    print("phone self-check OK")


if __name__ == "__main__":
    if "--self-check" in sys.argv:
        self_check()
    else:
        raise SystemExit(main([a for a in sys.argv[1:] if not a.startswith("-")]))
