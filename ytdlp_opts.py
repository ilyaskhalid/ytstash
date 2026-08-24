"""Everything that talks to yt-dlp's options dict lives here — the one place that
needs to know yt-dlp's parameter names."""

from pathlib import Path

OUT_DIR = Path.home() / "Downloads" / "yt"
BOT_CHECK = "Sign in to confirm"  # yt-dlp's error text when YouTube demands auth

# H.264/AAC in MP4 — the only combo iOS hardware-decodes. VP9/AV1 (what "best" gives
# you) has no hardware decoder on iPhone, so it software-decodes: hot phone, flat
# battery, and ~3x the file size. YouTube caps avc1 at 1080p, still above what an
# iPhone screen resolves. The trailing fallbacks cover the rare video with no avc1.
PHONE_FMT = "bv*[vcodec^=avc1][height<=1080]+ba[acodec^=mp4a]/b[ext=mp4]/b"


class SilentLogger:
    """Swallow yt-dlp's own console output — the caller renders status via rich instead.
    Errors still surface: yt-dlp raises DownloadError regardless of the logger."""

    def debug(self, msg):
        pass

    def warning(self, msg):
        pass

    def error(self, msg):
        pass


def build(private: bool, browser: str, progress_hook=None, pp_hook=None, fmt: str = PHONE_FMT) -> dict:
    o = {
        # The main speed lever: DASH video is thousands of small fragments;
        # pulling 8 at once saturates the link instead of round-tripping serially.
        # (Not the cause of the 403s — that was a stale yt-dlp. Measured innocent.)
        "concurrent_fragment_downloads": 8,
        # Phone-native by default (see PHONE_FMT) — the quality picker overrides it.
        "format": fmt,
        # ID in the name so re-downloads dedupe and titles can't collide.
        "outtmpl": str(OUT_DIR / "%(title)s [%(id)s].%(ext)s"),
        # Matroska on purpose, and it is the whole delivery mechanism: iOS and Photos
        # cannot open .mkv, so an AirDropped file can't silently disappear into the
        # photo library — Infuse is what claims it. (The old trick, a .infuse
        # extension, was removed from Infuse in 8.2.7; Apple dropped the underlying
        # feature.) Container only: H.264/AAC inside mkv still hardware-decodes.
        # merge_output_format handles split streams, remux_video the progressive
        # single-file case. Both are stream copies — measured at 0.1s for 17MB, with
        # identical codecs in and out. Nothing is ever re-encoded.
        "merge_output_format": "mkv",
        "remux_video": "mkv",
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,  # the caller renders progress itself
        "logger": SilentLogger(),
        "color": {"stdout": "no_color", "stderr": "no_color"},  # raw ANSI breaks rich's markup parser
        # A dropped wifi mid-download (screen sleep, moving rooms) shouldn't lose the
        # file: keep retrying instead of giving up, and resume the partial (default).
        # float("inf") not the CLI's "infinite" string — that alias is parsed by the
        # command-line layer only; the library API takes the number directly.
        "retries": float("inf"),
        "fragment_retries": float("inf"),
        # Lets yt-dlp fetch its official JS-challenge-solver script (needed to get
        # full format lists on signed-in requests). Without this it silently drops
        # to "only images available". github source, not npm — smaller trust surface.
        "remote_components": ["ejs:github"],
    }
    if progress_hook:
        o["progress_hooks"] = [progress_hook]
    if pp_hook:
        o["postprocessor_hooks"] = [pp_hook]
    if private:
        # Your own unlisted/private videos need auth. Off by default — reading browser
        # cookies isn't something to do on every run.
        o["cookiesfrombrowser"] = (browser,)
    return o
