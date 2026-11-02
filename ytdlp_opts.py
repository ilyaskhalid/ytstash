"""Everything that talks to yt-dlp's options dict lives here — the one place that
needs to know yt-dlp's parameter names."""

import re
import time
from pathlib import Path

OUT_DIR = Path.home() / "Downloads" / "yt"
# Finished videos land in OUT_DIR/<YYYY-MM-DD>/, so a folder of last spring's trip
# can't pass for today's. Anything not named like this is not ours to list.
DAY_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def day_dir(t: float | None = None) -> Path:
    return OUT_DIR / time.strftime("%Y-%m-%d", time.localtime(t))


def temp_dir() -> Path:
    """Where yt-dlp keeps everything unfinished: .part, -FragN, .ytdl, the thumbnail
    and subtitles waiting to be embedded. Dot-named so Finder hides it. Fixed rather
    than per-day, so a download paused at 23:59 still resumes the next morning."""
    return OUT_DIR / ".incomplete"
BOT_CHECK = "Sign in to confirm"  # yt-dlp's error text when YouTube demands auth
# Refuse to start a download with less than this free. A 4K hour can run past 10GB,
# and a full disk fails deep inside ffmpeg with an error nobody can read.
# ponytail: flat floor, not a per-video estimate — the real size isn't known until
# after format selection, which is already inside the download.
MIN_FREE_BYTES = 3 * 1024**3

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


def postprocessors(audio: bool, subs: bool = True) -> list[dict]:
    """The ffmpeg chain, in yt-dlp's own CLI order.

    The order is not ours to invent — it mirrors yt_dlp/__init__.py exactly, and each
    step depends on the one before: SponsorBlock has to fetch segments before anything
    reads them, subtitles must already be in the container before ModifyChapters cuts
    it, ModifyChapters must precede the metadata pass, and the thumbnail goes last
    because it survives no further container rewrite.

    Every step here is a stream copy — remux, concat and metadata all run with
    `-c copy`, so "never re-encodes" still holds end to end."""
    pps: list[dict] = [
        # YouTube serves .webp thumbnails, and an m4a/mp4 cover has to be jpeg or png —
        # embedding the webp straight in dies as "Conversion failed!" deep inside
        # ffmpeg. before_dl so the conversion happens off the downloaded file, not
        # mid-chain. Costs nothing on mkv, which would have taken either.
        {"key": "FFmpegThumbnailsConvertor", "format": "jpg", "when": "before_dl"},
        # Segments come from the SponsorBlock API keyed on the video id. `when` is
        # load-bearing: after_filter runs it before the download, so a video that is
        # entirely sponsor doesn't get fetched first and cut second.
        {"key": "SponsorBlock", "categories": {"sponsor"}, "when": "after_filter"},
    ]
    if not audio:
        # This is what --remux-video actually builds. The bare "remux_video" key that
        # used to sit in the opts dict was CLI-only — YoutubeDL never reads it — so a
        # progressive single-file download silently stayed .mp4 and AirDropped straight
        # into Photos, which is the one outcome the mkv container exists to prevent.
        pps.append({"key": "FFmpegVideoRemuxer", "preferedformat": "mkv"})
        # already_have_subtitle=False: delete the .vtt once it is inside the container,
        # so the folder Infuse lists stays just videos.
        if subs:
            pps.append({"key": "FFmpegEmbedSubtitle", "already_have_subtitle": False})
    pps += [
        # Stream-copy concat around the cut points. ponytail: cuts land on the nearest
        # keyframe, so a removal can be off by up to a GOP. force_keyframes fixes that
        # and re-encodes; not worth breaking the no-transcode promise for a second.
        {"key": "ModifyChapters", "remove_sponsor_segments": {"sponsor"}},
        # Chapters are the reason this is here: Infuse turns them into skip points,
        # which is the difference between scrubbing and navigating on a plane.
        {"key": "FFmpegMetadata", "add_metadata": True, "add_chapters": True},
        {"key": "EmbedThumbnail", "already_have_thumbnail": False},
    ]
    return pps


def build(private: bool, browser: str, progress_hook=None, pp_hook=None, fmt: str = PHONE_FMT,
          audio: bool = False, subs: bool = True) -> dict:
    o = {
        # Only bites on fragmented formats (HLS, livestream VODs). YouTube's normal
        # DASH formats arrive as plain https with fragments=0, so this does nothing
        # for them — measured. What actually saturates the line there is the 10MB
        # http_chunk_size YouTube's own extractor already attaches per format:
        # one connection tops out ~4MB/s, and eight in parallel measured *slower*.
        "concurrent_fragment_downloads": 8,
        # Phone-native by default (see PHONE_FMT) — the quality picker overrides it.
        "format": fmt,
        # A shared link is very often .../watch?v=X&list=Y, and without this yt-dlp
        # takes that as permission to queue the entire playlist.
        "noplaylist": True,
        # ID in the name so re-downloads dedupe and titles can't collide.
        "outtmpl": "%(title)s [%(id)s].%(ext)s",
        # yt-dlp's own split: work happens in temp, MoveFiles drops only the finished
        # file into home. Keeps the 50-file -FragN churn out of the folder you browse.
        "paths": {"home": str(day_dir()), "temp": str(temp_dir())},
        # Default sets mtime to YouTube's Last-Modified, i.e. the upload date. The
        # date that matters here is when *you* downloaded it.
        "updatetime": False,
        # Matroska on purpose, and it is the whole delivery mechanism: iOS and Photos
        # cannot open .mkv, so an AirDropped file can't silently disappear into the
        # photo library — Infuse is what claims it. (The old trick, a .infuse
        # extension, was removed from Infuse in 8.2.7; Apple dropped the underlying
        # feature.) Container only: H.264/AAC inside mkv still hardware-decodes.
        # This covers merged split streams; the remuxer in postprocessors() covers
        # the progressive single-file case.
        "merge_output_format": "mkv",
        "postprocessors": postprocessors(audio, subs),
        # Fetched so the postprocessors above have something to embed, then deleted
        # by them once embedded.
        "writethumbnail": True,
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
        # Costs ~0.5s per extraction; measured worth keeping for the cookies path.
        "remote_components": ["ejs:github"],
    }
    if not audio and subs:
        # Video only — there is no FFmpegEmbedSubtitle step on the audio path, so
        # anything downloaded here would be left behind as loose .vtt files.
        # Exact "en", not the en.* glob: that glob also matches YouTube's
        # machine-translated en-de/en-en tracks, and embedding three near-identical
        # subtitle streams just makes Infuse ask which one you want.
        o["writesubtitles"] = True
        o["writeautomaticsub"] = True
        o["subtitleslangs"] = ["en", "-live_chat"]
    if progress_hook:
        o["progress_hooks"] = [progress_hook]
    if pp_hook:
        o["postprocessor_hooks"] = [pp_hook]
    if private:
        # Your own unlisted/private videos need auth. Off by default — reading browser
        # cookies isn't something to do on every run.
        o["cookiesfrombrowser"] = (browser,)
    return o
