"""Local YouTube downloader — thin front-end over yt-dlp, served as a Flask web UI.

    uv run app.py

Max resolution, native container, never re-encodes — one hop from Google's CDN at
your line speed, no transcode. Only for content you own or have permission to
download.

One page, one local user, no auth. The page polls /state twice a second instead of
holding a websocket. Downloads run in daemon threads capped at 3 (LANES). Cancel
raises DownloadCancelled from inside yt-dlp's own progress hook, with a
match_filter gate covering the window before the first byte arrives.
"""

import contextlib
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
import webbrowser
from pathlib import Path

from flask import Flask, jsonify, request
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadCancelled, DownloadError
from yt_dlp.version import __version__ as YTDLP_VERSION

import quality
import system
import ytdlp_opts

PORT = 8765
# 3 at once saturates a normal line; more just invites YouTube's rate limiter,
# which already gave us a 403 once. ponytail: fixed cap, expose it if a fat
# connection ever sits idle behind it.
LANES = threading.Semaphore(3)
# Statuses where a worker still holds the job. "cancelling" is absent on purpose —
# the click already landed, the worker just hasn't hit its next checkpoint. "pausing"
# IS here: the download is still running, so the row must not offer Resume yet.
LIVE = ("waiting", "starting", "downloading", "merging", "pausing")

JOBS: dict[int, "Job"] = {}
_lock = threading.Lock()
app = Flask(__name__)

# The [videoId] the real file needs for dedupe is noise in Infuse, which shows filenames.
ID_RE = re.compile(r"\s*\[[A-Za-z0-9_-]{11}\]")
# Same id, read out of the URL instead — lets us show the thumbnail before any network call.
URL_ID = re.compile(r"(?:v=|youtu\.be/|/shorts/|/embed/|/live/)([A-Za-z0-9_-]{11})")
# A revoked media URL comes back as 403. Re-extracting mints a new one; this caps how
# many times we'll do that before admitting the download is actually broken.
RETRIES_403 = 3
BACKOFF = 2.0  # seconds, multiplied by attempt number; self-check zeroes it
# Set on the child after a self-update, so a failed upgrade can't become a restart loop.
UPDATED_ENV = "YT_DOWNLOADER_UPDATED"


def snippet(text, n: int = 150):
    """One tidy line out of a YouTube description, which is usually a wall of links."""
    if not text:
        return None
    s = " ".join(text.split())
    return s[:n].rstrip() + ("…" if len(s) > n else "")


class Job:
    def __init__(self, url: str, preset: str):
        self.url, self.preset = url, preset
        self.fmt = quality.selector_for(preset)
        self.title, self.status = url, "waiting"
        self.speed, self.eta = 0.0, None
        self.desc = self.author = None
        # Pulled straight out of the URL so the row has a picture on the very first
        # frame, instead of a grey box until extraction returns seconds later.
        m = URL_ID.search(url)
        self.vid = m.group(1) if m else None  # also what finds our .part files
        self.thumb = f"https://i.ytimg.com/vi/{self.vid}/mqdefault.jpg" if m else None
        self._t, self._b = time.monotonic(), 0  # throughput sampling window
        self.cancel = threading.Event()
        # Pause unwinds the same way cancel does; only the cleanup differs (see stop()).
        self.pause = threading.Event()
        self.streams: dict[str, tuple[int, int]] = {}

    def sample(self) -> None:
        """yt-dlp reports the speed of whichever fragment just ticked, so with several
        fragments in flight the number swings between them — that is the jitter, and it
        isn't the line. Measure aggregate throughput over a window instead, smooth it,
        and derive the ETA from that rather than trusting yt-dlp's per-fragment guess."""
        now = time.monotonic()
        dt = now - self._t
        if dt < 0.7:  # too short a window to divide by; wait for the next tick
            return
        got = sum(d for d, _ in self.streams.values())
        inst = max(0.0, got - self._b) / dt
        self.speed = inst if not self.speed else self.speed * 0.75 + inst * 0.25
        self._t, self._b = now, got
        left = sum(t for _, t in self.streams.values()) - got
        self.eta = int(left / self.speed) if self.speed > 1e4 and left > 0 else None

    @property
    def pct(self) -> float:
        total = sum(t for _, t in self.streams.values())
        return sum(d for d, _ in self.streams.values()) / total if total else 0.0

    def as_dict(self, jid: int) -> dict:
        return {"id": jid, "title": self.title, "status": self.status,
                "pct": round(self.pct * 100, 1), "speed": round(self.speed / 1e6, 1),
                "eta": self.eta, "preset": self.preset, "live": self.status in LIVE,
                "paused": self.status == "paused", "desc": self.desc, "author": self.author,
                "bytes": sum(d for d, _ in self.streams.values()),
                "total": sum(t for _, t in self.streams.values()), "thumb": self.thumb}


def drop_partials(job: Job) -> None:
    """Half-finished files would otherwise clutter the folder Infuse lists.

    Two sources: what the progress hook told us about, and — when the cancel lands
    before the first tick — whatever yt-dlp already opened. outtmpl always embeds
    [%(id)s], so globbing the video id attributes those to this job exactly, without
    touching a sibling download's partials."""
    for name in job.streams:
        for p in (name, name + ".part"):
            with contextlib.suppress(OSError):
                os.remove(p)
    for p in ytdlp_opts.OUT_DIR.glob(f"*[[]{job.vid}[]]*.part") if job.vid else []:
        with contextlib.suppress(OSError):
            p.unlink()


def with_403_retry(job: Job, run):
    """YouTube's media URLs are short-lived and bound to the session that got them; when
    one is revoked mid-transfer it comes back as 403, and fragment_retries cannot help
    because the URL itself is dead. Re-extracting mints a fresh one, and the .part files
    are still on disk, so the retry resumes from where the 403 hit rather than starting
    the file over. Only 403s are retried — everything else is a real failure."""
    for n in range(RETRIES_403 + 1):
        try:
            return run()
        except DownloadError as e:  # DownloadCancelled is a sibling class, never caught here
            if "403" not in str(e) or n == RETRIES_403:
                raise
            if job.cancel.is_set() or job.pause.is_set():
                raise DownloadCancelled() from e
            job.status = f"reconnecting ({n + 1}/{RETRIES_403})"
            job.speed = 0.0
            time.sleep(BACKOFF * (n + 1))
    return None


def prefill(job: Job) -> None:
    """Title and channel before the downloader thread has even finished extracting.

    oEmbed is a public JSON endpoint — no key, no cookies, one round trip — and it is
    the only thing fast enough to keep a raw URL from sitting in the table for the
    several seconds full extraction takes. Best-effort: extraction fills all of this
    in properly later, so a failure here is not worth reporting."""
    q = urllib.parse.quote(job.url, safe="")
    with contextlib.suppress(Exception):
        with urllib.request.urlopen(
                f"https://www.youtube.com/oembed?format=json&url={q}", timeout=6) as r:
            d = json.load(r)
        if job.title == job.url:
            job.title = d.get("title") or job.title
        job.author = job.author or d.get("author_name")
        job.thumb = d.get("thumbnail_url") or job.thumb


def stop(job: Job) -> None:
    """Both Pause and Cancel unwind through DownloadCancelled — this decides which one
    it was. Pausing must leave the .part files alone: resuming is nothing more than
    yt-dlp continuing them over HTTP Range, which is the whole reason pause is cheap."""
    job.speed, job.eta = 0.0, None  # a stale rate would keep ticking down on a dead row
    if job.pause.is_set():
        job.status = "paused"
    else:
        drop_partials(job)
        job.status = "cancelled"


def airdrop_dir() -> Path:
    # Derived on each call rather than fixed at import, so it follows OUT_DIR when the
    # self-check redirects it at a tempdir.
    return ytdlp_opts.OUT_DIR / "airdrop"


def final_file(job: Job) -> Path | None:
    """The finished file, found the same way drop_partials finds the unfinished ones:
    outtmpl embeds [%(id)s], so the id attributes it to this job. Beats tracking
    yt-dlp's renames through merge and postprocessing."""
    if not job.vid:
        return None
    hits = [p for p in ytdlp_opts.OUT_DIR.glob(f"*[[]{job.vid}[]]*")
            if p.suffix != ".part" and p.is_file()]
    return hits[0] if hits else None


def airdrop_link(src: Path) -> Path | None:
    """Hardlink src into the airdrop folder under a clean name.

    The extension is left alone — it's already .mkv, which is what routes the file to
    Infuse on the phone (see ytdlp_opts). Hardlink rather than copy: same inode, so a
    400MB video costs nothing to expose twice, and deleting either name leaves the
    other intact."""
    d = airdrop_dir()
    with contextlib.suppress(OSError):
        d.mkdir(parents=True, exist_ok=True)
    # Preferred name first; a title collision falls back to the id-bearing one rather
    # than clobbering a different video's link.
    for name in (ID_RE.sub("", src.name), src.name):
        link = d / name
        if link.exists():
            if link.samefile(src):
                return link  # already linked — re-adding a finished URL is a no-op
            continue
        with contextlib.suppress(OSError):
            os.link(src, link)
            return link
    return None


def run_job(job: Job) -> None:
    def hook(d):
        if job.cancel.is_set() or job.pause.is_set():
            raise DownloadCancelled()
        total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
        got = d.get("downloaded_bytes")
        job.streams[d.get("filename") or "?"] = (total if got is None else got, total)
        info = d.get("info_dict") or {}
        job.title = info.get("title") or job.title
        job.vid = info.get("id") or job.vid
        job.thumb = info.get("thumbnail") or job.thumb
        job.status = "downloading"
        job.sample()

    def gate(info, *, incomplete=False):
        # Runs right after extraction — earliest point a thumbnail is known, so it
        # shows up even during "starting", before any byte has downloaded.
        job.vid = info.get("id") or job.vid
        job.thumb = info.get("thumbnail") or job.thumb
        job.title = info.get("title") or job.title
        job.desc = snippet(info.get("description")) or job.desc
        job.author = info.get("uploader") or job.author
        return "stopped" if job.cancel.is_set() or job.pause.is_set() else None

    def go(cookies: bool):
        opts = ytdlp_opts.build(cookies, "chrome", progress_hook=hook, fmt=job.fmt,
                                pp_hook=lambda d: setattr(job, "status", "merging"))
        opts["match_filter"] = gate
        # Some yt-dlp internals write straight to stderr, bypassing the logger.
        with open(os.devnull, "w") as dn, contextlib.redirect_stderr(dn):
            with YoutubeDL(opts) as ydl:
                ydl.download([job.url])

    with LANES:
        try:
            if job.cancel.is_set():
                raise DownloadCancelled()
            job.status = "starting"
            job._t, job._b = time.monotonic(), sum(d for d, _ in job.streams.values())
            try:
                with_403_retry(job, lambda: go(False))
            except Exception as e:
                if ytdlp_opts.BOT_CHECK not in str(e):
                    raise
                job.status = "signing in with chrome cookies"
                with_403_retry(job, lambda: go(True))
            if job.cancel.is_set() or job.pause.is_set():
                raise DownloadCancelled()  # gate aborts by skipping, not raising
            # No progress hook ever fired => yt-dlp skipped it, file was already there.
            job.status, job.speed = ("done" if job.streams else "already on disk"), 0.0
            src = final_file(job)
            if src:
                airdrop_link(src)  # failures are silent: the download still succeeded
            system.ding()
        except DownloadCancelled:
            stop(job)
        except Exception as e:
            last = str(e).strip().splitlines()[-1] if str(e).strip() else "failed"
            # A 403 on every format is almost always a yt-dlp that YouTube has outgrown,
            # which costs an hour to work out from "Forbidden" alone. Say so instead.
            if "403" in last:
                last = "403 Forbidden — yt-dlp is likely stale: uv lock --upgrade-package yt-dlp"
            job.status = "error: " + last[:90]


PAGE = """<!doctype html><html lang="en"><head><meta charset="utf-8"><title>YouTube Downloader</title>
<meta name="viewport" content="width=device-width,initial-scale=1"><style>
/* System fonts on purpose — this thing runs on a plane. A webfont would be one more
   network round-trip before first paint, for a face nobody else will ever see. */
:root{--bg:#F5F5F5;--card:#fff;--ink:#0F0F0F;--mut:#737373;--line:#E5E5E5;
 --dark:#111;--track:#EDEDED;--ok:#16A34A;--bad:#DC2626;--soft:#F4F4F5}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--ink);font:15px/1.55 -apple-system,BlinkMacSystemFont,
 "Segoe UI",Roboto,sans-serif;-webkit-font-smoothing:antialiased}
button,input{font:inherit;color:inherit}
button{cursor:pointer;border:0;background:0}
/* The vertical rules framing the page are just the container's own borders. */
.wrap{max-width:1060px;min-height:100vh;margin:0 auto;padding:0 clamp(16px,4vw,44px) 90px;
 border-left:1px solid var(--line);border-right:1px solid var(--line);background:var(--bg)}

/* floating pill nav */
.nav{position:sticky;top:14px;z-index:40;display:flex;align-items:center;justify-content:space-between;
 gap:12px;margin-top:16px;padding:8px 8px 8px 18px;background:var(--card);border:1px solid var(--line);
 border-radius:999px;box-shadow:0 1px 2px rgba(0,0,0,.05)}
.mark{display:flex;align-items:center;gap:9px;font-weight:600;letter-spacing:-.02em}
.logo{width:26px;height:26px;border-radius:8px;background:var(--dark);display:grid;place-items:center}
.solid{background:var(--dark);color:#fff;border-radius:999px;padding:9px 16px;font-size:14px;
 font-weight:500;display:inline-flex;align-items:center;gap:7px}
.solid:hover{background:#000}

/* hero */
.hero{text-align:center;padding:74px 0 0}
.badge{display:inline-flex;align-items:center;gap:7px;background:var(--card);border:1px solid var(--line);
 border-radius:999px;padding:6px 14px;font-size:13px;font-weight:500;color:#525252}
h1{font-size:clamp(32px,5.6vw,54px);font-weight:700;letter-spacing:-.035em;line-height:1.06;
 margin-top:22px;text-wrap:balance}
.sub{color:var(--mut);font-size:clamp(15px,1.8vw,17px);margin:16px auto 0;max-width:44ch;
 text-wrap:balance}
.form{max-width:520px;margin:34px auto 0}
.bar{display:flex;align-items:center;gap:11px;background:var(--card);border:1px solid var(--line);
 border-radius:13px;padding:7px 7px 7px 16px;transition:box-shadow .2s,border-color .2s}
.bar:focus-within{border-color:#B5B5B5;box-shadow:0 0 0 4px rgba(0,0,0,.05)}
.bar input{flex:1;min-width:0;border:0;background:0;outline:0;padding:8px 0}
.bar input::placeholder{color:#A3A3A3}
/* The button sits on an offset outline and physically slides into it on press. */
.go{display:inline-flex;align-items:center;gap:7px;margin-top:18px;background:var(--dark);color:#fff;
 border:1.5px solid var(--dark);border-radius:11px;padding:10px 24px;font-size:15px;font-weight:600;
 letter-spacing:-.01em;box-shadow:4px 4px 0 0 var(--bg),4px 4px 0 1.5px var(--dark);
 transition:transform .13s cubic-bezier(.2,.8,.2,1),box-shadow .13s cubic-bezier(.2,.8,.2,1)}
.go:hover{transform:translate(2px,2px);box-shadow:2px 2px 0 0 var(--bg),2px 2px 0 1.5px var(--dark)}
.go:active{transform:translate(4px,4px);box-shadow:0 0 0 0 var(--bg),0 0 0 1.5px var(--dark)}
.fine{color:#A3A3A3;font-size:13px;margin-top:16px}

/* quality dropdown — shadcn select, hand-rolled: native <select> can't be styled,
   and a popover this small doesn't justify a component library. */
.sel{position:relative;flex:none}
.trg{display:flex;align-items:center;gap:7px;height:36px;padding:0 11px;background:var(--card);
 border:1px solid var(--line);border-radius:9px;font-size:13.5px;font-weight:500;
 box-shadow:0 1px 2px rgba(0,0,0,.05);transition:background .15s,box-shadow .15s}
.trg:hover{background:#FAFAFA}
.trg:focus-visible,.trg[aria-expanded=true]{outline:0;box-shadow:0 0 0 3px rgba(0,0,0,.08)}
.chev{transition:transform .2s cubic-bezier(.2,.8,.2,1);color:var(--mut)}
.trg[aria-expanded=true] .chev{transform:rotate(180deg)}
.menu{position:absolute;right:0;top:calc(100% + 7px);min-width:284px;padding:4px;text-align:left;
 background:var(--card);border:1px solid var(--line);border-radius:11px;z-index:60;
 box-shadow:0 12px 32px -10px rgba(0,0,0,.24),0 2px 6px rgba(0,0,0,.05);
 opacity:0;transform:scale(.95) translateY(-5px);transform-origin:top right;pointer-events:none;
 transition:opacity .14s ease,transform .14s cubic-bezier(.2,.85,.25,1)}
.menu.open{opacity:1;transform:none;pointer-events:auto}
.item{display:flex;align-items:center;gap:10px;padding:8px 9px 8px 11px;border-radius:7px;
 cursor:pointer;font-size:13.5px}
.item.hi{background:var(--soft)}
.item b{display:block;font-weight:500}
.item i{display:block;font-style:normal;color:var(--mut);font-size:12.5px;margin-top:1px}
.ck{margin-left:auto;flex:none;opacity:0;transition:opacity .12s}
.item.on .ck{opacity:1}

/* queue card */
.card{background:var(--card);border:1px solid var(--line);border-radius:16px;margin-top:56px;
 overflow:hidden;box-shadow:0 1px 2px rgba(0,0,0,.04)}
.head{display:flex;align-items:center;gap:12px;padding:14px 18px;border-bottom:1px solid var(--line);
 flex-wrap:wrap}
.head b{font-weight:600;letter-spacing:-.01em}
.cnt{color:var(--mut);font-size:14px}
.grow{flex:1}
.ghost{border:1px solid var(--line);border-radius:9px;padding:7px 12px;font-size:13.5px;color:#404040;
 display:inline-flex;align-items:center;gap:7px;transition:background .15s}
.ghost:hover{background:var(--soft)}

/* rows */
.job{display:flex;gap:16px;padding:16px 18px;align-items:center;animation:in .35s cubic-bezier(.2,.8,.2,1)}
@keyframes in{from{opacity:0;transform:translateY(-6px)}}
.job+.job{border-top:1px solid var(--line)}
.thumb{width:108px;height:61px;flex:none;border-radius:8px;background:var(--track) center/cover no-repeat}
.mid{flex:1;min-width:0}
.ti{display:block;font-weight:500;letter-spacing:-.01em;overflow:hidden;text-overflow:ellipsis;
 white-space:nowrap}
.sb{display:block;color:var(--mut);font-size:12.5px;margin-top:2px;overflow:hidden;
 text-overflow:ellipsis;white-space:nowrap}
.sb:empty{display:none}
.track{height:8px;background:var(--track);border-radius:99px;overflow:hidden;margin:10px 0 8px}
/* Smooth: a long eased tween absorbs the 500ms polling jitter, and the sheen keeps the
   bar visibly alive between ticks even when a fragment stalls. */
.fill{height:100%;width:0;border-radius:99px;background:var(--dark);
 transition:width .65s cubic-bezier(.22,.61,.36,1)}
.live .fill{background:linear-gradient(100deg,#151515 20%,#4a4a4a 50%,#151515 80%);
 background-size:220% 100%;animation:sheen 1.7s linear infinite}
@keyframes sheen{to{background-position:-220% 0}}
.fin .fill{background:var(--ok)}
.paused .fill{background:#A3A3A3}
.bad .fill{background:var(--bad)}
.bad .st{color:var(--bad)}
.meta{display:flex;justify-content:space-between;gap:14px;font-size:13px;color:var(--mut)}
.nu{white-space:nowrap;font-variant-numeric:tabular-nums}
.acts{display:flex;gap:7px;flex:none}
.x{border:1px solid var(--line);border-radius:9px;padding:6px 12px;font-size:13px;color:var(--mut);
 transition:.15s}
.x:hover{background:var(--soft);color:var(--ink)}
.cn:hover{border-color:#F0C3C3;color:var(--bad);background:#FEF5F5}
.x:disabled{opacity:.45;cursor:default;background:0}
.empty{padding:52px 20px;text-align:center;color:#A3A3A3;font-size:14px}

/* completion toast — stacked cards, straight off cal.com */
.toast{position:fixed;left:50%;bottom:30px;z-index:50;pointer-events:none;
 transform:translate(-50%,26px) scale(.96);opacity:0;
 transition:transform .5s cubic-bezier(.2,.9,.25,1),opacity .4s}
.toast.show{transform:translate(-50%,0) scale(1);opacity:1}
.tcard{position:relative;display:flex;align-items:center;gap:14px;max-width:min(92vw,440px);
 background:var(--card);border:1px solid var(--line);border-radius:15px;padding:14px 18px;
 box-shadow:0 18px 40px -14px rgba(0,0,0,.3)}
.tcard:before,.tcard:after{content:"";position:absolute;bottom:-7px;left:5%;right:5%;height:18px;
 background:var(--card);border:1px solid var(--line);border-radius:0 0 15px 15px;z-index:-1}
.tcard:after{bottom:-13px;left:11%;right:11%}
.tico{width:38px;height:38px;flex:none;border-radius:11px;background:var(--dark);color:#fff;
 display:grid;place-items:center;font-size:17px}
.tb{min-width:0}
.tb b{display:block;font-weight:600;letter-spacing:-.01em}
.tb span{display:block;color:var(--mut);font-size:13.5px;overflow:hidden;text-overflow:ellipsis;
 white-space:nowrap}
.tw{color:#A3A3A3;font-size:13px;flex:none}
@media(max-width:600px){.thumb{width:76px;height:43px}.job{padding:14px;gap:12px}
 .hero{padding-top:48px}.tw{display:none}.acts{flex-direction:column}}
</style></head><body><div class="wrap">

<nav class="nav">
 <div class="mark"><span class="logo"><svg width="12" height="12" viewBox="0 0 12 12"
   fill="#fff" aria-hidden="true"><path d="M2 1.6v8.8l8-4.4z"/></svg></span>Downloader</div>
 <button class="solid" id="fold">Open folder <span aria-hidden="true">›</span></button>
</nav>

<div class="hero">
 <span class="badge"><svg width="13" height="13" viewBox="0 0 14 14" fill="none" aria-hidden="true">
  <rect x="2.4" y="6.2" width="9.2" height="6" rx="1.6" stroke="#525252" stroke-width="1.4"/>
  <path d="M4.6 6.2V4.4a2.4 2.4 0 0 1 4.8 0v1.8" stroke="#525252" stroke-width="1.4"/></svg>
  Runs on your Mac. No account.</span>
 <h1>youtube downloader<br>that just fckn works</h1>
 <p class="sub">Paste a link. Full quality, straight from YouTube, in a file your phone plays offline.</p>
 <div class="form">
  <div class="bar">
   <svg width="17" height="17" viewBox="0 0 20 20" fill="none" aria-hidden="true">
    <path d="M8.2 11.8a3.2 3.2 0 0 0 4.6 0l2.6-2.6a3.2 3.2 0 0 0-4.6-4.6l-.6.6M11.8 8.2a3.2 3.2 0 0
     0-4.6 0l-2.6 2.6a3.2 3.2 0 0 0 4.6 4.6l.6-.6" stroke="#A3A3A3" stroke-width="1.6"
     stroke-linecap="round"/></svg>
   <input id="u" placeholder="Paste a YouTube link" autocomplete="off" autofocus aria-label="Video link">
   <div class="sel" id="sel">
    <button class="trg" id="trg" aria-haspopup="listbox" aria-expanded="false">
     <span id="trgl"></span>
     <svg class="chev" width="12" height="12" viewBox="0 0 12 12" fill="none" aria-hidden="true">
      <path d="M3 4.5 6 7.5l3-3" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"
       stroke-linejoin="round"/></svg></button>
    <div class="menu" id="menu" role="listbox" aria-label="Quality"></div>
   </div>
  </div>
  <button class="go" id="add">Download <span aria-hidden="true">›</span></button>
  <p class="fine" id="fine">Never re-encoded · nothing leaves your Mac</p>
 </div>
</div>

<div class="card">
 <div class="head">
  <b>Downloads</b><span class="cnt" id="cnt"></span>
  <span class="grow"></span>
  <button class="ghost" id="clr">Clear finished</button>
 </div>
 <div id="jobs"></div>
 <div class="empty" id="empty">Nothing here yet.</div>
</div>
</div>

<div class="toast" id="toast" role="status" aria-live="polite"><div class="tcard">
 <span class="tico">✓</span>
 <span class="tb"><b>Download complete</b><span id="tmsg"></span></span>
 <span class="tw">Just now</span>
</div></div>

<script>
const $=i=>document.getElementById(i),u=$("u");
const post=(p,b)=>fetch(p,{method:"POST",body:JSON.stringify(b)}).then(tick);

/* ---- quality dropdown ---- */
const Q=[["phone","1080p","H.264 — plays on any iPhone"],
         ["auto","Max quality","4K when available, bigger files"],
         ["audio","Audio only","m4a, no video"]];
const QL=Object.fromEntries(Q.map(q=>[q[0],q[1]]));
const menu=$("menu"),trg=$("trg");let pick=0,hi=0,open=false;
menu.innerHTML=Q.map((q,i)=>`<div class="item" role="option" data-i="${i}">
 <span><b>${q[1]}</b><i>${q[2]}</i></span>
 <svg class="ck" width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M2.5 7.3 5.6 10.4
  11.5 4.5" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"
  stroke-linejoin="round"/></svg></div>`).join("");
const items=[...menu.children];
function paint(){
 $("trgl").textContent=Q[pick][1];
 items.forEach((el,i)=>{el.className="item"+(i==pick?" on":"")+(i==hi&&open?" hi":"");
  el.setAttribute("aria-selected",i==pick)});
 menu.classList.toggle("open",open);
 trg.setAttribute("aria-expanded",open);
}
const setOpen=v=>{open=v;if(v)hi=pick;paint()};
trg.onclick=()=>setOpen(!open);
items.forEach(el=>{el.onclick=()=>{pick=+el.dataset.i;setOpen(false)};
 el.onmouseenter=()=>{hi=+el.dataset.i;paint()}});
document.addEventListener("click",e=>{if(!$("sel").contains(e.target))setOpen(false)});
document.addEventListener("keydown",e=>{
 if(e.key=="Escape")return setOpen(false);
 if(!open)return;
 if(e.key=="ArrowDown"||e.key=="ArrowUp"){e.preventDefault();
  hi=(hi+(e.key=="ArrowDown"?1:Q.length-1))%Q.length;paint()}
 else if(e.key=="Enter"||e.key==" "){e.preventDefault();pick=hi;setOpen(false)}});
paint();

/* ---- queue ---- */
const add=()=>{const v=u.value.trim();if(v){u.value="";post("/add",{url:v,preset:Q[pick][0]})}};
$("add").onclick=add;
u.onkeydown=e=>{if(e.key=="Enter")add()};
$("clr").onclick=()=>post("/clear",{});
$("fold").onclick=()=>post("/reveal",{});

const mb=b=>b>=1e9?(b/1e9).toFixed(2)+" GB":Math.round(b/1e6)+" MB";
// Raw seconds ("6585s left") are unreadable, and a per-second countdown on a
// ten-minute download is just noise — so the resolution drops as the number grows.
const dur=s=>s>=3600?`${s/3600|0}h ${s%3600/60|0}m`
 :s>=600?`${Math.round(s/60)}m`
 :s>=60?`${s/60|0}m ${(s%60/5|0)*5}s`
 :`${Math.max(5,(s/5|0)*5)}s`;

let toastT;
function toast(title){
 $("tmsg").textContent=title;
 $("toast").classList.add("show");
 clearTimeout(toastT);toastT=setTimeout(()=>$("toast").classList.remove("show"),6000);
}

const jobsEl=$("jobs"),cards={},was={};
// Cards are built once and mutated in place. Re-rendering innerHTML every tick
// destroyed the Cancel button mid-click, so clicks landing on a re-render were lost.
function mk(j){
 const c=document.createElement("div");c.className="job";
 c.innerHTML=`<div class="thumb"></div><div class="mid">
  <b class="ti"></b><span class="sb"></span>
  <div class="track"><div class="fill"></div></div>
  <div class="meta"><span class="st"></span><span class="nu"></span></div>
  </div><div class="acts"><button class="x pz"></button><button class="x cn">Cancel</button></div>`;
 c.q=s=>c.querySelector(s);
 // Reads c.paused at click time, not at build time — the same button is both actions.
 c.q(".pz").onclick=()=>post(c.paused?"/resume":"/pause",{id:j.id});
 c.q(".cn").onclick=()=>post("/cancel",{id:j.id});
 return c;
}
function upd(c,j){
 const bad=j.status.startsWith("error"),stopped=j.status=="cancelled";
 const done=!j.live&&!j.paused&&!bad&&!stopped;
 c.paused=j.paused;
 c.className="job"+(j.live?" live":"")+(j.paused?" paused":"")+(done?" fin":"")+(bad?" bad":"");
 c.q(".thumb").style.backgroundImage=j.thumb?`url(${JSON.stringify(j.thumb)})`:"none";
 c.q(".ti").textContent=j.title;
 c.q(".sb").textContent=[j.author,j.desc].filter(Boolean).join(" · ");
 c.q(".fill").style.width=j.pct+"%";
 c.q(".st").textContent=j.status+(j.preset!="phone"?" · "+QL[j.preset]:"");
 c.q(".nu").textContent=[j.pct+"%",j.total?mb(j.bytes)+" / "+mb(j.total):mb(j.bytes),
  j.speed?j.speed+" MB/s":0,j.eta?dur(j.eta)+" left":0].filter(Boolean).join(" · ");
 c.q(".pz").textContent=j.paused?"Resume":"Pause";
 c.q(".pz").disabled=j.status=="pausing";
 c.q(".acts").style.visibility=j.live||j.paused?"":"hidden";
}
async function tick(){
 let s;try{s=await(await fetch("/state")).json()}catch(e){return}
 $("fold").firstChild.textContent="Open "+s.out+" ";
 // Visible on purpose: a stale yt-dlp is what breaks downloads, so name the version.
 $("fine").textContent="Never re-encoded · nothing leaves your Mac · yt-dlp "+s.ver;
 const seen=new Set();
 for(const j of s.jobs){seen.add(j.id);
  if(!cards[j.id]){cards[j.id]=mk(j);jobsEl.prepend(cards[j.id])}
  upd(cards[j.id],j);
  // Fires on the transition only, so a finished job doesn't re-toast every 500ms.
  if(was[j.id]&&was[j.id]!=j.status&&!j.live&&!j.paused&&!j.status.startsWith("error")
   &&j.status!="cancelled")toast(j.title);
  was[j.id]=j.status}
 for(const id in cards)if(!seen.has(+id)){cards[id].remove();delete cards[id];delete was[id]}
 const live=s.jobs.filter(j=>j.live).length,held=s.jobs.filter(j=>j.paused).length;
 busy=live>0;
 $("empty").style.display=s.jobs.length?"none":"";
 $("cnt").textContent=[live?live+" downloading":0,held?held+" paused":0]
  .filter(Boolean).join(" · ")||(s.jobs.length?s.jobs.length+" finished":"");
}
setInterval(tick,500);tick();

/* ---- idle typing in the link bar ---- */
// Placeholder only: it never touches .value, so it can't be submitted by accident.
// Idle means idle — not merely unfocused, since the field is autofocused on load and
// would otherwise never animate at all. It yields on any keystroke for a few seconds.
const HINTS=["youtube.com/watch?v=dQw4w9WgXcQ","youtu.be/jNQXAC9IVRw",
 "youtube.com/watch?v=9bZkp7q19f0","youtube.com/shorts/1ZpXbCk2wPc"];
const IDLE="Paste a YouTube link";
let hn=0,cn=0,grow=true,busy=false,acted=0;
["keydown","paste","click"].forEach(e=>u.addEventListener(e,()=>{acted=Date.now();cn=0;grow=true}));
function typeTick(){
 if(busy||u.value||Date.now()-acted<4000){u.placeholder=IDLE;return setTimeout(typeTick,900)}
 const s=HINTS[hn];
 if(grow){if(++cn>=s.length){grow=false;u.placeholder=s;return setTimeout(typeTick,1800)}}
 else if(--cn<=0){grow=true;hn=(hn+1)%HINTS.length;u.placeholder=IDLE;return setTimeout(typeTick,800)}
 u.placeholder=s.slice(0,cn)+"\u258f";
 setTimeout(typeTick,grow?44+Math.random()*46:22);
}
if(!matchMedia("(prefers-reduced-motion: reduce)").matches)setTimeout(typeTick,1300);
</script></body></html>"""


@app.get("/")
def index():
    return PAGE


@app.get("/state")
def state():
    with _lock:
        jobs = [j.as_dict(i) for i, j in JOBS.items()]  # oldest first; the page prepends
    out = str(ytdlp_opts.OUT_DIR).replace(str(Path.home()), "~", 1)
    return jsonify(jobs=jobs, out=out, ver=YTDLP_VERSION)


@app.post("/add")
def add():
    # No Content-Type header from the page's fetch(), so force=True skips Flask's check.
    data = request.get_json(force=True)
    with _lock:
        preset = data.get("preset", "phone")
        job = Job(data["url"].strip(), preset if preset in quality.PRESETS else "phone")
        JOBS[max(JOBS, default=-1) + 1] = job
        threading.Thread(target=prefill, args=(job,), daemon=True).start()
        threading.Thread(target=run_job, args=(job,), daemon=True).start()
    return jsonify(ok=True)


@app.post("/cancel")
def cancel():
    data = request.get_json(force=True)
    with _lock:
        job = JOBS.get(data.get("id"))
        if job:
            job.cancel.set()
            if job.status == "paused":
                stop(job)  # no worker left to unwind it — clean up here instead
            elif job.status in LIVE:
                job.status = "cancelling"  # instant feedback, worker confirms
    return jsonify(ok=True)


@app.post("/pause")
def pause():
    data = request.get_json(force=True)
    with _lock:
        job = JOBS.get(data.get("id"))
        if job and job.status in LIVE:
            job.pause.set()
            job.status = "pausing"
    return jsonify(ok=True)


@app.post("/resume")
def resume():
    data = request.get_json(force=True)
    with _lock:
        job = JOBS.get(data.get("id"))
        if job and job.status == "paused":
            job.pause.clear()
            job.status = "waiting"
            # A fresh worker on the same Job: streams still hold the old byte counts,
            # so the bar picks up where it stopped instead of snapping back to zero.
            threading.Thread(target=run_job, args=(job,), daemon=True).start()
    return jsonify(ok=True)


@app.post("/reveal")
def reveal():
    ytdlp_opts.OUT_DIR.mkdir(parents=True, exist_ok=True)
    system.reveal(ytdlp_opts.OUT_DIR)
    return jsonify(ok=True)


@app.post("/clear")
def clear():
    with _lock:
        for i in [i for i, j in JOBS.items() if j.status not in LIVE]:
            del JOBS[i]
    return jsonify(ok=True)


def vparts(v: str) -> tuple:
    """(2026, 8, 19) from a version string. Numeric on purpose: PyPI normalises
    yt-dlp's own "2026.08.19" to "2026.8.19", so comparing the strings would call
    every single start an update — and "2026.9.1" would sort above "2026.10.1"."""
    return tuple(int(x) for x in re.findall(r"\d+", v))


def latest_ytdlp(timeout: float = 3.0):
    """Newest yt-dlp on PyPI, or None if we can't reach it. Being offline is a normal
    outcome for this app, not an error, so this never raises and never blocks long."""
    with contextlib.suppress(Exception):
        with urllib.request.urlopen("https://pypi.org/pypi/yt-dlp/json", timeout=timeout) as r:
            return json.load(r)["info"]["version"]
    return None


def update_ytdlp() -> bool:
    """Keep yt-dlp current on every start.

    A stale yt-dlp is the single most likely reason downloads suddenly stop working:
    YouTube changes something, and every format comes back 403 until yt-dlp catches up.
    Nothing in this app can work around that, so the fix belongs at startup.

    Returns True if a newer version was installed — the caller then has to re-exec,
    because this process imported the old yt_dlp module long before main() ran."""
    if os.environ.get(UPDATED_ENV):
        return False  # this IS the restarted process; checking again risks looping
    want = latest_ytdlp()
    if not want or vparts(want) <= vparts(YTDLP_VERSION):
        return False
    print(f"yt-dlp {YTDLP_VERSION} -> {want}, updating...", flush=True)
    here = Path(__file__).parent
    for cmd in (["uv", "lock", "--upgrade-package", "yt-dlp"], ["uv", "sync"]):
        r = subprocess.run(cmd, cwd=here, capture_output=True, text=True, check=False)
        if r.returncode:
            tail = (r.stderr or r.stdout).strip().splitlines()[-1:] or ["failed"]
            print(f"  skipped: {' '.join(cmd)}: {tail[0]}", flush=True)
            return False  # not fatal — the old version still downloads most things
    return True


def lan_ip() -> str:
    """The address this Mac has on the local network. Connecting a UDP socket sends
    no packet — it just makes the OS pick the route it would use, and name the
    interface behind it."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s, contextlib.suppress(OSError):
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    return "127.0.0.1"


def main(lan: bool = False, update: bool = True) -> int:
    if update and update_ytdlp():
        # execv replaces this process, so the new yt-dlp is imported from scratch.
        # Nothing is running yet — no jobs, no server — so there is nothing to lose.
        os.environ[UPDATED_ENV] = "1"
        # execv discards Python's buffered stdout along with the process image, so
        # without this the user just sees an unexplained pause at startup.
        print("restarting on the new version", flush=True)
        sys.stdout.flush()
        os.execv(sys.executable, [sys.executable, *sys.argv])
    ytdlp_opts.OUT_DIR.mkdir(parents=True, exist_ok=True)
    url = f"http://127.0.0.1:{PORT}"
    print(f"downloads → {url}   (yt-dlp {YTDLP_VERSION}, ctrl-c to quit)")
    if lan:
        # Opt-in, never the default: there's no login on this thing, so anyone on
        # the same network can queue downloads and read the output folder. Fine on
        # home wifi, not on a café's.
        name = system.mdns_name()
        if name:
            print(f"phone      → http://{name}.local:{PORT}")
        print(f"           → http://{lan_ip()}:{PORT}   (if .local doesn't resolve)")
        print("no password — only do this on a network you trust")
    else:
        webbrowser.open(url)
    awake = system.stay_awake()
    try:
        app.run(host="0.0.0.0" if lan else "127.0.0.1", port=PORT, threaded=True)
    finally:
        if awake:
            awake.terminate()
    return 0


def self_check() -> None:
    import tempfile

    # yt-dlp opts sanity — fails loudly if the opts dict rots, no network/bytes.
    o = ytdlp_opts.build(private=False, browser="chrome")
    assert o["retries"] == float("inf") and o["fragment_retries"] == float("inf")
    # mkv is what gets the file into Infuse from AirDrop; mp4 lands in Photos instead.
    assert o["merge_output_format"] == "mkv" and o["remux_video"] == "mkv"
    assert "%(id)s" in o["outtmpl"], "id must be in filename for dedupe"
    assert "cookiesfrombrowser" not in o, "cookies must be off unless private"
    assert ytdlp_opts.build(private=True, browser="chrome")["cookiesfrombrowser"] == ("chrome",)
    assert ytdlp_opts.BOT_CHECK  # retry trigger string must exist
    with YoutubeDL({**o, "simulate": True, "quiet": True}):
        pass
    quality.self_check()

    j = Job("u", "phone")
    assert j.pct == 0.0 and j.fmt == ytdlp_opts.PHONE_FMT  # no streams: must not divide by zero
    assert j.thumb is None and j.as_dict(0)["thumb"] is None
    j.streams["v"] = (50, 100)
    j.streams["a"] = (50, 100)  # audio starting must not reset the bar
    assert j.pct == 0.5
    j.streams["v"] = (100, 100)
    d = j.as_dict(3)
    assert (d["id"], d["pct"], d["bytes"], d["total"], d["live"]) == (3, 75.0, 150, 200, True)
    for s in ("done", "cancelled", "cancelling", "error: x", "paused"):
        j.status = s
        assert j.as_dict(3)["live"] is False, s  # only running jobs offer Pause
    # "pausing" is still running: the row must not offer Resume before the worker stops.
    j.status = "pausing"
    assert j.as_dict(3)["live"] is True and j.as_dict(3)["paused"] is False
    j.status = "paused"
    assert j.as_dict(3)["paused"] is True
    assert Job("u", "audio").fmt.startswith("ba[acodec^=mp4a]")  # m4a, not opus/webm

    # URL -> id -> thumbnail: the row must have a picture before any network call.
    for link in ("https://www.youtube.com/watch?v=abc123XYZ_-",
                 "https://youtu.be/abc123XYZ_-?t=5",
                 "https://www.youtube.com/shorts/abc123XYZ_-"):
        assert Job(link, "phone").vid == "abc123XYZ_-", link
    guessed = Job("https://youtu.be/abc123XYZ_-", "phone")
    assert guessed.thumb.endswith("/abc123XYZ_-/mqdefault.jpg")
    blind = Job("https://example.com/x", "phone")
    assert blind.vid is None and blind.thumb is None, "no id means no invented thumbnail"
    assert snippet(None) is None and snippet("a\n\n b  c") == "a b c"
    assert len(snippet("x" * 400)) == 151 and snippet("x" * 400).endswith("…")

    # Smoothed throughput. The point is the ETA stops swinging, so prove one bad
    # sampling window moves the average without owning it.
    sp = Job("u", "phone")
    sp.streams["v"] = (8_000_000, 40_000_000)
    sp._t -= 1.0
    sp.sample()
    assert 7e6 < sp.speed < 9e6, sp.speed
    assert sp.eta == int(32_000_000 / sp.speed), "eta must come off the smoothed rate"
    fast = sp.speed
    sp.streams["v"] = (8_100_000, 40_000_000)  # next window: a near-stall
    sp._t -= 1.0
    sp.sample()
    assert fast * 0.7 < sp.speed < fast, "one bad window must not tank or be ignored"
    sp._t = time.monotonic()
    sp.sample()  # window too short to divide by — must be skipped, not counted as a stall
    assert sp._b == 8_100_000

    # 403 retry: the whole point is that a revoked URL doesn't kill the download, but
    # that a genuinely dead video still fails instead of looping.
    global BACKOFF
    keep, BACKOFF = BACKOFF, 0.0
    try:
        tries = []

        def boom(msg="HTTP Error 403: Forbidden", ok_after=None):
            tries.append(1)
            if ok_after and len(tries) >= ok_after:
                return "ok"
            raise DownloadError(msg)

        try:
            with_403_retry(Job("u", "phone"), boom)
        except DownloadError:
            pass
        else:
            raise AssertionError("a 403 that never clears must eventually surface")
        assert len(tries) == RETRIES_403 + 1, tries

        tries.clear()
        assert with_403_retry(Job("u", "phone"), lambda: boom(ok_after=2)) == "ok"
        assert len(tries) == 2, "a 403 that clears must not restart the job"

        tries.clear()
        try:
            with_403_retry(Job("u", "phone"), lambda: boom("Video unavailable"))
        except DownloadError:
            pass
        assert len(tries) == 1, "only 403s are worth retrying"

        tries.clear()
        held = Job("u", "phone")
        held.pause.set()
        try:
            with_403_retry(held, boom)
        except DownloadCancelled:
            pass
        else:
            raise AssertionError("a pause during retry must stop, not keep reconnecting")
    finally:
        BACKOFF = keep

    # The updater re-execs the process, and this env guard is the only thing standing
    # between a failed upgrade and an infinite restart loop. Prove it short-circuits
    # before touching the network.
    os.environ[UPDATED_ENV] = "1"
    try:
        assert update_ytdlp() is False, "the restarted process must not update again"
    finally:
        del os.environ[UPDATED_ENV]
    # The version compare has to survive PyPI's normalisation, or the updater declares
    # itself stale forever and re-execs on every launch.
    assert vparts("2026.08.19") == vparts("2026.8.19"), "PyPI drops the leading zero"
    assert vparts("2026.07.04") < vparts("2026.8.19") < vparts("2026.9.1")
    assert vparts("2026.9.1") < vparts("2026.10.1"), "numeric, not lexicographic"

    # drop_partials deletes files — prove it only ever hits its own job's.
    real = ytdlp_opts.OUT_DIR
    try:
        with tempfile.TemporaryDirectory() as tmp:
            ytdlp_opts.OUT_DIR = Path(tmp)
            mine, sibling, keep = (Path(tmp, n) for n in (
                "V [abc123XYZ_-].f137.mp4.part", "Other [zzz999].f140.m4a.part",
                "V [abc123XYZ_-].mp4"))
            for f in (mine, sibling, keep):
                f.write_text("x")
            v = Job("u", "phone")
            v.vid = "abc123XYZ_-"
            drop_partials(v)
            assert not mine.exists(), "own partial should go"
            assert sibling.exists() and keep.exists(), "must not touch others or finished files"

            # The one thing that makes Pause worth having: it must leave the partial on
            # disk. Deleting it would silently turn every resume into a restart from 0.
            held = Path(tmp, "V [abc123XYZ_-].f137.mp4.part")
            held.write_text("x")
            v.pause.set()
            stop(v)
            assert held.exists() and v.status == "paused", "pause must keep the partial"
            v.pause.clear()
            stop(v)  # same unwind, no pause flag: now it is a real cancel
            assert not held.exists() and v.status == "cancelled"

        # AirDrop links: must be links (not 400MB copies), must drop the id for Infuse,
        # and a same-title collision must never clobber another video's link.
        with tempfile.TemporaryDirectory() as tmp:
            ytdlp_opts.OUT_DIR = Path(tmp)
            src = Path(tmp, "Clip [abc123XYZ_-].mkv")
            src.write_text("x")
            link = airdrop_link(src)
            assert link.name == "Clip.mkv", link.name  # .mkv preserved: it routes to Infuse
            assert link.stat().st_ino == src.stat().st_ino, "must hardlink, never copy"
            assert airdrop_link(src) == link, "re-linking a finished file must be a no-op"

            other = Path(tmp, "Clip [zzz999yyy88].mkv")   # same title, different video
            other.write_text("y")
            link2 = airdrop_link(other)
            assert link2.name == "Clip [zzz999yyy88].mkv", link2.name
            assert link.read_text() == "x", "collision must not overwrite the first link"

            # A job that never learned its id must not guess at a file.
            j2 = Job("u", "phone")
            assert final_file(j2) is None
            j2.vid = "abc123XYZ_-"
            assert final_file(j2) == src
    finally:
        ytdlp_opts.OUT_DIR = real
    print("self-check OK")


if __name__ == "__main__":
    if "--self-check" in sys.argv:
        self_check()
    else:
        raise SystemExit(main(lan="--lan" in sys.argv, update="--no-update" not in sys.argv))
