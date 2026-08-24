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
import os
import re
import socket
import sys
import threading
import webbrowser
from pathlib import Path

from flask import Flask, jsonify, request
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadCancelled

import quality
import system
import ytdlp_opts

PORT = 8765
# 3 at once saturates a normal line; more just invites YouTube's rate limiter,
# which already gave us a 403 once. ponytail: fixed cap, expose it if a fat
# connection ever sits idle behind it.
LANES = threading.Semaphore(3)
# Statuses that still show a Cancel button. "cancelling" is absent on purpose —
# the click already landed, the worker just hasn't hit its next checkpoint.
LIVE = ("waiting", "starting", "downloading", "merging")

JOBS: dict[int, "Job"] = {}
_lock = threading.Lock()
app = Flask(__name__)

# The [videoId] the real file needs for dedupe is noise in Infuse, which shows filenames.
ID_RE = re.compile(r"\s*\[[A-Za-z0-9_-]{11}\]")


class Job:
    def __init__(self, url: str, preset: str):
        self.url, self.preset = url, preset
        self.fmt = quality.selector_for(preset)
        self.title, self.status = url, "waiting"
        self.speed, self.eta = 0.0, None
        self.vid = None  # youtube id, learned during extraction — finds our .part files
        self.thumb = None
        self.cancel = threading.Event()
        self.streams: dict[str, tuple[int, int]] = {}

    @property
    def pct(self) -> float:
        total = sum(t for _, t in self.streams.values())
        return sum(d for d, _ in self.streams.values()) / total if total else 0.0

    def as_dict(self, jid: int) -> dict:
        return {"id": jid, "title": self.title, "status": self.status,
                "pct": round(self.pct * 100, 1), "speed": round(self.speed / 1e6, 1),
                "eta": self.eta, "preset": self.preset, "live": self.status in LIVE,
                "bytes": sum(d for d, _ in self.streams.values()), "thumb": self.thumb}


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
        if job.cancel.is_set():
            raise DownloadCancelled()
        total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
        got = d.get("downloaded_bytes")
        job.streams[d.get("filename") or "?"] = (total if got is None else got, total)
        info = d.get("info_dict") or {}
        job.title = info.get("title") or job.title
        job.vid = info.get("id") or job.vid
        job.thumb = info.get("thumbnail") or job.thumb
        job.speed, job.eta, job.status = d.get("speed") or 0.0, d.get("eta"), "downloading"

    def gate(info, *, incomplete=False):
        # Runs right after extraction — earliest point a thumbnail is known, so it
        # shows up even during "starting", before any byte has downloaded.
        job.vid = info.get("id") or job.vid
        job.thumb = info.get("thumbnail") or job.thumb
        return "cancelled" if job.cancel.is_set() else None

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
            try:
                go(False)
            except Exception as e:
                if ytdlp_opts.BOT_CHECK not in str(e):
                    raise
                job.status = "signing in with chrome cookies"
                go(True)
            if job.cancel.is_set():
                raise DownloadCancelled()  # gate aborts by skipping, not raising
            # No progress hook ever fired => yt-dlp skipped it, file was already there.
            job.status, job.speed = ("done" if job.streams else "already on disk"), 0.0
            src = final_file(job)
            if src:
                airdrop_link(src)  # failures are silent: the download still succeeded
            system.ding()
        except DownloadCancelled:
            drop_partials(job)
            job.status = "cancelled"
        except Exception as e:
            last = str(e).strip().splitlines()[-1] if str(e).strip() else "failed"
            job.status = "error: " + last[:90]


PAGE = """<!doctype html><html><head><meta charset="utf-8"><title>Downloads</title>
<meta name="viewport" content="width=device-width,initial-scale=1"><style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#F0EFEE;color:#111827;font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",
 Inter,sans-serif;-webkit-font-smoothing:antialiased;padding:40px 20px 60px}
.wrap{max-width:660px;margin:0 auto}
h1{font-size:26px;font-weight:600;letter-spacing:-.4px}
.sub{color:#6B7280;font-size:14px;margin:4px 0 26px}
.card{background:#fff;border-radius:18px;padding:20px;margin-bottom:16px;
 box-shadow:0 1px 2px rgba(0,0,0,.05),0 10px 26px rgba(0,0,0,.05)}
.row{display:flex;gap:14px}
.thumb{width:120px;height:68px;flex:none;border-radius:10px;background:#EFEFEF center/cover no-repeat}
.body{flex:1;min-width:0}
label{font-size:14px;font-weight:500;display:block;margin-bottom:8px}
input{width:100%;font:inherit;background:#F4F4F5;border:0;border-radius:12px;padding:14px;
 color:#111827;outline:0}
input::placeholder{color:#9CA3AF}
input:focus{box-shadow:0 0 0 2px #111827 inset}
.seg{display:inline-flex;background:#F1F1F1;border-radius:13px;padding:4px;margin:16px 0 18px}
.seg button{border:0;background:0;font:inherit;font-size:14px;color:#6B7280;padding:8px 18px;
 border-radius:10px;cursor:pointer}
.seg button.on{background:#fff;color:#111827;font-weight:500;box-shadow:0 1px 3px rgba(0,0,0,.1)}
.btn{font:inherit;font-size:14px;font-weight:500;border:0;border-radius:12px;padding:12px 22px;
 cursor:pointer}
.dark{background:#111827;color:#fff}
.dark:hover{background:#000}
.ghost{background:#fff;border:1px solid #E5E5E5;color:#111827}
.ghost:hover{background:#FAFAFA}
.right{display:flex;justify-content:flex-end;gap:10px;flex-wrap:wrap}
.top{display:flex;justify-content:space-between;align-items:center;gap:14px;margin-bottom:14px}
.top b{font-weight:500;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.tag{flex:none;font-size:12px;color:#6B7280;background:#F4F4F5;border-radius:8px;padding:4px 10px}
.track{height:8px;background:#EFEFEF;border-radius:99px;overflow:hidden}
.fill{height:100%;background:#22C55E;border-radius:99px;transition:width .3s linear}
.meta{display:flex;justify-content:space-between;gap:12px;color:#6B7280;font-size:13px;
 margin-top:12px}
.done .fill{background:#D1D5DB}
.bad{color:#DC2626}
.empty{color:#9CA3AF;text-align:center;padding:28px 0;font-size:14px}
</style></head><body><div class="wrap">
<h1>Downloads</h1><div class="sub" id="out"></div>
<div class="card">
<label for="u">Video link</label>
<input id="u" placeholder="Paste a YouTube URL" autocomplete="off" autofocus>
<div class="seg" id="seg"></div>
<div class="right"><button class="btn ghost" id="air">AirDrop folder</button>
<button class="btn ghost" id="clr">Clear finished</button>
<button class="btn dark" id="go">Download</button></div>
</div>
<div id="jobs"></div><div class="card empty" id="empty">Nothing here yet.</div></div>
<script>
const P=[["Phone","phone"],["Best","auto"],["Audio","audio"]];let pi=0;
const seg=document.getElementById("seg"),u=document.getElementById("u");
seg.innerHTML=P.map((p,i)=>`<button data-i="${i}">${p[0]}</button>`).join("");
const paint=()=>[...seg.children].forEach((b,i)=>b.className=i==pi?"on":"");
seg.onclick=e=>{if(e.target.dataset.i){pi=+e.target.dataset.i;paint()}};paint();
const post=(p,b)=>fetch(p,{method:"POST",body:JSON.stringify(b)}).then(tick);
const add=()=>{const v=u.value.trim();if(v){u.value="";post("/add",{url:v,preset:P[pi][1]})}};
document.getElementById("go").onclick=add;
u.onkeydown=e=>{if(e.key=="Enter")add()};
document.getElementById("clr").onclick=()=>post("/clear",{});
document.getElementById("air").onclick=()=>post("/airdrop",{});
const mb=b=>b>1e9?(b/1e9).toFixed(2)+" GB":(b/1e6).toFixed(0)+" MB";
const LBL=Object.fromEntries(P.map(p=>[p[1],p[0]]));
const jobsEl=document.getElementById("jobs"),empty=document.getElementById("empty"),cards={};
// Cards are built once and mutated in place. Re-rendering innerHTML every tick
// destroyed the Cancel button mid-click, so clicks landing on a re-render were lost.
function mk(j){
 const c=document.createElement("div");c.className="card";
 c.innerHTML=`<div class="row"><div class="thumb"></div><div class="body">
  <div class="top"><b></b><span class="tag"></span></div>
  <div class="track"><div class="fill"></div></div>
  <div class="meta"><span class="st"></span><span class="nu"></span></div>
  <div class="right act" style="margin-top:14px"><button class="btn ghost">Cancel</button></div>
  </div></div>`;
 c.q=s=>c.querySelector(s);
 c.q("button").onclick=()=>post("/cancel",{id:j.id});
 return c;
}
function upd(c,j){
 c.className="card"+(j.live?"":" done");
 c.q(".thumb").style.backgroundImage=j.thumb?`url(${JSON.stringify(j.thumb)})`:"none";
 c.q("b").textContent=j.title;c.q(".tag").textContent=LBL[j.preset]||j.preset;
 c.q(".fill").style.width=j.pct+"%";
 c.q(".st").textContent=j.status;c.q(".st").className="st"+(j.status.startsWith("error")?" bad":"");
 c.q(".nu").textContent=[j.pct+"%",mb(j.bytes),j.speed?j.speed+" MB/s":0,
  j.eta?j.eta+"s left":0].filter(Boolean).join(" · ");
 c.q(".act").style.display=j.live?"":"none";
}
async function tick(){
 let s;try{s=await(await fetch("/state")).json()}catch(e){return}
 document.getElementById("out").textContent=s.out;
 const seen=new Set();
 for(const j of s.jobs){seen.add(j.id);
  if(!cards[j.id]){cards[j.id]=mk(j);jobsEl.prepend(cards[j.id])}
  upd(cards[j.id],j)}
 for(const id in cards)if(!seen.has(+id)){cards[id].remove();delete cards[id]}
 empty.style.display=s.jobs.length?"none":"";
}
setInterval(tick,500);tick();
</script></body></html>"""


@app.get("/")
def index():
    return PAGE


@app.get("/state")
def state():
    with _lock:
        jobs = [j.as_dict(i) for i, j in JOBS.items()]  # oldest first; the page prepends
    return jsonify(jobs=jobs, out=str(ytdlp_opts.OUT_DIR))


@app.post("/add")
def add():
    # No Content-Type header from the page's fetch(), so force=True skips Flask's check.
    data = request.get_json(force=True)
    with _lock:
        preset = data.get("preset", "phone")
        job = Job(data["url"].strip(), preset if preset in quality.PRESETS else "phone")
        JOBS[max(JOBS, default=-1) + 1] = job
        threading.Thread(target=run_job, args=(job,), daemon=True).start()
    return jsonify(ok=True)


@app.post("/cancel")
def cancel():
    data = request.get_json(force=True)
    with _lock:
        job = JOBS.get(data.get("id"))
        if job:
            job.cancel.set()
            if job.status in LIVE:
                job.status = "cancelling"  # instant feedback, worker confirms
    return jsonify(ok=True)


@app.post("/airdrop")
def airdrop():
    d = airdrop_dir()
    d.mkdir(parents=True, exist_ok=True)  # so the button works before the first download
    system.reveal(d)
    return jsonify(ok=True)


@app.post("/clear")
def clear():
    with _lock:
        for i in [i for i, j in JOBS.items() if j.status not in LIVE]:
            del JOBS[i]
    return jsonify(ok=True)


def lan_ip() -> str:
    """The address this Mac has on the local network. Connecting a UDP socket sends
    no packet — it just makes the OS pick the route it would use, and name the
    interface behind it."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s, contextlib.suppress(OSError):
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    return "127.0.0.1"


def main(lan: bool = False) -> int:
    ytdlp_opts.OUT_DIR.mkdir(parents=True, exist_ok=True)
    url = f"http://127.0.0.1:{PORT}"
    print(f"downloads → {url}   (ctrl-c to quit)")
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
    assert (d["id"], d["pct"], d["bytes"], d["live"]) == (3, 75.0, 150, True)
    for s in ("done", "cancelled", "cancelling", "error: x"):
        j.status = s
        assert j.as_dict(3)["live"] is False, s  # only running jobs offer Cancel
    assert Job("u", "audio").fmt.startswith("ba[acodec^=mp4a]")  # m4a, not opus/webm

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
        raise SystemExit(main(lan="--lan" in sys.argv))
