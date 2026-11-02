"""Copy finished downloads into Infuse on a plugged-in iPhone.

Same mechanism as dragging onto Infuse in Finder's Files tab: house_arrest, the
service that exposes an app's Documents folder over USB. ~/Downloads/yt stays the
backup; the phone gets <day>/<clean title>.mkv, so trips stay apart there too.

A ledger (OUT_DIR/.on-phone) remembers what was already sent. Without it, deleting
a watched video on the phone would just bring it back on the next plug-in."""

import asyncio
import contextlib
import re
import threading

import ytdlp_opts

INFUSE = "com.firecore.infuse"
ID_RE = re.compile(r"\s*\[[A-Za-z0-9_-]{11}\]")
POLL = 5  # seconds between "is a phone plugged in?" checks
# What the page's iPhone pill reads. One writer (the loop thread), plain dict reads.
state = {"phone": None, "copying": None, "done": 0, "error": None}
wake = threading.Event()  # set by the toggle so switching on reacts now, not in 5s


def off_flag():
    # A file, not a setting in memory: "I turned it off" has to survive a restart.
    return ytdlp_opts.OUT_DIR / ".phone-sync-off"


def enabled() -> bool:
    return not off_flag().exists()


def set_enabled(on: bool) -> None:
    if on:
        off_flag().unlink(missing_ok=True)
    else:
        off_flag().touch()
    wake.set()


class Stopped(Exception):
    """Sync was switched off mid-copy."""


def ledger():
    return ytdlp_opts.OUT_DIR / ".on-phone"


def finished() -> list:
    """Every finished file in the day folders, oldest first — so a long copy session
    delivers the trip in the order it was downloaded."""
    root = ytdlp_opts.OUT_DIR
    files = [f for f in root.glob("*/*") if ytdlp_opts.DAY_RE.fullmatch(f.parent.name)
             and f.is_file() and not f.name.startswith(".")]
    return sorted(files, key=lambda f: f.stat().st_mtime)


def key(f) -> str:
    return f"{f.parent.name}/{f.name}"


def pending() -> list:
    """What still has to go to the phone. First run seeds the ledger with everything
    already on disk: those are either on the phone by hand already or deliberately
    old — refilling the phone with months of backlog is not what plugging in means."""
    if not ledger().exists():
        ledger().write_text("".join(key(f) + "\n" for f in finished()))
        return []
    sent = set(ledger().read_text().splitlines())
    return [f for f in finished() if key(f) not in sent]


def mark(f) -> None:
    with ledger().open("a") as fh:
        fh.write(key(f) + "\n")


async def phone_name():
    """Name of a plugged-in phone, or None. usbmux only lists devices — no pairing
    handshake, no service started — so this is cheap enough to ask every few seconds."""
    from pymobiledevice3.lockdown import create_using_usbmux
    from pymobiledevice3.usbmux import list_devices

    if not await list_devices():
        return None
    if state["phone"]:
        return state["phone"]
    async with await create_using_usbmux() as ld:
        return ld.short_info.get("DeviceName") or "iPhone"


async def push(todo: list) -> None:
    # Imported here: it is a heavy import, and only this thread ever needs it.
    from pymobiledevice3.lockdown import create_using_usbmux
    from pymobiledevice3.services.afc import MAXIMUM_READ_SIZE
    from pymobiledevice3.services.house_arrest import HouseArrestService

    # Both contexts close their sockets before asyncio.run tears the loop down, or
    # every tick spews SSL noise.
    async with (await create_using_usbmux() as ld,
                await HouseArrestService.create(ld, INFUSE, documents_only=True) as ha):
        for n, f in enumerate(todo, 1):
            day = f"/Documents/{f.parent.name}"
            dest = f"{day}/{ID_RE.sub('', f.name)}"
            size = f.stat().st_size or 1
            cur = {"name": ID_RE.sub("", f.stem), "n": n, "of": len(todo), "pct": 0.0}
            state["copying"] = cur
            if not await ha.exists(day):
                await ha.makedirs(day)
            if not await ha.exists(dest):
                # Written under a temp name, renamed when complete: unplugging mid-copy
                # must not leave a truncated video in Infuse that looks like the real one.
                # Own chunk loop rather than ha.push: it is the only way to get a
                # percentage out, and to stop between chunks when switched off.
                h = await ha.fopen(dest + ".part", "w")
                try:
                    with open(f, "rb") as src:  # noqa: ASYNC230 — this loop is the sync thread's alone
                        sent = 0
                        while chunk := src.read(MAXIMUM_READ_SIZE):
                            if not enabled():
                                raise Stopped
                            await ha.fwrite(h, chunk)
                            sent += len(chunk)
                            cur["pct"] = 100 * sent / size
                finally:
                    await ha.fclose(h)
                    if not enabled():
                        await ha.rm(dest + ".part", force=True)
                await ha.rename(dest + ".part", dest)
            mark(f)
            state["done"] += 1


def loop() -> None:
    while True:
        try:
            state["phone"] = asyncio.run(phone_name()) if enabled() else None
            state["error"] = None
            todo = pending() if state["phone"] else []
            if todo:
                asyncio.run(push(todo))
        except Stopped:
            pass
        except Exception as e:  # noqa: BLE001 — locked phone, unplugged mid-copy: next tick
            state["error"] = None if "NoDevice" in type(e).__name__ else str(e)[:90]
        state["copying"] = None
        wake.wait(POLL)
        wake.clear()


def start() -> None:
    with contextlib.suppress(OSError):
        ytdlp_opts.OUT_DIR.mkdir(parents=True, exist_ok=True)
    threading.Thread(target=loop, daemon=True).start()


def self_check() -> None:
    import tempfile
    from pathlib import Path

    real = ytdlp_opts.OUT_DIR
    try:
        with tempfile.TemporaryDirectory() as tmp:
            ytdlp_opts.OUT_DIR = Path(tmp)
            Path(tmp, "2026-03-01").mkdir()
            old = Path(tmp, "2026-03-01", "Old [abc123XYZ_-].mkv")
            old.write_text("x")
            assert pending() == [], "first run must not push the backlog"
            new = Path(tmp, "2026-03-01", "New [zzz999yyy88].mkv")
            new.write_text("x")
            Path(tmp, ".incomplete").mkdir()
            Path(tmp, ".incomplete", "Half [qqq999yyy88].mkv.part").write_text("x")
            assert pending() == [new], pending()
            mark(new)
            assert pending() == [], "sent once, never again — even if deleted on the phone"
    finally:
        ytdlp_opts.OUT_DIR = real
    print("iphone self-check OK")


if __name__ == "__main__":
    self_check()
