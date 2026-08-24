"""Quality presets: phone-native MP4 (default), best available, or audio only."""

import ytdlp_opts

PRESETS = {
    "phone": ytdlp_opts.PHONE_FMT,
    "auto": "bv*+ba/b",
    # m4a first: "best audio" is opus-in-webm, which iOS won't play natively.
    "audio": "ba[acodec^=mp4a]/ba/b",
}


def selector_for(picked) -> str:
    """int -> cap at that height. Otherwise a PRESETS name — anything else raises,
    because a typo used to silently build `bv*[height<=best]` and only blow up later
    inside yt-dlp as "Invalid filter"."""
    if isinstance(picked, int):
        return f"bv*[height<={picked}]+ba/b"
    if picked not in PRESETS:
        raise ValueError(f"unknown quality preset {picked!r}")
    return PRESETS[picked]


def self_check() -> None:
    assert selector_for("auto") == "bv*+ba/b"
    assert selector_for("audio").startswith("ba[acodec^=mp4a]")  # not opus/webm
    assert selector_for(720) == "bv*[height<=720]+ba/b"
    assert selector_for("phone") == ytdlp_opts.PHONE_FMT
    for bad in ("best", "1080p", ""):
        try:
            selector_for(bad)
        except ValueError:
            pass
        else:
            raise AssertionError(f"{bad!r} should not silently build a selector")
    # Phone preset must stay avc1/mp4a — that's the whole point of it existing.
    assert "avc1" in ytdlp_opts.PHONE_FMT and "mp4a" in ytdlp_opts.PHONE_FMT
    print("quality self-check OK")
