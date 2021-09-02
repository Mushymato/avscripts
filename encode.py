#!/usr/bin/env python3
import sys
import os
import re
import subprocess
import json
from urllib.parse import quote

STAGING_TORRENT_DIR = os.path.abspath("D:\Downloads\avscripts\staging")
TAG_LANGUAGE = "TAG:language"
SUB_EVAL_KEY = "TAG:NUMBER_OF_FRAMES-eng"
FFMPEG_ESCAPE = re.compile(r"([\\'])")

NISEMONO = "https://u.nisemo.no/"
MKV = ".mkv"
MP4 = "mp4"


def ffprobe_streams(source_path, stream_type):
    process = subprocess.Popen(
        [
            "ffprobe",
            "-v",
            "error",
            "-of",
            "default=noprint_wrappers=1",
            "-show_streams",
            "-select_streams",
            stream_type,
            source_path,
        ],
        stdout=subprocess.PIPE,
        universal_newlines=True,
    )
    results = []
    current_data = None
    for line in iter(process.stdout.readline, ""):
        try:
            key, value = line.strip().split("=")
        except ValueError:
            continue
        try:
            value = float(value)
        except ValueError:
            pass
        if key == "index":
            current_data = {}
            results.append(current_data)
        if current_data is not None:
            current_data[key] = value
    return results


def ffprobe_duration(source_path):
    process = subprocess.Popen(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            source_path,
        ],
        stdout=subprocess.PIPE,
        universal_newlines=True,
    )

    output = process.stdout.readline().strip()
    try:
        return int(round(float(output), 0))
    except ValueError:
        return None


def process(source_dir, target_dir, filename):
    print(f"process({source_dir!r}, {target_dir!r}, {filename!r}) ", flush=True, end="")
    source_path = FFMPEG_ESCAPE.sub(r"\\\1", os.path.join(source_dir, filename))
    basename = os.path.splitext(filename)[0]
    target_path = FFMPEG_ESCAPE.sub(r"\\\1", os.path.join(target_dir, basename + "." + MP4))
    ffmpeg_call = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        source_path,
        "-movflags",
        "+faststart",
        "-pix_fmt",
        "yuv420p",
        "-crf",
        "23",
        "-preset",
        "veryfast",
        "-tune",
        "animation",
        "-c:v",
        "libx264",
        "-c:a",
        "aac",
    ]
    # check which audio track to use
    audio_tracks = ffprobe_streams(source_path, "a")
    audio_idx = None
    for idx, data in enumerate(audio_tracks):
        if data.get(TAG_LANGUAGE) != "jpn":
            continue
        if audio_idx is None:
            audio_idx = idx
            break
    if audio_idx is not None and audio_idx != 0:
        ffmpeg_call.append(f"-map 0:a:{audio_idx}")
    # check which sub track to use
    sub_tracks = ffprobe_streams(source_path, "s")
    sub_idx = None
    for idx, data in enumerate(sub_tracks):
        if data.get(TAG_LANGUAGE) != "eng":
            continue
        if sub_idx is None or sub_tracks[sub_idx].get(SUB_EVAL_KEY, 0) < data.get(SUB_EVAL_KEY, 0):
            sub_idx = idx
    if sub_idx is not None:
        sub = sub_tracks[sub_idx]
        ffmpeg_call.append("-filter_complex")
        if sub.get("codec_name") == "dvdsub":
            # bitmap subs from old dvd rips
            ffmpeg_call.append(f"[0:v][{sub_idx}:s]overlay")
        elif sub.get("DISPOSITION:default") or len(sub_tracks) == 1:
            # already default sub track
            ffmpeg_call.append(f"subtitles='{source_path}'")
        else:
            # remap subtitle
            ffmpeg_call.append(f"subtitles='{source_path}:si={sub_idx}'")
    ffmpeg_call.append(target_path)
    subprocess.run(ffmpeg_call)
    # metadata json
    duration = ffprobe_duration(target_path)
    if duration is None:
        return False
    url = f"{NISEMONO}{os.path.basename(target_dir)}/{quote(basename)}.{MP4}"
    metadata = {
        "title": basename,
        "duration": duration,
        "live": False,
        "sources": [
            {
                "url": url,
                "contentType": f"video/{MP4}",
                "quality": 1080,
            }
        ],
    }
    metadata_path = os.path.join(target_dir, basename + ".json")
    with open(metadata_path, "w") as fn:
        json.dump(metadata, fn)
    print("done")
    return source_path, target_path, metadata_path, url


def deluge_post(tid, tname, tpath):
    # change to deluge-console eventually
    torrent_dir = os.path.abspath(tpath)
    if not STAGING_TORRENT_DIR in torrent_dir:
        return

    for root, _, files in os.walk(torrent_dir):
        for filename in files:
            if not filename.endswith(MKV):
                continue
            # TODO: finish this stuff


def local_process(tpath):
    import paramiko
    from scp import SCPClient

    with open("./scp_args.json", "r") as fn:
        scp_args = json.load(fn)

    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(scp_args["server"], scp_args["port"], scp_args["user"], scp_args["password"])
    scp = SCPClient(ssh.get_transport())

    uploaded = []
    for filename in os.listdir(tpath):
        if not filename.endswith(MKV):
            continue
        result = process(tpath, tpath, filename)
        if not result:
            continue
        _, target_path, metadata_path, url = result
        scp.put(target_path, remote_path=f"/var/www/uploads/{os.path.basename(tpath)}")
        scp.put(metadata_path, remote_path=f"/var/www/uploads/{os.path.basename(tpath)}")
        uploaded.append(url)

    print(",".join(uploaded))


if __name__ == "__main__":
    if len(sys.argv) == 2:
        local_process(sys.argv[1])
    else:
        deluge_post(sys.argv[1], sys.argv[2], sys.argv[3])
