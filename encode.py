#!/usr/bin/env python3
import shutil
import sys
import os
import glob
import subprocess
import json
from urllib.parse import quote
from pprint import pprint

STAGING_TORRENT_DIR = os.path.abspath("D:\\Downloads\\avscripts\\staging")
TARGET_SERVING_DIR = os.path.abspath("D:\\xampp\\htdocs\\uploads")
TAG_LANGUAGE = "TAG:language"
DISPOSITION_DEFAULT = "DISPOSITION:default"
SUB_EVAL_KEY = "TAG:NUMBER_OF_BYTES-eng"
CODEC_NAME = "codec_name"

IMAGE_BASED_SUBS = ("hdmv_pgs_subtitle", "dvdsub")

NISEMONO = "https://u.nisemo.no/"
MKV = ".mkv"
MP4 = ".mp4"
WEBM = ".webm"
VTT = ".vtt"
SRT = ".srt"
ASS = ".ass"


def get_ffmpeg_call(source_path, ext):
    if ext == MP4:
        return [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-stats",
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
            # "-tune",
            # "animation",
            "-c:v",
            "libx264",
            "-c:a",
            "aac",
        ]
    elif ext == WEBM:
        # too slow and or shit looking to use atm
        return [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-stats",
            "-y",
            "-i",
            source_path,
            # "-deadline",
            # "realtime",
            # "-cpu-used",
            # "4",
            "-crf",
            "30",
            "-c:v",
            "libvpx-vp9",
            "-c:a",
            "libvorbis",
        ]


def scp_progress(filename, size, sent):
    sys.stdout.write(f"{str(filename)}: {float(sent) / float(size):.2%}\r")


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


def process(source_dir, target_dir, filename, ext=MP4):
    print(f"process({source_dir!r}, {target_dir!r}, {filename!r}) ", flush=True)
    source_path = os.path.join(source_dir, filename)
    # ffmpeg rly hates single quotes in filter_complex stuff
    if "'" in filename:
        filename = filename.replace("'", "")
        new_source = os.path.join(source_dir, filename)
        os.rename(source_path, new_source)
        source_path = new_source
    basename = os.path.splitext(filename)[0]
    os.makedirs(target_dir, exist_ok=True)
    target_basename = basename.replace(".x265-RARBG", "")
    target_path = os.path.join(target_dir, target_basename + ext)

    vtt_subs = False
    vtt_sub_path = os.path.join(target_dir, target_basename + VTT)
    # check for external subs, and convert them to vtt
    ass_subs = None
    if os.path.isfile(vtt_sub_path):
        vtt_subs = True
    else:
        # for file_path in glob.glob(os.path.join(target_dir, basename + ".*")):
        #     if any((file_path.endswith(sub_ext) for sub_ext in SUB_EXTS)):
        #         subprocess.run(["ffmpeg", "-i", file_path, vtt_sub_path])
        #         vtt_subs = True
        #         break
        for file_path in os.listdir(target_dir):
            if not file_path.startswith(basename):
                continue
            if file_path.endswith(SRT):
                subprocess.run(["ffmpeg", "-i", os.path.join(target_dir, file_path), vtt_sub_path])
                vtt_subs = True
                break
            if file_path.endswith(ASS):
                ass_subs = os.path.join(target_dir, file_path)
                break

    if not os.path.isfile(target_path):
        ffmpeg_call = get_ffmpeg_call(source_path, ext)
        # check which audio track to use
        audio_tracks = ffprobe_streams(source_path, "a")
        audio_idx = None
        default_idx = 0
        for idx, data in enumerate(audio_tracks):
            if data.get(DISPOSITION_DEFAULT):
                default_idx = idx
            if data.get(TAG_LANGUAGE) == "jpn" and audio_idx is None:
                audio_idx = idx
        if audio_idx not in (default_idx, None):
            ffmpeg_call.append("-map")
            ffmpeg_call.append(f"{default_idx}:a:{audio_idx}")
        # check which sub track to use
        if ass_subs:
            ffmpeg_call.append("-filter_complex")
            escaped_ass = ass_subs.replace("\\", "\\\\\\").replace(":", "\:")
            ffmpeg_call.append(f"subtitles='{escaped_ass}'")
        elif not vtt_subs:
            sub_tracks = ffprobe_streams(source_path, "s")
            if sub_tracks:
                sub_idx = None
                img_sub_idx = None
                for idx, data in enumerate(sub_tracks):
                    if data.get(TAG_LANGUAGE) != "eng":
                        continue
                    if data.get(CODEC_NAME) in IMAGE_BASED_SUBS:
                        if img_sub_idx is None or (sub_tracks[img_sub_idx].get(SUB_EVAL_KEY, 0) < data.get(SUB_EVAL_KEY, 0)):
                            img_sub_idx = idx
                    elif sub_idx is None or (sub_tracks[sub_idx].get(SUB_EVAL_KEY, 0) < data.get(SUB_EVAL_KEY, 0)):
                        sub_idx = idx
                if sub_idx is None:
                    sub_idx = img_sub_idx or 0
                if sub_idx is not None:
                    sub = sub_tracks[sub_idx]
                    ffmpeg_call.append("-filter_complex")
                    # dum
                    escaped_source = source_path.replace("\\", "\\\\\\").replace(":", "\:")
                    if sub.get(CODEC_NAME) in IMAGE_BASED_SUBS:
                        # bitmap subs from bd/dvd
                        # overlay=x=-240:y=0 to adjust positions when needed
                        ffmpeg_call.append(f"[0:v][0:s:{sub_idx}]overlay")
                    elif sub.get("DISPOSITION:default") or len(sub_tracks) == 1:
                        # already default sub track
                        ffmpeg_call.append(f"subtitles='{escaped_source}'")
                    else:
                        # remap subtitle
                        ffmpeg_call.append(f"subtitles='{escaped_source}:si={sub_idx}'")
        ffmpeg_call.append(target_path)
        print(" ".join(ffmpeg_call), flush=True)
        subprocess.run(ffmpeg_call)

    # metadata json
    duration = ffprobe_duration(target_path)
    if duration is None:
        return False
    prefix = os.path.basename(target_dir.strip("/"))
    url = f"{NISEMONO}{prefix}/{quote(target_basename)}{ext}"
    metadata = {
        "title": target_basename,
        "duration": duration,
        "live": False,
        "sources": [
            {
                "url": url,
                "contentType": f"video/{ext[1:]}",
                "quality": 1080,
            }
        ],
    }
    if vtt_subs:
        metadata["textTracks"] = [
            {
                "url": f"{NISEMONO}{prefix}/{quote(target_basename)}{VTT}",
                "contentType": "text/vtt",
                "name": "English",
                "default": True,
            }
        ]
    else:
        vtt_sub_path = None

    metadata_path = os.path.join(target_dir, target_basename + ".json")
    with open(metadata_path, "w") as fn:
        json.dump(metadata, fn)
    return source_path, target_path, metadata_path, vtt_sub_path, f"{NISEMONO}{prefix}/{quote(target_basename)}.json"


def deluge_post(tid, tname, tpath):
    # change to deluge-console eventually
    torrent_dir = os.path.abspath(tpath)
    print(torrent_dir)
    if not STAGING_TORRENT_DIR in torrent_dir:
        return

    prefix = os.path.basename(torrent_dir.replace(STAGING_TORRENT_DIR, "").strip("/"))
    target_dir = os.path.join(TARGET_SERVING_DIR, prefix)
    for root, _, files in os.walk(torrent_dir):
        for filename in files:
            if not filename.endswith(MKV):
                continue
            process(root, target_dir, filename)

    # need to remove the torrent somehow


def local_process(tpath):
    import paramiko
    from scp import SCPClient

    with open("./scp_args", "r") as fn:
        scp_args = json.load(fn)

    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(scp_args["server"], scp_args["port"], scp_args["user"], scp_args["password"])
    scp = SCPClient(ssh.get_transport(), progress=scp_progress)

    uploaded = []
    prefix = os.path.basename(tpath.strip("/"))
    for filename in sorted(os.listdir(tpath)):
        if not filename.endswith(MKV) and not filename.endswith(".x265-RARBG" + MP4):
            continue
        result = process(tpath, tpath, filename)
        if not result:
            continue
        _, target_path, metadata_path, vtt_sub_path, url = result
        scp.put(target_path, remote_path=f"/var/www/uploads/{prefix}/")
        scp.put(metadata_path, remote_path=f"/var/www/uploads/{prefix}/")
        if vtt_sub_path:
            scp.put(vtt_sub_path, remote_path=f"/var/www/uploads/{prefix}/")
        uploaded.append(url)
        if prefix == "seasonal":
            shutil.move(os.path.join(tpath, filename), os.path.join(tpath, "done"))

    print()
    print(",".join(uploaded))


if __name__ == "__main__":
    if len(sys.argv) <= 2:
        try:
            tpath = sys.argv[1]
        except IndexError:
            tpath = "./"
        local_process(tpath)
    else:
        deluge_post(sys.argv[1], sys.argv[2], sys.argv[3])
