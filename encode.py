#!/usr/bin/env python3
import hashlib
import shutil
import sys
import os
import glob
import base64
import subprocess
import json
from urllib import request, parse
from urllib.error import HTTPError
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
JSON = ".json"


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
    target_basename = str(basename)
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
                subprocess.run(
                    ["ffmpeg", "-i", os.path.join(target_dir, file_path), vtt_sub_path]
                )
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
                        if img_sub_idx is None or (
                            sub_tracks[img_sub_idx].get(SUB_EVAL_KEY, 0)
                            < data.get(SUB_EVAL_KEY, 0)
                        ):
                            img_sub_idx = idx
                    elif sub_idx is None or (
                        sub_tracks[sub_idx].get(SUB_EVAL_KEY, 0)
                        < data.get(SUB_EVAL_KEY, 0)
                    ):
                        sub_idx = idx
                if sub_idx is None:
                    sub_idx = img_sub_idx or 0
                if sub_idx is not None:
                    sub = sub_tracks[sub_idx]
                    ffmpeg_call.append("-filter_complex")
                    # dum
                    escaped_source = source_path.replace("\\", "\\\\\\").replace(
                        ":", "\:"
                    )
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

    if not vtt_subs:
        vtt_sub_path = None

    return (target_path, vtt_sub_path)


def write_metadata(target_path, target_url, vtt_sub_path, vtt_sub_url):
    target_basename, ext = os.path.splitext(os.path.basename(target_path))
    target_dir = os.path.dirname(target_path)
    duration = ffprobe_duration(target_path)
    if duration is None:
        return False
    metadata = {
        "title": target_basename,
        "duration": duration,
        "live": False,
        "sources": [
            {
                "url": target_url,
                "contentType": f"video/{ext[1:]}",
                "quality": 1080,
            }
        ],
    }
    if vtt_sub_path:
        metadata["textTracks"] = [
            {
                "url": vtt_sub_url,
                "contentType": "text/vtt",
                "name": "English",
                "default": True,
            }
        ]

    metadata_path = os.path.join(target_dir, f"{target_basename}{JSON}")
    with open(metadata_path, "w") as fn:
        json.dump(metadata, fn)
    return metadata_path


class SCPUploader:
    def __init__(self) -> None:
        import paramiko
        from scp import SCPClient

        with open("./scp_args", "r") as fn:
            scp_args = json.load(fn)

        self._ssh = paramiko.SSHClient()
        self._ssh.load_system_host_keys()
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._ssh.connect(
            scp_args["server"], scp_args["port"], scp_args["user"], scp_args["password"]
        )
        self._scp = SCPClient(self._ssh.get_transport(), progress=scp_progress)

    def put(self, target_path, vtt_sub_path, prefix) -> None:
        remote_path = f"/var/www/uploads/{prefix}/"
        self._scp.put(target_path, remote_path=remote_path)
        target_url = f"{NISEMONO}{prefix}/{parse.quote(os.path.basename(target_path))}"
        vtt_sub_url = None
        if vtt_sub_path:
            self._scp.put(vtt_sub_path, remote_path=remote_path)
            vtt_sub_url = (
                f"{NISEMONO}{prefix}/{parse.quote(os.path.basename(vtt_sub_path))}"
            )
        metadata_path = write_metadata(
            target_path, target_url, vtt_sub_path, vtt_sub_url
        )
        self._scp.put(metadata_path, remote_path=remote_path)
        metadata_url = (
            f"{NISEMONO}{prefix}/{parse.quote(os.path.basename(metadata_path))}"
        )
        return metadata_url


class BackblazeUploader:
    @staticmethod
    def _send_req(req):
        try:
            with request.urlopen(req) as response:
                return json.loads(response.read())
        except HTTPError as err:
            pprint(json.loads(err.read()))
            raise err

    def _send_api_req(self, api_name, data=None):
        if data:
            data = json.dumps(data).encode("utf8")
        else:
            data = None
        req = request.Request(
            f"{self.api_url}/b2api/v2/{api_name}",
            data=data,
            headers={"Authorization": self.auth_token},
        )
        return self._send_req(req)

    def __init__(self) -> None:
        # https://api.backblazeb2.com/b2api/v3/b2_authorize_account
        with open("./backblaze_args", "r") as fn:
            backblaze_args = json.load(fn)
        req = request.Request(
            "https://api.backblazeb2.com/b2api/v3/b2_authorize_account"
        )
        authorization = f"{backblaze_args['keyID']}:{backblaze_args['key']}"
        authorization = base64.b64encode(authorization.encode("ascii")).decode("ascii")
        req.add_header("Authorization", f"Basic{authorization}")
        auth_info = self._send_req(req)
        storage_api = auth_info["apiInfo"]["storageApi"]
        self.api_url = storage_api["apiUrl"]
        self.bucket_name = storage_api["bucketName"]
        self.bucket_id = storage_api["bucketId"]
        self.min_part_size = storage_api["absoluteMinimumPartSize"]
        self.rec_part_size = storage_api["recommendedPartSize"]
        self.auth_token = auth_info["authorizationToken"]
        # /b2api/v2/b2_get_upload_url (for small files)
        upload_info = self._send_api_req(f"b2_get_upload_url?bucketId={self.bucket_id}")
        self.upload_url = upload_info["uploadUrl"]
        self.upload_token = upload_info["authorizationToken"]

    def _upload(self, path, prefix):
        if os.stat(path).st_size > self.min_part_size:
            return self._upload_large_file(path, prefix)
        else:
            return self._upload_small_file(path, prefix)

    @staticmethod
    def _content_type(ext):
        if ext == JSON:
            return "application/json"
        elif ext == VTT:
            return "text/vtt"
        else:
            return f"video/{ext[1:]}"

    @staticmethod
    def _filename(prefix, basename, ext):
        return f"{prefix}/{basename}{ext}"

    def _upload_small_file(self, path, prefix):
        with open(path, "rb", buffering=0) as fn:
            file_data = fn.read()
            fn.seek(0)
            file_sha1 = hashlib.file_digest(fn, hashlib.sha1).hexdigest()
        req = request.Request(self.upload_url, data=file_data)
        basename, ext = os.path.splitext(os.path.basename(path))
        filename = self._filename(prefix, basename, ext)
        print(f"Upload {filename}")
        req.add_header("Authorization", self.upload_token)
        req.add_header("Content-Type", self._content_type(ext))
        quoted_filename = parse.quote(filename, safe="/")
        req.add_header("X-Bz-File-Name", quoted_filename.encode("utf8"))
        req.add_header("Content-Length", os.stat(path).st_size)
        req.add_header("X-Bz-Content-Sha1", file_sha1)
        _upload_result = self._send_req(req)
        return f"{self.api_url}/file/aminoacids/{quoted_filename}"

    def _upload_large_file(self, path, prefix):
        basename, ext = os.path.splitext(os.path.basename(path))
        filename = self._filename(prefix, basename, ext)
        # b2_list_file_names
        filelist = self._send_api_req(
            f"b2_list_file_names?bucketId={self.bucket_id}&startFileName={parse.quote(filename)}"
        )
        if filelist["files"]:
            return f"{self.api_url}/b2api/v1/b2_download_file_by_id?fileId={filelist['files'][0]['fileId']}"
        # b2_start_large_file
        start_info = self._send_api_req(
            "b2_start_large_file",
            {
                "bucketId": self.bucket_id,
                "fileName": filename,
                "contentType": self._content_type(ext),
            },
        )
        print(f"Upload {filename}")
        file_id = start_info["fileId"]
        try:
            # b2_get_upload_part_url (for each thread that are are uploading)
            upload_part_url = self._send_api_req(
                f"b2_get_upload_part_url?fileId={file_id}",
            )
            upload_url = upload_part_url["uploadUrl"]
            upload_token = upload_part_url["authorizationToken"]
            # b2_upload_part or b2_copy_part (for each part of the file)
            part_number = 1
            part_count = os.stat(path).st_size // self.rec_part_size + 1
            all_sha1 = []
            with open(path, "rb", buffering=0) as fn:
                while True:
                    chunk = fn.read(self.rec_part_size)
                    if not chunk:
                        break
                    print(f"Part {part_number}/{part_count}")
                    req = request.Request(upload_url, data=chunk)
                    req.add_header("Authorization", upload_token)
                    req.add_header("Content-Length", len(chunk))
                    req.add_header("X-Bz-Part-Number", part_number)
                    chunk_sha1 = hashlib.sha1(chunk).hexdigest()
                    all_sha1.append(chunk_sha1)
                    req.add_header("X-Bz-Content-Sha1", chunk_sha1)
                    self._send_req(req)
                    part_number += 1
            # b2_finish_large_file
            finish_info = self._send_api_req(
                "b2_finish_large_file",
                {"fileId": file_id, "partSha1Array": all_sha1},
            )
            return f"{self.api_url}/b2api/v1/b2_download_file_by_id?fileId={finish_info['fileId']}"
        except Exception as err:
            # b2_cancel_large_file
            cancel_info = self._send_api_req(
                "b2_cancel_large_file", {"fileId": file_id}
            )
            print(f"Canceled {cancel_info['fileName']}")
            raise err

    def put(self, target_path, vtt_sub_path, prefix):
        target_url = self._upload(target_path, prefix)
        vtt_sub_url = None
        if vtt_sub_path:
            vtt_sub_url = self._upload(vtt_sub_path, prefix)
        metadata_path = write_metadata(
            target_path, target_url, vtt_sub_path, vtt_sub_url
        )
        return self._upload(metadata_path, prefix)


def local_process(tpath, opath):
    uploaded = []
    prefix = os.path.basename(tpath.strip("/"))

    uploader = SCPUploader()
    # uploader = BackblazeUploader()

    for filename in sorted(os.listdir(tpath)):
        if not filename.endswith(MKV) and not filename.endswith(MP4):
            continue
        result = process(tpath, opath, filename)
        if not result:
            continue
        target_path, vtt_sub_path = result
        url = uploader.put(target_path, vtt_sub_path, prefix)
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
        opath = os.path.join(tpath, ".out")
        local_process(tpath, opath)
    # uploader = BackblazeUploader()
    # uploader._upload(
    #     "/home/michelle/Downloads/avscripts/testup/[赤ずきんチャチャ][24][私が伝説の王女様？].json",
    #     "testup",
    # )
