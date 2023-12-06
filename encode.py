#!/usr/bin/env python3
import os
import sys
import time
import json
import base64
import shutil
import hashlib
import argparse
import subprocess
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
    ffmpeg_call = [
        "nice",
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-stats",
        "-y",
        "-i",
        source_path,
    ]
    if ext == MP4:
        ffmpeg_call.extend(
            [
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
        )
    elif ext == WEBM:
        # bad settings, need more fiddling
        ffmpeg_call.extend(
            [
                "-deadline",
                "realtime",
                "-cpu-used",
                "4",
                "-crf",
                "30",
                "-c:v",
                "libvpx-vp9",
                "-c:a",
                "libvorbis",
            ]
        )
    return ffmpeg_call


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


def get_audio_track(source_path):
    audio_tracks = ffprobe_streams(source_path, "a")
    audio_idx = None
    default_idx = 0
    for idx, data in enumerate(audio_tracks):
        if data.get(DISPOSITION_DEFAULT):
            default_idx = idx
        if data.get(TAG_LANGUAGE) == "jpn" and audio_idx is None:
            audio_idx = idx
    if audio_idx not in (default_idx, None):
        return ["-map", f"{default_idx}:a:{audio_idx}"]
    return tuple()


def get_subtitle_track(source_path, ass_subs, vtt_subs):
    # check which sub track to use
    if ass_subs:
        escaped_ass = ass_subs.replace("\\", "\\\\\\").replace(":", "\:")
        return ["-filter_complex", f"subtiltes='{escaped_ass}'"]
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
                    sub_tracks[sub_idx].get(SUB_EVAL_KEY, 0) < data.get(SUB_EVAL_KEY, 0)
                ):
                    sub_idx = idx
            if sub_idx is None:
                sub_idx = img_sub_idx or 0
            if sub_idx is not None:
                sub = sub_tracks[sub_idx]
                ffmpeg_args = ["-filter_complex"]
                # dum
                escaped_source = source_path.replace("\\", "\\\\\\").replace(":", "\:")
                if sub.get(CODEC_NAME) in IMAGE_BASED_SUBS:
                    # bitmap subs from bd/dvd
                    # overlay=x=-240:y=0 to adjust positions when needed
                    ffmpeg_args.append(f"[0:v][0:s:{sub_idx}]overlay")
                elif sub.get("DISPOSITION:default") or len(sub_tracks) == 1:
                    # already default sub track
                    ffmpeg_args.append(f"subtitles='{escaped_source}'")
                else:
                    # remap subtitle
                    ffmpeg_args.append(f"subtitles='{escaped_source}:si={sub_idx}'")
                return ffmpeg_args
    return []


def process(source_dir, target_dir, filename, ext=MP4):
    print(f"process({source_dir}/{filename})", flush=True)
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
        ffmpeg_call.extend(get_audio_track(source_path))
        ffmpeg_call.extend(get_subtitle_track(source_path, ass_subs, vtt_subs))
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
    """Useful for querying, but slower than b2 cli for uploads since I didnt thread this"""

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
        self.count = 0
        # b2 auth
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

    def _filename(self, prefix, _basename, ext):
        return f"{prefix}/{ext[1]}/{self.count:02}{ext}"

    def _fileurl(self, filename):
        return (
            f"{self.api_url}/file/{self.bucket_name}/{parse.quote(filename, safe='/')}"
        )

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
        req.add_header("X-Bz-File-Name", parse.quote(filename, safe="/").encode("utf8"))
        req.add_header("Content-Length", os.stat(path).st_size)
        req.add_header("X-Bz-Content-Sha1", file_sha1)
        upload_result = self._send_req(req)
        return self._fileurl(upload_result["fileName"])

    def _upload_large_file(self, path, prefix):
        basename, ext = os.path.splitext(os.path.basename(path))
        filename = self._filename(prefix, basename, ext)
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
        part_number = 0
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
            print("Part:", end="")
            with open(path, "rb", buffering=0) as fn:
                while True:
                    chunk = fn.read(self.rec_part_size)
                    if not chunk:
                        break
                    print(f" {part_number}/{part_count}", end="", flush=True)
                    req = request.Request(upload_url, data=chunk)
                    req.add_header("Authorization", upload_token)
                    req.add_header("Content-Length", len(chunk))
                    req.add_header("X-Bz-Part-Number", part_number)
                    chunk_sha1 = hashlib.sha1(chunk).hexdigest()
                    all_sha1.append(chunk_sha1)
                    req.add_header("X-Bz-Content-Sha1", chunk_sha1)
                    self._send_req(req)
                    part_number += 1
                    time.sleep(1)
            # b2_finish_large_file
            upload_result = self._send_api_req(
                "b2_finish_large_file",
                {"fileId": file_id, "partSha1Array": all_sha1},
            )
            print()
            return self._fileurl(upload_result["fileName"])
        except Exception as err:
            # b2_cancel_large_file
            cancel_info = self._send_api_req(
                "b2_cancel_large_file", {"fileId": file_id}
            )
            print(f"\nCanceled {cancel_info['fileName']} after part {part_number}")
            raise err

    def put(self, target_path, vtt_sub_path, prefix):
        self.count += 1
        target_url = self._upload(target_path, prefix)
        vtt_sub_url = None
        if vtt_sub_path:
            vtt_sub_url = self._upload(vtt_sub_path, prefix)
        metadata_path = write_metadata(
            target_path, target_url, vtt_sub_path, vtt_sub_url
        )
        return self._upload(metadata_path, prefix)

    def print_urls(self, prefix):
        # b2_list_file_names
        result = self._send_api_req(
            f"b2_list_file_names?bucketId={self.bucket_id}&prefix={prefix}/j/"
        )
        print(
            ",".join(
                self._fileurl(fileinfo["fileName"])
                for fileinfo in result.get("files", tuple())
            )
        )

    def remove_files(self, prefix):
        # b2_list_file_names
        result = self._send_api_req(
            f"b2_list_file_names?bucketId={self.bucket_id}&prefix={prefix}/"
        )
        # b2_delete_file_version
        for fileinfo in result.get("files", tuple()):
            print(f"rm {fileinfo['fileName']}")
            self._send_api_req(
                "b2_delete_file_version",
                {
                    "fileName": fileinfo["fileName"],
                    "fileId": fileinfo["fileId"],
                },
            )


class B2Uploader:
    """Upload with b2 upload-file cli"""

    def __init__(self) -> None:
        self.count = 0
        # b2 auth
        with open("./backblaze_args", "r") as fn:
            backblaze_args = json.load(fn)
        self.b2_env = {
            "B2_APPLICATION_KEY_ID": backblaze_args["keyID"],
            "B2_APPLICATION_KEY": backblaze_args["key"],
        }
        self.api_url = backblaze_args["apiUrl"]
        self.bucket_name = backblaze_args["bucketName"]

    @staticmethod
    def _content_type(ext):
        if ext == JSON:
            return "application/json"
        elif ext == VTT:
            return "text/vtt"
        else:
            return f"video/{ext[1:]}"

    def _filename(self, prefix, _basename, ext):
        return f"{prefix}/{ext[1]}/{self.count:02}{ext}"

    def _fileurl(self, filename):
        return (
            f"{self.api_url}/file/{self.bucket_name}/{parse.quote(filename, safe='/')}"
        )

    def _upload(self, path, prefix):
        basename, ext = os.path.splitext(os.path.basename(path))
        filename = self._filename(prefix, basename, ext)
        content_type = self._content_type(ext)
        subprocess.check_call(
            [
                "./b2",
                "upload-file",
                "--contentType",
                content_type,
                self.bucket_name,
                path,
                filename,
            ],
            env=self.b2_env,
        )
        print(f"Upload {filename}")
        return self._fileurl(filename)

    def put(self, target_path, vtt_sub_path, prefix):
        self.count += 1
        target_url = self._upload(target_path, prefix)
        vtt_sub_url = None
        if vtt_sub_path:
            vtt_sub_url = self._upload(vtt_sub_path, prefix)
        metadata_path = write_metadata(
            target_path, target_url, vtt_sub_path, vtt_sub_url
        )
        return self._upload(metadata_path, prefix)


class B2SyncUploader(B2Uploader):
    """Upload with b2 sync cli"""

    STG = ".stg"

    def __init__(self) -> None:
        super().__init__()
        self.prefixes = set()
        try:
            shutil.rmtree(self.STG)
        except FileNotFoundError:
            pass

    def _upload(self, path, prefix):
        basename, ext = os.path.splitext(os.path.basename(path))
        filename = self._filename(prefix, basename, ext)
        staging = os.path.join(self.STG, filename)
        os.makedirs(os.path.dirname(staging), exist_ok=True)
        os.link(path, staging)
        self.prefixes.add(prefix)
        return self._fileurl(filename)

    def finalize(self):
        for prefix in self.prefixes:
            subprocess.check_call(
                [
                    "./b2",
                    "sync",
                    "--delete",
                    "--replaceNewer",
                    f"{self.STG}/{prefix}",
                    f"b2://{self.bucket_name}/{prefix}",
                ],
                env=self.b2_env,
            )


class DebugUploader:
    def put(self, target_path, vtt_sub_path, prefix):
        print(f"DEBUG: {target_path!r} {vtt_sub_path!r} {prefix!r}")
        metadata_path = write_metadata(
            target_path, target_path, vtt_sub_path, vtt_sub_path
        )
        return metadata_path


def local_process(ipath, upt, opath, ext):
    if not opath:
        opath = os.path.join(ipath, ".out")
    uploaded = []
    prefix = os.path.basename(ipath.strip("/"))

    if upt == "scp":
        uploader = SCPUploader()
    elif upt == "b2":
        uploader = B2SyncUploader()
    else:
        uploader = DebugUploader()

    for filename in sorted(os.listdir(ipath)):
        if not filename.endswith(MKV) and not filename.endswith(MP4):
            continue
        result = process(ipath, opath, filename, ext=ext)
        if not result:
            continue
        target_path, vtt_sub_path = result
        url = uploader.put(target_path, vtt_sub_path, prefix)
        uploaded.append(url)

    try:
        uploader.finalize()
    except AttributeError:
        pass

    print()
    print(",".join(uploaded))


def b2_opt(ipath, opt):
    uploader = BackblazeUploader()
    prefix = os.path.basename(ipath.strip("/"))
    if opt == "ls":
        uploader.print_urls(prefix)
    elif opt == "rm":
        uploader.remove_files(prefix)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("ipath", help="directory to encode")
    parser.add_argument(
        "-upt", default="b2", choices=["scp", "b2", "debug"], help="upload target"
    )
    parser.add_argument("-opath", default=None, help="output path, default ipath/.out")
    parser.add_argument(
        "-ext", default=MP4, choices=[MP4, WEBM], help="target format, default .mp4"
    )
    parser.add_argument(
        "-opt",
        default="up",
        choices=["up", "ls", "rm"],
        help="up: upload, ls: print json links, rm: remove",
    )
    args = parser.parse_args()

    if args.upt == "b2" and args.opt != "up":
        b2_opt(args.ipath, args.opt)
        exit()

    local_process(args.ipath, args.upt, args.opath, args.ext)
