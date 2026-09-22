import os
import re
import json
import time
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import requests
import subprocess
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
import firebase_admin
from firebase_admin import credentials, db as firebase_db

# ================= ENVS & CONFIGS =================
API_ID = int(os.environ.get("TG_API_ID", 0))
API_HASH = os.environ.get("TG_API_HASH", "")
BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")

GITHUB_REPOSITORY = os.environ.get("GITHUB_REPOSITORY", "")  # "username/repo-name"
GH_TOKEN = os.environ.get("GH_TOKEN", "")
RELEASE_TAG = "episodes-store"

GITHUB_API = "https://api.github.com"
GH_HEADERS = {
    "Authorization": f"Bearer {GH_TOKEN}",
    "Accept": "application/vnd.github+json",
}

WORKER_BASE_URL = os.environ.get("WORKER_BASE_URL", "https://streamer.ayushkum099766.workers.dev")
FIREBASE_DATABASE_URL = os.environ.get("FIREBASE_DATABASE_URL", "https://animeverse-9eada-default-rtdb.firebaseio.com/")

# Shown in every status message's footer/header — change this (or set the
# BOT_BRAND_NAME env var / repo secret) to your own channel/bot name.
BOT_BRAND_NAME = os.environ.get("BOT_BRAND_NAME", "Ꭺɴɪᴍᴇ Ζᴏɴᴇ Bot")

# How many qualities can remux/encode at the same time. GitHub-hosted standard
# runners are 2-core, so 2 is a safe default (matches core count, leaves the
# 3rd quality's encode queued rather than fighting for CPU with the other two).
# Bump this via env var / repo secret if you're on a bigger (4-core+) or
# self-hosted runner.
MAX_PARALLEL_ENCODES = int(os.environ.get("MAX_PARALLEL_ENCODES", 2))

# How many simultaneous MTProto connections Pyrogram uses for a single large
# file transfer (download or upload). 1 = one connection (slow). Higher values
# split the transfer across multiple connections to Telegram's servers, which
# is usually the single biggest lever for Telegram download/upload speed.
TG_MAX_CONCURRENT_TRANSMISSIONS = int(os.environ.get("TG_MAX_CONCURRENT_TRANSMISSIONS", 4))

# How many byte-range connections to open per direct-link (URL) download, when
# the source server supports Range requests. Splits one file into N pieces
# downloaded in parallel instead of one slow sequential stream.
URL_DOWNLOAD_CONNECTIONS = int(os.environ.get("URL_DOWNLOAD_CONNECTIONS", 4))

# ================= FIREBASE INITIALIZATION =================
if not firebase_admin._apps:
    firebase_creds_raw = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
    if firebase_creds_raw:
        try:
            firebase_creds_dict = json.loads(firebase_creds_raw)
            cred = credentials.Certificate(firebase_creds_dict)
            firebase_admin.initialize_app(cred, {"databaseURL": FIREBASE_DATABASE_URL})
        except Exception as e:
            print(f"Firebase Init Error: {e}")

# Global State Context & Locks
current_context = {"slug": None, "season": None}
pending_episodes = {}

# Queue Lock: Prevents disk runner crash by running 1 episode process at a time
PROCESSING_SEMAPHORE = asyncio.Semaphore(1)
STATE_LOCK = asyncio.Lock()

# Caps how many qualities remux/encode concurrently (see MAX_PARALLEL_ENCODES
# above) — uploads are NOT limited by this, only the CPU-heavy ffmpeg step.
ENCODE_SEMAPHORE = asyncio.Semaphore(MAX_PARALLEL_ENCODES)


# ================= RELEASE BUCKET STATE =================
# GitHub hard-limits a single Release to 1000 assets. Once a bucket (release)
# fills up, the bot auto-rolls forward to RELEASE_TAG-2, RELEASE_TAG-3, etc.
# The current bucket index is cached in Firebase so a fresh GitHub Actions run
# doesn't have to "re-discover" it by hitting a 422 first.
def load_current_release_index() -> int:
    try:
        val = firebase_db.reference("bot_state/current_release_index").get()
        if isinstance(val, int) and val >= 1:
            return val
    except Exception as e:
        print(f"Could not load release bucket index from Firebase (defaulting to 1): {e}")
    return 1


def save_current_release_index(index: int):
    try:
        firebase_db.reference("bot_state/current_release_index").set(index)
    except Exception as e:
        print(f"Could not save release bucket index to Firebase: {e}")


current_release_index = load_current_release_index()


def release_tag_for_index(i: int) -> str:
    return RELEASE_TAG if i <= 1 else f"{RELEASE_TAG}-{i}"


# ================= HELPER FUNCTIONS =================
def human_size(num_bytes):
    for unit in ["B", "KB", "MB", "GB"]:
        if num_bytes < 1024:
            return f"{num_bytes:.1f}{unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f}TB"


def human_time(seconds) -> str:
    seconds = int(max(0, seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def make_bar(percent, length=20):
    filled = int(length * percent // 100)
    return "▓" * filled + "░" * (length - filled)


class ProgressTracker:
    def __init__(self, status_message, label, loop):
        self.status_message = status_message
        self.label = label
        self.loop = loop
        self.start_time = time.time()
        self.last_time = 0
        self.last_percent = -1

    def update(self, current, total):
        if not total or total <= 0:
            return
        percent = int(current * 100 / total)
        now = time.time()
        is_final = percent >= 100

        if not is_final and (percent == self.last_percent or (now - self.last_time) < 3):
            return

        self.last_time = now
        self.last_percent = percent
        bar = make_bar(percent)
        elapsed = now - self.start_time
        speed = current / elapsed if elapsed > 0 else 0

        if is_final:
            header = f"✅ {self.label} — Done"
            lines = [
                header,
                "",
                f"╭ Size » {human_size(total)}",
                f"╰ Time Taken » {human_time(elapsed)}",
            ]
        else:
            eta = (total - current) / speed if speed > 0 else 0
            lines = [
                self.label,
                "",
                f"╭ Progress » `[{bar}] {percent}%`",
                f"├ Done » {human_size(current)} / {human_size(total)}",
                f"├ Speed » {human_size(speed)}/s",
                f"╰ ETA » {human_time(eta)}",
            ]

        text = "\n".join(lines)
        try:
            asyncio.run_coroutine_threadsafe(
                self.status_message.edit_text(text), self.loop
            )
        except Exception:
            pass


def extract_url(text: str):
    if not text:
        return None
    match = re.search(r'(https?://[^\s]+)', text)
    if match:
        return match.group(1)
    return None


def extract_episode_number(text: str, url: str = None):
    targets_to_search = []
    
    if text:
        targets_to_search.append(text)
    
    if url:
        clean_url = url.split("?")[0]
        filename = clean_url.split("/")[-1]
        targets_to_search.append(filename)
        targets_to_search.append(url)

    for target in targets_to_search:
        match = re.search(r'(?:ep|episode|e|s\d+e)[-_\s:]?(\d{1,4})', target, re.IGNORECASE)
        if match:
            return int(match.group(1))

        if target.strip().isdigit():
            return int(target.strip())

    return None


# ================= MEDIA INFO / REMUX TO STREAMING-SAFE MP4 =================
async def probe_media_info(input_path: str):
    """
    One ffprobe call that returns (container, video_codec, audio_codec) for a
    file, regardless of what its filename/extension claims. This is the source
    of truth used both for the "file info" message shown to the user and for
    deciding whether remux needs to fix anything.
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error",
            "-show_entries", "format=format_name:stream=index,codec_type,codec_name",
            "-of", "json",
            input_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
        data = json.loads(stdout.decode() or "{}")

        container = (data.get("format") or {}).get("format_name") or None
        video_codec = None
        audio_codec = None
        for stream in data.get("streams", []):
            codec_type = stream.get("codec_type")
            if codec_type == "video" and video_codec is None:
                video_codec = stream.get("codec_name")
            elif codec_type == "audio" and audio_codec is None:
                audio_codec = stream.get("codec_name")

        return (
            container.lower() if container else None,
            video_codec.lower() if video_codec else None,
            audio_codec.lower() if audio_codec else None,
        )
    except Exception as e:
        print(f"ffprobe media info failed for {input_path}: {e}")
        return None, None, None


_CONTAINER_NAMES = {
    "matroska": "MKV (Matroska)",
    "webm": "WebM",
    "mov,mp4,m4a,3gp,3g2,mj2": "MP4",
    "avi": "AVI",
    "flv": "FLV",
    "mpegts": "MPEG-TS",
}
_VIDEO_CODEC_NAMES = {
    "h264": "H.264 (AVC)", "hevc": "HEVC (H.265)", "vp9": "VP9",
    "av1": "AV1", "mpeg4": "MPEG-4", "mpeg2video": "MPEG-2",
}
_AUDIO_CODEC_NAMES = {
    "aac": "AAC", "ac3": "AC3", "eac3": "E-AC3", "dts": "DTS",
    "mp3": "MP3", "opus": "Opus", "flac": "FLAC", "vorbis": "Vorbis",
    "pcm_s16le": "PCM",
}


def friendly_container(raw: str) -> str:
    if not raw:
        return "Unknown"
    for key, label in _CONTAINER_NAMES.items():
        if key in raw:
            return label
    return raw


def friendly_codec(raw: str, table: dict) -> str:
    if not raw:
        return "None"
    return table.get(raw, raw.upper())


async def remux_to_streaming_mp4(input_path: str, video_codec: str = None, audio_codec: str = None) -> str:
    """
    Guarantees the file we upload is a genuine, browser-friendly MP4:
      - Any non-mp4 container (MKV, AVI, MOV, etc.) gets remuxed to real MP4 —
        just renaming an .mkv to .mp4 does NOT make it playable in Chrome,
        since the browser reads the actual container, not the file extension.
      - Audio is transcoded to AAC only if it isn't already AAC. Chrome's
        <video> tag cannot decode AC3/DTS (common in smaller/"compressed"
        anime releases), but if the audio is already AAC, re-encoding it is
        pure wasted time, so it's stream-copied instead.
      - Video codec is checked first: if it's already H.264, it's kept as a
        fast stream-copy (no quality loss, seconds to process). If it's
        something Chrome can't play at all — most commonly HEVC/H.265, which
        smaller "compressed"/lower-quality releases often use to save space —
        it's re-encoded to H.264. This is why such files play fine in
        MX Player/VLC (which decode HEVC themselves) but fail completely in
        a browser. Re-encoding takes noticeably longer than a plain copy
        (minutes, not seconds), but only kicks in when actually needed.
      - Even an already-mp4/H.264 source gets `-movflags +faststart` applied,
        moving the metadata (moov atom) to the front — Chrome's <video> tag
        needs this to reliably start playback/seeking.
    video_codec/audio_codec can be passed in (from an earlier probe_media_info
    call) to skip re-probing the file; if omitted, they're probed here.
    If ffmpeg isn't available or every attempt fails, the original file is
    returned unchanged as a best-effort fallback.
    """
    output_path = input_path.rsplit(".", 1)[0] + "_remuxed.mp4"
    if os.path.exists(output_path):
        os.remove(output_path)

    if video_codec is None and audio_codec is None:
        _, video_codec, audio_codec = await probe_media_info(input_path)

    needs_video_reencode = video_codec not in (None, "h264", "avc1")
    if needs_video_reencode:
        print(f"Video codec `{video_codec}` for {input_path} isn't browser-safe (likely HEVC) — re-encoding to H.264.")
        video_args = ["-c:v", "libx264", "-preset", "ultrafast", "-crf", "23", "-threads", "0"]
    else:
        video_args = ["-c:v", "copy"]

    # Audio transcode is cheap — audio data is tiny next to video, so even a
    # full-episode AAC encode is a matter of seconds, not minutes. So it's
    # worth always guaranteeing browser-safe audio: copy if already AAC
    # (instant), transcode to AAC otherwise (fast, and fixes AC3/DTS sources
    # that would otherwise play silently or fail in Chrome).
    needs_audio_reencode = audio_codec not in ("aac",)
    if needs_audio_reencode:
        audio_args = ["-c:a", "aac", "-b:a", "192k"]
    else:
        audio_args = ["-c:a", "copy"]

    attempts = [
        # 1) Fix video/audio codecs only if needed (else fast copy both).
        ["ffmpeg", "-y", "-i", input_path, "-map", "0:v:0", "-map", "0:a:0?",
         *video_args, *audio_args,
         "-movflags", "+faststart", output_path],
        # 2) Fallback: force AAC audio + drop anything odd (extra tracks,
        #    embedded subs/attachments) that might be tripping up attempt 1.
        ["ffmpeg", "-y", "-i", input_path, "-map", "0:v:0", "-map", "0:a:0?",
         *video_args, "-c:a", "aac", "-b:a", "128k", "-ac", "2",
         "-movflags", "+faststart", output_path],
        # 3) Last resort: pure container remux, no codec fixes at all — still
        #    better than uploading the raw file completely unprocessed.
        ["ffmpeg", "-y", "-i", input_path, "-map", "0", "-c", "copy",
         "-movflags", "+faststart", output_path],
    ]

    for cmd in attempts:
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()
            if proc.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                return output_path
            print(f"Remux attempt failed for {input_path}: {stderr.decode(errors='ignore')[-500:]}")
        except FileNotFoundError:
            print("ffmpeg not found on this runner — uploading file as-is (no remux).")
            return input_path
        except Exception as e:
            print(f"Remux attempt exception for {input_path}: {e}")

    print(f"All remux attempts failed for {input_path}; uploading original file as-is.")
    return input_path



def find_asset_by_name_in_release(release_id: int, filename: str, max_pages: int = 15):
    """Paginated search for an asset by name inside one release (a release can hold
    up to 1000 assets, but the API only returns ~30-100 per page by default)."""
    for page in range(1, max_pages + 1):
        url = f"{GITHUB_API}/repos/{GITHUB_REPOSITORY}/releases/{release_id}/assets"
        resp = requests.get(url, headers=GH_HEADERS, params={"per_page": 100, "page": page}, timeout=15)
        if resp.status_code != 200:
            break
        assets = resp.json()
        if not assets:
            break
        for asset in assets:
            if asset.get("name") == filename:
                return asset
        if len(assets) < 100:
            break
    return None


def delete_asset_by_id(asset_id: int):
    del_url = f"{GITHUB_API}/repos/{GITHUB_REPOSITORY}/releases/assets/{asset_id}"
    requests.delete(del_url, headers=GH_HEADERS, timeout=15)


def delete_existing_copies_everywhere(filename: str, max_buckets: int = 50):
    """
    Scans every release bucket (episodes-store, episodes-store-2, ...) in order
    for an asset with this exact filename and deletes it, so re-uploading the
    same episode/quality always overwrites cleanly — no matter which bucket the
    old copy ended up in — instead of GitHub rejecting the upload with a
    422 'already_exists' error.
    """
    for i in range(1, max_buckets + 1):
        tag = release_tag_for_index(i)
        url = f"{GITHUB_API}/repos/{GITHUB_REPOSITORY}/releases/tags/{tag}"
        resp = requests.get(url, headers=GH_HEADERS, timeout=15)
        if resp.status_code != 200:
            break  # buckets are created in order, so nothing further exists yet

        release = resp.json()
        asset = find_asset_by_name_in_release(release["id"], filename)
        if asset:
            delete_asset_by_id(asset["id"])
            print(f"Overwrite: deleted old copy of {filename} from bucket `{tag}`.")
            time.sleep(1)


def get_or_create_release(tag: str):
    url = f"{GITHUB_API}/repos/{GITHUB_REPOSITORY}/releases/tags/{tag}"
    resp = requests.get(url, headers=GH_HEADERS, timeout=15)

    if resp.status_code == 200:
        return resp.json()

    create_url = f"{GITHUB_API}/repos/{GITHUB_REPOSITORY}/releases"
    resp = requests.post(create_url, headers=GH_HEADERS, json={
        "tag_name": tag,
        "name": "Episodes Storage" if tag == RELEASE_TAG else f"Episodes Storage ({tag})",
        "body": "Automated media assets release bucket. A new bucket like this is auto-created "
                "whenever the previous one hits GitHub's 1000-assets-per-release limit.",
    }, timeout=15)
    
    if resp.status_code not in [200, 201]:
        raise Exception(f"GitHub Release Creation Failed (Status {resp.status_code}): {resp.text}")

    return resp.json()


async def upload_to_github_release_async(local_path: str, filename: str, tracker: ProgressTracker, max_retries=3) -> str:
    global current_release_index
    loop = asyncio.get_running_loop()

    # Guarantee overwrite semantics: wipe out any older copy of this exact
    # filename anywhere across all buckets before we even attempt the upload.
    await loop.run_in_executor(None, delete_existing_copies_everywhere, filename)

    last_error = "Unknown error"
    max_bucket_hops = 10  # safety cap so a misconfigured token etc. can't loop forever creating releases

    for hop in range(max_bucket_hops):
        tag = release_tag_for_index(current_release_index)
        release = await loop.run_in_executor(None, get_or_create_release, tag)

        upload_url_template = release["upload_url"]
        upload_url = upload_url_template.split("{")[0] + f"?name={filename}"

        response_file = f"/tmp/gh_upload_resp_{filename}.json"
        bucket_full = False

        for attempt in range(1, max_retries + 1):
            if os.path.exists(response_file):
                os.remove(response_file)

            # -o writes the JSON body to a file, -w prints ONLY the http status code to stdout.
            # This lets us actually verify GitHub accepted the upload, instead of trusting
            # curl's exit code (which is 0 even when GitHub returns a 4xx/5xx error).
            curl_command = [
                "curl",
                "-s",
                "-o", response_file,
                "-w", "%{http_code}",
                "--retry", "3",
                "--retry-delay", "2",
                "-X", "POST",
                "-H", f"Authorization: Bearer {GH_TOKEN}",
                "-H", "Content-Type: application/octet-stream",
                "-H", "Accept: application/vnd.github+json",
                "--upload-file", local_path,
                upload_url
            ]

            try:
                proc = await asyncio.create_subprocess_exec(
                    *curl_command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await proc.communicate()
                http_code = stdout.decode().strip()

                body = ""
                if os.path.exists(response_file):
                    with open(response_file, "r", errors="ignore") as f:
                        body = f.read()

                if proc.returncode == 0 and http_code == "201":
                    try:
                        if json.loads(body).get("id"):
                            if os.path.exists(response_file):
                                os.remove(response_file)
                            return filename  # upload confirmed; caller builds the watch/download URLs
                    except Exception:
                        pass

                # This specific bucket is full (GitHub's 1000-assets-per-release limit).
                # Stop retrying here — roll forward to the next bucket instead.
                if "file_count" in body:
                    bucket_full = True
                    last_error = f"Bucket `{tag}` is full (1000 assets) — rolling over to the next bucket."
                    print(last_error)
                    break

                # Rare race: name conflict slipped past our upfront cleanup
                # (e.g. another process uploaded it in between). Delete the
                # conflicting asset in THIS bucket and retry immediately.
                if "already_exists" in body:
                    print(f"Name conflict on `{filename}` in bucket `{tag}` — deleting and retrying.")
                    conflicting_asset = await loop.run_in_executor(
                        None, find_asset_by_name_in_release, release["id"], filename
                    )
                    if conflicting_asset:
                        await loop.run_in_executor(None, delete_asset_by_id, conflicting_asset["id"])
                        await asyncio.sleep(1)
                    last_error = f"Name conflict on `{filename}` — retried after deleting the old copy."
                    continue

                last_error = f"HTTP {http_code}: {body[:300] or stderr.decode().strip()}"
                print(f"cURL Attempt {attempt} Failed ({filename}, bucket {tag}): {last_error}")
            except Exception as e:
                last_error = str(e)
                print(f"cURL Exception Attempt {attempt}: {e}")
            finally:
                if os.path.exists(response_file):
                    os.remove(response_file)

            await asyncio.sleep(2 * attempt)

        if bucket_full:
            current_release_index += 1
            save_current_release_index(current_release_index)
            continue  # immediately try the next bucket

        # Genuine (non-capacity) failure after max_retries in this bucket
        raise RuntimeError(f"GitHub upload failed after {max_retries} attempts. Last error: {last_error}")

    raise RuntimeError(f"Exhausted {max_bucket_hops} release buckets while uploading. Last error: {last_error}")


def _download_single_stream(url: str, headers: dict, local_path: str, tracker: ProgressTracker, known_size: int = 0):
    """Fallback: plain sequential download, used when the server doesn't
    support Range requests (or reports no content-length)."""
    res = requests.get(url, headers=headers, stream=True, allow_redirects=True, timeout=120)
    res.raise_for_status()

    total_size = known_size or int(res.headers.get('content-length', 0))
    downloaded = 0

    with open(local_path, 'wb') as f:
        for chunk in res.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
                tracker.update(downloaded, total_size)


def download_from_url_sync(url: str, local_path: str, tracker: ProgressTracker, num_connections: int = None):
    """
    Downloads a direct link as fast as the source server allows:
      - If the server advertises Range-request support (most CDNs/direct video
        hosts do) and we know the file size, the file is split into N byte
        ranges and downloaded concurrently over N connections (classic
        download-accelerator technique) — often multiple times faster than one
        sequential stream, since a single HTTP connection is frequently the
        real bottleneck, not the runner's or the server's total bandwidth.
      - Otherwise, falls back to the original single-stream sequential
        download so this always works, just not always at max speed.
    """
    if num_connections is None:
        num_connections = URL_DOWNLOAD_CONNECTIONS

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "*/*"
    }

    total_size = 0
    accepts_ranges = False
    try:
        head = requests.head(url, headers=headers, allow_redirects=True, timeout=30)
        total_size = int(head.headers.get('content-length', 0))
        accepts_ranges = head.headers.get('accept-ranges', '').lower() == 'bytes'
    except Exception as e:
        print(f"HEAD probe failed for {url} (falling back to single-stream): {e}")

    if not accepts_ranges or total_size <= 0 or num_connections <= 1:
        _download_single_stream(url, headers, local_path, tracker, total_size)
        return

    part_size = total_size // num_connections
    ranges = []
    for i in range(num_connections):
        start = i * part_size
        end = (total_size - 1) if i == num_connections - 1 else (start + part_size - 1)
        ranges.append((start, end))

    # Pre-allocate the output file so each thread can seek + write its own
    # non-overlapping slice independently.
    with open(local_path, 'wb') as f:
        f.truncate(total_size)

    progress_lock = threading.Lock()
    downloaded_total = 0

    def fetch_range(start, end):
        nonlocal downloaded_total
        range_headers = dict(headers)
        range_headers["Range"] = f"bytes={start}-{end}"
        r = requests.get(url, headers=range_headers, stream=True, timeout=120)
        r.raise_for_status()
        with open(local_path, 'r+b') as f:
            f.seek(start)
            for chunk in r.iter_content(chunk_size=256 * 1024):
                if chunk:
                    f.write(chunk)
                    with progress_lock:
                        downloaded_total += len(chunk)
                        tracker.update(downloaded_total, total_size)

    try:
        with ThreadPoolExecutor(max_workers=num_connections) as executor:
            futures = [executor.submit(fetch_range, s, e) for s, e in ranges]
            for fut in futures:
                fut.result()  # re-raises if any range download failed
    except Exception as e:
        print(f"Multi-connection download failed for {url}, retrying single-stream: {e}")
        _download_single_stream(url, headers, local_path, tracker, total_size)


# ================= TELEGRAM BOT HANDLERS =================
app = Client(
    "gh_actions_uploader_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True,
    max_concurrent_transmissions=TG_MAX_CONCURRENT_TRANSMISSIONS,
)


@app.on_message(filters.command("setup") & filters.private)
async def handle_setup(client, message):
    args = message.command[1:]
    if len(args) < 2:
        await message.reply_text(
            "⚠️ **Usage:** `/setup <anime-slug> <season-number>`\n"
            "Example: `/setup grand-blue-dreaming 3`"
        )
        return

    slug = args[0].lower()
    try:
        season = int(args[1])
    except ValueError:
        await message.reply_text("❌ Season number integer hona chahiye!")
        return

    async with STATE_LOCK:
        current_context["slug"] = slug
        current_context["season"] = season

    await message.reply_text(
        f"✅ **Setup Successful!**\n\n"
        f"🎬 **Anime:** `{slug}`\n"
        f"📌 **Season:** `{season}`\n\n"
        f"Ab is episode ke **3 videos** ya **3 download links** bhej/forward karein.\n"
        f"Bot link ya caption se automatic episode number detect kar lega!"
    )


async def process_episode_batch(client, message, key, items):
    async with PROCESSING_SEMAPHORE:
        loop = asyncio.get_running_loop()
        slug, season, episode = key
        batch_start_time = time.time()

        async def download_one_item(idx, item_data):
            local_path = f"/tmp/input_file_{episode}_{idx}.mp4"
            if os.path.exists(local_path):
                os.remove(local_path)

            if item_data["type"] == "telegram_file":
                msg = item_data["message"]
                status_msg = await message.reply_text(f"⬇️ [EP {episode}] Downloading Video ({idx}/3)...\n\n╭ Progress » Starting...\n╰ {BOT_BRAND_NAME}")
                tracker = ProgressTracker(status_msg, f"⬇️ [EP {episode}] Downloading Video ({idx}/3)", loop)

                try:
                    await client.download_media(
                        message=msg,
                        file_name=local_path,
                        progress=tracker.update
                    )
                except FloodWait as e:
                    await asyncio.sleep(e.value + 1)
                    await client.download_media(
                        message=msg,
                        file_name=local_path,
                        progress=tracker.update
                    )
            else:
                link = item_data["url"]
                status_msg = await message.reply_text(f"⬇️ [EP {episode}] Downloading Link ({idx}/3)...\n\n╭ Progress » Starting...\n╰ {BOT_BRAND_NAME}")
                tracker = ProgressTracker(status_msg, f"⬇️ [EP {episode}] Downloading Link ({idx}/3)", loop)
                try:
                    await loop.run_in_executor(None, download_from_url_sync, link, local_path, tracker)
                except Exception as e:
                    await message.reply_text(f"❌ Download failed for link {idx}: `{e}`")
                    return None

            if os.path.exists(local_path):
                file_size = os.path.getsize(local_path)
                if file_size < 10 * 1024 * 1024:
                    await message.reply_text(f"⚠️ Warning: File {idx} corrupt/chhoti hai ({human_size(file_size)}). Ignored.")
                    os.remove(local_path)
                    return None
                return {"path": local_path, "size": file_size}
            return None

        # All 3 items download concurrently instead of one-by-one — this is
        # purely network-bound (Telegram / the source server), so running
        # them together cuts the download phase's wall-clock time roughly by
        # however much the connections can share bandwidth without fighting.
        download_results = await asyncio.gather(
            *(download_one_item(idx, item_data) for idx, item_data in enumerate(items, 1))
        )
        downloaded_files = [r for r in download_results if r]

        if len(downloaded_files) < 3:
            await message.reply_text(f"❌ Episode {episode} ke sabhi 3 items download nahi huye. Process Cancelled.")
            for f in downloaded_files:
                if os.path.exists(f["path"]):
                    os.remove(f["path"])
            return

        # Auto Quality Allocation by File Size
        downloaded_files.sort(key=lambda x: x["size"])
        quality_labels = ["480p", "720p", "1080p"]
        title = f"{slug.replace('-', ' ').title()} S{season:02d}E{episode:02d}"

        async def process_one_quality(quality, file_info):
            src_path = file_info["path"]
            safe_name = f"{slug}-s{season:02d}e{episode:02d}-{quality}.mp4"
            final_local_path = f"/tmp/{safe_name}"

            try:
                container, video_codec, audio_codec = await probe_media_info(src_path)
                await message.reply_text(
                    f"📼 [EP {episode}] File Info ({quality})\n\n"
                    f"╭ Container » {friendly_container(container)}\n"
                    f"├ Video » {friendly_codec(video_codec, _VIDEO_CODEC_NAMES)}\n"
                    f"╰ Audio » {friendly_codec(audio_codec, _AUDIO_CODEC_NAMES)}"
                )

                remux_msg = await message.reply_text(f"🔄 [EP {episode}] Preparing ({quality}) for streaming...\n\n╰ {BOT_BRAND_NAME}")
                async with ENCODE_SEMAPHORE:
                    remuxed_path = await remux_to_streaming_mp4(src_path, video_codec=video_codec, audio_codec=audio_codec)
                try:
                    await remux_msg.delete()
                except Exception:
                    pass

                if remuxed_path != src_path and os.path.exists(src_path):
                    os.remove(src_path)

                os.rename(remuxed_path, final_local_path)

                upload_status_msg = await message.reply_text(
                    f"⬆️ [EP {episode}] Uploading ({quality})...\n\n"
                    f"╭ File » {safe_name}\n"
                    f"╰ Size » {human_size(file_info['size'])}"
                )
                upload_tracker = ProgressTracker(upload_status_msg, f"⬆️ Uploading ({quality})", loop)
                upload_start_time = time.time()

                uploaded_filename = await upload_to_github_release_async(final_local_path, safe_name, upload_tracker)

                try:
                    await upload_status_msg.edit_text(
                        f"✅ [EP {episode}] Uploaded ({quality})\n\n"
                        f"╭ File » {safe_name}\n"
                        f"├ Size » {human_size(file_info['size'])}\n"
                        f"╰ Time Taken » {human_time(time.time() - upload_start_time)}"
                    )
                except Exception:
                    pass

                if os.path.exists(final_local_path):
                    os.remove(final_local_path)

                # /watch/  -> worker proxies the file inline (proper in-browser streaming, no forced download)
                # /download/ -> worker redirects straight to the GitHub asset (forces a real download)
                streaming_url = f"{WORKER_BASE_URL.rstrip('/')}/watch/{uploaded_filename}"
                download_url = f"{WORKER_BASE_URL.rstrip('/')}/download/{uploaded_filename}"

                ref_path = f"server2_links/{slug}/S{season}/E{episode}/{quality}"
                firebase_db.reference(ref_path).set({
                    "link": streaming_url,
                    "dl_link": download_url,
                    "server": "Server 2",
                    "time": int(time.time()),
                })

                # Separate "dl" folder just for download links, as requested
                dl_ref_path = f"dl_links/{slug}/S{season}/E{episode}/{quality}"
                firebase_db.reference(dl_ref_path).set({
                    "link": download_url,
                    "server": "Server 2",
                    "time": int(time.time()),
                })

                return (quality, streaming_url, download_url)

            except Exception as e:
                if os.path.exists(final_local_path):
                    os.remove(final_local_path)
                await message.reply_text(f"❌ Upload failed for EP {episode} {quality}: `{e}`")
                return None

        # Run all 3 qualities concurrently: while one quality is uploading
        # (network-bound), the others keep remuxing/encoding (CPU-bound) in
        # the background instead of waiting their turn — this overlap is
        # what actually cuts wall-clock time versus doing them one by one.
        raw_results = await asyncio.gather(
            *(process_one_quality(q, f) for q, f in zip(quality_labels, downloaded_files))
        )
        results = [r for r in raw_results if r]

        # Website ke "Added Today" (naya episode) section ke liye entry save karo —
        # website ISI date-format (UTC, YYYY-MM-DD) se check karti hai, isliye yahan bhi UTC use kar rahe hain
        if results:
            try:
                today_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
                firebase_db.reference(f"added_today/{today_date}/{slug}").set({
                    "e": str(episode),
                    "id": slug,
                    "s": str(season),
                    "timestamp": int(time.time()),
                })
            except Exception as e:
                print(f"Added Today Firebase Write Error: {e}")

        summary = "\n\n".join(
            f"▶️ {q}\n    Stream » {s}\n    Download » {d}" for q, s, d in results
        )

        total_size = sum(f["size"] for f in downloaded_files)
        elapsed = time.time() - batch_start_time

        requester = "Unknown"
        if message.from_user:
            requester = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name

        await message.reply_text(
            f"✅ {BOT_BRAND_NAME}\n\n"
            f" -  {title}\n"
            f"╭ Task Size » {human_size(total_size)}\n"
            f"├ Time Taken » {human_time(elapsed)}\n"
            f"├ Qualities » {', '.join(q for q, _, _ in results) or 'None'}\n"
            f"├ Total Files » {len(results)}\n"
            f"╰ Requested By » {requester}\n\n"
            f"〶 Links :\n\n{summary}"
        )


@app.on_message((filters.video | filters.document | filters.text) & filters.private)
async def handle_incoming_media(client, message):
    async with STATE_LOCK:
        slug = current_context["slug"]
        season = current_context["season"]

    if not slug:
        await message.reply_text("⚠️ Pehle `/setup <anime-slug> <season-number>` command chalayein.")
        return

    text_content = message.caption or message.text or ""
    url = extract_url(text_content)
    episode = extract_episode_number(text_content, url)

    if episode is None:
        await message.reply_text("⚠️ Episode number nahi mila! Caption/Link me `ep1`, `ep2` zaroor hona chahiye.")
        return

    is_video = bool(message.video or message.document)
    if not is_video and not url:
        return

    key = (slug, season, episode)

    item = {
        "type": "telegram_file" if is_video else "direct_link",
        "message": message,
        "url": url
    }

    async with STATE_LOCK:
        pending_episodes.setdefault(key, [])
        pending_episodes[key].append(item)
        count = len(pending_episodes[key])

        # Exact Counter Status Response
        if count == 1:
            await message.reply_text(
                f"📥 **Episode {episode} Status:** `[1/3]` Video Received!\n"
                f"⏳ Baaki 2 files/qualities ka wait hai..."
            )
            return
        elif count == 2:
            await message.reply_text(
                f"📥 **Episode {episode} Status:** `[2/3]` Video Received!\n"
                f"⏳ Last 1 file bhejte hi processing start ho jayegi..."
            )
            return
        elif count == 3:
            await message.reply_text(
                f"🎯 **Episode {episode} Status:** `[3/3]` Complete!\n"
                f"🚀 Queue me add ho gaya hai, download & upload start ho raha hai..."
            )
            items = pending_episodes.pop(key)

    # Run in background queue
    asyncio.create_task(process_episode_batch(client, message, key, items))


if __name__ == "__main__":
    app.run()
