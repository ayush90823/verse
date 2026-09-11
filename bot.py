import os
import re
import json
import time
import asyncio
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


# ================= HELPER FUNCTIONS =================
def human_size(num_bytes):
    for unit in ["B", "KB", "MB", "GB"]:
        if num_bytes < 1024:
            return f"{num_bytes:.1f}{unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f}TB"


def make_bar(percent, length=18):
    filled = int(length * percent // 100)
    return "█" * filled + "░" * (length - filled)


class ProgressTracker:
    def __init__(self, status_message, label, loop):
        self.status_message = status_message
        self.label = label
        self.loop = loop
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
        status_word = "✅ Complete" if is_final else self.label
        text = (
            f"{status_word}\n"
            f"`[{bar}]` {percent}%\n"
            f"{human_size(current)} / {human_size(total)}"
        )
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


# ================= GITHUB RELEASE ENGINE =================
def get_or_create_release():
    url = f"{GITHUB_API}/repos/{GITHUB_REPOSITORY}/releases/tags/{RELEASE_TAG}"
    resp = requests.get(url, headers=GH_HEADERS, timeout=15)

    if resp.status_code == 200:
        return resp.json()

    create_url = f"{GITHUB_API}/repos/{GITHUB_REPOSITORY}/releases"
    resp = requests.post(create_url, headers=GH_HEADERS, json={
        "tag_name": RELEASE_TAG,
        "name": "Episodes Storage",
        "body": "Automated media assets release bucket.",
    }, timeout=15)
    
    if resp.status_code not in [200, 201]:
        raise Exception(f"GitHub Release Creation Failed (Status {resp.status_code}): {resp.text}")

    return resp.json()


async def upload_to_github_release_async(local_path: str, filename: str, tracker: ProgressTracker, max_retries=3) -> str:
    loop = asyncio.get_running_loop()
    release = await loop.run_in_executor(None, get_or_create_release)
    
    assets_url = f"{GITHUB_API}/repos/{GITHUB_REPOSITORY}/releases/{release['id']}/assets"
    assets_resp = await loop.run_in_executor(None, lambda: requests.get(assets_url, headers=GH_HEADERS, timeout=15))
    if assets_resp.status_code == 200:
        for asset in assets_resp.json():
            if asset.get("name") == filename:
                del_url = f"{GITHUB_API}/repos/{GITHUB_REPOSITORY}/releases/assets/{asset['id']}"
                await loop.run_in_executor(None, lambda: requests.delete(del_url, headers=GH_HEADERS, timeout=15))
                await asyncio.sleep(1)

    upload_url_template = release["upload_url"]
    upload_url = upload_url_template.split("{")[0] + f"?name={filename}"

    curl_command = [
        "curl",
        "-s",
        "--retry", "3",
        "--retry-delay", "2",
        "-X", "POST",
        "-H", f"Authorization: Bearer {GH_TOKEN}",
        "-H", "Content-Type: application/octet-stream",
        "-H", "Accept: application/vnd.github+json",
        "--upload-file", local_path,
        upload_url
    ]

    for attempt in range(1, max_retries + 1):
        try:
            proc = await asyncio.create_subprocess_exec(
                *curl_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            if proc.returncode == 0:
                return f"{WORKER_BASE_URL.rstrip('/')}/watch/{filename}"
            else:
                print(f"cURL Attempt {attempt} Failed: {stderr.decode().strip()}")
        except Exception as e:
            print(f"cURL Exception Attempt {attempt}: {e}")
        
        await asyncio.sleep(2 * attempt)

    raise RuntimeError("cURL Upload failed after maximum retries.")


def download_from_url_sync(url: str, local_path: str, tracker: ProgressTracker):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "*/*"
    }
    res = requests.get(url, headers=headers, stream=True, allow_redirects=True, timeout=120)
    res.raise_for_status()
    
    total_size = int(res.headers.get('content-length', 0))
    downloaded = 0
    
    with open(local_path, 'wb') as f:
        for chunk in res.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
                tracker.update(downloaded, total_size)


# ================= TELEGRAM BOT HANDLERS =================
app = Client(
    "gh_actions_uploader_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True,
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
        downloaded_files = []

        for idx, item_data in enumerate(items, 1):
            local_path = f"/tmp/input_file_{episode}_{idx}.mp4"
            if os.path.exists(local_path):
                os.remove(local_path)

            if item_data["type"] == "telegram_file":
                msg = item_data["message"]
                status_msg = await message.reply_text(f"⬇️ **[EP {episode}] Downloading Video ({idx}/3)...**")
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
                status_msg = await message.reply_text(f"⬇️ **[EP {episode}] Downloading Link ({idx}/3)...**")
                tracker = ProgressTracker(status_msg, f"⬇️ [EP {episode}] Downloading Link ({idx}/3)", loop)
                try:
                    await loop.run_in_executor(None, download_from_url_sync, link, local_path, tracker)
                except Exception as e:
                    await message.reply_text(f"❌ Download failed for link {idx}: `{e}`")
                    continue

            if os.path.exists(local_path):
                file_size = os.path.getsize(local_path)
                if file_size < 10 * 1024 * 1024:
                    await message.reply_text(f"⚠️ Warning: File {idx} corrupt/chhoti hai ({human_size(file_size)}). Ignored.")
                    os.remove(local_path)
                else:
                    downloaded_files.append({"path": local_path, "size": file_size})

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
        results = []

        for quality, file_info in zip(quality_labels, downloaded_files):
            src_path = file_info["path"]
            safe_name = f"{slug}-s{season:02d}e{episode:02d}-{quality}.mp4"
            final_local_path = f"/tmp/{safe_name}"

            try:
                os.rename(src_path, final_local_path)

                upload_status_msg = await message.reply_text(f"⬆️ **[EP {episode}] Uploading ({quality}):** `{safe_name}`\nSize: `{human_size(file_info['size'])}`")
                upload_tracker = ProgressTracker(upload_status_msg, f"⬆️ Uploading ({quality})", loop)
                
                streaming_url = await upload_to_github_release_async(final_local_path, safe_name, upload_tracker)

                if os.path.exists(final_local_path):
                    os.remove(final_local_path)

                ref_path = f"server2_links/{slug}/S{season}/E{episode}/{quality}"
                firebase_db.reference(ref_path).set({
                    "link": streaming_url,
                    "dl_link": streaming_url,
                    "server": "Server 2",
                    "time": int(time.time()),
                })

                results.append((quality, streaming_url))

            except Exception as e:
                if os.path.exists(final_local_path):
                    os.remove(final_local_path)
                await message.reply_text(f"❌ Upload failed for EP {episode} {quality}: `{e}`")

        summary = "\n".join(f"• **{q}:** {u}" for q, u in results)
        await message.reply_text(
            f"🎉 **EP {episode} Process Complete!**\n\n"
            f"📌 **Title:** {title}\n\n"
            f"🔗 **Links:**\n{summary}"
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
