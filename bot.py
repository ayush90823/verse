import os
import re
import json
import time
import requests
from pyrogram import Client, filters
import firebase_admin
from firebase_admin import credentials, db as firebase_db

# ================= ENVS & CONFIGS =================
API_ID = int(os.environ.get("TG_API_ID", 0))
API_HASH = os.environ.get("TG_API_HASH", "")
BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")

GITHUB_REPOSITORY = os.environ.get("GITHUB_REPOSITORY", "")  # Format: "username/repo-name"
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

# Global State Context
current_context = {"slug": None, "season": None}
pending_episodes = {}


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
    def __init__(self, status_message, label):
        self.status_message = status_message
        self.label = label
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
            self.status_message.edit_text(text)
        except Exception:
            pass


def extract_episode_number(text: str):
    if not text:
        return None
    match = re.search(r'(?:ep|episode|e)\s*[-:]?\s*(\d{1,4})', text, re.IGNORECASE)
    if match:
        return int(match.group(1))
    if text.strip().isdigit():
        return int(text.strip())
    return None


def extract_url(text: str):
    if not text:
        return None
    match = re.search(r'(https?://[^\s]+)', text)
    if match:
        return match.group(1)
    return None


# ================= GITHUB RELEASE ENGINE =================
def get_or_create_release():
    url = f"{GITHUB_API}/repos/{GITHUB_REPOSITORY}/releases/tags/{RELEASE_TAG}"
    resp = requests.get(url, headers=GH_HEADERS)

    if resp.status_code == 200:
        return resp.json()

    create_url = f"{GITHUB_API}/repos/{GITHUB_REPOSITORY}/releases"
    resp = requests.post(create_url, headers=GH_HEADERS, json={
        "tag_name": RELEASE_TAG,
        "name": "Episodes Storage",
        "body": "Automated media assets release bucket.",
    })
    
    if resp.status_code not in [200, 201]:
        raise Exception(f"GitHub Release Creation Failed (Status {resp.status_code}): {resp.text}")

    return resp.json()


class ProgressFile:
    def __init__(self, path, tracker: ProgressTracker):
        self.file = open(path, "rb")
        self.total_size = os.path.getsize(path)
        self.uploaded = 0
        self.tracker = tracker

    def read(self, size=-1):
        chunk = self.file.read(size)
        self.uploaded += len(chunk)
        self.tracker.update(self.uploaded, self.total_size)
        return chunk

    def __len__(self):
        return self.total_size

    def close(self):
        self.file.close()


def upload_to_github_release(local_path: str, filename: str, tracker: ProgressTracker) -> str:
    release = get_or_create_release()
    upload_url_template = release["upload_url"]
    upload_url = upload_url_template.split("{")[0] + f"?name={filename}"
    headers = {**GH_HEADERS, "Content-Type": "application/octet-stream"}

    max_attempts = 4
    last_error = None

    for attempt in range(1, max_attempts + 1):
        pf = ProgressFile(local_path, tracker)
        try:
            resp = requests.post(upload_url, headers=headers, data=pf, timeout=300)
        except requests.exceptions.RequestException as e:
            last_error = e
            wait = 5 * attempt
            try:
                tracker.status_message.edit_text(
                    f"⚠️ Network issue (Attempt {attempt}/{max_attempts}), "
                    f"Retrying in {wait}s..."
                )
            except Exception:
                pass
            time.sleep(wait)
            continue
        finally:
            pf.close()

        if resp.status_code == 422:
            assets_url = f"{GITHUB_API}/repos/{GITHUB_REPOSITORY}/releases/{release['id']}/assets"
            assets = requests.get(assets_url, headers=GH_HEADERS).json()
            if isinstance(assets, list):
                for asset in assets:
                    if asset.get("name") == filename:
                        requests.delete(f"{GITHUB_API}/repos/{GITHUB_REPOSITORY}/releases/assets/{asset['id']}", headers=GH_HEADERS)
            continue

        if resp.status_code >= 500:
            last_error = Exception(f"Server error {resp.status_code}")
            time.sleep(5 * attempt)
            continue

        resp.raise_for_status()
        return f"{WORKER_BASE_URL.rstrip('/')}/watch/{filename}"

    raise RuntimeError(f"Upload failed after {max_attempts} attempts: {last_error}")


def download_from_url(url: str, local_path: str, tracker: ProgressTracker):
    """Direct URL se video download karta hai chunk by chunk."""
    res = requests.get(url, stream=True, timeout=60)
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
def handle_setup(client, message):
    args = message.command[1:]
    if len(args) < 2:
        message.reply_text(
            "⚠️ **Usage:** `/setup <anime-slug> <season-number>`\n"
            "Example: `/setup grand-blue-dreaming 3`"
        )
        return

    slug = args[0].lower()
    try:
        season = int(args[1])
    except ValueError:
        message.reply_text("❌ Season number integer hona chahiye!")
        return

    current_context["slug"] = slug
    current_context["season"] = season

    message.reply_text(
        f"✅ **Setup Successful!**\n\n"
        f"🎬 **Anime:** `{slug}`\n"
        f"📌 **Season:** `{season}`\n\n"
        f"Ab is episode ke **3 videos** ya **3 download links** bhej/forward karein.\n"
        f"Message/Caption ke end me episode number hona zaroori hai (e.g. `ep 2` ya `EP2`)."
    )


@app.on_message((filters.video | filters.document | filters.text) & filters.private)
def handle_incoming_media(client, message):
    if not current_context["slug"]:
        message.reply_text("⚠️ Pehle `/setup <anime-slug> <season-number>` command chalayein.")
        return

    text_content = message.text or message.caption or ""
    episode = extract_episode_number(text_content)

    if episode is None:
        message.reply_text("⚠️ Episode number nahi mila! Text/Caption ke aakhir me `ep 2` ya `EP2` zaroor likhein.")
        return

    url = extract_url(text_content)
    is_video = bool(message.video or message.document)

    if not is_video and not url:
        message.reply_text("⚠️ Message mein koi Video File ya Download Link nahi mila!")
        return

    slug = current_context["slug"]
    season = current_context["season"]
    key = (slug, season, episode)

    # Payload construct karein (File ho ya Link)
    item = {
        "type": "telegram_file" if is_video else "direct_link",
        "message": message,
        "url": url
    }

    pending_episodes.setdefault(key, []).append(item)
    count = len(pending_episodes[key])

    if count < 3:
        message.reply_text(
            f"📥 Episode {episode} ({count}/3) received.\nBaaki {3 - count} item(s) aur bhejein."
        )
        return

    # 3 Items mil chuke hain -> Process start
    items = pending_episodes.pop(key)
    
    # 1. First Download all 3 to local disk to check file sizes
    downloaded_files = []
    
    for idx, item_data in enumerate(items, 1):
        local_path = f"/tmp/temp_input_{idx}.mkv"
        
        if item_data["type"] == "telegram_file":
            msg = item_data["message"]
            status_msg = message.reply_text(f"⬇️ **Downloading TG File ({idx}/3)...**")
            tracker = ProgressTracker(status_msg, f"⬇️ Downloading TG File ({idx}/3)")
            client.download_media(msg, file_name=local_path, progress=tracker.update)
        else:
            link = item_data["url"]
            status_msg = message.reply_text(f"⬇️ **Downloading Link ({idx}/3)...**")
            tracker = ProgressTracker(status_msg, f"⬇️ Downloading Link ({idx}/3)")
            try:
                download_from_url(link, local_path, tracker)
            except Exception as e:
                message.reply_text(f"❌ Link download failed: `{e}`")
                continue

        if os.path.exists(local_path):
            file_size = os.path.getsize(local_path)
            downloaded_files.append({"path": local_path, "size": file_size})

    if len(downloaded_files) < 3:
        message.reply_text("❌ Teeno items sahi se download nahi ho paaye. Process cancelled.")
        for f in downloaded_files:
            if os.path.exists(f["path"]):
                os.remove(f["path"])
        return

    # 2. Sort by size (Smallest -> 480p, Medium -> 720p, Largest -> 1080p)
    downloaded_files.sort(key=lambda x: x["size"])
    quality_labels = ["480p", "720p", "1080p"]
    title = f"{slug.replace('-', ' ').title()} S{season:02d}E{episode:02d}"
    results = []

    # 3. Rename & Upload to GitHub Release
    for quality, file_info in zip(quality_labels, downloaded_files):
        src_path = file_info["path"]
        safe_name = f"{slug}-s{season:02d}e{episode:02d}-{quality}.mp4"
        final_local_path = f"/tmp/{safe_name}"

        try:
            os.rename(src_path, final_local_path)

            upload_status_msg = message.reply_text(f"⬆️ **Uploading ({quality}):** `{safe_name}`")
            upload_tracker = ProgressTracker(upload_status_msg, f"⬆️ Uploading ({quality})")
            streaming_url = upload_to_github_release(final_local_path, safe_name, upload_tracker)

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
            message.reply_text(f"❌ Upload failed for {quality}: `{e}`")

    summary = "\n".join(f"• **{q}:** {u}" for q, u in results)
    message.reply_text(
        f"🎉 **Process Complete!**\n\n"
        f"📌 **Title:** {title}\n\n"
        f"🔗 **Links Generated:**\n{summary}"
    )


if __name__ == "__main__":
    app.run()
