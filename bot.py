import os
import re
import json
import time
import requests
import subprocess
from pyrogram import Client, filters
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


def extract_url(text: str):
    if not text:
        return None
    match = re.search(r'(https?://[^\s]+)', text)
    if match:
        return match.group(1)
    return None


def extract_episode_number(text: str, url: str = None):
    """Text, Caption, ya URL ke ending filename se episode number dhoondhta hai."""
    targets_to_search = []
    
    if text:
        targets_to_search.append(text)
    
    if url:
        # URL ke end part (filename/query parameters) ko inspect karte hain
        clean_url = url.split("?")[0] # Query parameters alag karein
        filename = clean_url.split("/")[-1]
        targets_to_search.append(filename)
        targets_to_search.append(url)

    for target in targets_to_search:
        # Match pattern: ep1, ep-01, episode 2, e03, s01e02, ep_05
        match = re.search(r'(?:ep|episode|e|s\d+e)[-_\s:]?(\d{1,4})', target, re.IGNORECASE)
        if match:
            return int(match.group(1))

        # Direct number match agar text me sirf digit ho
        if target.strip().isdigit():
            return int(target.strip())

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


def upload_to_github_release(local_path: str, filename: str, tracker: ProgressTracker) -> str:
    """cURL command ka use karke 1080p SSL connection drops/errors ko bypass karta hai."""
    release = get_or_create_release()
    
    # Existing duplicate asset ko delete karein
    assets_url = f"{GITHUB_API}/repos/{GITHUB_REPOSITORY}/releases/{release['id']}/assets"
    assets_resp = requests.get(assets_url, headers=GH_HEADERS)
    if assets_resp.status_code == 200:
        for asset in assets_resp.json():
            if asset.get("name") == filename:
                requests.delete(f"{GITHUB_API}/repos/{GITHUB_REPOSITORY}/releases/assets/{asset['id']}", headers=GH_HEADERS)
                time.sleep(1)

    upload_url_template = release["upload_url"]
    upload_url = upload_url_template.split("{")[0] + f"?name={filename}"

    curl_command = [
        "curl",
        "-X", "POST",
        "-H", f"Authorization: Bearer {GH_TOKEN}",
        "-H", "Content-Type: application/octet-stream",
        "-H", "Accept: application/vnd.github+json",
        "--upload-file", local_path,
        upload_url
    ]

    try:
        result = subprocess.run(curl_command, capture_output=True, text=True, check=True)
        return f"{WORKER_BASE_URL.rstrip('/')}/watch/{filename}"
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"cURL Upload failed: {e.stderr}")


def download_from_url(url: str, local_path: str, tracker: ProgressTracker):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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
        f"Bot link ya caption se automatic episode number detect kar lega!"
    )


@app.on_message((filters.video | filters.document | filters.text) & filters.private)
def handle_incoming_media(client, message):
    if not current_context["slug"]:
        message.reply_text("⚠️ Pehle `/setup <anime-slug> <season-number>` command chalayein.")
        return

    text_content = message.caption or message.text or ""
    url = extract_url(text_content)
    
    # Episode number parsing (Text + URL dono me dhoondhta hai)
    episode = extract_episode_number(text_content, url)

    if episode is None:
        message.reply_text("⚠️ Episode number nahi mila! Link ya Caption me `ep1`, `ep2`, ya `e02` zaroor hona chahiye.")
        return

    is_video = bool(message.video or message.document)

    if not is_video and not url:
        message.reply_text("⚠️ Message mein Video ya Download Link nahi mila!")
        return

    slug = current_context["slug"]
    season = current_context["season"]
    key = (slug, season, episode)

    item = {
        "type": "telegram_file" if is_video else "direct_link",
        "message": message,
        "url": url
    }

    pending_episodes.setdefault(key, []).append(item)
    count = len(pending_episodes[key])

    if count < 3:
        message.reply_text(
            f"📥 Episode {episode} ({count}/3) received.\nBaaki {3 - count} items aur bhejein."
        )
        return

    # 3 Items complete -> Processing
    items = pending_episodes.pop(key)
    downloaded_files = []

    for idx, item_data in enumerate(items, 1):
        local_path = f"/tmp/input_file_{idx}.mp4"
        
        if os.path.exists(local_path):
            os.remove(local_path)

        if item_data["type"] == "telegram_file":
            msg = item_data["message"]
            status_msg = message.reply_text(f"⬇️ **Downloading TG Video ({idx}/3)...**")
            tracker = ProgressTracker(status_msg, f"⬇️ Downloading TG Video ({idx}/3)")
            
            client.download_media(
                message=msg,
                file_name=local_path,
                progress=tracker.update
            )
        else:
            link = item_data["url"]
            status_msg = message.reply_text(f"⬇️ **Downloading Link ({idx}/3)...**")
            tracker = ProgressTracker(status_msg, f"⬇️ Downloading Link ({idx}/3)")
            try:
                download_from_url(link, local_path, tracker)
            except Exception as e:
                message.reply_text(f"❌ Download failed for link {idx}: `{e}`")
                continue

        # Validating actual downloaded file size
        if os.path.exists(local_path):
            file_size = os.path.getsize(local_path)
            if file_size < 15 * 1024 * 1024:
                message.reply_text(f"⚠️ Warning: File {idx} corrupt/chhoti hai ({human_size(file_size)}). Ignore kiya gaya.")
                os.remove(local_path)
            else:
                downloaded_files.append({"path": local_path, "size": file_size})

    if len(downloaded_files) < 3:
        message.reply_text("❌ Subhi 3 videos/links sahi se download nahi ho sake. Process Cancelled.")
        for f in downloaded_files:
            if os.path.exists(f["path"]):
                os.remove(f["path"])
        return

    # Sort files by size
    downloaded_files.sort(key=lambda x: x["size"])
    quality_labels = ["480p", "720p", "1080p"]
    title = f"{slug.replace('-', ' ').title()} S{season:02d}E{episode:02d}"
    results = []

    # Upload to GitHub Releases
    for quality, file_info in zip(quality_labels, downloaded_files):
        src_path = file_info["path"]
        safe_name = f"{slug}-s{season:02d}e{episode:02d}-{quality}.mp4"
        final_local_path = f"/tmp/{safe_name}"

        try:
            os.rename(src_path, final_local_path)

            upload_status_msg = message.reply_text(f"⬆️ **Uploading ({quality}):** `{safe_name}`\nSize: `{human_size(file_info['size'])}`")
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
        f"🔗 **Links:**\n{summary}"
    )


if __name__ == "__main__":
    app.run()
