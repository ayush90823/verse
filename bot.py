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

# Global State
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
        if not total:
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


def extract_episode_number(caption: str):
    if not caption:
        return None
    match = re.search(r'(?:ep|episode|e)\s*[-:]?\s*(\d{1,4})', caption, re.IGNORECASE)
    if match:
        return int(match.group(1))
    if caption.strip().isdigit():
        return int(caption.strip())
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
    resp.raise_for_status()
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
            for asset in assets:
                if asset["name"] == filename:
                    requests.delete(f"{GITHUB_API}/repos/{GITHUB_REPOSITORY}/releases/assets/{asset['id']}", headers=GH_HEADERS)
            continue

        if resp.status_code >= 500:
            last_error = Exception(f"Server error {resp.status_code}")
            time.sleep(5 * attempt)
            continue

        resp.raise_for_status()
        return f"{WORKER_BASE_URL.rstrip('/')}/watch/{filename}"

    raise RuntimeError(f"Upload failed after {max_attempts} attempts: {last_error}")


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
            "Example: `/setup demon-slayer 4`"
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
        f"Ab is episode ke 3 videos forward karein."
    )


@app.on_message((filters.video | filters.document) & filters.private)
def handle_forwarded_video(client, message):
    if not current_context["slug"]:
        message.reply_text("⚠️ Pehle `/setup <anime-slug> <season-number>` bhejein.")
        return

    caption = message.caption or message.text or ""
    episode = extract_episode_number(caption)
    if episode is None:
        message.reply_text("⚠️ Caption se episode number nahi mila! E.g. `EP15` likhein.")
        return

    slug = current_context["slug"]
    season = current_context["season"]
    key = (slug, season, episode)

    pending_episodes.setdefault(key, []).append(message)
    count = len(pending_episodes[key])

    if count < 3:
        message.reply_text(
            f"📥 Episode {episode} ({count}/3) recived. Baaki {3 - count} aur bhejein."
        )
        return

    messages = pending_episodes.pop(key)

    def get_size(m):
        return m.video.file_size if m.video else m.document.file_size

    messages_sorted = sorted(messages, key=get_size)
    quality_labels = ["480p", "720p", "1080p"]
    title = f"{slug.replace('-', ' ').title()} S{season:02d}E{episode:02d}"
    results = []

    for quality, msg in zip(quality_labels, messages_sorted):
        local_path = None
        try:
            original_name = (
                msg.video.file_name if msg.video else msg.document.file_name
            ) or "episode.mp4"
            ext = os.path.splitext(original_name)[1] or ".mp4"
            safe_name = f"{slug}-s{season:02d}e{episode:02d}-{quality}{ext}"
            local_path = f"/tmp/{safe_name}"

            status_msg = message.reply_text(f"⬇️ **Downloading ({quality}):** `{safe_name}`")
            download_tracker = ProgressTracker(status_msg, f"⬇️ Downloading ({quality})")
            client.download_media(msg, file_name=local_path, progress=download_tracker.update)

            upload_status_msg = message.reply_text(f"⬆️ **Uploading ({quality}):** `{safe_name}`")
            upload_tracker = ProgressTracker(upload_status_msg, f"⬆️ Uploading ({quality})")
            streaming_url = upload_to_github_release(local_path, safe_name, upload_tracker)

            if os.path.exists(local_path):
                os.remove(local_path)

            ref_path = f"server2_links/{slug}/S{season}/E{episode}/{quality}"
            firebase_db.reference(ref_path).set({
                "link": streaming_url,
                "dl_link": streaming_url,
                "server": "Server 2",
                "time": int(time.time()),
            })

            results.append((quality, streaming_url))

        except Exception as e:
            if local_path and os.path.exists(local_path):
                os.remove(local_path)
            message.reply_text(f"❌ Failed for {quality}: `{e}`")
            continue

    summary = "\n".join(f"• **{q}:** {u}" for q, u in results)
    message.reply_text(
        f"🎉 **Process Complete!**\n\n"
        f"📌 **Title:** {title}\n\n"
        f"🔗 **Links:**\n{summary}"
    )


if __name__ == "__main__":
    app.run()
