import os
import re
import glob
import time
import shutil
import asyncio
import subprocess
from datetime import datetime
from urllib.parse import urlparse, unquote
import httpx
from dotenv import load_dotenv
from pyrogram import Client, filters, StopTransmission
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# بارگذاری متغیرها از فایل .env
load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPO = os.getenv("GITHUB_REPO", "")  # فرمت: username/repo

CHUNK_SIZE_MB = 1900
CHUNK_SIZE_BYTES = CHUNK_SIZE_MB * 1024 * 1024

DOWNLOAD_DIR = "/tmp/tg_gh_uploads"
COOKIES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookies.txt")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

app = Client("github_uploader_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
upload_lock = asyncio.Lock()

# متغیرهای وضعیت سراسری
cancel_requested = False
pending_custom_name: str | None = None
pending_zip_mode: bool = False
pending_zip_password: str | None = None
awaiting_gh_rename_asset_id: int | None = None
active_download_ext: str = ""
active_final_filename: str = ""

# وضعیت صف آپلود گروهی (Batch Queue)
batch_state = {
    "active": False,
    "zip_mode": False,       # آیا کل مجموعه یکجا زیپ شود؟
    "password": None,        # رمز زیپ در حالت batch zip
    "title": "",             # نام مجموعه / ریلیز
    "queue": []              # لیست آیتم‌های در صف انتظار برای پردازش در /done
}

GH_HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28"
}

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
}


def sanitize_filename(name: str) -> str:
    """پاکسازی نام فایل برای جلوگیری از خراب شدن لینک دانلود"""
    name = name.strip().replace("\n", "_")
    name = re.sub(r'[\\/*?:"<>|\s]+', '_', name)
    return name.strip('_') or f"file_{int(time.time())}"


def build_filename_with_ext(custom_text: str, orig_ext: str) -> str:
    """ساخت نام تمیز و حفظ پسوند اصلی در صورت عدم ورود پسوند"""
    clean = sanitize_filename(custom_text)
    _, ext = os.path.splitext(clean)
    if not ext and orig_ext:
        clean += orig_ext
    return clean


def resolve_initial_filename(orig_name: str, explicit_custom: str | None) -> tuple[str, str]:
    """تعیین نام اولیه فایل بر اساس اولویت‌ها"""
    global pending_custom_name
    _, orig_ext = os.path.splitext(orig_name)

    if pending_custom_name:
        chosen = build_filename_with_ext(pending_custom_name, orig_ext)
        pending_custom_name = None
        return chosen, orig_ext

    if explicit_custom:
        first_line = explicit_custom.strip().splitlines()[0].strip()
        if not first_line.startswith("/") and not first_line.startswith("http"):
            chosen = build_filename_with_ext(first_line, orig_ext)
            return chosen, orig_ext

    return sanitize_filename(orig_name), orig_ext


def human_size(size_bytes: int) -> str:
    """تبدیل بایت به واحدهای خوانا"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"


def get_cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ لغو عملیات", callback_data="cancel_task")]
    ])


def get_file_manage_kb(direct_link: str, release_id: int, asset_id: int | None, release_html_url: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("🔗 دانلود مستقیم فایل", url=direct_link)],
        [InlineKeyboardButton("🌐 مشاهده صفحه Release در گیت‌هاب", url=release_html_url)]
    ]
    if asset_id:
        rows.append([
            InlineKeyboardButton("✏️ تغییر نام در گیت‌هاب", callback_data=f"ren_{asset_id}"),
            InlineKeyboardButton("🗑 حذف از گیت‌هاب", callback_data=f"del_{release_id}")
        ])
    else:
        rows.append([InlineKeyboardButton("🗑 حذف کل Release از گیت‌هاب", callback_data=f"del_{release_id}")])
    return InlineKeyboardMarkup(rows)


def is_ytdlp_url(url: str) -> bool:
    """تشخیص لینک‌های شبکه‌های اجتماعی برای دانلود با yt-dlp"""
    domains = [
        "youtube.com", "youtu.be", "instagram.com", "twitter.com", "x.com",
        "tiktok.com", "soundcloud.com", "twitch.tv", "vimeo.com", "reddit.com"
    ]
    host = urlparse(url).netloc.lower()
    return any(d in host for d in domains)


def parse_batch_args(args: list[str]) -> tuple[bool, str | None, str]:
    """
    پارس کردن هوشمند دستور /batch:
    - /batch [name]
    - /batch zip
    - /batch zip -p PASS [name]
    - /batch zip PASS name
    - /batch zip name
    """
    default_name = f"Pack_{datetime.now().strftime('%Y%m%d_%H%M')}"
    if not args:
        return False, None, default_name

    if args[0].lower() != "zip":
        return False, None, sanitize_filename(" ".join(args))

    rem = args[1:]
    if not rem:
        return True, None, default_name

    if rem[0] in ("-p", "--pass") and len(rem) >= 2:
        pwd = rem[1]
        name = sanitize_filename(" ".join(rem[2:])) if len(rem) > 2 else default_name
        return True, pwd, name

    if len(rem) == 1:
        return True, None, sanitize_filename(rem[0])

    pwd = rem[0]
    name = sanitize_filename(" ".join(rem[1:]))
    return True, pwd, name


async def create_store_zip_volumes(
    target_paths: list[str],
    work_dir: str,
    base_archive_name: str,
    password: str | None,
    force_split: bool,
    status_msg: Message
) -> list[str]:
    """
    بسته‌بندی در حالت Store (-mx=0 بدون فشار به CPU) با فرمت استاندارد ZIP
    که مستقیماً در WinRAR و 7-Zip باز می‌شود.
    """
    clean_base = re.sub(r'\.(zip|7z|rar)$', '', base_archive_name, flags=re.IGNORECASE)
    archive_base_path = os.path.join(work_dir, f"{clean_base}.zip")

    total_input_size = sum(os.path.getsize(p) for p in target_paths if os.path.exists(p))
    need_split = force_split or (total_input_size > CHUNK_SIZE_BYTES)

    pw_info = f" (رمز: `{password}`)" if password else " (بدون رمز)"
    split_info = f"چندبخشی ({CHUNK_SIZE_MB}MB)" if need_split else "تک‌فایلی"

    await status_msg.edit_text(
        f"📦 **در حال ساخت آرشیو استاندارد ZIP (حالت سریع Store)...**\n\n"
        f"📄 نام پایه: `{clean_base}.zip`\n"
        f"⚙️ نوع خروجی: `{split_info}`{pw_info}",
        reply_markup=get_cancel_kb()
    )

    cmd = ["7z", "a", "-tzip", "-mx=0"]
    if need_split:
        cmd.append(f"-v{CHUNK_SIZE_MB}m")
    if password:
        cmd.extend([f"-p{password}", "-mem=AES256"])

    cmd.append(archive_base_path)
    cmd.extend(target_paths)

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await proc.communicate()

    if proc.returncode != 0:
        raise Exception(f"خطا در ساخت آرشیو زیپ: {stderr.decode('utf-8', errors='ignore')[-300:]}")

    for p in target_paths:
        if os.path.exists(p):
            try:
                os.remove(p)
            except Exception:
                pass

    parts = sorted(glob.glob(f"{archive_base_path}*"))
    if not parts:
        raise Exception("فایل آرشیو خروجی یافت نشد.")

    if len(parts) == 1 and parts[0].endswith(".zip.001"):
        normal_zip = parts[0][:-4]
        os.rename(parts[0], normal_zip)
        parts = [normal_zip]

    return parts


async def tg_progress_bar(current, total, client: Client, status_msg: Message, start_time: float, stage: str):
    """نمایش پیشرفت دانلود از تلگرام"""
    global cancel_requested, active_final_filename
    if cancel_requested:
        client.stop_transmission()
        return

    now = time.time()
    last_edit = getattr(status_msg, "_last_edit", 0)
    if now - last_edit < 4 and current != total:
        return
    status_msg._last_edit = now

    percentage = current * 100 / total if total else 0
    elapsed = now - start_time
    speed = current / elapsed if elapsed > 0 else 0
    eta = (total - current) / speed if speed > 0 else 0

    text = (
        f"🔄 **{stage}**\n\n"
        f"📄 نام فایل: `{active_final_filename}`\n"
        f"📊 پیشرفت: `{percentage:.1f}%`\n"
        f"📦 حجم: `{human_size(current)}` از `{human_size(total)}`\n"
        f"🚀 سرعت: `{human_size(speed)}/s`\n"
        f"⏳ زمان باقی‌مانده: `{int(eta)} ثانیه`\n\n"
        f"💡 *(برای تغییر نام، همین الان نام جدید را در چت بفرستید)*"
    )
    try:
        await status_msg.edit_text(text, reply_markup=get_cancel_kb())
    except Exception:
        pass


async def async_file_sender(local_path: str, total_size: int, status_msg: Message, clean_filename: str, part_label: str = ""):
    """خواندن غیرهمزمان فایل برای آپلود به گیت‌هاب"""
    global cancel_requested
    current = 0
    start_t = time.time()
    chunk_size = 512 * 1024

    with open(local_path, "rb") as f:
        while chunk := f.read(chunk_size):
            if cancel_requested:
                raise asyncio.CancelledError("عملیات توسط کاربر لغو شد.")
            current += len(chunk)

            now = time.time()
            last_edit = getattr(status_msg, "_last_edit", 0)
            if now - last_edit >= 4 or current == total_size:
                status_msg._last_edit = now
                elapsed = now - start_t
                speed = current / elapsed if elapsed > 0 else 0
                pct = (current * 100 / total_size) if total_size else 0
                eta = (total_size - current) / speed if speed > 0 else 0
                header_str = f"☁️ **در حال آپلود روی گیت‌هاب {part_label}...**"
                try:
                    await status_msg.edit_text(
                        f"{header_str}\n\n"
                        f"📄 فایل: `{clean_filename}`\n"
                        f"📊 پیشرفت: `{pct:.1f}%`\n"
                        f"📦 آپلود شده: `{human_size(current)}` از `{human_size(total_size)}`\n"
                        f"🚀 سرعت: `{human_size(speed)}/s`\n"
                        f"⏳ زمان باقی‌مانده: `{int(eta)} ثانیه`",
                        reply_markup=get_cancel_kb()
                    )
                except Exception:
                    pass

            yield chunk
            await asyncio.sleep(0)


async def create_github_release(http: httpx.AsyncClient, title: str, body: str, status_msg: Message) -> tuple[int, str, str, str]:
    """ایجاد یک Release جدید در گیت‌هاب"""
    tag_time = datetime.now().strftime("%Y%m%d-%H%M%S-%f")[:19]
    tag_name = f"v{tag_time}"

    await status_msg.edit_text("⚙️ در حال ایجاد Release جدید در گیت‌هاب...", reply_markup=get_cancel_kb())
    create_resp = await http.post(
        f"https://api.github.com/repos/{GITHUB_REPO}/releases",
        headers=GH_HEADERS,
        json={
            "tag_name": tag_name,
            "name": title,
            "body": body,
            "draft": False,
            "prerelease": False
        }
    )
    if create_resp.status_code != 201:
        raise Exception(f"خطا در ساخت Release: {create_resp.text}")

    data = create_resp.json()
    return data["id"], data["upload_url"].split("{")[0], tag_name, data["html_url"]


async def upload_asset_to_release(
    http: httpx.AsyncClient,
    upload_url: str,
    local_path: str,
    asset_name: str,
    status_msg: Message,
    part_label: str = ""
) -> tuple[int, str, int]:
    """آپلود یک فایل داخل Release و حذف فوری آن از دیسک سرور"""
    file_size = os.path.getsize(local_path)
    upload_headers = {
        **GH_HEADERS,
        "Content-Type": "application/octet-stream",
        "Content-Length": str(file_size)
    }

    up_resp = await http.post(
        upload_url,
        params={"name": asset_name},
        headers=upload_headers,
        content=async_file_sender(local_path, file_size, status_msg, asset_name, part_label)
    )

    if up_resp.status_code != 201:
        raise Exception(f"خطا در آپلود `{asset_name}`: {up_resp.text}")

    if os.path.exists(local_path):
        try:
            os.remove(local_path)
        except Exception:
            pass

    asset_data = up_resp.json()
    return asset_data["id"], asset_data["browser_download_url"], file_size


async def upload_links_txt_asset(http: httpx.AsyncClient, upload_url: str, work_dir: str, links: list[str], status_msg: Message) -> str | None:
    """ساخت و آپلود فایل links.txt داخل Release برای دانلود یکجا با IDM"""
    if len(links) <= 1:
        return None
    links_path = os.path.join(work_dir, "links.txt")
    with open(links_path, "w", encoding="utf-8") as f:
        f.write("\n".join(links) + "\n")

    _, dl_url, _ = await upload_asset_to_release(http, upload_url, links_path, "links.txt", status_msg, "(لیست لینک‌ها)")
    return dl_url


async def process_and_upload_single_target(local_path: str, status_msg: Message):
    """پردازش فایل تکی، پارت‌بندی خودکار بالای 2GB یا زیپ، و آپلود در گیت‌هاب"""
    global pending_zip_mode, pending_zip_password, active_final_filename

    work_dir = os.path.dirname(local_path)
    final_clean_name = active_final_filename
    raw_size = os.path.getsize(local_path)

    do_zip = pending_zip_mode
    zip_pwd = pending_zip_password
    pending_zip_mode = False
    pending_zip_password = None

    clean_local_path = os.path.join(work_dir, final_clean_name)
    if local_path != clean_local_path:
        shutil.move(local_path, clean_local_path)
        local_path = clean_local_path

    is_over_2gb = raw_size > CHUNK_SIZE_BYTES

    if do_zip or is_over_2gb:
        files_to_upload = await create_store_zip_volumes(
            [local_path], work_dir, final_clean_name, zip_pwd, is_over_2gb, status_msg
        )
    else:
        files_to_upload = [local_path]

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    release_title = f"{final_clean_name} | {now_str}"
    repo_url = f"https://github.com/{GITHUB_REPO}"

    async with httpx.AsyncClient(timeout=None) as http:
        body_note = (
            f"Uploaded via Telegram Bot on {now_str}\n"
            f"Original Name: `{final_clean_name}` ({human_size(raw_size)})\n"
        )
        if len(files_to_upload) > 1:
            body_note += "\n💡 **نحوه استخراج:** تمام پارت‌ها را در یک پوشه دانلود کرده و روی پارت `.zip.001` با برنامه **WinRAR** یا **7-Zip** کلیک راست کرده و Extract کنید."

        release_id, upload_url, tag_name, release_html_url = await create_github_release(
            http, release_title, body_note, status_msg
        )

        try:
            uploaded_items = []
            total_parts = len(files_to_upload)
            for idx, fpath in enumerate(files_to_upload, 1):
                fname = os.path.basename(fpath)
                plabel = f"(پارت {idx} از {total_parts})" if total_parts > 1 else ""
                aid, durl, fsize = await upload_asset_to_release(http, upload_url, fpath, fname, status_msg, plabel)
                uploaded_items.append((fname, durl, fsize, aid))

            all_links = [item[1] for item in uploaded_items]
            links_txt_url = await upload_links_txt_asset(http, upload_url, work_dir, all_links, status_msg)

        except Exception as e:
            await http.delete(f"https://api.github.com/repos/{GITHUB_REPO}/releases/{release_id}", headers=GH_HEADERS)
            await http.delete(f"https://api.github.com/repos/{GITHUB_REPO}/git/refs/tags/{tag_name}", headers=GH_HEADERS)
            raise e

    pw_line = f"\n🔐 **رمز عبور آرشیو:** `{zip_pwd}`" if zip_pwd else ""

    if len(uploaded_items) == 1:
        fname, durl, fsize, aid = uploaded_items[0]
        await status_msg.edit_text(
            f"✅ **فایل با موفقیت در گیت‌هاب آپلود شد!**\n\n"
            f"🏷 **عنوان ریلیز:** `{release_title}`\n"
            f"📄 **نام فایل:** `{fname}`\n"
            f"📦 **حجم:** `{human_size(fsize)}`{pw_line}\n\n"
            f"🔗 **لینک مستقیم فایل:**\n`{durl}`\n\n"
            f"🏛 **صفحه ریلیز:** [مشاهده در گیت‌هاب]({release_html_url})\n"
            f"📂 **مخزن (Repo):** `{repo_url}`",
            reply_markup=get_file_manage_kb(durl, release_id, aid, release_html_url),
            disable_web_page_preview=True
        )
    else:
        lines = [
            f"✅ **فایل در {len(uploaded_items)} پارت در گیت‌هاب آپلود شد!**\n",
            f"🏷 **عنوان ریلیز:** `{release_title}`",
            f"📦 **حجم کل:** `{human_size(raw_size)}`{pw_line}",
            f"💡 **نحوه باز کردن:** همه پارت‌ها را دانلود کنید و فایل `.zip.001` را با **WinRAR** یا **7-Zip** استخراج کنید.\n"
        ]
        for idx, (fname, durl, fsize, _) in enumerate(uploaded_items, 1):
            lines.append(f"🔹 **پارت {idx}** (`{human_size(fsize)}`): `{fname}`\n`{durl}`")

        if links_txt_url:
            lines.append(f"\n📋 **فایل لیست لینک‌ها (مناسب IDM):**\n`{links_txt_url}`")

        lines.append(f"\n🏛 **صفحه ریلیز:** [مشاهده در گیت‌هاب]({release_html_url})")
        lines.append(f"📂 **لینک مخزن:** `{repo_url}`")

        await status_msg.edit_text(
            "\n".join(lines),
            reply_markup=get_file_manage_kb(uploaded_items[0][1], release_id, None, release_html_url),
            disable_web_page_preview=True
        )


async def download_external_tool(cmd_list: list[str], work_dir: str, status_msg: Message, label: str) -> str:
    """اجرای غیرهمزمان ابزارهای دانلود خارجی (megadl, gdown, yt-dlp)"""
    global cancel_requested
    proc = await asyncio.create_subprocess_exec(
        *cmd_list,
        cwd=work_dir,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT
    )

    start_t = time.time()
    while proc.returncode is None:
        if cancel_requested:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=3)
            except asyncio.TimeoutError:
                proc.kill()
            raise asyncio.CancelledError("عملیات توسط کاربر لغو شد.")

        await asyncio.sleep(2)
        now = time.time()
        if now - getattr(status_msg, "_last_edit", 0) >= 4:
            status_msg._last_edit = now
            files = glob.glob(os.path.join(work_dir, "*"))
            cur_size = sum(os.path.getsize(f) for f in files if os.path.isfile(f))
            elapsed = int(now - start_t)
            try:
                await status_msg.edit_text(
                    f"🌐 **{label}**\n\n"
                    f"📄 نام نهایی: `{active_final_filename}`\n"
                    f"📦 حجم دریافت شده تا الان: `{human_size(cur_size)}`\n"
                    f"⏱ زمان سپری شده: `{elapsed} ثانیه`\n\n"
                    f"💡 *(برای تغییر نام، همین الان نام جدید را در چت بفرستید)*",
                    reply_markup=get_cancel_kb()
                )
            except Exception:
                pass

    out, _ = await proc.communicate()
    if proc.returncode != 0:
        err_tail = out.decode("utf-8", errors="ignore")[-400:]
        raise Exception(f"خطا در دانلود ({label}):\n{err_tail}")

    downloaded = [
        os.path.join(work_dir, f) for f in os.listdir(work_dir)
        if os.path.isfile(os.path.join(work_dir, f)) and not f.endswith(".part")
    ]
    if not downloaded:
        raise Exception("فایلی پس از اتمام دانلود یافت نشد.")

    downloaded.sort(key=lambda x: os.path.getsize(x), reverse=True)
    return downloaded[0]


async def download_url_to_disk(url: str, explicit_name: str | None, job_dir: str, status_msg: Message) -> tuple[str, str]:
    """دانلود هر نوع لینک در پوشه مشخص شده"""
    global active_download_ext, active_final_filename

    host = urlparse(url).netloc.lower()

    # ۱. دانلود از Mega.nz
    if "mega.nz" in host or "mega.co.nz" in host:
        initial_name, orig_ext = resolve_initial_filename("mega_file.bin", explicit_name)
        active_download_ext = orig_ext
        active_final_filename = initial_name
        await status_msg.edit_text("☁️ در حال اتصال به سرورهای **Mega.nz**...", reply_markup=get_cancel_kb())

        local_path = await download_external_tool(
            ["megadl", "--no-progress", "--path", job_dir, url],
            job_dir, status_msg, "در حال دانلود از Mega.nz..."
        )
        real_name = os.path.basename(local_path)
        _, real_ext = os.path.splitext(real_name)
        active_download_ext = real_ext
        if active_final_filename == "mega_file.bin":
            active_final_filename = sanitize_filename(real_name)
        else:
            active_final_filename = build_filename_with_ext(active_final_filename, real_ext)
        return local_path, active_final_filename

    # ۲. دانلود از Google Drive
    if "drive.google.com" in host:
        initial_name, orig_ext = resolve_initial_filename("gdrive_file.bin", explicit_name)
        active_download_ext = orig_ext
        active_final_filename = initial_name
        await status_msg.edit_text("☁️ در حال دانلود از **Google Drive**...", reply_markup=get_cancel_kb())

        gdown_bin = os.path.join(os.path.dirname(os.path.abspath(__file__)), "venv", "bin", "gdown")
        if not os.path.exists(gdown_bin):
            gdown_bin = "gdown"

        local_path = await download_external_tool(
            [gdown_bin, "--fuzzy", url, "-O", f"{job_dir}/"],
            job_dir, status_msg, "در حال دانلود از Google Drive..."
        )
        real_name = os.path.basename(local_path)
        _, real_ext = os.path.splitext(real_name)
        active_download_ext = real_ext
        if active_final_filename == "gdrive_file.bin":
            active_final_filename = sanitize_filename(real_name)
        else:
            active_final_filename = build_filename_with_ext(active_final_filename, real_ext)
        return local_path, active_final_filename

    # ۳. دانلود با yt-dlp
    if is_ytdlp_url(url):
        initial_name, _ = resolve_initial_filename("video.mp4", explicit_name)
        active_download_ext = ".mp4"
        active_final_filename = initial_name
        await status_msg.edit_text("🎬 در حال استخراج رسانه با **yt-dlp**...", reply_markup=get_cancel_kb())

        ytdlp_bin = os.path.join(os.path.dirname(os.path.abspath(__file__)), "venv", "bin", "yt-dlp")
        if not os.path.exists(ytdlp_bin):
            ytdlp_bin = "yt-dlp"

        cmd = [
            ytdlp_bin, "--no-playlist", "--merge-output-format", "mp4",
            "-o", "%(title).80s.%(ext)s", url
        ]
        if os.path.exists(COOKIES_FILE):
            cmd.extend(["--cookies", COOKIES_FILE])

        local_path = await download_external_tool(
            cmd, job_dir, status_msg, "در حال دانلود رسانه با yt-dlp..."
        )
        real_name = os.path.basename(local_path)
        _, real_ext = os.path.splitext(real_name)
        active_download_ext = real_ext
        if active_final_filename == "video.mp4":
            active_final_filename = sanitize_filename(real_name)
        else:
            active_final_filename = build_filename_with_ext(active_final_filename, real_ext)
        return local_path, active_final_filename

    # ۴. لینک مستقیم HTTP/HTTPS
    parsed_path = unquote(urlparse(url).path)
    orig_name = os.path.basename(parsed_path) or f"url_file_{int(time.time())}.bin"
    initial_name, orig_ext = resolve_initial_filename(orig_name, explicit_name)
    active_download_ext = orig_ext
    active_final_filename = initial_name
    local_path = os.path.join(job_dir, initial_name)

    start_t = time.time()
    await status_msg.edit_text(
        f"🌐 شروع دانلود از لینک مستقیم:\n`{active_final_filename}`",
        reply_markup=get_cancel_kb()
    )

    req_headers = dict(BROWSER_HEADERS)
    if "github.com" in host and GITHUB_TOKEN:
        req_headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    async with httpx.AsyncClient(follow_redirects=True, timeout=60, headers=req_headers) as http:
        async with http.stream("GET", url) as resp:
            if resp.status_code == 404:
                raise Exception(f"لینک ارسالی وجود ندارد یا از سرور مبدا حذف شده است (404 Not Found):\n{url}")
            resp.raise_for_status()
            total = int(resp.headers.get("Content-Length", 0))
            current = 0
            with open(local_path, "wb") as f:
                async for chunk in resp.aiter_bytes(chunk_size=1024 * 512):
                    if cancel_requested:
                        raise asyncio.CancelledError("عملیات توسط کاربر لغو شد.")
                    f.write(chunk)
                    current += len(chunk)

                    now = time.time()
                    if now - getattr(status_msg, "_last_edit", 0) >= 4:
                        status_msg._last_edit = now
                        elapsed = now - start_t
                        speed = current / elapsed if elapsed > 0 else 0
                        pct = (current * 100 / total) if total else 0
                        try:
                            await status_msg.edit_text(
                                f"🌐 **در حال دانلود از لینک به سرور...**\n\n"
                                f"📄 نام نهایی: `{active_final_filename}`\n"
                                f"📊 پیشرفت: `{pct:.1f}%`\n"
                                f"📦 دریافت شده: `{human_size(current)}` از `{human_size(total) if total else 'نامشخص'}`\n"
                                f"🚀 سرعت: `{human_size(speed)}/s`\n\n"
                                f"💡 *(برای تغییر نام، همین الان نام جدید را در چت بفرستید)*",
                                reply_markup=get_cancel_kb()
                            )
                        except Exception:
                            pass

    return local_path, active_final_filename


@app.on_message(filters.command(["start", "help"]) & filters.user(ADMIN_ID))
async def start_cmd(client: Client, message: Message):
    help_text = (
        "👋 **به ربات پیشرفته آپلودر گیت‌هاب خوش آمدید!**\n\n"
        "📌 **۱. آپلود تکی (پشتیبانی خودکار از فایل‌های بالای ۲ گیگابایت):**\n"
        "• هر فایل، ویدیو یا لینکی (**مستقیم، Mega.nz، Google Drive، یوتیوب، توییتر و...**) بفرستید.\n"
        "• اگر حجم فایل یا لینک **بیشتر از ۲ گیگابایت** باشد، ربات به طور خودکار آن را در حالت سریع `Store` به پارت‌های `1900MB` (`.zip.001`, `.zip.002`) تقسیم و آپلود می‌کند که مستقیماً با **WinRAR / 7-Zip** باز می‌شوند!\n\n"
        "📦 **۲. آپلود گروهی و زیپ چندبخشی (`/batch`):**\n"
        "• **حالت اول (بدون فشرده‌سازی):**\n"
        "  `/batch MyCourse`\n"
        "  تمام فایل‌ها و لینک‌های بعدی را با فرمت اصلی در یک Release واحد آپلود می‌کند.\n"
        "• **حالت دوم (تبدیل همه به یک فایل زیپ چندبخشی):**\n"
        "  `/batch zip` *(بدون رمز و با نام خودکار)*\n"
        "  `/batch zip MyArchive` *(بدون رمز و با نام دلخواه)*\n"
        "  `/batch zip 1234 MyArchive` *(با رمز `1234` و نام دلخواه)*\n"
        "  `/batch zip -p 1234` *(با رمز `1234` و نام خودکار)*\n"
        "• `/pop` : حذف آخرین فایل/لینک اضافه‌شده به صف `/batch`\n"
        "• `/done` : نمایش پیش‌نمایش صف و شروع دانلود و آپلود گروهی\n\n"
        "🔐 **۳. زیپ کردن تک‌فایل (`/zip`):**\n"
        "• `/zip` *(زیپ کردن فایل بعدی بدون رمز)*\n"
        "• `/zip 1234` *(زیپ کردن فایل بعدی با رمز `1234`)*\n\n"
        "✏️ **۴. روش‌های تغییر نام فایل (۵ روش):**\n"
        "• ارسال نام قبل از فایل | ارسال نام در حین دانلود | خط دوم لینک | کپشن فایل ارسالی | دکمه `✏️ تغییر نام در گیت‌هاب` بعد از آپلود.\n\n"
        "🛠 **۵. دستورات مدیریتی:**\n"
        "• `/files` : مشاهده ۱۰ ریلیز آخر، ویرایش نام و حذف\n"
        "• `/search <نام>` : جستجو در فایل‌های گیت‌هاب\n"
        "• `/cookies` : ریپلای روی `cookies.txt` برای یوتیوب/اینستاگرام\n"
        "• `/cancel` : لغو عملیات یا پاک کردن صف `/batch`\n"
        "• `/update` : آپدیت خودکار کد از گیت‌هاب (`git pull`) و ری‌استارت"
    )
    await message.reply_text(help_text, disable_web_page_preview=True)


@app.on_message(filters.command("batch") & filters.user(ADMIN_ID))
async def batch_start_cmd(client: Client, message: Message):
    global pending_custom_name
    pending_custom_name = None
    zip_mode, pwd, title = parse_batch_args(message.command[1:])
    batch_state["active"] = True
    batch_state["zip_mode"] = zip_mode
    batch_state["password"] = pwd
    batch_state["title"] = title
    batch_state["queue"] = []

    mode_str = "🗜 **تبدیل همه به آرشیو ZIP یکپارچه (حالت Store)**" if zip_mode else "📂 **آپلود مستقیم فایل‌ها بدون فشرده‌سازی**"
    pw_str = f"\n🔑 **رمز عبور آرشیو:** `{pwd}`" if pwd else ("\n🔓 **رمز عبور:** ندارد" if zip_mode else "")

    await message.reply_text(
        f"📚 **صف آپلود گروهی (Batch Queue) فعال شد!**\n\n"
        f"🏷 **نام مجموعه:** `{title}`\n"
        f"⚙️ **حالت:** {mode_str}{pw_str}\n\n"
        f"📥 حالا فایل‌ها را فوروارد/ارسال کنید یا لینک‌ها را بفرستید تا در صف قرار بگیرند.\n"
        f"🚀 در پایان دستور `/done` را بزنید (یا با `/pop` آخرین مورد صف را حذف کنید).\n"
        f"*(برای لغو کامل، دستور `/cancel` را بزنید)*"
    )


@app.on_message(filters.command("pop") & filters.user(ADMIN_ID))
async def batch_pop_cmd(client: Client, message: Message):
    if not batch_state["active"] or not batch_state["queue"]:
        return await message.reply_text("ℹ️ صف گروهی خالی است.")
    removed = batch_state["queue"].pop()
    label = removed.get("name") or removed.get("url") or "آیتم"
    await message.reply_text(f"🗑 آیتم آخر (`{label}`) از صف حذف شد. تعداد باقی‌مانده در صف: `{len(batch_state['queue'])}`")


@app.on_message(filters.command("done") & filters.user(ADMIN_ID))
async def batch_done_cmd(client: Client, message: Message):
    global cancel_requested, active_final_filename, pending_custom_name
    pending_custom_name = None

    if not batch_state["active"]:
        return await message.reply_text("ℹ️ حالت گروهی فعال نیست. برای شروع از `/batch` یا `/batch zip` استفاده کنید.")

    queue = list(batch_state["queue"])
    zip_mode = batch_state["zip_mode"]
    pwd = batch_state["password"]
    title = batch_state["title"]

    batch_state["active"] = False
    batch_state["queue"] = []

    if not queue:
        return await message.reply_text("📭 صف گروهی خالی بود و بسته شد.")

    known_bytes = sum(item.get("size", 0) for item in queue)
    est_parts = max(1, (known_bytes // CHUNK_SIZE_BYTES) + 1) if known_bytes > 0 else "نامشخص (شامل لینک)"
    preview_text = (
        f"📊 **پیش‌نمایش صف گروهی «{title}»:**\n\n"
        f"🔢 **تعداد آیتم‌ها در صف:** `{len(queue)} مورد`\n"
        f"📦 **حجم فایل‌های تلگرامی:** `{human_size(known_bytes)}`\n"
        f"🧩 **تعداد پارت‌های تخمینی:** `{est_parts}` (سقف هر پارت `{CHUNK_SIZE_MB}MB`)\n"
        f"⚙️ **نوع پردازش:** `{'آرشیو یکپارچه ZIP' if zip_mode else 'آپلود مجزا در یک ریلیز'}`\n\n"
        f"⏳ در حال شروع دانلود و پردازش صف..."
    )
    status_msg = await message.reply_text(preview_text, reply_markup=get_cancel_kb())

    job_dir = os.path.join(DOWNLOAD_DIR, f"batch_{int(time.time()*1000)}")
    os.makedirs(job_dir, exist_ok=True)
    repo_url = f"https://github.com/{GITHUB_REPO}"

    async with upload_lock:
        cancel_requested = False
        try:
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            release_title = f"📦 {title} | {now_str}"

            async with httpx.AsyncClient(timeout=None) as http:
                body_text = (
                    f"Batch Release created on {now_str}\n"
                    f"Title: **{title}** | Items: `{len(queue)}`\n"
                )
                if zip_mode:
                    body_text += "\n💡 **نحوه باز کردن:** تمام پارت‌ها را دانلود کرده و فایل اول (`.zip` یا `.zip.001`) را با **WinRAR** یا **7-Zip** استخراج کنید."

                release_id, upload_url, tag_name, release_html_url = await create_github_release(
                    http, release_title, body_text, status_msg
                )

                try:
                    uploaded_items = []
                    failed_items = []
                    total_bytes_processed = 0

                    # حالت الف: دانلود همه آیتم‌ها و ساخت آرشیو ZIP یکپارچه
                    if zip_mode:
                        downloaded_paths = []
                        for idx, item in enumerate(queue, 1):
                            if cancel_requested:
                                raise asyncio.CancelledError()

                            try:
                                if item["type"] == "tg":
                                    tg_msg: Message = item["msg"]
                                    fname = item["name"]
                                    active_final_filename = fname
                                    fpath = os.path.join(job_dir, f"{idx:02d}_{fname}")
                                    start_t = time.time()
                                    await tg_msg.download(
                                        file_name=fpath,
                                        progress=tg_progress_bar,
                                        progress_args=(client, status_msg, start_t, f"دانلود آیتم {idx} از {len(queue)}...")
                                    )
                                    downloaded_paths.append(fpath)
                                    total_bytes_processed += os.path.getsize(fpath)
                                else:
                                    fpath, fname = await download_url_to_disk(item["url"], item["name"], job_dir, status_msg)
                                    ranked_path = os.path.join(job_dir, f"{idx:02d}_{fname}")
                                    shutil.move(fpath, ranked_path)
                                    downloaded_paths.append(ranked_path)
                                    total_bytes_processed += os.path.getsize(ranked_path)
                            except asyncio.CancelledError:
                                raise
                            except Exception as item_err:
                                failed_items.append(f"آیتم {idx}: {str(item_err)[:120]}")

                        if not downloaded_paths:
                            raise Exception("هیچ‌کدام از آیتم‌های صف قابل دانلود نبودند:\n" + "\n".join(failed_items))

                        parts = await create_store_zip_volumes(
                            downloaded_paths, job_dir, title, pwd, False, status_msg
                        )

                        for p_idx, part_path in enumerate(parts, 1):
                            pname = os.path.basename(part_path)
                            plabel = f"(پارت {p_idx} از {len(parts)})"
                            aid, durl, fsize = await upload_asset_to_release(
                                http, upload_url, part_path, pname, status_msg, plabel
                            )
                            uploaded_items.append((pname, durl, fsize, aid))

                    # حالت ب: آپلود تک‌به‌تک بدون فشرده‌سازی (مگر اینکه فایلی بالای 2GB باشد)
                    else:
                        for idx, item in enumerate(queue, 1):
                            if cancel_requested:
                                raise asyncio.CancelledError()

                            item_dir = os.path.join(job_dir, f"item_{idx}")
                            os.makedirs(item_dir, exist_ok=True)

                            try:
                                if item["type"] == "tg":
                                    tg_msg: Message = item["msg"]
                                    fname = item["name"]
                                    active_final_filename = fname
                                    fpath = os.path.join(item_dir, fname)
                                    start_t = time.time()
                                    await tg_msg.download(
                                        file_name=fpath,
                                        progress=tg_progress_bar,
                                        progress_args=(client, status_msg, start_t, f"دانلود فایل {idx} از {len(queue)}...")
                                    )
                                else:
                                    fpath, fname = await download_url_to_disk(item["url"], item["name"], item_dir, status_msg)
                                    clean_p = os.path.join(item_dir, fname)
                                    if fpath != clean_p:
                                        shutil.move(fpath, clean_p)
                                        fpath = clean_p

                                fsize = os.path.getsize(fpath)
                                total_bytes_processed += fsize

                                if fsize > CHUNK_SIZE_BYTES:
                                    parts = await create_store_zip_volumes([fpath], item_dir, fname, pwd, True, status_msg)
                                else:
                                    parts = [fpath]

                                for part_path in parts:
                                    pname = os.path.basename(part_path)
                                    existing_names = [x[0] for x in uploaded_items]
                                    if pname in existing_names:
                                        pname = f"{idx:02d}_{pname}"
                                    aid, durl, psize = await upload_asset_to_release(
                                        http, upload_url, part_path, pname, status_msg, f"(آیتم {idx}/{len(queue)})"
                                    )
                                    uploaded_items.append((pname, durl, psize, aid))
                            except asyncio.CancelledError:
                                raise
                            except Exception as item_err:
                                failed_items.append(f"آیتم {idx}: {str(item_err)[:120]}")
                            finally:
                                shutil.rmtree(item_dir, ignore_errors=True)

                        if not uploaded_items:
                            raise Exception("هیچ‌کدام از آیتم‌های صف آپلود نشدند:\n" + "\n".join(failed_items))

                    all_links = [x[1] for x in uploaded_items]
                    links_txt_url = await upload_links_txt_asset(http, upload_url, job_dir, all_links, status_msg)

                except Exception as e:
                    await http.delete(f"https://api.github.com/repos/{GITHUB_REPO}/releases/{release_id}", headers=GH_HEADERS)
                    await http.delete(f"https://api.github.com/repos/{GITHUB_REPO}/git/refs/tags/{tag_name}", headers=GH_HEADERS)
                    raise e

            pw_line = f"\n🔐 **رمز عبور آرشیو:** `{pwd}`" if pwd else ""
            lines = [
                f"🎉 **آپلود گروهی «{title}» با موفقیت کامل شد!**\n",
                f"📦 **مجموع حجم:** `{human_size(total_bytes_processed)}` | **تعداد خروجی:** `{len(uploaded_items)} فایل`{pw_line}\n"
            ]
            for idx, (fname, durl, fsize, _) in enumerate(uploaded_items, 1):
                lines.append(f"{idx}. 📄 `{fname}` (`{human_size(fsize)}`)\n`{durl}`")

            if links_txt_url:
                lines.append(f"\n📋 **فایل لیست تمام لینک‌ها (links.txt برای IDM):**\n`{links_txt_url}`")

            if failed_items:
                lines.append("\n⚠️ **موارد رد شده (خطا در دانلود):**")
                for fl in failed_items:
                    lines.append(f"• `{fl}`")

            lines.append(f"\n🏛 **صفحه ریلیز:** [مشاهده در گیت‌هاب]({release_html_url})")
            lines.append(f"📂 **لینک مخزن (Repo):** `{repo_url}`")

            await status_msg.edit_text(
                "\n".join(lines),
                reply_markup=get_file_manage_kb(uploaded_items[0][1], release_id, None, release_html_url),
                disable_web_page_preview=True
            )

        except (asyncio.CancelledError, StopTransmission):
            await status_msg.edit_text("🛑 **عملیات گروهی لغو شد و تمام فایل‌های موقت از سرور پاک گردیدند.**")
        except Exception as e:
            await status_msg.edit_text(f"❌ **خطا در پردازش گروهی:**\n`{str(e)}`")
        finally:
            shutil.rmtree(job_dir, ignore_errors=True)


@app.on_message(filters.command("zip") & filters.user(ADMIN_ID))
async def zip_cmd(client: Client, message: Message):
    global pending_zip_mode, pending_zip_password
    pending_zip_mode = True
    if len(message.command) >= 2:
        pending_zip_password = message.command[1].strip()
        await message.reply_text(
            f"🔐 **حالت زیپ با رمز `{pending_zip_password}` برای فایل/لینک بعدی فعال شد!**\n\n"
            f"حالا فایل یا لینک خود را بفرستید (حتی اگر بالای ۲ گیگابایت باشد، خودکار به پارت‌های `.zip.001` تقسیم و با WinRAR باز می‌شود)."
        )
    else:
        pending_zip_password = None
        await message.reply_text(
            "🗜 **حالت زیپ (بدون رمز) برای فایل/لینک بعدی فعال شد!**\n\n"
            "حالا فایل یا لینک خود را بفرستید تا در حالت سریع Store زیپ و آپلود شود.\n"
            "*(اگر می‌خواهید رمز هم داشته باشد، بزنید: `/zip 1234`)*"
        )


@app.on_message(filters.command("cookies") & filters.user(ADMIN_ID))
async def save_cookies_cmd(client: Client, message: Message):
    if not message.reply_to_message or not message.reply_to_message.document:
        return await message.reply_text(
            "🍪 برای تنظیم کوکی یوتیوب/اینستاگرام، فایل `cookies.txt` را در چت بفرستید و روی آن `/cookies` را ریپلای کنید."
        )
    await message.reply_to_message.download(file_name=COOKIES_FILE)
    await message.reply_text("✅ فایل `cookies.txt` ذخیره شد.")


@app.on_message(filters.command("cancel") & filters.user(ADMIN_ID))
async def cancel_cmd(client: Client, message: Message):
    global cancel_requested, pending_custom_name, pending_zip_mode, pending_zip_password, awaiting_gh_rename_asset_id
    pending_custom_name = None
    pending_zip_mode = False
    pending_zip_password = None
    awaiting_gh_rename_asset_id = None
    batch_state["active"] = False
    batch_state["queue"] = []

    if upload_lock.locked():
        cancel_requested = True
        await message.reply_text("🛑 درخواست لغو ثبت شد؛ عملیات در حال توقف و پاکسازی است...")
    else:
        await message.reply_text("ℹ️ تمام وضعیت‌های موقت و صف `/batch` پاکسازی شدند.")


@app.on_callback_query(filters.regex(r"^cancel_task$") & filters.user(ADMIN_ID))
async def cancel_cb(client: Client, cb: CallbackQuery):
    global cancel_requested
    if upload_lock.locked():
        cancel_requested = True
        await cb.answer("🛑 درخواست لغو عملیات ارسال شد...", show_alert=True)
    else:
        await cb.answer("عملیاتی در جریان نیست.", show_alert=True)


@app.on_callback_query(filters.regex(r"^ren_(\d+)$") & filters.user(ADMIN_ID))
async def rename_gh_asset_cb(client: Client, cb: CallbackQuery):
    global awaiting_gh_rename_asset_id
    asset_id = int(cb.matches[0].group(1))
    awaiting_gh_rename_asset_id = asset_id
    await cb.answer()
    await cb.message.reply_text(
        f"✏️ **تغییر نام مستقیم در گیت‌هاب (Asset ID: `{asset_id}`)**\n\n"
        f"لطفاً نام جدید فایل را (همراه با پسوند) ارسال کنید:\n"
        f"*(برای انصراف `/cancel` را بزنید)*"
    )


@app.on_message(filters.command("files") & filters.user(ADMIN_ID))
async def list_files_cmd(client: Client, message: Message):
    status = await message.reply_text("🔍 در حال دریافت لیست ۱۰ ریلیز آخر از گیت‌هاب...")
    async with httpx.AsyncClient(timeout=30) as http:
        resp = await http.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/releases?per_page=10",
            headers=GH_HEADERS
        )
        if resp.status_code != 200:
            return await status.edit_text(f"❌ خطا در دریافت لیست: `{resp.status_code}`")

        releases = resp.json()
        if not releases:
            return await status.edit_text("📭 هیچ فایلی در بخش Releases یافت نشد.")

        await status.delete()
        for rel in releases:
            rel_id = rel["id"]
            rel_html = rel.get("html_url", f"https://github.com/{GITHUB_REPO}/releases")
            title = rel["name"] or rel["tag_name"]
            assets = rel.get("assets", [])

            if len(assets) == 1:
                a = assets[0]
                text = (
                    f"📄 **{title}**\n"
                    f"📦 حجم: `{human_size(a['size'])}` | 📥 دانلود: `{a.get('download_count', 0)} بار`\n"
                    f"🔗 `{a['browser_download_url']}`\n"
                    f"🏛 [صفحه ریلیز در گیت‌هاب]({rel_html})"
                )
                kb = get_file_manage_kb(a["browser_download_url"], rel_id, a["id"], rel_html)
            elif len(assets) > 1:
                lines = [f"📚 **{title}** ({len(assets)} فایل):"]
                buttons = []
                for idx, a in enumerate(assets, 1):
                    lines.append(
                        f"{idx}. `{a['name']}` (`{human_size(a['size'])}` | 📥 {a.get('download_count', 0)})\n"
                        f"🔗 `{a['browser_download_url']}`"
                    )
                    if idx <= 5 and a["name"] != "links.txt":
                        buttons.append([InlineKeyboardButton(f"✏️ تغییر نام فایل {idx}", callback_data=f"ren_{a['id']}")])
                lines.append(f"\n🏛 [مشاهده صفحه Release در گیت‌هاب]({rel_html})")
                buttons.append([InlineKeyboardButton("🗑 حذف کل این Release", callback_data=f"del_{rel_id}")])
                text = "\n".join(lines)
                kb = InlineKeyboardMarkup(buttons)
            else:
                text = f"📄 **{title}** (بدون فایل ضمیمه)"
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("🗑 حذف از گیت‌هاب", callback_data=f"del_{rel_id}")]])

            await message.reply_text(text, reply_markup=kb, disable_web_page_preview=True)


@app.on_message(filters.command("search") & filters.user(ADMIN_ID))
async def search_files_cmd(client: Client, message: Message):
    if len(message.command) < 2:
        return await message.reply_text("⚠️ لطفاً عبارت مورد نظر را وارد کنید.\nمثال: `/search movie`")

    query = " ".join(message.command[1:]).lower()
    status = await message.reply_text(f"🔍 در حال جستجوی `{query}` در گیت‌هاب...")

    async with httpx.AsyncClient(timeout=30) as http:
        resp = await http.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/releases?per_page=100",
            headers=GH_HEADERS
        )
        if resp.status_code != 200:
            return await status.edit_text(f"❌ خطا در ارتباط با گیت‌هاب: `{resp.status_code}`")

        releases = resp.json()
        matched = [
            rel for rel in releases
            if query in (rel.get("name") or rel.get("tag_name") or "").lower()
            or any(query in a["name"].lower() for a in rel.get("assets", []))
        ]

        if not matched:
            return await status.edit_text(f"📭 هیچ فایلی شامل عبارت `{query}` پیدا نشد.")

        await status.delete()
        for rel in matched[:10]:
            rel_id = rel["id"]
            rel_html = rel.get("html_url", f"https://github.com/{GITHUB_REPO}/releases")
            title = rel["name"] or rel["tag_name"]
            assets = rel.get("assets", [])
            if assets:
                a = assets[0]
                text = (
                    f"📄 **{title}**\n"
                    f"📦 حجم: `{human_size(a['size'])}` | 📥 دانلود: `{a.get('download_count', 0)} بار`\n"
                    f"🔗 `{a['browser_download_url']}`\n"
                    f"🏛 [صفحه ریلیز در گیت‌هاب]({rel_html})"
                )
                kb = get_file_manage_kb(a["browser_download_url"], rel_id, a["id"], rel_html)
            else:
                text = f"📄 **{title}**"
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("🗑 حذف از گیت‌هاب", callback_data=f"del_{rel_id}")]])

            await message.reply_text(text, reply_markup=kb, disable_web_page_preview=True)


async def handle_Incoming_url(message: Message, url: str, explicit_name: str | None):
    """افزودن لینک به صف /batch یا دانلود و آپلود فوری آن"""
    global cancel_requested, pending_custom_name

    if batch_state["active"]:
        parsed_path = unquote(urlparse(url).path)
        orig_name = os.path.basename(parsed_path) or f"link_{len(batch_state['queue'])+1}.bin"
        has_custom = bool(explicit_name or pending_custom_name)
        chosen_name, _ = resolve_initial_filename(orig_name, explicit_name)
        batch_state["queue"].append({
            "type": "url",
            "url": url,
            "name": chosen_name if has_custom else None,
            "size": 0
        })
        return await message.reply_text(
            f"➕ **لینک شماره `{len(batch_state['queue'])}` به صف گروهی «{batch_state['title']}» اضافه شد!**\n"
            f"🔗 `{url}`\n"
            f"📄 نام تنظیم‌شده: `{chosen_name if has_custom else 'خودکار'}`\n\n"
            f"*(مورد بعدی را بفرستید، با `/pop` حذف کنید، یا برای شروع `/done` را بزنید)*",
            disable_web_page_preview=True
        )

    status_msg = await message.reply_text("⏳ در صف انتظار برای پردازش لینک...")
    job_dir = os.path.join(DOWNLOAD_DIR, f"job_{int(time.time()*1000)}")
    os.makedirs(job_dir, exist_ok=True)

    async with upload_lock:
        cancel_requested = False
        try:
            local_path, _ = await download_url_to_disk(url, explicit_name, job_dir, status_msg)
            await process_and_upload_single_target(local_path, status_msg)
        except asyncio.CancelledError:
            await status_msg.edit_text("🛑 **عملیات دانلود/آپلود با موفقیت لغو شد و فایل موقت پاک گردید.**")
        except Exception as e:
            await status_msg.edit_text(f"❌ **خطا در پردازش لینک:**\n`{str(e)}`")
        finally:
            shutil.rmtree(job_dir, ignore_errors=True)


@app.on_message(filters.command("url") & filters.user(ADMIN_ID))
async def url_leech_cmd(client: Client, message: Message):
    text_lines = message.text.strip().splitlines()
    first_line_parts = text_lines[0].split(maxsplit=2)

    if len(first_line_parts) < 2:
        return await message.reply_text(
            "⚠️ لطفاً لینک فایل را وارد کنید یا مستقیم لینک را بدون دستور بفرستید.\n"
            "مثال همراه با نام در خط دوم:\n"
            "`https://example.com/bigfile.iso`\n"
            "`ubuntu_image.iso`"
        )

    url = first_line_parts[1].strip()
    custom_name = None
    if len(text_lines) > 1 and text_lines[1].strip():
        custom_name = text_lines[1].strip()
    elif len(first_line_parts) > 2:
        custom_name = first_line_parts[2].strip()

    await handle_Incoming_url(message, url, custom_name)


@app.on_message(filters.command("update") & filters.user(ADMIN_ID))
async def update_bot_cmd(client: Client, message: Message):
    status = await message.reply_text("🔄 در حال دریافت آخرین نسخه کد از گیت‌هاب (`git pull`)...")
    try:
        proc = subprocess.run(["git", "pull"], capture_output=True, text=True, timeout=30)
        output = (proc.stdout + "\n" + proc.stderr).strip()

        if proc.returncode != 0:
            return await status.edit_text(f"❌ **خطا در اجرای git pull:**\n`{output}`")

        await status.edit_text(
            f"✅ **کد با موفقیت بروزرسانی شد!**\n\n"
            f"📋 خروجی:\n`{output}`\n\n"
            f"♻️ ربات در حال ری‌استارت شدن است..."
        )
        os._exit(0)
    except Exception as e:
        await status.edit_text(f"❌ **خطا در بروزرسانی:**\n`{str(e)}`")


@app.on_message((filters.video | filters.document | filters.audio) & filters.user(ADMIN_ID))
async def handle_media(client: Client, message: Message):
    global cancel_requested, active_download_ext, active_final_filename
    media = message.video or message.document or message.audio
    file_size = media.file_size or 0

    tag_time = datetime.now().strftime("%Y%m%d-%H%M%S-%f")[:19]
    orig_name = getattr(media, "file_name", None)
    if not orig_name:
        ext = ".mp4" if message.video else (".mp3" if message.audio else ".bin")
        orig_name = f"media_{datetime.now().strftime('%Y%m%d_%H%M%S')}{ext}"

    is_forwarded = bool(message.forward_date or message.forward_from or message.forward_from_chat or message.forward_sender_name)
    allowed_caption = None if is_forwarded else message.caption
    initial_name, orig_ext = resolve_initial_filename(orig_name, allowed_caption)

    if batch_state["active"]:
        batch_state["queue"].append({
            "type": "tg",
            "msg": message,
            "name": initial_name,
            "size": file_size
        })
        total_q_size = sum(x.get("size", 0) for x in batch_state["queue"])
        return await message.reply_text(
            f"➕ **فایل شماره `{len(batch_state['queue'])}` به صف «{batch_state['title']}» اضافه شد!**\n"
            f"📄 نام: `{initial_name}` (`{human_size(file_size)}`)\n"
            f"📊 جمع حجم صف تا الان: `{human_size(total_q_size)}`\n\n"
            f"*(مورد بعدی را بفرستید، با `/pop` حذف کنید، یا برای شروع `/done` را بزنید)*"
        )

    job_dir = os.path.join(DOWNLOAD_DIR, f"tg_{tag_time}")
    os.makedirs(job_dir, exist_ok=True)
    local_path = os.path.join(job_dir, initial_name)

    status_msg = await message.reply_text("⏳ در صف انتظار برای پردازش...")

    async with upload_lock:
        cancel_requested = False
        active_download_ext = orig_ext
        active_final_filename = initial_name

        try:
            start_t = time.time()
            await status_msg.edit_text(
                f"📥 شروع دانلود `{active_final_filename}` از تلگرام...\n\n"
                f"💡 *(می‌توانید در حین دانلود، نام جدید را در چت بفرستید)*",
                reply_markup=get_cancel_kb()
            )
            dl_result = await message.download(
                file_name=local_path,
                progress=tg_progress_bar,
                progress_args=(client, status_msg, start_t, "در حال دانلود از تلگرام به سرور...")
            )

            if cancel_requested or not dl_result or not os.path.exists(local_path):
                raise asyncio.CancelledError("عملیات توسط کاربر لغو شد.")

            await process_and_upload_single_target(local_path, status_msg)

        except (asyncio.CancelledError, StopTransmission):
            await status_msg.edit_text("🛑 **عملیات دانلود/آپلود با موفقیت لغو شد و فایل موقت از سرور پاک گردید.**")
        except Exception as e:
            await status_msg.edit_text(f"❌ **خطا در پردازش فایل:**\n`{str(e)}`")
        finally:
            shutil.rmtree(job_dir, ignore_errors=True)


@app.on_message(filters.text & filters.user(ADMIN_ID) & ~filters.regex(r"^/"))
async def handle_plain_text_or_links(client: Client, message: Message):
    """مدیریت پیام‌های متنی: تغییر نام در گیت‌هاب، تشخیص لینک، یا تغییر نام قبل/حین دانلود"""
    global pending_custom_name, active_final_filename, active_download_ext, awaiting_gh_rename_asset_id

    lines = [line.strip() for line in message.text.strip().splitlines() if line.strip()]
    if not lines:
        return
    first_line = lines[0]

    # ۱. تغییر نام مستقیم در گیت‌هاب
    if awaiting_gh_rename_asset_id and not first_line.startswith("http"):
        asset_id = awaiting_gh_rename_asset_id
        awaiting_gh_rename_asset_id = None
        new_asset_name = sanitize_filename(first_line)
        status = await message.reply_text(f"⚙️ در حال تغییر نام فایل در گیت‌هاب به `{new_asset_name}`...")

        async with httpx.AsyncClient(timeout=30) as http:
            resp = await http.patch(
                f"https://api.github.com/repos/{GITHUB_REPO}/releases/assets/{asset_id}",
                headers=GH_HEADERS,
                json={"name": new_asset_name}
            )
            if resp.status_code == 200:
                data = resp.json()
                return await status.edit_text(
                    f"✅ **نام فایل در گیت‌هاب بدون آپلود مجدد تغییر یافت!**\n\n"
                    f"📄 **نام جدید:** `{data['name']}`\n"
                    f"🔗 **لینک مستقیم جدید:**\n`{data['browser_download_url']}`",
                    disable_web_page_preview=True
                )
            else:
                return await status.edit_text(f"❌ خطا در تغییر نام: `{resp.status_code}`\n`{resp.text}`")

    # ۲. تشخیص لینک مستقیم، مگا، گوگل درایو، یوتیوب و... (فقط اگر خط اول واقعاً لینک باشد)
    if first_line.startswith("http://") or first_line.startswith("https://"):
        parts = first_line.split(maxsplit=1)
        url = parts[0]
        custom_name = None
        if len(lines) > 1:
            custom_name = lines[1]
        elif len(parts) > 1:
            custom_name = parts[1]

        return await handle_Incoming_url(message, url, custom_name)

    # ۳. تغییر نام در حین دانلود (Live Rename)
    if upload_lock.locked():
        new_name = build_filename_with_ext(first_line, active_download_ext)
        active_final_filename = new_name
        return await message.reply_text(
            f"✏️ **نام فایل در حال دانلود تغییر یافت به:**\n`{new_name}`"
        )

    # ۴. ذخیره نام برای فایل/لینک بعدی (هم در حالت عادی و هم داخل صف /batch)
    pending_custom_name = sanitize_filename(first_line)
    await message.reply_text(
        f"📌 **نام `{pending_custom_name}` برای فایل/لینک بعدی ذخیره شد!**\n"
        f"حالا فایل یا لینک خود را ارسال کنید."
    )


@app.on_callback_query(filters.regex(r"^del_(\d+)$") & filters.user(ADMIN_ID))
async def delete_release_cb(client: Client, cb: CallbackQuery):
    release_id = cb.matches[0].group(1)
    await cb.answer("در حال حذف از گیت‌هاب...")

    async with httpx.AsyncClient(timeout=30) as http:
        get_resp = await http.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/releases/{release_id}",
            headers=GH_HEADERS
        )
        tag_name = get_resp.json().get("tag_name") if get_resp.status_code == 200 else None

        del_resp = await http.delete(
            f"https://api.github.com/repos/{GITHUB_REPO}/releases/{release_id}",
            headers=GH_HEADERS
        )

        if del_resp.status_code == 204:
            if tag_name:
                await http.delete(
                    f"https://api.github.com/repos/{GITHUB_REPO}/git/refs/tags/{tag_name}",
                    headers=GH_HEADERS
                )
            await cb.message.edit_text("🗑 **این فایل و Release مربوط به آن به طور کامل از گیت‌هاب حذف شد.**")
        else:
            await cb.answer(f"❌ خطا در حذف: {del_resp.status_code}", show_alert=True)


if __name__ == "__main__":
    print("🚀 GitHub Uploader Bot is running...")
    app.run()
