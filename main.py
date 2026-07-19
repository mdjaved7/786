import os
import re
import time
import logging
import asyncio
import tempfile
import aiohttp
from typing import Dict, Any, Optional, Tuple

# Telethon Imports
from telethon import TelegramClient, events
from telethon.tl.types import DocumentAttributeAudio, Message
from telethon.errors import FloodWaitError, MessageNotModifiedError

# Mutagen Metadata Imports
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, TIT2, TPE1, TALB, APIC, error as ID3Error
from mutagen.mp4 import MP4, MP4Cover

# --- Logging Configuration ---
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("TaggerBot")

# --- Environment Variables ---
API_ID = 34801155
API_HASH = "d7846c4d0f2c343dd5b67c80d45409e8"
BOT_TOKEN = "8949289098:AAHtP1BrSBVXWCLhV-rOb0nLmeh0u11qTqM"

# --- Constants ---
ARTIST_NAME = "@AllstoryFM2 JOIN"
STATE_TTL_SECONDS = 600
ALLOWED_EXTENSIONS = {'.mp3', '.m4a', '.mp4'}

# --- State Management & Queue System ---
pending_files: Dict[int, list] = {}  # Now a LIST per chat_id (multiple files before URLs)
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(5)

class ChatQueue:
    """Production Grade Queue Manager with Locks and Cancellation"""
    def __init__(self):
        self.lock = asyncio.Lock()
        self.condition = asyncio.Condition()
        self.active_tasks: set = set()
        
        self.cancelled = False
        self.next_assign_seq = 0
        self.current_upload_seq = 0
        
        self.total_count = 0
        self.completed_count = 0
        self.processed_count = 0
        self.failed_count = 0
        self.current_uploading = "None"
        
        self.queue_message = None
        self.last_dashboard_update = 0.0
        self.last_completed_count = 0

chat_queues: Dict[int, ChatQueue] = {}

def get_chat_queue(chat_id: int) -> ChatQueue:
    if chat_id not in chat_queues:
        chat_queues[chat_id] = ChatQueue()
    return chat_queues[chat_id]

# --- Native Async Health Check Web Server ---
from aiohttp import web

async def health_check_handler(request: web.Request) -> web.Response:
    return web.Response(text="Bot is alive and running successfully!", content_type="text/plain")

async def start_health_server() -> None:
    app = web.Application()
    app.router.add_get('/', health_check_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"Health check server active on port {port}")

async def track_and_expire_states() -> None:
    while True:
        await asyncio.sleep(60)
        now = time.time()
        expired = [
            cid for cid, queue in pending_files.items()
            if queue and now - queue[-1].get("timestamp", 0) > STATE_TTL_SECONDS
        ]
        for cid in expired:
            pending_files.pop(cid, None)

# --- Helpers ---
def sanitize_filename(filename: str) -> str:
    name = os.path.basename(filename or "audio.mp3")
    return re.sub(r'[\\/*?:"<>|]', "", name)

def extract_episode_number(filename: str, caption: str = "") -> str:
    for text in [filename, caption]:
        match = re.search(r'(?:ep|episode|story)[-_\s]*(\d+)', text, re.IGNORECASE)
        if match: return match.group(1)
    fallback = re.search(r'\d+', filename)
    if fallback: return fallback.group()
    if caption:
        fallback_cap = re.search(r'\d+', caption)
        if fallback_cap: return fallback_cap.group()
    return "Unknown"

def get_image_mime_and_format(data: bytes) -> Tuple[str, int]:
    if data.startswith(b'\xff\xd8'):
        return 'image/jpeg', MP4Cover.FORMAT_JPEG
    elif data.startswith(b'\x89PNG\r\n\x1a\n'):
        return 'image/png', MP4Cover.FORMAT_PNG
    return 'image/jpeg', MP4Cover.FORMAT_JPEG

def get_audio_duration(file_path: str, ext: str) -> int:
    try:
        if ext == '.mp3':
            return int(MP3(file_path).info.length)
        elif ext in ('.m4a', '.mp4'):
            return int(MP4(file_path).info.length)
    except Exception as e:
        logger.error(f"Failed to read audio duration: {e}")
    return 0

# --- Advanced FloodWait Handlers ---
async def safe_download_media(client, media, path):
    retries = 5
    for attempt in range(retries):
        try:
            result = await client.download_media(media, path)
            # CRITICAL: Validate the download actually happened
            if result is None or not os.path.isfile(path) or os.path.getsize(path) == 0:
                if attempt == retries - 1:
                    raise RuntimeError(f"Download returned None or empty file after {retries} attempts")
                await asyncio.sleep(2)
                continue
            return result
        except FloodWaitError as e:
            logger.warning(f"FloodWait on download! Sleeping {e.seconds}s")
            await asyncio.sleep(e.seconds + 2)
        except Exception as e:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(2)

async def safe_send_file(client, chat_id, file, **kwargs):
    retries = 5
    for attempt in range(retries):
        try:
            return await client.send_file(chat_id, file, **kwargs)
        except FloodWaitError as e:
            logger.warning(f"FloodWait on upload! Sleeping {e.seconds}s")
            await asyncio.sleep(e.seconds + 2)
        except Exception as e:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(3 * (attempt + 1))

async def safe_delete_message(message):
    if not message:
        return
    retries = 3
    for attempt in range(retries):
        try:
            await message.delete()
            return
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
        except Exception:
            if attempt == retries - 1:
                pass
            await asyncio.sleep(1)

# --- Smart Dashboard Updater ---
async def update_dashboard(client, chat_id, chat_queue, force=False):
    async with chat_queue.lock:
        if chat_queue.cancelled:
            return
            
        now = time.time()
        time_diff = now - chat_queue.last_dashboard_update
        completed_diff = chat_queue.completed_count - chat_queue.last_completed_count
        is_finished = (chat_queue.processed_count >= chat_queue.total_count and chat_queue.total_count > 0)
        
        if not force and chat_queue.queue_message and not is_finished:
            if time_diff < 2.0 and completed_diff < 5:
                return

        remaining = chat_queue.total_count - (chat_queue.completed_count + chat_queue.failed_count)
        if remaining < 0:
            remaining = 0
            
        text = (
            "📊 **Audio Processing Dashboard**\n\n"
            f"📥 **Total Queue :** {chat_queue.total_count}\n"
            f"📤 **Uploading :** {chat_queue.current_uploading}\n"
            f"✅ **Completed :** {chat_queue.completed_count}\n"
            f"❌ **Failed :** {chat_queue.failed_count}\n"
            f"⏳ **Remaining :** {remaining}"
        )
        
        msg_obj = chat_queue.queue_message
        chat_queue.last_dashboard_update = now
        chat_queue.last_completed_count = chat_queue.completed_count

    try:
        if msg_obj:
            try:
                await msg_obj.edit(text)
            except MessageNotModifiedError:
                pass
            except FloodWaitError:
                pass  # Skip this update cycle
        elif not is_finished:
            try:
                new_msg = await client.send_message(chat_id, text)
                async with chat_queue.lock:
                    if not chat_queue.cancelled:
                        chat_queue.queue_message = new_msg
            except FloodWaitError:
                pass
    except Exception as e:
        logger.error(f"Failed to update dashboard: {e}")
        async with chat_queue.lock:
            chat_queue.queue_message = None

# --- Image Downloaders ---
async def download_image_from_url(url: str) -> Optional[bytes]:
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return None
                content_type = response.headers.get('content-type', '').lower()
                if 'image' not in content_type:
                    return None
                return await response.read()
    except Exception as e:
        logger.error(f"Error fetching direct image URL: {e}")
    return None

async def download_image_from_tg(client, url: str) -> Optional[bytes]:
    match = re.match(r'https?://t\.me/(?:c/)?([^/]+)/(\d+)', url)
    if not match:
        return None
    try:
        channel_ref = match.group(1)
        message_id = int(match.group(2))
        if channel_ref.isdigit():
            channel_ref = int(f"-100{channel_ref}") if not channel_ref.startswith("-100") else int(channel_ref)
        try:
            entity = await client.get_entity(channel_ref)
        except Exception:
            entity = channel_ref
            
        msg = await client.get_messages(entity, ids=message_id)
        if not msg:
            return None
        if msg.photo:
            return await client.download_media(msg.photo, bytes)
        elif msg.document and msg.document.mime_type and msg.document.mime_type.startswith('image/'):
            return await client.download_media(msg.document, bytes)
        elif msg.media and hasattr(msg.media, 'webpage') and msg.media.webpage and getattr(msg.media.webpage, 'photo', None):
            return await client.download_media(msg.media.webpage.photo, bytes)
    except Exception as e:
        logger.error(f"Error resolving Telegram link: {e}")
    return None

# --- Metadata Engine ---
def process_mp3_metadata(file_path: str, title: str, artist: str, album: str, image_data: bytes) -> bool:
    try:
        try:
            audio = MP3(file_path, ID3=ID3)
        except ID3Error:
            audio = MP3(file_path)
            audio.add_tags()
        if audio.tags is None:
            audio.add_tags()
        
        for tag in ["TIT2", "TPE1", "TALB"]:
            audio.tags.delall(tag)
        keys_to_delete = [k for k in audio.tags.keys() if k.startswith("APIC")]
        for key in keys_to_delete:
            audio.tags.pop(key, None)
            
        audio.tags.add(TIT2(encoding=3, text=title))
        audio.tags.add(TPE1(encoding=3, text=artist))
        audio.tags.add(TALB(encoding=3, text=album))
        mime_type, _ = get_image_mime_and_format(image_data)
        audio.tags.add(APIC(encoding=3, mime=mime_type, type=3, desc='Cover', data=image_data))
        audio.save()
        return True
    except Exception as e:
        logger.error(f"Error processing MP3 metadata: {e}")
        return False

def process_m4a_metadata(file_path: str, title: str, artist: str, album: str, image_data: bytes) -> bool:
    try:
        audio = MP4(file_path)
        audio["\xa9nam"] = [title]
        audio["\xa9ART"] = [artist]
        audio["\xa9alb"] = [album]
        _, img_format = get_image_mime_and_format(image_data)
        audio["covr"] = [MP4Cover(image_data, imageformat=img_format)]
        audio.save()
        return True
    except Exception as e:
        logger.error(f"Error processing M4A metadata: {e}")
        return False

# --- Telegram Client ---
bot = TelegramClient('tagger_bot_session', API_ID, API_HASH)

@bot.on(events.NewMessage(incoming=True, pattern='/start'))
async def start_handler(event):
    await event.respond(
        "👋 **नमस्ते! मैं Production-Grade ऑटोमैटिक ऑडियो टैगर बॉट हूँ।**\n\n"
        "आप एक साथ 10,000+ फाइल्स भेज सकते हैं।\n\n"
        "**दो तरीके:**\n"
        "1️⃣ ऑडियो फाइल के साथ कैप्शन में फोटो का लिंक डालें\n"
        "2️⃣ पहले ऑडियो भेजें, फिर फोटो का लिंक\n\n"
        "💡 कतार रीसेट करने के लिए /clear"
    )

@bot.on(events.NewMessage(incoming=True, pattern='^/clear$'))
async def clear_handler(event):
    chat_id = event.chat_id
    pending_files.pop(chat_id, None)
    
    queue = chat_queues.get(chat_id)
    if queue:
        async with queue.lock:
            queue.cancelled = True
            
            for task in list(queue.active_tasks):
                if not task.done():
                    task.cancel()
            queue.active_tasks.clear()
            
            if queue.queue_message:
                asyncio.create_task(safe_delete_message(queue.queue_message))
                queue.queue_message = None
            
            queue.total_count = 0
            queue.completed_count = 0
            queue.processed_count = 0
            queue.failed_count = 0
            queue.next_assign_seq = 0
            queue.current_upload_seq = 0
            queue.current_uploading = "None"
        
        async with queue.condition:
            queue.condition.notify_all()
            
    await event.respond("✅ **Upload queue cleared. All tasks cancelled.**")

@bot.on(events.NewMessage(incoming=True))
async def incoming_message_handler(event):
    message = event.message
    chat_id = event.chat_id
    
    # Skip commands
    if message.text and (message.text.startswith('/start') or message.text.startswith('/clear')):
        return

    # --- Audio file detected ---
    try:
        ext = message.file.ext.lower() if message.file and message.file.ext else None
    except Exception:
        ext = None

    if ext in ALLOWED_EXTENSIONS:
        raw_name = message.file.name or f"audio{ext}"
        file_name = sanitize_filename(raw_name)
        caption_text = message.message or ""
        url_match = re.search(r'(https?://[^\s]+)', caption_text)
        
        if url_match:
            # Single-step: audio + caption has URL
            chat_queue = get_chat_queue(chat_id)
            async with chat_queue.lock:
                chat_queue.cancelled = False
                chat_queue.total_count += 1
                seq = chat_queue.next_assign_seq
                chat_queue.next_assign_seq += 1
                
            task = asyncio.create_task(
                hybrid_pipeline_worker(event, seq, chat_queue, message.media,
                                       file_name, url_match.group(1), caption_text)
            )
            async with chat_queue.lock:
                chat_queue.active_tasks.add(task)
            await update_dashboard(bot, chat_id, chat_queue)
        else:
            # Two-step: store in pending list
            if chat_id not in pending_files:
                pending_files[chat_id] = []
            pending_files[chat_id].append({
                "media": message.media,
                "file_name": file_name,
                "timestamp": time.time()
            })
            await event.respond(
                f"📥 **ऑडियो मिल गया!** ({len(pending_files[chat_id])} pending)\n"
                "अब फोटो का लिंक भेजें।"
            )
        return

    # --- Text message (URL for two-step) ---
    if message.text and not message.text.startswith('/'):
        input_url = message.text.strip()
        url_match = re.search(r'(https?://[^\s]+)', input_url)
        if not url_match:
            return
            
        queue = pending_files.get(chat_id)
        if queue:
            # Pop the FIRST pending audio (FIFO)
            file_data = queue.pop(0)
            if not queue:
                del pending_files[chat_id]
            
            chat_queue = get_chat_queue(chat_id)
            async with chat_queue.lock:
                chat_queue.cancelled = False
                chat_queue.total_count += 1
                seq = chat_queue.next_assign_seq
                chat_queue.next_assign_seq += 1
                
            task = asyncio.create_task(
                hybrid_pipeline_worker(event, seq, chat_queue, file_data["media"],
                                       file_data["file_name"], url_match.group(1), "")
            )
            async with chat_queue.lock:
                chat_queue.active_tasks.add(task)
            await update_dashboard(bot, chat_id, chat_queue)
        else:
            await event.respond("ℹ️ **पहले मुझे एक AUDIO फाइल भेजें।**")

# --- Hybrid Pipeline Worker ---
async def hybrid_pipeline_worker(event, seq, chat_queue, file_media, file_name, image_url, caption_text):
    chat_id = event.chat_id
    ep_num = extract_episode_number(file_name, caption_text)
    current_task = asyncio.current_task()
    
    if chat_queue.cancelled:
        return

    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            local_audio_path = os.path.join(temp_dir, file_name)
            
            # Phase 1: Download audio + image in parallel
            async with DOWNLOAD_SEMAPHORE:
                if chat_queue.cancelled:
                    return
                    
                await safe_download_media(bot, file_media, local_audio_path)
                
                if chat_queue.cancelled:
                    return
                
                # Download image
                image_data = await download_image_from_tg(bot, image_url)
                if not image_data:
                    image_data = await download_image_from_url(image_url)
                if not image_data:
                    raise ValueError(f"Failed to fetch image from provided link for Ep {ep_num}")

                if chat_queue.cancelled:
                    return

                title = f"Ep {ep_num}"
                album = f"Ep {ep_num} - Single"
                ext = os.path.splitext(file_name)[1].lower()
                
                # Metadata processing
                if ext == '.mp3':
                    success = await asyncio.to_thread(
                        process_mp3_metadata, local_audio_path, title, ARTIST_NAME, album, image_data
                    )
                elif ext in ('.m4a', '.mp4'):
                    success = await asyncio.to_thread(
                        process_m4a_metadata, local_audio_path, title, ARTIST_NAME, album, image_data
                    )
                else:
                    raise RuntimeError(f"Unsupported extension: {ext}")
                    
                if not success:
                    raise RuntimeError(f"Metadata writing failed for Ep {ep_num}")
                
                duration = get_audio_duration(local_audio_path, ext)
                thumb_path = os.path.join(temp_dir, "thumb.jpg")
                with open(thumb_path, "wb") as f:
                    f.write(image_data)
                audio_attributes = [DocumentAttributeAudio(duration=duration, title=title, performer=ARTIST_NAME)]

            # Phase 2: Sequential upload
            async with chat_queue.condition:
                while chat_queue.current_upload_seq != seq:
                    if chat_queue.cancelled:
                        return
                    await chat_queue.condition.wait()
                
                if chat_queue.cancelled:
                    return
                
                async with chat_queue.lock:
                    chat_queue.current_uploading = f"Ep {ep_num}"
                    
                await update_dashboard(bot, chat_id, chat_queue, force=True)
                
                await safe_send_file(
                    bot, chat_id, local_audio_path,
                    caption=(
                        f"✅ **Completed!**\n\n"
                        f"📌 **Title:** {title}\n"
                        f"🎤 **Artist:** {ARTIST_NAME}\n"
                        f"⏱️ **Duration:** {duration}s"
                    ),
                    attributes=audio_attributes,
                    thumb=thumb_path,
                    supports_streaming=True
                )
                
                async with chat_queue.lock:
                    chat_queue.completed_count += 1

    except asyncio.CancelledError:
        logger.info(f"Task for Ep {ep_num} cancelled gracefully.")
    except Exception as e:
        logger.error(f"Error in pipeline for Ep {ep_num}: {str(e)}")
        async with chat_queue.lock:
            if not chat_queue.cancelled:
                chat_queue.failed_count += 1
    finally:
        is_finished = False
        msg_to_delete = None
        
        async with chat_queue.lock:
            if current_task and current_task in chat_queue.active_tasks:
                chat_queue.active_tasks.remove(current_task)
            chat_queue.processed_count += 1

        async with chat_queue.condition:
            if chat_queue.current_upload_seq == seq:
                chat_queue.current_upload_seq += 1
                chat_queue.condition.notify_all()

        async with chat_queue.lock:
            if chat_queue.total_count > 0 and chat_queue.processed_count >= chat_queue.total_count:
                is_finished = True
                msg_to_delete = chat_queue.queue_message
                chat_queue.queue_message = None
                
                if not chat_queue.cancelled:
                    chat_queue.total_count = 0
                    chat_queue.completed_count = 0
                    chat_queue.processed_count = 0
                    chat_queue.failed_count = 0
                    chat_queue.next_assign_seq = 0
                    chat_queue.current_upload_seq = 0
                    chat_queue.current_uploading = "None"
                    
        if is_finished:
            if msg_to_delete:
                asyncio.create_task(safe_delete_message(msg_to_delete))
        else:
            if not chat_queue.cancelled:
                await update_dashboard(bot, chat_id, chat_queue)

async def main():
    await bot.start(bot_token=BOT_TOKEN)
    logger.info("Production Bot authenticated successfully. Ready for 10,000+ files.")
    await start_health_server()
    asyncio.create_task(track_and_expire_states())
    await bot.run_until_disconnected()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped cleanly.")
