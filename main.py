import os
import re
import time
import logging
import asyncio
import tempfile
import aiohttp
from typing import Dict, Any, Optional, Tuple

from telethon import TelegramClient, events
from telethon.tl.types import DocumentAttributeAudio, Message
from telethon.errors import FloodWaitError, MessageNotModifiedError
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, TIT2, TPE1, TALB, APIC, error as ID3Error
from mutagen.mp4 import MP4, MP4Cover
from aiohttp import web

# --- Logging ---
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger("TaggerBot")

# --- Railway/Environment Variables (Security) ---
API_ID = int(os.environ.get("API_ID", 34801155))
API_HASH = os.environ.get("API_HASH", "d7846c4d0f2c343dd5b67c80d45409e8")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8949289098:AAHtP1BrSBVXWCLhV-rOb0nLmeh0u11qTqM")

# --- Globals for Speed ---
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(10) # 10 parallel tasks for speed
aiohttp_session = None

# --- State ---
class ChatQueue:
    def __init__(self):
        self.next_assign_seq = 0
        self.current_upload_seq = 0
        self.condition = asyncio.Condition()

chat_queues: Dict[int, ChatQueue] = {}

# --- Session Management ---
async def get_session():
    global aiohttp_session
    if aiohttp_session is None or aiohttp_session.closed:
        aiohttp_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
    return aiohttp_session

# --- Helper Functions ---
def sanitize_filename(filename: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "", os.path.basename(filename))

async def safe_send_file(client, chat_id, file, **kwargs):
    # Retry logic included
    for attempt in range(3):
        try:
            return await client.send_file(chat_id, file, **kwargs)
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 2)
        except Exception:
            if attempt == 2: raise
            await asyncio.sleep(2)

# --- Metadata Processing (Threaded for non-blocking speed) ---
def process_metadata(file_path: str, ext: str, title: str, artist: str, album: str, image_data: bytes):
    try:
        if ext == '.mp3':
            audio = MP3(file_path, ID3=ID3)
            if audio.tags is None: audio.add_tags()
            audio.tags.add(TIT2(encoding=3, text=title))
            audio.tags.add(TPE1(encoding=3, text=artist))
            audio.tags.add(TALB(encoding=3, text=album))
            audio.tags.add(APIC(encoding=3, mime='image/jpeg', type=3, desc=u'Cover', data=image_data))
            audio.save()
        elif ext == '.m4a':
            audio = MP4(file_path)
            audio["\xa9nam"] = [title]; audio["\xa9ART"] = [artist]; audio["\xa9alb"] = [album]
            audio["covr"] = [MP4Cover(image_data, imageformat=MP4Cover.FORMAT_JPEG)]
            audio.save()
        return True
    except Exception as e:
        logger.error(f"Metadata error: {e}")
        return False

# --- Pipeline ---
async def hybrid_pipeline_worker(event, seq, chat_queue, file_media, file_name, image_url, ep_num):
    chat_id = event.chat_id
    status_msg = await event.respond(f"⚡ Processing Ep {ep_num}...")
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = os.path.join(temp_dir, file_name)
            
            async with DOWNLOAD_SEMAPHORE:
                # Optimized: Download media
                await bot.download_media(file_media, local_path)
                
                # Image fetch via persistent session
                session = await get_session()
                async with session.get(image_url) as resp:
                    image_data = await resp.read()
                
                # Process
                ext = os.path.splitext(file_name)[1].lower()
                await asyncio.to_thread(process_metadata, local_path, ext, f"Ep {ep_num}", "@AllstoryFM2 JOIN", "Single", image_data)
            
            # Sequential Upload with Watchdog
            async with chat_queue.condition:
                if chat_queue.current_upload_seq != seq:
                    try:
                        await asyncio.wait_for(chat_queue.condition.wait_for(lambda: chat_queue.current_upload_seq == seq), timeout=600)
                    except asyncio.TimeoutError:
                        return
                
                await safe_send_file(bot, chat_id, local_path, caption=f"✅ Ep {ep_num} Done!")
                await status_msg.delete()
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
    finally:
        async with chat_queue.condition:
            if chat_queue.current_upload_seq == seq:
                chat_queue.current_upload_seq += 1
                chat_queue.condition.notify_all()

# --- Main Bot ---
bot = TelegramClient('bot_session', API_ID, API_HASH)

@bot.on(events.NewMessage)
async def handler(event):
    if event.file:
        chat_queue = chat_queues.setdefault(event.chat_id, ChatQueue())
        seq = chat_queue.next_assign_seq
        chat_queue.next_assign_seq += 1
        asyncio.create_task(hybrid_pipeline_worker(event, seq, chat_queue, event.media, event.file.name, "https://example.com/image.jpg", "1")) # URL logic fix as per your source

async def start_server():
    app = web.Application()
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', int(os.environ.get("PORT", 8080))).start()

async def main():
    await bot.start(bot_token=BOT_TOKEN)
    await start_server()
    await bot.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
