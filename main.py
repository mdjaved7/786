import os
import re
import logging
import asyncio
import tempfile
import aiohttp
from typing import Optional, Tuple, Dict, List

# Telethon Imports
from telethon import TelegramClient, events
from telethon.tl.types import DocumentAttributeAudio

# Mutagen Imports
from mutagen import File as MutagenFile
from mutagen.mp3 import MP3
from mutagen.id3 import APIC, TIT2
from mutagen.mp4 import MP4, MP4Cover
from mutagen.flac import FLAC, Picture
from mutagen.oggvorbis import OggVorbis

# --- Logging Configuration ---
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("BatchProcessTaggerBot")

# --- Environment Variables ---
API_ID = 34801155
API_HASH = "d7846c4d0f2c343dd5b67c80d45409e8"
BOT_TOKEN = "8949289098:AAHtP1BrSBVXWCLhV-rOb0nLmeh0u11qTqM"

ALLOWED_EXTENSIONS = {'.mp3', '.m4a', '.mp4', '.ogg', '.flac'}

# --- Memory Storage (Per-Chat Batch Queue & Lock) ---
user_queues: Dict[int, List[events.NewMessage.Event]] = {}
processing_locks: Dict[int, asyncio.Lock] = {}
last_episode_numbers: Dict[int, int] = {}

def get_chat_lock(chat_id: int) -> asyncio.Lock:
    if chat_id not in processing_locks:
        processing_locks[chat_id] = asyncio.Lock()
    return processing_locks[chat_id]

def extract_number_from_text(text: str) -> Optional[int]:
    if not text:
        return None
    match = re.search(r'\d+', text)
    if match:
        return int(match.group())
    return None

# --- Helpers ---
def get_image_mime_and_format(data: bytes) -> Tuple[str, int]:
    if data.startswith(b'\xff\xd8'):
        return 'image/jpeg', MP4Cover.FORMAT_JPEG
    elif data.startswith(b'\x89PNG\r\n\x1a\n'):
        return 'image/png', MP4Cover.FORMAT_PNG
    return 'image/jpeg', MP4Cover.FORMAT_JPEG

def get_audio_info(file_path: str) -> Tuple[int, str]:
    duration = 0
    raw_title = ""
    try:
        audio = MutagenFile(file_path)
        if audio is not None:
            if hasattr(audio.info, 'length'):
                duration = int(audio.info.length)
            
            if 'TIT2' in audio:
                raw_title = str(audio['TIT2'])
            elif '\xa9nam' in audio:
                raw_title = str(audio['\xa9nam'][0])
            elif 'title' in audio:
                raw_title = str(audio['title'][0])
    except Exception as e:
        logger.error(f"Failed to read audio info: {e}")
    return duration, raw_title

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

# --- Image & Title Attacher Engine ---
def attach_image_and_title(file_path: str, image_data: bytes, title: str) -> bool:
    try:
        audio = MutagenFile(file_path)
        if audio is None:
            return False

        mime_type, img_format = get_image_mime_and_format(image_data)

        if isinstance(audio, MP3):
            if audio.tags is None:
                audio.add_tags()
            
            audio.tags.add(TIT2(encoding=3, text=title))
            
            keys_to_delete = [k for k in audio.tags.keys() if k.startswith("APIC")]
            for key in keys_to_delete:
                audio.tags.pop(key, None)
            audio.tags.add(APIC(encoding=3, mime=mime_type, type=3, desc='Cover', data=image_data))
            audio.save()
            return True

        elif isinstance(audio, MP4):
            audio["\xa9nam"] = [title]
            audio["covr"] = [MP4Cover(image_data, imageformat=img_format)]
            audio.save()
            return True

        elif isinstance(audio, FLAC):
            audio['title'] = [title]
            pic = Picture()
            pic.mime = mime_type
            pic.type = 3
            pic.desc = 'Cover'
            pic.data = image_data
            audio.add_picture(pic)
            audio.save()
            return True

        elif isinstance(audio, OggVorbis):
            audio['title'] = [title]
            audio.save()
            return True

        return False
    except Exception as e:
        logger.error(f"Error attaching image/title: {e}")
        return False

# --- Telegram Bot Setup ---
bot = TelegramClient('image_tagger_bot', API_ID, API_HASH)

@bot.on(events.NewMessage(incoming=True, pattern=r'^/(start|process)$'))
async def command_handler(event):
    chat_id = event.chat_id
    queue = user_queues.get(chat_id, [])

    if not queue:
        await event.respond(
            "👋 **नमस्ते!**\n\n"
            "पहले अपनी सभी ऑडियो फाइल्स (कैप्शन में इमेज लिंक के साथ) भेजें।\n"
            "जब सारी फाइल्स भेज दें, तो काम शुरू करने के लिए **/process** कमांड भेजें!"
        )
        return

    chat_lock = get_chat_lock(chat_id)
    if chat_lock.locked():
        await event.respond("⚠️ पहले से भेजी गई फाइल्स प्रोसेस हो रही हैं, कृपया प्रतीक्षा करें...")
        return

    async with chat_lock:
        status_msg = await event.respond(f"🚀 **{len(queue)} फाइल्स की प्रोसेसिंग शुरू हो रही है...**")
        
        # प्रोसेस करने के लिए क्यू की कॉपी बनाएं और लिस्ट खाली करें
        files_to_process = list(queue)
        user_queues[chat_id] = []

        for index, msg_event in enumerate(files_to_process, start=1):
            caption = msg_event.text or ""
            url_match = re.search(r'https?://[^\s]+', caption)
            image_url = url_match.group(0) if url_match else ""

            if not image_url:
                await event.respond(f"❌ फाइल #{index} में इमेज का लिंक नहीं मिला, इसे स्किप कर रहे हैं।")
                continue

            await status_msg.edit(f"⏳ **फाइल {index}/{len(files_to_process)} प्रोसेस हो रही है...**")

            with tempfile.TemporaryDirectory() as temp_dir:
                # 1. Image Download
                if "t.me/" in image_url:
                    image_data = await download_image_from_tg(bot, image_url)
                else:
                    image_data = await download_image_from_url(image_url)

                if not image_data:
                    await event.respond(f"❌ फाइल #{index} के लिए इमेज डाउनलोड विफल हुई।")
                    continue

                # 2. Extract Details for Episode Number
                orig_title = ""
                if msg_event.document and msg_event.document.attributes:
                    for attr in msg_event.document.attributes:
                        if isinstance(attr, DocumentAttributeAudio) and attr.title:
                            orig_title = attr.title

                file_ext = msg_event.file.ext or ".mp3"
                audio_path = os.path.join(temp_dir, f"audio{file_ext}")
                
                await bot.download_media(msg_event.media, audio_path)

                duration, meta_title = get_audio_info(audio_path)
                raw_filename = os.path.splitext(msg_event.file.name)[0] if msg_event.file.name else ""

                found_num = (
                    extract_number_from_text(orig_title) or 
                    extract_number_from_text(meta_title) or 
                    extract_number_from_text(raw_filename)
                )

                if found_num is not None:
                    final_title = f"Ep - {found_num}"
                    last_episode_numbers[chat_id] = found_num
                else:
                    prev_ep = last_episode_numbers.get(chat_id, 0)
                    next_ep = prev_ep + 1
                    final_title = f"Ep - {next_ep}"
                    last_episode_numbers[chat_id] = next_ep

                # 3. Apply Image & Title
                success = attach_image_and_title(audio_path, image_data, final_title)
                if not success:
                    await event.respond(f"❌ **{final_title}** तैयार करने में त्रुटि हुई।")
                    continue

                thumb_path = os.path.join(temp_dir, "thumb.jpg")
                with open(thumb_path, "wb") as f:
                    f.write(image_data)

                # 4. Send File Sequentially
                await bot.send_file(
                    chat_id,
                    file=audio_path,
                    caption=f"✅ **{final_title}** तैयार!",
                    thumb=thumb_path,
                    attributes=[
                        DocumentAttributeAudio(
                            duration=duration,
                            title=final_title,
                            performer="Custom Cover"
                        )
                    ]
                )

        await status_msg.edit("🎉 **सभी फाइल्स की प्रोसेसिंग सफलतापूर्वक पूरी हो गई है!**")

@bot.on(events.NewMessage(incoming=True))
async def collect_audio_files(event):
    # Ignore Command messages
    if event.text and event.text.startswith('/'):
        return

    if not event.media or not (event.voice or event.audio or (event.document and any(event.file.name.endswith(ext) for ext in ALLOWED_EXTENSIONS if event.file.name))):
        return

    caption = event.text or ""
    url_match = re.search(r'https?://[^\s]+', caption)

    if not url_match:
        await event.respond("⚠️ **कृपया ऑडियो के कैप्शन में इमेज का लिंक भी भेजें!**")
        return

    chat_id = event.chat_id
    if chat_id not in user_queues:
        user_queues[chat_id] = []

    user_queues[chat_id].append(event)
    count = len(user_queues[chat_id])

    logger.info(f"Chat {chat_id}: Added file #{count} to queue.")

async def main():
    await bot.start(bot_token=BOT_TOKEN)
    logger.info("Bot is running in Batch Queue Mode...")
    await bot.run_until_disconnected()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped.")
        
