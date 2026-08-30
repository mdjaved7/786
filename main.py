import os
import re
import logging
import asyncio
import tempfile
import aiohttp
from typing import Optional, Tuple, Dict

# Telethon Imports
from telethon import TelegramClient, events, Button
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
logger = logging.getLogger("GetStartedTaggerBot")

# --- Environment Variables ---
API_ID = 34801155
API_HASH = "d7846c4d0f2c343dd5b67c80d45409e8"
BOT_TOKEN = "8949289098:AAHtP1BrSBVXWCLhV-rOb0nLmeh0u11qTqM"

ALLOWED_EXTENSIONS = {'.mp3', '.m4a', '.mp4', '.ogg', '.flac'}

# --- State Management (Per-Chat Queue, Episode Counter & Pending Messages) ---
processing_locks: Dict[int, asyncio.Lock] = {}
last_episode_numbers: Dict[int, int] = {}
pending_tasks: Dict[str, events.NewMessage.Event] = {}  # Pending events standard memory

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

@bot.on(events.NewMessage(incoming=True, pattern='/start'))
async def start_handler(event):
    await event.respond(
        "👋 **नमस्ते! मैं एपिसोड टैगर बॉट हूँ।**\n\n"
        "1️⃣ मुझे ऑडियो फाइल के साथ इमेज का लिंक भेजें।\n"
        "2️⃣ नीचे दिए गए **Get Started** बटन पर क्लिक करें, और काम शुरू हो जाएगा!"
    )

@bot.on(events.NewMessage(incoming=True))
async def handle_audio_arrival(event):
    if not event.media or not (event.voice or event.audio or (event.document and any(event.file.name.endswith(ext) for ext in ALLOWED_EXTENSIONS if event.file.name))):
        return

    caption = event.text or ""
    url_match = re.search(r'https?://[^\s]+', caption)

    if not url_match:
        await event.respond("⚠️ **कृपया ऑडियो के साथ कैप्शन में इमेज का लिंक भी भेजें।**")
        return

    # यूनिक टास्क आईडी (Chat ID + Msg ID)
    task_key = f"{event.chat_id}_{event.id}"
    pending_tasks[task_key] = event

    # "Get Started" बटन भेजें
    await event.respond(
        "📁 **फाइल प्राप्त हो गई है!**\n\nप्रोसेसिंग शुरू करने के लिए नीचे **Get Started** बटन पर क्लिक करें 👇",
        buttons=[
            [Button.inline("🚀 Get Started", data=f"start_{task_key}")]
        ]
    )

@bot.on(events.CallbackQuery(pattern=r'^start_'))
async def process_callback(event):
    task_key = event.data.decode('utf-8').replace("start_", "")
    
    msg_event = pending_tasks.get(task_key)
    if not msg_event:
        await event.answer("⚠️ यह फाइल पहले ही प्रोसेस हो चुकी है या पुरानी है!", alert=True)
        return

    await event.answer("🚀 काम शुरू किया जा रहा है...")

    caption = msg_event.text or ""
    url_match = re.search(r'https?://[^\s]+', caption)
    image_url = url_match.group(0) if url_match else ""

    chat_id = msg_event.chat_id
    chat_lock = get_chat_lock(chat_id)

    # बटन हटाने के बाद स्टेटस मैसेज अपडेट करें
    status_msg = await event.edit("⏳ **प्रोसेसिंग शुरू हो रही है...**", buttons=None)

    async with chat_lock:
        with tempfile.TemporaryDirectory() as temp_dir:
            # 1. Download Image
            if "t.me/" in image_url:
                image_data = await download_image_from_tg(bot, image_url)
            else:
                image_data = await download_image_from_url(image_url)

            if not image_data:
                await status_msg.edit("❌ **इमेज डाउनलोड नहीं हो सकी। लिंक जांचें!**")
                pending_tasks.pop(task_key, None)
                return

            # 2. Extract Existing Details for Episode Number
            orig_title = ""
            if msg_event.document and msg_event.document.attributes:
                for attr in msg_event.document.attributes:
                    if isinstance(attr, DocumentAttributeAudio) and attr.title:
                        orig_title = attr.title

            file_ext = msg_event.file.ext or ".mp3"
            audio_path = os.path.join(temp_dir, f"audio{file_ext}")
            
            await status_msg.edit("📥 **ऑडियो डाउनलोड हो रहा है...**")
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

            # 3. Apply Image & Ep - X Title
            await status_msg.edit(f"🖼️ **{final_title}** तैयार किया जा रहा है...")
            success = attach_image_and_title(audio_path, image_data, final_title)

            if not success:
                await status_msg.edit("❌ **इमेज या टाइटल सेट करने में त्रुटि हुई।**")
                pending_tasks.pop(task_key, None)
                return

            thumb_path = os.path.join(temp_dir, "thumb.jpg")
            with open(thumb_path, "wb") as f:
                f.write(image_data)

            # 4. Upload Back Sequentially
            await status_msg.edit("📤 **अपलोड हो रहा है...**")
            await bot.send_file(
                chat_id,
                file=audio_path,
                caption=f"✅ **{final_title}** सफलतापूर्वक तैयार कर दिया गया है!",
                thumb=thumb_path,
                attributes=[
                    DocumentAttributeAudio(
                        duration=duration,
                        title=final_title,
                        performer="Custom Cover"
                    )
                ]
            )
            await status_msg.delete()
            pending_tasks.pop(task_key, None)

async def main():
    await bot.start(bot_token=BOT_TOKEN)
    logger.info("Bot with 'Get Started' button is running...")
    await bot.run_until_disconnected()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped.")
        
