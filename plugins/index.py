import time
import asyncio
from pymongo import MongoClient

from hydrogram import Client, filters, enums
from hydrogram.errors import FloodWait, MessageNotModified
from hydrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from info import ADMINS, DATA_DATABASE_URL, DATABASE_NAME, INDEX_LOG_CHANNEL
from database.ia_filterdb import save_file
from utils import get_readable_time

# =====================================================
# GLOBALS
# =====================================================
LOCK = asyncio.Lock()
CANCEL = False
# WAITING_SKIP हटा दिया गया है क्योंकि अब इसकी जरूरत नहीं है

# =====================================================
# RESUME DB
# =====================================================
mongo = MongoClient(DATA_DATABASE_URL)
db = mongo[DATABASE_NAME]
resume_col = db["index_resume"]

def get_resume(chat_id):
    d = resume_col.find_one({"_id": chat_id})
    return d["last_id"] if d else None

def set_resume(chat_id, msg_id):
    resume_col.update_one(
        {"_id": chat_id},
        {"$set": {"last_id": msg_id}},
        upsert=True
    )

# =====================================================
# HELPERS
# =====================================================
async def auto_delete(bot, chat_id, msg_id, delay=120):
    await asyncio.sleep(delay)
    try:
        await bot.delete_messages(chat_id, msg_id)
    except:
        pass

async def send_log(bot, text):
    if not INDEX_LOG_CHANNEL:
        return
    try:
        await bot.send_message(INDEX_LOG_CHANNEL, text)
    except:
        pass

# =====================================================
# ENTRY POINT
# forward / link → DIRECT CONFIRMATION
# =====================================================
@Client.on_message(filters.private & filters.user(ADMINS) & filters.incoming)
async def start_index(bot, message):
    global CANCEL

    if LOCK.locked():
        return await message.reply("⏳ Indexing already running")

    try:
        # ---- LINK ----
        if message.text and message.text.startswith("https://t.me"):
            parts = message.text.split("/")
            last_msg_id = int(parts[-1])
            raw = parts[-2]
            chat_id = int("-100" + raw) if raw.isdigit() else raw

        # ---- FORWARD ----
        elif message.forward_from_chat and message.forward_from_chat.type == enums.ChatType.CHANNEL:
            last_msg_id = message.forward_from_message_id
            chat_id = message.forward_from_chat.id

        else:
            return

        chat = await bot.get_chat(chat_id)
        if chat.type != enums.ChatType.CHANNEL:
            return await message.reply("❌ Only channels supported")

    except Exception as e:
        return await message.reply(f"❌ Error: `{e}`")

    # ---- DIRECT BUTTON (NO SKIP ASK) ----
    # यहाँ Skip को डिफ़ॉल्ट 0 सेट कर दिया है
    skip = 0 
    
    btn = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "✅ START INDEXING",
            callback_data=f"idx#start#{chat_id}#{last_msg_id}#{skip}"
        )],
        [InlineKeyboardButton("❌ CANCEL", callback_data="idx#close")]
    ])

    await message.reply(
        f"📢 **Channel:** `{chat.title}`\n"
        f"🆔 **ID:** `{chat_id}`\n"
        f"📊 **Last Message:** `{last_msg_id}`",
        reply_markup=btn
    )

# Note: handle_skip फंक्शन पूरी तरह हटा दिया गया है

# =====================================================
# CALLBACK
# =====================================================
@Client.on_callback_query(filters.regex("^idx#"))
async def index_callback(bot, query):
    global CANCEL
    data = query.data.split("#")

    if data[1] == "close":
        return await query.message.edit("❌ Cancelled")

    _, _, chat_id, last_id, skip = data
    chat = await bot.get_chat(int(chat_id))

    await query.message.edit("⚡ Indexing started…")

    async with LOCK:
        CANCEL = False
        await index_worker(
            bot,
            query.message,
            int(chat_id),
            int(last_id),
            int(skip),
            chat.title
        )

# =====================================================
# CORE INDEX LOOP
# =====================================================
async def index_worker(bot, status, chat_id, last_msg_id, skip, channel_title):
    global CANCEL

    start_time = time.time()
    saved = dup = err = nomedia = 0
    processed = 0

    # Resume Logic
    old_resume_id = get_resume(chat_id)
    stop_id = old_resume_id if old_resume_id else 0
    
    current_id = last_msg_id - skip

    try:
        while current_id > stop_id:
            if CANCEL:
                break

            try:
                msg = await bot.get_messages(chat_id, current_id)
            except FloodWait as e:
                await asyncio.sleep(e.value)
                continue
            except Exception:
                current_id -= 1
                continue

            processed += 1

            # Status Update (Every 50 msgs)
            if processed % 50 == 0:
                elapsed = time.time() - start_time
                speed = processed / elapsed if elapsed else 0
                eta = (current_id - stop_id) / speed if speed else 0

                try:
                    btn = InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🛑 STOP", callback_data="idx#cancel")]]
                    )
                    await status.edit(
                        f"📊 `{processed}` scanned\n"
                        f"✅ `{saved}` | ♻️ `{dup}` | ❌ `{err}`\n"
                        f"⚡ `{speed:.2f}/s`\n"
                        f"⏳ `{get_readable_time(eta)}`",
                        reply_markup=btn
                    )
                except MessageNotModified:
                    pass

            # Validate Media
            if not msg or not msg.media:
                nomedia += 1
                current_id -= 1
                continue

            if msg.media not in (
                enums.MessageMediaType.VIDEO,
                enums.MessageMediaType.DOCUMENT
            ):
                nomedia += 1
                current_id -= 1
                continue

            media = getattr(msg, msg.media.value, None)
            if not media:
                current_id -= 1
                continue

            media.caption = msg.caption
            res = await save_file(media)

            if res == "suc":
                saved += 1
            elif res == "dup":
                dup += 1
            else:
                err += 1

            current_id -= 1
        
        if not CANCEL:
            set_resume(chat_id, last_msg_id)

    except Exception as e:
        await status.edit(f"❌ Failed: `{e}`")
        return

    total_time = get_readable_time(time.time() - start_time)

    final_msg = await status.edit(
        f"✅ **Index Completed**\n\n"
        f"📢 `{channel_title}`\n"
        f"🆔 `{chat_id}`\n\n"
        f"✅ `{saved}` | ♻️ `{dup}` | ❌ `{err}` | 🚫 `{nomedia}`\n"
        f"⏱ `{total_time}`"
    )
    asyncio.create_task(auto_delete(bot, final_msg.chat.id, final_msg.id, 120))

    await send_log(
        bot,
        "📊 **Index Report**\n\n"
        f"📢 **Channel:** `{channel_title}`\n"
        f"🆔 **Channel ID:** `{chat_id}`\n\n"
        f"✅ **Saved:** `{saved}`\n"
        f"♻️ **Duplicate:** `{dup}`\n"
        f"❌ **Errors:** `{err}`\n"
        f"🚫 **Non-media:** `{nomedia}`\n"
        f"⏱ **Time:** `{total_time}`"
    )

# =====================================================
# STOP
# =====================================================
@Client.on_callback_query(filters.regex("^idx#cancel"))
async def stop_index(bot, query):
    global CANCEL
    CANCEL = True
    await query.answer("Stopping…", show_alert=True)

