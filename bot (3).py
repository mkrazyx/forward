import asyncio
import hashlib
import logging
import os
import re
import secrets
import time

from aiohttp import web
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError
from pyrogram import Client, enums, filters, idle
from pyrogram.errors import FloodWait

API_ID = int(os.environ.get("API_ID", 20354559))
API_HASH = os.environ.get("API_HASH", "bbdf772b35141fa8b661740dddb840bf")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8510132550:AAEQCl8szVwJAxWIv7gUrnXO2nn8aaKyyjU")
MONGO_URL = os.environ.get("MONGODB_URL", "mongodb+srv://test:test@test.i5mjcij.mongodb.net/?appName=test")
OWNER_ID = int(os.environ.get("OWNER", 1058015838))
PORT = int(os.environ.get("PORT", 8080))
EXTENSIONS = {"mkv", "mp4"}
BATCH = 200

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("pyrogram").setLevel(logging.ERROR)
log = logging.getLogger("cleaner")

db = AsyncIOMotorClient(MONGO_URL)[os.environ.get("DB_NAME", "cleaner")]
cleans, flagged, meta = db.cleans, db.flagged_posts, db.cleaner_meta
tables = {}
app = Client("cleaner", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN,
             in_memory=True, parse_mode=enums.ParseMode.DISABLED)
alerts = {}


async def alert(text):
    now = time.time()
    if now - alerts.get(text, 0) < 1800:
        return
    alerts[text] = now
    try:
        await app.send_message(OWNER_ID, text)
    except Exception:
        log.exception("Could not alert the owner")


async def load_tables():
    doc = await meta.find_one({"_id": "names"})
    if not doc:
        doc = {"_id": "names", "seen": "seen_" + secrets.token_hex(4), "names": "names_" + secrets.token_hex(4)}
        try:
            await meta.insert_one(doc)
        except DuplicateKeyError:
            doc = await meta.find_one({"_id": "names"})
    tables["seen"], tables["names"] = db[doc["seen"]], db[doc["names"]]
    await tables["names"].create_index("ns")


async def peer_hash(chat):
    try:
        return getattr(await app.resolve_peer(chat), "access_hash", None)
    except Exception:
        return None


async def restore_peers():
    async for doc in cleans.find({"ah": {"$ne": None}}):
        try:
            await app.storage.update_peers([(doc["_id"], doc["ah"], "channel", None, None)])
        except Exception:
            log.exception("Could not restore peer %s", doc["_id"])


def file_keys(message):
    media = message.video or message.document
    if not media:
        return None
    full = getattr(media, "file_name", None) or ""
    ext = full.rsplit(".", 1)[-1].lower() if "." in full else ""
    if ext not in EXTENSIONS and not (message.video and not full):
        return None
    name = re.sub(r"[^a-z0-9]", "", full.lower())
    size = getattr(media, "file_size", None) or 0
    parts = [size] + [getattr(media, key, None) or 0 for key in ("duration", "width", "height")]
    raw = "|".join(map(str, parts + [getattr(media, "mime_type", None) or "", name]))
    return media.file_unique_id, hashlib.sha1(raw.encode()).hexdigest(), name, size, full


async def fetch(chat, ids):
    while True:
        try:
            return await app.get_messages(chat, ids, replies=0)
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)


async def delete_ids(chat, ids):
    while True:
        try:
            return await app.delete_messages(chat, ids)
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)


async def is_duplicate(chat, message_id, keys):
    for key in (f"{chat}:u:{keys[0]}", f"{chat}:s:{keys[1]}"):
        try:
            await tables["seen"].insert_one({"_id": key, "m": message_id})
        except DuplicateKeyError:
            first = await tables["seen"].find_one({"_id": key})
            if first and first["m"] != message_id:
                return True
    return False


async def flag_same_name_size(chat, message_id, keys):
    if not keys[2]:
        return
    ns = f"{chat}:{keys[2]}:{keys[3]}"
    other = await tables["names"].find_one({"ns": ns, "m": {"$ne": message_id}})
    try:
        await tables["names"].insert_one({"_id": f"{ns}:{message_id}", "ns": ns, "m": message_id})
    except DuplicateKeyError:
        pass
    if other:
        await flagged.update_one(
            {"_id": ns},
            {"$addToSet": {"m": {"$each": [other["m"], message_id]}},
             "$set": {"chat": chat, "name": keys[4], "size": keys[3]}},
            upsert=True)


async def report(chat, job, scanned, dups, position, finished):
    groups = await flagged.count_documents({"chat": chat})
    dry = job.get("dry")
    head = "✅ Clean Finished" if finished else "🔍 Checking" if dry else "🧹 Cleaning"
    where = f"{position:,}" + (f" / {job['end']:,}" if job.get("end") else "")
    text = (f"{head}: {job.get('title')} ({chat})\n━━━━━━━━━━━━━━\n"
            f"📥 Scanned: {scanned:,} posts\n📍 At message: {where}\n"
            f"🗑 Duplicates {'found' if dry else 'deleted'}: {dups:,}\n🚩 Same name and size groups: {groups:,}")
    try:
        if not job.get("pm"):
            raise ValueError("no progress message yet")
        await app.edit_message_text(OWNER_ID, job["pm"], text)
    except FloodWait:
        pass
    except Exception:
        try:
            sent = await app.send_message(OWNER_ID, text)
            await cleans.update_one({"_id": chat}, {"$set": {"pm": sent.id}})
        except Exception:
            log.exception("Could not show the clean progress")


async def clean_step(job):
    chat, start = job["_id"], job["next"]
    messages = await fetch(chat, list(range(start, start + BATCH)))
    found = [m for m in messages if not m.empty]
    doomed = []
    for message in found:
        keys = file_keys(message)
        if not keys:
            continue
        if await is_duplicate(chat, message.id, keys):
            doomed.append(message.id)
        else:
            await flag_same_name_size(chat, message.id, keys)
    if doomed and not job.get("dry"):
        for index in range(0, len(doomed), 100):
            await delete_ids(chat, doomed[index:index + 100])
            await asyncio.sleep(1)
    empty = 0 if found else job.get("empty", 0) + 1
    following = max(m.id for m in found) + 1 if found else start + BATCH
    finished = not found and following > job.get("end", 0) and empty >= 3
    scanned, dups = job.get("scanned", 0) + len(found), job.get("dups", 0) + len(doomed)
    await cleans.update_one({"_id": chat}, {"$set": {"scanned": scanned, "dups": dups, "next": following, "empty": empty, "active": not finished}})
    if finished or scanned // 1000 > job.get("scanned", 0) // 1000:
        await report(chat, job, scanned, dups, following, finished)
    if finished:
        prefix = {"$regex": f"^{chat}:"}
        await tables["seen"].delete_many({"_id": prefix})
        await tables["names"].delete_many({"_id": prefix})
        verb = "found (nothing deleted)" if job.get("dry") else "deleted"
        groups = await flagged.count_documents({"chat": chat})
        await alert(f"✅ Clean finished for {job.get('title')} ({chat})\n"
                    f"🗑 {dups:,} duplicates {verb}\n"
                    f"🚩 {groups:,} same name and size groups flagged (collection flagged_posts)")


async def cleaner():
    while True:
        try:
            job = await cleans.find_one({"active": True}, sort=[("o", 1)])
            if not job:
                await asyncio.sleep(10)
                continue
            try:
                await clean_step(job)
            except Exception as e:
                log.exception("Clean failed for %s", job["_id"])
                await cleans.update_one({"_id": job["_id"]}, {"$set": {"active": False}})
                await alert(f"❌ Clean stopped for {job['_id']}: {e}\nThe bot needs the admin right to delete messages there. Send the command again to resume.")
        except Exception:
            log.exception("Cleaner error")
            await asyncio.sleep(10)


@app.on_message(filters.channel, group=1)
async def channel_seen(_, message):
    chat = message.chat.id
    if await cleans.find_one({"_id": chat, "pending": True}):
        await cleans.update_one({"_id": chat}, {"$set": {"ah": await peer_hash(chat), "title": message.chat.title, "pending": False, "active": True}})


owner = filters.private & filters.user(OWNER_ID)


def number(text):
    return int(re.findall(r"\d+", text)[-1])


@app.on_message(filters.command(["clean", "cleancheck"]) & owner)
async def clean_cmd(_, message):
    dry = message.command[0] == "cleancheck"
    args = message.command[1:]
    try:
        chat_id = int(args[0])
        start = number(args[1]) if len(args) > 1 else 1
        end = number(args[2]) if len(args) > 2 else 0
    except Exception:
        return await message.reply("Usage: /clean <channel_id> [start] [end]\n/cleancheck <channel_id> only counts, deletes nothing")
    try:
        chat = await app.get_chat(chat_id)
        ah, title = await peer_hash(chat.id), chat.title
    except Exception:
        ah = title = None
    await tables["seen"].delete_many({"_id": {"$regex": f"^{chat_id}:"}})
    await tables["names"].delete_many({"_id": {"$regex": f"^{chat_id}:"}})
    await cleans.update_one(
        {"_id": chat_id},
        {"$set": {"active": bool(ah), "pending": not ah, "dry": dry, "next": start, "end": end, "empty": 0, "dups": 0,
                  "scanned": 0, "title": title, "ah": ah, "o": time.time_ns()}, "$unset": {"pm": ""}},
        upsert=True)
    mode = "Checking" if dry else "Cleaning"
    if ah:
        sent = await message.reply(f"🧹 {mode} {title} ({chat_id}) from message {start}. Later copies of a file are {'only counted' if dry else 'deleted'}; the oldest post stays.\nProgress updates here every 1,000 posts.")
        await cleans.update_one({"_id": chat_id}, {"$set": {"pm": sent.id}})
    else:
        await message.reply(f"⏳ Saved ({chat_id}). The bot can't see this channel yet. Make it admin (with Delete messages) and post any message there, then it starts by itself.")


@app.on_message(filters.command("stopclean") & owner)
async def stop_clean(_, message):
    try:
        chat_id = int(message.command[1])
    except Exception:
        return await message.reply("Usage: /stopclean <channel_id>")
    await cleans.update_one({"_id": chat_id}, {"$set": {"active": False, "pending": False}})
    await message.reply(f"⏹ Clean stopped: {chat_id}")


@app.on_message(filters.command("cleanstatus") & owner)
async def clean_status(_, message):
    lines = []
    async for c in cleans.find({}):
        state = "running" if c.get("active") else "waiting for a post in the channel" if c.get("pending") else "stopped"
        lines.append(f"🧹 {c.get('title')} ({c['_id']}): {state}\nat message {c.get('next', 1):,} | scanned {c.get('scanned', 0):,} | duplicates {c.get('dups', 0):,}{' (check only)' if c.get('dry') else ''}"
                     f" | flagged groups {await flagged.count_documents({'chat': c['_id']}):,}")
    await message.reply("\n\n".join(lines) or "No cleans yet. Use /clean <channel_id>")


@app.on_message(filters.command("flagged") & owner)
async def show_flagged(_, message):
    try:
        chat_id = int(message.command[1])
    except Exception:
        return await message.reply("Usage: /flagged <channel_id>")
    base = str(chat_id)[4:] if str(chat_id).startswith("-100") else str(abs(chat_id))
    lines, total = [], await flagged.count_documents({"chat": chat_id})
    async for group in flagged.find({"chat": chat_id}):
        if len(lines) == 10:
            break
        links = " ".join(f"t.me/c/{base}/{m}" for m in sorted(group["m"])[:5])
        lines.append(f"{group.get('name')} ({group.get('size'):,} bytes)\n{links}")
    await message.reply(f"🚩 {total:,} same name and size groups\n\n" + "\n\n".join(lines) if total else "🚩 Nothing flagged for this channel")


async def health(request):
    return web.Response(text="ok")


async def main():
    web_app = web.Application()
    web_app.router.add_get("/", health)
    runner = web.AppRunner(web_app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    await app.start()
    await load_tables()
    await restore_peers()
    task = asyncio.create_task(cleaner())
    await idle()
    task.cancel()
    await app.stop()


if __name__ == "__main__":
    app.run(main())
