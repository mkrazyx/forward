import asyncio
import hashlib
import logging
import os
import re
import time

from aiohttp import web
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError
from pyrogram import Client, enums, filters, idle
from pyrogram.errors import FloodWait, MessageIdInvalid, RPCError

API_ID = int(os.environ.get("API_ID", 20354559))
API_HASH = os.environ.get("API_HASH", "bbdf772b35141fa8b661740dddb840bf")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8510132550:AAEQCl8szVwJAxWIv7gUrnXO2nn8aaKyyjU")
MONGO_URL = os.environ.get("MONGODB_URL", "mongodb+srv://test:test@test.i5mjcij.mongodb.net/?appName=test")
OWNER_ID = int(os.environ.get("OWNER", 1058015838))
TARGETS = os.environ.get("TARGETS", "-1004207192628,-1003923896491,-1004464694764")
LIMIT = int(os.environ.get("TARGET_LIMIT", 950000))
BASE_DELAY = float(os.environ.get("DELAY", 1))
PORT = int(os.environ.get("PORT", 8080))
EXTENSIONS = {"mkv", "mp4"}
QUEUE_MAX = 3000
BATCH = 200

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("pyrogram").setLevel(logging.ERROR)
log = logging.getLogger("forwarder")

db = AsyncIOMotorClient(MONGO_URL)[os.environ.get("DB_NAME", "forwarder")]
files, queue, targets, sources = db.files, db.queue, db.targets, db.sources
app = Client("forwarder", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN,
             in_memory=True, parse_mode=enums.ParseMode.DISABLED)
state = {"delay": BASE_DELAY, "targets": set(), "alerts": {}}


async def alert(text):
    now = time.time()
    if now - state["alerts"].get(text, 0) < 1800:
        return
    state["alerts"][text] = now
    try:
        await app.send_message(OWNER_ID, text)
    except Exception:
        log.exception("Could not alert the owner")


async def peer_hash(chat):
    try:
        return getattr(await app.resolve_peer(chat), "access_hash", None)
    except Exception:
        return None


async def restore_peers():
    for col in (sources, targets):
        async for doc in col.find({"ah": {"$ne": None}}):
            try:
                await app.storage.update_peers([(doc["_id"], doc["ah"], "channel", None, None)])
            except Exception:
                log.exception("Could not restore peer %s", doc["_id"])


async def load_targets():
    state["targets"] = {doc["_id"] async for doc in targets.find({})}


def file_keys(message):
    media = message.video or message.document
    if not media:
        return None
    full = getattr(media, "file_name", None) or ""
    ext = full.rsplit(".", 1)[-1].lower() if "." in full else ""
    if ext not in EXTENSIONS and not (message.video and not full):
        return None
    name = re.sub(r"[^a-z0-9]", "", (getattr(media, "file_name", None) or "").lower())
    parts = [getattr(media, key, None) or 0 for key in ("file_size", "duration", "width", "height")]
    raw = "|".join(map(str, parts + [getattr(media, "mime_type", None) or "", name]))
    return media.file_unique_id, hashlib.sha1(raw.encode()).hexdigest(), ext


async def enqueue(message):
    keys = file_keys(message)
    if not keys:
        return
    chat = message.chat.id
    try:
        await queue.insert_one({"_id": f"{chat}:{message.id}", "c": chat, "m": message.id,
                                "u": keys[0], "s": keys[1], "x": keys[2], "n": time.time_ns()})
    except DuplicateKeyError:
        pass


async def claim(key, jid):
    try:
        await files.insert_one({"_id": key, "j": jid, "done": False})
        return True
    except DuplicateKeyError:
        owner = await files.find_one({"_id": key})
        return bool(owner) and owner["j"] == jid


async def send(job):
    while True:
        target = await targets.find_one({"sent": {"$lt": LIMIT}}, sort=[("o", 1)])
        if not target:
            await alert("⚠️ No target channel has free space. Add one with /addtarget <channel_id>")
            await asyncio.sleep(60)
            continue
        try:
            await app.copy_message(target["_id"], job["c"], job["m"])
        except FloodWait as e:
            state["delay"] = min(state["delay"] + 1, 30)
            await asyncio.sleep(e.value + 1)
            continue
        except (MessageIdInvalid, ValueError):
            return "gone"
        except RPCError as e:
            await alert(f"❌ Could not copy {job['c']}:{job['m']} to {target['_id']}\n{e}")
            return "retry"
        await targets.update_one({"_id": target["_id"]}, {"$inc": {"sent": 1}})
        return target["_id"]


async def process(job):
    jid = job["_id"]
    keys = ["u:" + job["u"], "s:" + job["s"]]
    if not await claim(keys[0], jid) or not await claim(keys[1], jid):
        return "duplicate"
    if (await files.find_one({"_id": keys[0]}))["done"]:
        return "duplicate"
    result = await send(job)
    if result == "gone":
        await files.delete_many({"_id": {"$in": keys}, "j": jid})
    elif result != "retry":
        await files.update_many({"_id": {"$in": keys}}, {"$set": {"done": True, "t": result}})
        return "sent"
    return result


async def worker():
    while True:
        try:
            job = await queue.find_one({}, sort=[("n", 1)])
            if not job:
                await asyncio.sleep(5)
                continue
            result = await process(job)
            if result == "retry":
                await queue.update_one({"_id": job["_id"]}, {"$set": {"n": time.time_ns()}})
                await asyncio.sleep(5)
                continue
            await queue.delete_one({"_id": job["_id"]})
            if result == "sent":
                state["delay"] = max(BASE_DELAY, state["delay"] - 0.05)
                await asyncio.sleep(state["delay"])
        except Exception:
            log.exception("Worker error")
            await asyncio.sleep(10)


async def fetch(chat, ids):
    while True:
        try:
            return await app.get_messages(chat, ids, replies=0)
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)


async def scan(source):
    chat = source["_id"]
    start = source.get("next", 1)
    while await queue.estimated_document_count() < QUEUE_MAX:
        messages = await fetch(chat, list(range(start, start + BATCH)))
        found = [m for m in messages if not m.empty]
        for message in found:
            await enqueue(message)
        if found:
            start = max(m.id for m in found) + 1
        elif start + BATCH > max(source.get("top", 0), source.get("end", 0)):
            break
        else:
            start += BATCH
        await sources.update_one({"_id": chat}, {"$set": {"next": start}})


async def poller():
    while True:
        try:
            async for source in sources.find({}):
                if not source.get("ah"):
                    continue
                try:
                    await scan(source)
                except Exception as e:
                    log.exception("Scan failed for %s", source["_id"])
                    await alert(f"⚠️ Scan of {source['_id']} failed: {e}")
        except Exception:
            log.exception("Poller error")
        await asyncio.sleep(60)


@app.on_message(filters.channel, group=1)
async def channel_seen(_, message):
    chat = message.chat.id
    if str(chat) in TARGETS.replace(" ", "").split(","):
        if chat not in state["targets"]:
            await seed_targets()
            await load_targets()
    elif await sources.find_one({"_id": chat, "ah": None}):
        await sources.update_one({"_id": chat}, {"$set": {"ah": await peer_hash(chat), "title": message.chat.title}})


@app.on_message(filters.channel & (filters.document | filters.video))
async def live(_, message):
    if message.chat.id in state["targets"] or str(message.chat.id) in TARGETS.replace(" ", "").split(","):
        return
    await sources.update_one(
        {"_id": message.chat.id},
        {"$setOnInsert": {"next": 1}, "$max": {"top": message.id},
         "$set": {"ah": await peer_hash(message.chat.id), "title": message.chat.title}},
        upsert=True)
    await enqueue(message)


owner = filters.private & filters.user(OWNER_ID)


@app.on_message(filters.command("addtarget") & owner)
async def add_target(_, message):
    try:
        chat = await app.get_chat(int(message.command[1]))
        sent = int(message.command[2]) if len(message.command) > 2 else None
    except Exception as e:
        return await message.reply(f"Usage: /addtarget <channel_id> [already_sent]\n❌ {e}")
    update = {"$setOnInsert": {"o": time.time_ns()}, "$set": {"ah": await peer_hash(chat.id), "title": chat.title}}
    update["$set" if sent is not None else "$setOnInsert"]["sent"] = sent or 0
    await targets.update_one({"_id": chat.id}, update, upsert=True)
    await sources.delete_one({"_id": chat.id})
    await load_targets()
    await message.reply(f"✅ Target added: {chat.title} ({chat.id})")


@app.on_message(filters.command("deltarget") & owner)
async def del_target(_, message):
    try:
        chat_id = int(message.command[1])
    except Exception:
        return await message.reply("Usage: /deltarget <channel_id>")
    await targets.delete_one({"_id": chat_id})
    await load_targets()
    await message.reply(f"🗑 Target removed: {chat_id}")


def number(text):
    return int(re.findall(r"\d+", text)[-1])


@app.on_message(filters.command(["source", "addsource"]) & owner)
async def add_source(_, message):
    args = message.command[1:]
    try:
        chat_id = int(args[0])
        start = number(args[1]) if len(args) > 1 else 1
        end = number(args[2]) if len(args) > 2 else 0
    except Exception:
        return await message.reply("Usage: /source <channel_id> [start_message_id_or_link] [end_message_id_or_link]")
    try:
        chat = await app.get_chat(chat_id)
        ah, title = await peer_hash(chat.id), chat.title
    except Exception:
        ah = title = None
    await sources.update_one({"_id": chat_id}, {"$set": {"next": start, "end": end, "ah": ah, "title": title}}, upsert=True)
    if ah:
        await message.reply(f"✅ Source added: {title} ({chat_id}) from message {start}")
    else:
        await message.reply(f"⏳ Source saved ({chat_id}) from message {start}.\nThe bot can't see this channel yet. Make it admin there and post any message in the channel, then it starts by itself.")


@app.on_message(filters.command("delsource") & owner)
async def del_source(_, message):
    try:
        chat_id = int(message.command[1])
    except Exception:
        return await message.reply("Usage: /delsource <channel_id>")
    await sources.delete_one({"_id": chat_id})
    await message.reply(f"🗑 Source removed: {chat_id}")


@app.on_message(filters.command("status") & owner)
async def status(_, message):
    lines = ["📤 Targets"]
    async for t in targets.find({}):
        lines.append(f"{t.get('title')} ({t['_id']}): {t['sent']:,} / {LIMIT:,}")
    lines.append("\n📥 Sources")
    async for s in sources.find({}):
        lines.append(f"{s.get('title')} ({s['_id']}): scanned up to {s.get('next', 1):,}, latest seen {s.get('top', 0):,}")
    lines.append(f"\n⏳ Queue: {await queue.estimated_document_count():,}")
    lines.append(f"🗃 Files remembered: {await files.estimated_document_count():,} (2 per file)")
    lines.append(f"⏱ Delay between posts: {state['delay']:.1f}s")
    await message.reply("\n".join(lines))


async def health(request):
    return web.Response(text="ok")


async def seed_targets():
    for index, raw in enumerate(filter(None, TARGETS.replace(" ", "").split(","))):
        try:
            chat = await app.get_chat(int(raw))
        except Exception as e:
            await alert(f"⚠️ Could not open target {raw}: {e}\nMake the bot admin there, then send /addtarget {raw}")
            continue
        await targets.update_one(
            {"_id": chat.id},
            {"$setOnInsert": {"sent": 0, "o": index}, "$set": {"ah": await peer_hash(chat.id), "title": chat.title}},
            upsert=True)


async def migrate():
    async for job in queue.find({"x": None}):
        await sources.update_one({"_id": job["c"]}, {"$min": {"next": job["m"]}})
        await queue.delete_one({"_id": job["_id"]})


async def main():
    web_app = web.Application()
    web_app.router.add_get("/", health)
    runner = web.AppRunner(web_app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    await app.start()
    await queue.create_index("n")
    await restore_peers()
    await seed_targets()
    await load_targets()
    await migrate()
    tasks = [asyncio.create_task(worker()), asyncio.create_task(poller())]
    await idle()
    for task in tasks:
        task.cancel()
    await app.stop()


if __name__ == "__main__":
    app.run(main())
