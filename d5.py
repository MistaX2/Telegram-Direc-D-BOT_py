import asyncio
import json
import os
import re
import urllib.parse
from pyrogram import Client, filters, idle
from aiohttp import web

# --- CONFIG ---
API_ID = 30522731
API_HASH = "_"
BOT_TOKEN = "_"
BIN_CHANNEL = -ID
PORT = 8000
VPS_IP = "Doamin or IP" 
DB_FILE = "database.json"
P_THUMB = "https://github.com/MistaX2/vps/blob/main/photo_2026-05-04_22-32-40.jpg?raw=true"

if os.path.exists(DB_FILE):
    with open(DB_FILE, "r") as f: file_db = json.load(f)
else: file_db = {}

def save_db():
    with open(DB_FILE, "w") as f: json.dump(file_db, f)

app = Client("my_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, sleep_threshold=120)
request_queue = asyncio.Queue()

async def stream_handler(request):
    try:
        msg_id = int(request.match_info['msg_id'])
        msg = await app.get_messages(BIN_CHANNEL, msg_id)
        if not msg or not msg.media:
            return web.Response(text="File not found", status=404)

        file_obj = msg.document or msg.video or msg.audio
        file_size = file_obj.file_size
        file_name = getattr(file_obj, 'file_name', None) or 'file.mp4'

        try:
            encoded_name = urllib.parse.quote(file_name)
        except:
            encoded_name = "file.mp4"

        range_header = request.headers.get('Range')
        from_bytes = 0
        to_bytes = file_size - 1

        if range_header:
            match = re.search(r'bytes=(\d+)-(\d*)', range_header)
            if match:
                from_bytes = int(match.group(1))
                if match.group(2):
                    to_bytes = int(match.group(2))

        tg_offset = (from_bytes // 1048576) * 1048576
        skip_bytes = from_bytes - tg_offset

        headers = {
            'Content-Type': getattr(file_obj, 'mime_type', 'application/octet-stream'),
            'Accept-Ranges': 'bytes',
            'Content-Disposition': f'attachment; filename="{encoded_name}"',
        }

        if range_header:
            headers['Content-Range'] = f'bytes {from_bytes}-{to_bytes}/{file_size}'
            headers['Content-Length'] = str(to_bytes - from_bytes + 1)
            response = web.StreamResponse(status=206, headers=headers)
        else:
            headers['Content-Length'] = str(file_size)
            response = web.StreamResponse(status=200, headers=headers)

        await response.prepare(request)
        current_pos = 0
        limit_pos = int(headers['Content-Length'])

        async for chunk in app.stream_media(msg, offset=tg_offset):
            if skip_bytes > 0:
                if skip_bytes >= len(chunk):
                    skip_bytes -= len(chunk)
                    continue
                else:
                    chunk = chunk[skip_bytes:]
                    skip_bytes = 0
            if current_pos + len(chunk) > limit_pos:
                chunk = chunk[:limit_pos - current_pos]
            await response.write(chunk)
            current_pos += len(chunk)
            if current_pos >= limit_pos:
                break
        return response
    except Exception as e:
        return web.Response(status=500, text=str(e))

async def worker():
    while True:
        message = await request_queue.get()
        try:
            file_obj = message.document or message.video or message.audio
            unique_id = file_obj.file_unique_id
            file_name = getattr(file_obj, 'file_name', 'Unknown_File')

            if unique_id in file_db:
                msg_id = file_db[unique_id]
            else:
                forwarded = await message.forward(BIN_CHANNEL)
                msg_id = forwarded.id
                file_db[unique_id] = msg_id
                save_db()

            download_link = f"http://{VPS_IP}:{PORT}/{msg_id}"
            caption = (
                f"✅ **ගොනුව සූදානම්!**\n"
                f"**Name:** `{file_name}`\n\n"
                f"🚀 **Link:** `{download_link}`\n\n"
                f"DᕮᐯᕮᒪOᑭᕮD ᗷY [MrX](https://t.me/Mr_X_2z0)"
            )

            try:
                await message.reply_photo(photo=P_THUMB, caption=caption)
            except:
                await message.reply_text(caption)
            await asyncio.sleep(2)
        except Exception as e:
            print(f"Worker Error: {e}")
        finally:
            request_queue.task_done()

@app.on_message(filters.command("start") & filters.private)
async def start_cmd(c, m):
    try:
        await m.reply_photo(photo=P_THUMB, caption="👋 බොට් සූදානම්! මට ෆයිල් එකක් එවන්න.")
    except:
        await m.reply_text("👋 බොට් සූදානම්! මට ෆයිල් එකක් එවන්න.")

@app.on_message((filters.document | filters.video | filters.audio) & filters.private)
async def handle_file(c, m):
    await request_queue.put(m)
    if request_queue.qsize() > 1:
        await m.reply_text(f"📥 පෝලිමට එක් කළා. ස්ථානය: {request_queue.qsize()}")

async def main():
    await app.start()
    server = web.Application()
    server.router.add_get('/{msg_id}', stream_handler)
    runner = web.AppRunner(server)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    asyncio.create_task(worker())
    print(f"🚀 Bot Running on Port {PORT}!")
    await idle()

if __name__ == "__main__":
    app.run(main())
