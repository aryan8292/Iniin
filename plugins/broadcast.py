from pyrogram import Client, filters
import datetime
import time
from database.users_chats_db import db
from info import ADMINS
from utils import broadcast_messages, broadcast_messages_group
import asyncio

BATCH_SIZE = 50  # Increase the number of concurrent broadcasts

async def broadcast_user(user_id, b_msg):
    try:
        pti, sh = await broadcast_messages(user_id, b_msg)
        if pti:
            return 'success'
        elif sh == "Blocked":
            return 'blocked'
        elif sh == "Deleted":
            return 'deleted'
        else:
            return 'failed'
    except Exception:
        return 'failed'

async def broadcast_group_msg(group_id, b_msg):
    try:
        pti, sh = await broadcast_messages_group(group_id, b_msg)
        if pti:
            return 'success'
        else:
            return 'failed'
    except Exception:
        return 'failed'

@Client.on_message(filters.command("broadcast") & filters.user(ADMINS) & filters.reply)
async def verupikkals(bot, message):
    users = await db.get_all_users()
    b_msg = message.reply_to_message
    sts = await message.reply_text(text='Broadcasting your messages...')
    start_time = time.time()
    total_users = await db.total_users_count()
    done, blocked, deleted, failed, success = 0, 0, 0, 0, 0

    user_ids = [user['id'] for user in users if 'id' in user]

    for i in range(0, len(user_ids), BATCH_SIZE):
        batch = user_ids[i:i + BATCH_SIZE]
        results = await asyncio.gather(*(broadcast_user(user_id, b_msg) for user_id in batch))

        for result in results:
            if result == 'success':
                success += 1
            elif result == 'blocked':
                blocked += 1
            elif result == 'deleted':
                deleted += 1
            elif result == 'failed':
                failed += 1

        done += len(batch)

        if done % (BATCH_SIZE * 5) == 0:  # Update less frequently to reduce overhead
            await sts.edit(f"Broadcast in progress:\n\nTotal Users {total_users}\nCompleted: {done} / {total_users}\nSuccess: {success}\nBlocked: {blocked}\nDeleted: {deleted}")

    time_taken = datetime.timedelta(seconds=int(time.time() - start_time))
    await sts.edit(f"Broadcast Completed:\nCompleted in {time_taken} seconds.\n\nTotal Users {total_users}\nCompleted: {done} / {total_users}\nSuccess: {success}\nBlocked: {blocked}\nDeleted: {deleted}")

@Client.on_message(filters.command("grp_broadcast") & filters.user(ADMINS) & filters.reply)
async def broadcast_group(bot, message):
    groups = await db.get_all_chats()
    b_msg = message.reply_to_message
    sts = await message.reply_text(text='Broadcasting your messages To Groups...')
    start_time = time.time()
    total_groups = await db.total_chat_count()
    done, failed, success = 0, 0, 0

    group_ids = [group['id'] for group in groups]

    for i in range(0, len(group_ids), BATCH_SIZE):
        batch = group_ids[i:i + BATCH_SIZE]
        results = await asyncio.gather(*(broadcast_group_msg(group_id, b_msg) for group_id in batch))

        for result in results:
            if result == 'success':
                success += 1
            elif result == 'failed':
                failed += 1

        done += len(batch)

        if done % (BATCH_SIZE * 5) == 0:  # Update less frequently to reduce overhead
            await sts.edit(f"Broadcast in progress:\n\nTotal Groups {total_groups}\nCompleted: {done} / {total_groups}\nSuccess: {success}")

    time_taken = datetime.timedelta(seconds=int(time.time() - start_time))
    await sts.edit(f"Broadcast Completed:\nCompleted in {time_taken} seconds.\n\nTotal Groups {total_groups}\nCompleted: {done} / {total_groups}\nSuccess: {success}")
