export async function handleMediaGroupImpl({
    msg,
    env,
    ctx,
    direction,
    targetChat,
    threadId,
    tgCall,
    withMessageThreadId,
    safeGetJSON,
    delaySend,
    CONFIG
}) {
    const groupId = msg.media_group_id;
    const key = `mg:${direction}:${groupId}`;
    const item = extractMedia(msg);
    if (!item) {
        await tgCall(env, 'copyMessage', withMessageThreadId({
            chat_id: targetChat,
            from_chat_id: msg.chat.id,
            message_id: msg.message_id
        }, threadId));
        return;
    }
    let rec = await safeGetJSON(env, key, null);
    if (!rec) rec = { direction, targetChat, threadId: (threadId === null ? undefined : threadId), items: [], last_ts: Date.now() };
    rec.items.push({ ...item, msg_id: msg.message_id });
    rec.last_ts = Date.now();
    await env.TOPIC_MAP.put(key, JSON.stringify(rec), { expirationTtl: CONFIG.MEDIA_GROUP_EXPIRE_SECONDS });
    ctx.waitUntil(delaySend(env, key, rec.last_ts));
}

function extractMedia(msg) {
    if (msg.photo && msg.photo.length > 0) {
        const highestResolution = msg.photo[msg.photo.length - 1];
        return {
            type: 'photo',
            id: highestResolution.file_id,
            cap: msg.caption || ''
        };
    }

    if (msg.video) {
        return {
            type: 'video',
            id: msg.video.file_id,
            cap: msg.caption || ''
        };
    }

    if (msg.document) {
        return {
            type: 'document',
            id: msg.document.file_id,
            cap: msg.caption || ''
        };
    }

    if (msg.audio) {
        return {
            type: 'audio',
            id: msg.audio.file_id,
            cap: msg.caption || ''
        };
    }

    if (msg.animation) {
        return {
            type: 'animation',
            id: msg.animation.file_id,
            cap: msg.caption || ''
        };
    }

    return null;
}

export async function flushExpiredMediaGroupsImpl({ env, now, getAllKeys, safeGetJSON, Logger }) {
    try {
        const prefix = 'mg:';
        const allKeys = await getAllKeys(env, prefix);
        let deletedCount = 0;

        for (const { name } of allKeys) {
            const rec = await safeGetJSON(env, name, null);
            if (rec && rec.last_ts && (now - rec.last_ts > 300000)) {
                await env.TOPIC_MAP.delete(name);
                deletedCount++;
            }
        }

        if (deletedCount > 0) {
            Logger.info('media_groups_cleaned', { deletedCount });
        }
    } catch (e) {
        Logger.error('media_group_cleanup_failed', e);
    }
}

export async function delaySendImpl({
    env,
    key,
    ts,
    CONFIG,
    safeGetJSON,
    Logger,
    tgCall,
    withMessageThreadId
}) {
    await new Promise(r => setTimeout(r, CONFIG.MEDIA_GROUP_DELAY_MS));

    const rec = await safeGetJSON(env, key, null);

    if (rec && rec.last_ts === ts) {
        if (!rec.items || rec.items.length === 0) {
            Logger.warn('media_group_empty', { key });
            await env.TOPIC_MAP.delete(key);
            return;
        }

        const media = rec.items.map((it, i) => {
            if (!it.type || !it.id) {
                Logger.warn('media_group_invalid_item', { key, item: it });
                return null;
            }
            const caption = i === 0 ? (it.cap || '').substring(0, 1024) : '';
            return {
                type: it.type,
                media: it.id,
                caption
            };
        }).filter(Boolean);

        if (media.length > 0) {
            try {
                const result = await tgCall(env, 'sendMediaGroup', withMessageThreadId({
                    chat_id: rec.targetChat,
                    media
                }, rec.threadId));

                if (!result.ok) {
                    Logger.error('media_group_send_failed', result.description, {
                        key,
                        mediaCount: media.length
                    });
                } else {
                    Logger.info('media_group_sent', {
                        key,
                        mediaCount: media.length,
                        targetChat: rec.targetChat
                    });
                }
            } catch (e) {
                Logger.error('media_group_send_exception', e, { key });
            }
        }

        await env.TOPIC_MAP.delete(key);
    }
}
