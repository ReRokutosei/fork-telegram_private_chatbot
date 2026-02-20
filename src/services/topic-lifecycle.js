export async function getOrCreateUserTopicRecImpl({
    from,
    key,
    env,
    userId,
    hasD1,
    dbUserGet,
    safeGetJSON,
    createTopic,
    topicCreateInFlight
}) {
    if (hasD1(env)) {
        const existing = await dbUserGet(env, userId);
        if (existing && existing.thread_id) return existing;
    } else {
        const existing = await safeGetJSON(env, key, null);
        if (existing && existing.thread_id) return existing;
    }

    const inflight = topicCreateInFlight.get(String(userId));
    if (inflight) return await inflight;

    const p = (async () => {
        if (hasD1(env)) {
            const again = await dbUserGet(env, userId);
            if (again && again.thread_id) return again;
        } else {
            const again = await safeGetJSON(env, key, null);
            if (again && again.thread_id) return again;
        }
        return await createTopic(from, key, env, userId);
    })();

    topicCreateInFlight.set(String(userId), p);
    try {
        return await p;
    } finally {
        if (topicCreateInFlight.get(String(userId)) === p) {
            topicCreateInFlight.delete(String(userId));
        }
    }
}

export async function resetUserVerificationAndRequireReverifyImpl({
    env,
    userId,
    userKey,
    oldThreadId,
    pendingMsgId,
    reason,
    hasD1,
    dbUserUpdate,
    dbThreadDelete,
    CONFIG,
    threadHealthCache,
    Logger,
    sendVerificationChallenge
}) {
    if (hasD1(env)) {
        await dbUserUpdate(env, userId, { verify_state: null, verify_expires_at: null });
    } else {
        await env.TOPIC_MAP.delete(`verified:${userId}`);
    }
    await env.TOPIC_MAP.put(`needs_verify:${userId}`, '1', { expirationTtl: CONFIG.NEEDS_REVERIFY_TTL_SECONDS });
    await env.TOPIC_MAP.delete(`retry:${userId}`);

    if (userKey) {
        if (hasD1(env)) {
            await dbUserUpdate(env, userId, { thread_id: null, title: null, closed: false });
        } else {
            await env.TOPIC_MAP.delete(userKey);
        }
    }

    if (oldThreadId !== undefined && oldThreadId !== null) {
        if (hasD1(env)) {
            await dbThreadDelete(env, oldThreadId);
        } else {
            await env.TOPIC_MAP.delete(`thread:${oldThreadId}`);
        }
        await env.TOPIC_MAP.delete(`thread_ok:${oldThreadId}`);
        threadHealthCache.delete(oldThreadId);
    }

    Logger.info('verification_reset_due_to_topic_loss', {
        userId,
        oldThreadId,
        pendingMsgId,
        reason
    });

    await sendVerificationChallenge(userId, env, pendingMsgId || null);
}

export function buildTopicTitleImpl(from, CONFIG) {
    const firstName = (from.first_name || '').trim().substring(0, CONFIG.MAX_NAME_LENGTH);
    const lastName = (from.last_name || '').trim().substring(0, CONFIG.MAX_NAME_LENGTH);

    let username = '';
    if (from.username) {
        username = from.username
            .replace(/[^\w]/g, '')
            .substring(0, 20);
    }

    const cleanName = (firstName + ' ' + lastName)
        .replace(/[\u0000-\u001F\u007F-\u009F]/g, '')
        .replace(/\s+/g, ' ')
        .trim();

    const name = cleanName || 'User';
    const usernameStr = username ? ` @${username}` : '';

    const title = (name + usernameStr).substring(0, CONFIG.MAX_TITLE_LENGTH);
    return title;
}

export async function createTopicImpl({
    from,
    key,
    env,
    userId,
    CONFIG,
    tgCall,
    hasD1,
    dbUserUpdate,
    dbThreadPut,
    putWithMetadata,
    buildTopicTitle
}) {
    const title = buildTopicTitle(from);
    if (!env.SUPERGROUP_ID.toString().startsWith('-100')) throw new Error('SUPERGROUP_ID必须以-100开头');
    const res = await tgCall(env, 'createForumTopic', { chat_id: env.SUPERGROUP_ID, name: title });
    if (!res.ok) throw new Error(`创建话题失败: ${res.description}`);

    const rec = { thread_id: res.result.message_thread_id, title, closed: false };

    if (hasD1(env)) {
        await dbUserUpdate(env, userId, {
            thread_id: rec.thread_id,
            title: rec.title,
            closed: false
        });
        if (userId) {
            await dbThreadPut(env, rec.thread_id, userId);
        }
    } else {
        await putWithMetadata(env, key, rec, {
            expirationTtl: null,
            metadata: {
                userId: String(userId),
                threadId: res.result.message_thread_id
            }
        });

        if (userId) {
            await env.TOPIC_MAP.put(`thread:${rec.thread_id}`, String(userId));
        }
    }
    return rec;
}

export async function updateThreadStatusImpl({
    threadId,
    isClosed,
    env,
    hasD1,
    dbThreadGetUserId,
    dbUserGet,
    dbUserUpdate,
    dbThreadDelete,
    safeGetJSON,
    getAllKeys,
    Logger
}) {
    try {
        if (hasD1(env)) {
            const mappedUser = await dbThreadGetUserId(env, threadId);
            if (mappedUser) {
                const rec = await dbUserGet(env, mappedUser);
                if (rec && Number(rec.thread_id) === Number(threadId)) {
                    await dbUserUpdate(env, mappedUser, { closed: isClosed });
                    Logger.info('thread_status_updated', { threadId, isClosed, updatedCount: 1 });
                    return;
                }
                await dbThreadDelete(env, threadId);
            }

            const result = await env.TG_BOT_DB
                .prepare('SELECT user_id FROM users WHERE thread_id = ?')
                .bind(String(threadId))
                .all();

            const rows = result?.results || [];
            for (const row of rows) {
                await dbUserUpdate(env, row.user_id, { closed: isClosed });
            }
            Logger.info('thread_status_updated', { threadId, isClosed, updatedCount: rows.length });
            return;
        }

        const mappedUser = await env.TOPIC_MAP.get(`thread:${threadId}`);
        if (mappedUser) {
            const userKey = `user:${mappedUser}`;
            const rec = await safeGetJSON(env, userKey, null);
            if (rec && Number(rec.thread_id) === Number(threadId)) {
                rec.closed = isClosed;
                await env.TOPIC_MAP.put(userKey, JSON.stringify(rec));
                Logger.info('thread_status_updated', { threadId, isClosed, updatedCount: 1 });
                return;
            }
            await env.TOPIC_MAP.delete(`thread:${threadId}`);
        }

        const allKeys = await getAllKeys(env, 'user:');
        const updates = [];

        for (const { name } of allKeys) {
            const rec = await safeGetJSON(env, name, null);
            if (rec && Number(rec.thread_id) === Number(threadId)) {
                rec.closed = isClosed;
                updates.push(env.TOPIC_MAP.put(name, JSON.stringify(rec)));
            }
        }

        await Promise.all(updates);
        Logger.info('thread_status_updated', { threadId, isClosed, updatedCount: updates.length });
    } catch (e) {
        Logger.error('thread_status_update_failed', e, { threadId, isClosed });
        throw e;
    }
}
