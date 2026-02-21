export async function handlePrivateMessageImpl(msg, env, ctx, deps) {
    const { forwardToTopic, withUserLock, tgCall, Logger } = deps;
    const userId = msg.chat?.id;
    if (!userId || !withUserLock) {
        await forwardToTopic(msg, env, ctx);
        return;
    }

    try {
        await withUserLock(env, userId, async () => {
            await forwardToTopic(msg, env, ctx);
        });
    } catch (e) {
        if (e?.code === 'USER_LOCK_TIMEOUT' || e?.code === 'USER_LOCK_LOST') {
            await tgCall(env, "sendMessage", {
                chat_id: userId,
                text: "⏳ 当前请求处理中，请稍后重试。"
            });
            Logger.warn('user_lock_blocked', { userId, code: e.code, error: e.message });
            return;
        }
        throw e;
    }
}

export async function forwardToTopicImpl(msg, env, ctx, deps) {
    const {
        checkRateLimit,
        CONFIG,
        tgCall,
        hasD1,
        dbIsBanned,
        dbGetVerifyState,
        sendVerificationChallenge,
        getFilterText,
        matchKeyword,
        Logger,
        dbUserGet,
        safeGetJSON,
        getOrCreateUserTopicRec,
        sendWelcomeCard,
        dbThreadGetUserId,
        dbThreadPut,
        threadHealthCache,
        probeForumThread,
        resetUserVerificationAndRequireReverify,
        handleMediaGroup,
        normalizeTgDescription,
        isTopicMissingOrDeleted,
        dbMessageMapPut
    } = deps;

    const userId = msg.chat.id;
    const key = `user:${userId}`;

    // 速率限制检查
    const rateLimit = await checkRateLimit(userId, env, 'message', CONFIG.RATE_LIMIT_MESSAGE, CONFIG.RATE_LIMIT_WINDOW);
    if (!rateLimit.allowed) {
        await tgCall(env, "sendMessage", {
            chat_id: userId,
            text: "⚠️ 发送过于频繁，请稍后再试。"
        });
        return;
    }

    // 拦截普通用户命令
    if (msg.text && msg.text.startsWith("/") && msg.text.trim() !== "/start") {
        return;
    }

    // 检查封禁
    const isBanned = hasD1(env)
        ? await dbIsBanned(env, userId)
        : await env.TOPIC_MAP.get(`banned:${userId}`);
    if (isBanned) return;

    // 检查验证状态
    const verified = hasD1(env)
        ? await dbGetVerifyState(env, userId)
        : await env.TOPIC_MAP.get(`verified:${userId}`);
    if (!verified) {
        const isStart = msg.text && msg.text.trim() === "/start";
        const pendingMsgId = isStart ? null : msg.message_id;
        await sendVerificationChallenge(userId, env, pendingMsgId);
        return;
    }

    // 检查是否需要重新验证
    const needsVerify = await env.TOPIC_MAP.get(`needs_verify:${userId}`);
    if (needsVerify) {
        await sendVerificationChallenge(userId, env, msg.message_id || null);
        return;
    }

    // 关键词过滤
    const filterText = getFilterText(msg);
    if (filterText) {
        const hitKeyword = await matchKeyword(env, filterText);
        if (hitKeyword) {
            await tgCall(env, "sendMessage", {
                chat_id: userId,
                text: "⚠️ 该消息触发过滤条件，已被拦截。"
            });
            Logger.info('keyword_blocked', { userId, keyword: hitKeyword });
            return;
        }
    }

    // 获取用户话题记录
    let rec = hasD1(env)
        ? await dbUserGet(env, userId)
        : await safeGetJSON(env, key, null);

    if (rec && rec.closed) {
        await tgCall(env, "sendMessage", { chat_id: userId, text: "🚫 当前对话已被管理员关闭。" });
        return;
    }

    // 重试计数器
    const retryKey = `retry:${userId}`;
    let retryCount = parseInt(await env.TOPIC_MAP.get(retryKey) || "0");

    if (retryCount > CONFIG.MAX_RETRY_ATTEMPTS) {
        await tgCall(env, "sendMessage", {
            chat_id: userId,
            text: "❌ 系统繁忙，请稍后再试。"
        });
        await env.TOPIC_MAP.delete(retryKey);
        return;
    }

    if (!rec || !rec.thread_id) {
        rec = await getOrCreateUserTopicRec(msg.from, key, env, userId);
        if (!rec || !rec.thread_id) {
            throw new Error("创建话题失败");
        }

        // 新用户接入：发送用户信息卡片
        await sendWelcomeCard(env, rec.thread_id, userId, msg.from);
    }

    // 补建 thread->user 映射（兼容旧数据）
    if (rec && rec.thread_id) {
        if (hasD1(env)) {
            const mappedUser = await dbThreadGetUserId(env, rec.thread_id);
            if (!mappedUser) {
                await dbThreadPut(env, rec.thread_id, userId);
            }
        } else {
            const mappedUser = await env.TOPIC_MAP.get(`thread:${rec.thread_id}`);
            if (!mappedUser) {
                await env.TOPIC_MAP.put(`thread:${rec.thread_id}`, String(userId));
            }
        }
    }

    // 验证话题健康状态
    if (rec && rec.thread_id) {
        const cacheKey = rec.thread_id;
        const now = Date.now();
        const cached = threadHealthCache.get(cacheKey);
        const withinTTL = cached && (now - cached.ts < CONFIG.THREAD_HEALTH_TTL_MS);

        if (!withinTTL) {
            const kvHealthKey = `thread_ok:${rec.thread_id}`;
            const kvHealthOk = await env.TOPIC_MAP.get(kvHealthKey);
            if (kvHealthOk === "1") {
                threadHealthCache.set(cacheKey, { ts: now, ok: true });
            } else {
                const probe = await probeForumThread(env, rec.thread_id, { userId, reason: "health_check" });

                if (probe.status === "redirected" || probe.status === "missing" || probe.status === "missing_thread_id") {
                    await resetUserVerificationAndRequireReverify(env, {
                        userId,
                        userKey: key,
                        oldThreadId: rec.thread_id,
                        pendingMsgId: msg.message_id,
                        reason: `health_check:${probe.status}`,
                        userFrom: msg.from
                    });
                    return;
                } else if (probe.status === "probe_invalid") {
                    Logger.warn('topic_health_probe_invalid_message', {
                        userId,
                        threadId: rec.thread_id,
                        errorDescription: probe.description
                    });
                    threadHealthCache.set(cacheKey, { ts: now, ok: true });
                    await env.TOPIC_MAP.put(kvHealthKey, "1", { expirationTtl: Math.ceil(CONFIG.THREAD_HEALTH_TTL_MS / 1000) });
                } else if (probe.status === "unknown_error") {
                    Logger.warn('topic_test_failed_unknown', {
                        userId,
                        threadId: rec.thread_id,
                        errorDescription: probe.description
                    });
                } else {
                    await env.TOPIC_MAP.delete(retryKey);
                    threadHealthCache.set(cacheKey, { ts: now, ok: true });
                    await env.TOPIC_MAP.put(kvHealthKey, "1", { expirationTtl: Math.ceil(CONFIG.THREAD_HEALTH_TTL_MS / 1000) });
                }
            }
        }
    }

    // 处理媒体组
    if (msg.media_group_id) {
        await handleMediaGroup(msg, env, ctx, {
            direction: "p2t",
            targetChat: env.SUPERGROUP_ID,
            threadId: rec.thread_id
        });
        return;
    }

    // 转发消息
    const copyResult = await tgCall(env, "copyMessage", {
        chat_id: env.SUPERGROUP_ID,
        from_chat_id: userId,
        message_id: msg.message_id,
        message_thread_id: rec.thread_id,
    });

    // 检测静默重定向到 General
    const resThreadId = copyResult.result?.message_thread_id;
    if (copyResult.ok && resThreadId !== undefined && resThreadId !== null && Number(resThreadId) !== Number(rec.thread_id)) {
        Logger.warn('forward_redirected_to_general', {
            userId,
            expectedThreadId: rec.thread_id,
            actualThreadId: resThreadId
        });

        if (copyResult.result?.message_id) {
            try {
                await tgCall(env, "deleteMessage", {
                    chat_id: env.SUPERGROUP_ID,
                    message_id: copyResult.result.message_id
                });
            } catch (e) {
                // 忽略删除失败
            }
        }
        await resetUserVerificationAndRequireReverify(env, {
            userId,
            userKey: key,
            oldThreadId: rec.thread_id,
            pendingMsgId: msg.message_id,
            reason: "forward_redirected_to_general",
            userFrom: msg.from
        });
        return;
    }

    // 兜底：检查返回结果是否缺少线程ID
    if (copyResult.ok && (resThreadId === undefined || resThreadId === null)) {
        const probe = await probeForumThread(env, rec.thread_id, { userId, reason: "forward_result_missing_thread_id" });
        if (probe.status !== "ok") {
            Logger.warn('forward_suspected_redirect_or_missing', {
                userId,
                expectedThreadId: rec.thread_id,
                probeStatus: probe.status,
                probeDescription: probe.description
            });

            if (copyResult.result?.message_id) {
                try {
                    await tgCall(env, "deleteMessage", {
                        chat_id: env.SUPERGROUP_ID,
                        message_id: copyResult.result.message_id
                    });
                } catch (e) {
                    // 忽略删除失败
                }
            }
            await resetUserVerificationAndRequireReverify(env, {
                userId,
                userKey: key,
                oldThreadId: rec.thread_id,
                pendingMsgId: msg.message_id,
                reason: `forward_missing_thread_id:${probe.status}`,
                userFrom: msg.from
            });
            return;
        }
    }

    // 额外检查：转发失败情况
    if (!copyResult.ok) {
        const desc = normalizeTgDescription(copyResult.description);
        if (isTopicMissingOrDeleted(desc)) {
            Logger.warn('forward_failed_topic_missing', {
                userId,
                threadId: rec.thread_id,
                errorDescription: copyResult.description
            });
            await resetUserVerificationAndRequireReverify(env, {
                userId,
                userKey: key,
                oldThreadId: rec.thread_id,
                pendingMsgId: msg.message_id,
                reason: "forward_failed_topic_missing",
                userFrom: msg.from
            });
            return;
        }

        if (desc.includes("chat not found")) throw new Error(`群组ID错误: ${env.SUPERGROUP_ID}`);
        if (desc.includes("not enough rights")) throw new Error("机器人权限不足 (需 Manage Topics)");

        await tgCall(env, "sendMessage", {
            chat_id: userId,
            text: "❌ 消息发送失败，请稍后重试。"
        });
        return;
    }

    // 记录消息映射关系
    if (hasD1(env)) {
        await dbMessageMapPut(env, userId, msg.message_id, env.SUPERGROUP_ID, copyResult.result.message_id);
    } else {
        const mapKey = `msg_map:${String(userId)}:${msg.message_id}`;
        const mapValue = JSON.stringify({
            targetChatId: String(env.SUPERGROUP_ID),
            targetMsgId: copyResult.result.message_id,
            createdAt: Date.now()
        });
        await env.TOPIC_MAP.put(mapKey, mapValue, {
            expirationTtl: CONFIG.MESSAGE_MAP_TTL_SECONDS
        });
    }
}
