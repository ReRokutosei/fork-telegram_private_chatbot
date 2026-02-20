export async function handleCleanupCommandImpl({
    threadId,
    env,
    CONFIG,
    hasD1,
    dbListUsers,
    probeForumThread,
    resetUserVerificationAndRequireReverify,
    Logger,
    safeGetJSON,
    deleteBulk,
    tgCall,
    withMessageThreadId
}) {
    const lockKey = 'cleanup:lock';
    const locked = await env.TOPIC_MAP.get(lockKey);
    if (locked) {
        await tgCall(env, 'sendMessage', withMessageThreadId({
            chat_id: env.SUPERGROUP_ID,
            text: '⏳ **已有清理任务正在运行，请稍后再试。**',
            parse_mode: 'Markdown'
        }, threadId));
        return;
    }

    await env.TOPIC_MAP.put(lockKey, '1', { expirationTtl: CONFIG.CLEANUP_LOCK_TTL_SECONDS });

    await tgCall(env, 'sendMessage', withMessageThreadId({
        chat_id: env.SUPERGROUP_ID,
        text: '🔄 **正在扫描需要清理的用户...**',
        parse_mode: 'Markdown'
    }, threadId));

    let cleanedCount = 0;
    let errorCount = 0;
    const cleanedUsers = [];
    let scannedCount = 0;

    try {
        if (hasD1(env)) {
            let offset = 0;
            while (true) {
                const rows = await dbListUsers(env, CONFIG.CLEANUP_BATCH_SIZE, offset);
                if (!rows.length) break;
                scannedCount += rows.length;

                const results = await Promise.allSettled(
                    rows.map(async (row) => {
                        if (!row.thread_id) return null;
                        const userId = row.user_id;
                        const topicThreadId = row.thread_id;

                        const probe = await probeForumThread(env, topicThreadId, {
                            userId,
                            reason: 'cleanup_check',
                            doubleCheckOnMissingThreadId: false
                        });

                        if (probe.status === 'redirected' || probe.status === 'missing') {
                            await resetUserVerificationAndRequireReverify(env, {
                                userId,
                                userKey: null,
                                oldThreadId: topicThreadId,
                                pendingMsgId: null,
                                reason: 'cleanup_check'
                            });

                            return {
                                userId,
                                threadId: topicThreadId,
                                title: row.title || '未知'
                            };
                        } else if (probe.status === 'probe_invalid') {
                            Logger.warn('cleanup_probe_invalid_message', {
                                userId,
                                threadId: topicThreadId,
                                errorDescription: probe.description
                            });
                        } else if (probe.status === 'unknown_error') {
                            Logger.warn('cleanup_probe_failed_unknown', {
                                userId,
                                threadId: topicThreadId,
                                errorDescription: probe.description
                            });
                        } else if (probe.status === 'missing_thread_id') {
                            Logger.warn('cleanup_probe_missing_thread_id', { userId, threadId: topicThreadId });
                        }

                        return null;
                    })
                );

                results.forEach(result => {
                    if (result.status === 'fulfilled' && result.value) {
                        cleanedCount++;
                        cleanedUsers.push(result.value);
                        Logger.info('cleanup_user', {
                            userId: result.value.userId,
                            threadId: result.value.threadId
                        });
                    } else if (result.status === 'rejected') {
                        errorCount++;
                        Logger.error('cleanup_batch_error', result.reason);
                    }
                });

                offset += rows.length;
                await new Promise(r => setTimeout(r, 200));
            }
        } else {
            const keysToDelete = [];
            let cursor = undefined;
            do {
                const result = await env.TOPIC_MAP.list({ prefix: 'user:', cursor });
                const names = (result.keys || []).map(k => k.name);
                scannedCount += names.length;

                for (let i = 0; i < names.length; i += CONFIG.CLEANUP_BATCH_SIZE) {
                    const batch = names.slice(i, i + CONFIG.CLEANUP_BATCH_SIZE);

                    const results = await Promise.allSettled(
                        batch.map(async (name) => {
                            const rec = await safeGetJSON(env, name, null);
                            if (!rec || !rec.thread_id) return null;

                            const userId = name.slice(5);
                            const topicThreadId = rec.thread_id;

                            const probe = await probeForumThread(env, topicThreadId, {
                                userId,
                                reason: 'cleanup_check',
                                doubleCheckOnMissingThreadId: false
                            });

                            if (probe.status === 'redirected' || probe.status === 'missing') {
                                keysToDelete.push(
                                    name,
                                    `verified:${userId}`,
                                    `thread:${topicThreadId}`
                                );

                                return {
                                    userId,
                                    threadId: topicThreadId,
                                    title: rec.title || '未知'
                                };
                            } else if (probe.status === 'probe_invalid') {
                                Logger.warn('cleanup_probe_invalid_message', {
                                    userId,
                                    threadId: topicThreadId,
                                    errorDescription: probe.description
                                });
                            } else if (probe.status === 'unknown_error') {
                                Logger.warn('cleanup_probe_failed_unknown', {
                                    userId,
                                    threadId: topicThreadId,
                                    errorDescription: probe.description
                                });
                            } else if (probe.status === 'missing_thread_id') {
                                Logger.warn('cleanup_probe_missing_thread_id', { userId, threadId: topicThreadId });
                            }

                            return null;
                        })
                    );

                    results.forEach(result => {
                        if (result.status === 'fulfilled' && result.value) {
                            cleanedCount++;
                            cleanedUsers.push(result.value);
                            Logger.info('cleanup_user', {
                                userId: result.value.userId,
                                threadId: result.value.threadId
                            });
                        } else if (result.status === 'rejected') {
                            errorCount++;
                            Logger.error('cleanup_batch_error', result.reason);
                        }
                    });

                    if (i + CONFIG.CLEANUP_BATCH_SIZE < names.length) {
                        await new Promise(r => setTimeout(r, 600));
                    }
                }

                cursor = result.list_complete ? undefined : result.cursor;

                if (cursor) {
                    await new Promise(r => setTimeout(r, 200));
                }
            } while (cursor);

            if (keysToDelete.length > 0) {
                const deletedCount = await deleteBulk(env, keysToDelete);
                Logger.info('cleanup_bulk_delete', { deletedKeyCount: deletedCount });
            }
        }

        let reportText = `✅ **清理完成**

`;
        reportText += `📊 **统计信息**
`;
        reportText += `- 扫描用户数: ${scannedCount}
`;
        reportText += `- 已清理用户数: ${cleanedCount}
`;
        reportText += `- 错误数: ${errorCount}

`;

        if (cleanedCount > 0) {
            reportText += `🗑️ **已清理的用户** (话题已删除):
`;
            for (const user of cleanedUsers.slice(0, CONFIG.MAX_CLEANUP_DISPLAY)) {
                reportText += `- UID: [${user.userId}](tg://user?id=${user.userId}) | 话题: ${user.title}
`;
            }
            if (cleanedUsers.length > CONFIG.MAX_CLEANUP_DISPLAY) {
                reportText += `
...(还有 ${cleanedUsers.length - CONFIG.MAX_CLEANUP_DISPLAY} 个用户)
`;
            }
            reportText += `
💡 这些用户下次发消息时将重新进行人机验证并创建新话题。`;
        } else {
            reportText += `✨ 没有发现需要清理的用户记录。`;
        }

        Logger.info('cleanup_completed', {
            cleanedCount,
            errorCount,
            totalUsers: scannedCount
        });

        await tgCall(env, 'sendMessage', withMessageThreadId({
            chat_id: env.SUPERGROUP_ID,
            text: reportText,
            parse_mode: 'Markdown'
        }, threadId));

    } catch (e) {
        Logger.error('cleanup_failed', e, { threadId });
        await tgCall(env, 'sendMessage', withMessageThreadId({
            chat_id: env.SUPERGROUP_ID,
            text: `❌ **清理过程出错**

错误信息: \`${e.message}\``,
            parse_mode: 'Markdown'
        }, threadId));
    } finally {
        await env.TOPIC_MAP.delete(lockKey);
    }
}
