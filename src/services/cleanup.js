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
    withMessageThreadId,
    resolveUserProfileStatus
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
    let firstTimeCount = 0;
    let repeatCount = 0;
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
                            const cleanupType = (await env.TOPIC_MAP.get(`needs_verify:${userId}`))
                                ? 'repeat'
                                : 'first_time';
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
                                title: row.title || '未知',
                                cleanupType
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
                        if (result.value.cleanupType === 'repeat') {
                            repeatCount++;
                        } else {
                            firstTimeCount++;
                        }
                        cleanedUsers.push(result.value);
                        Logger.info('cleanup_user', {
                            userId: result.value.userId,
                            threadId: result.value.threadId,
                            cleanupType: result.value.cleanupType
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
                                const cleanupType = (await env.TOPIC_MAP.get(`needs_verify:${userId}`))
                                    ? 'repeat'
                                    : 'first_time';
                                keysToDelete.push(
                                    name,
                                    `verified:${userId}`,
                                    `thread:${topicThreadId}`
                                );

                                return {
                                    userId,
                                    threadId: topicThreadId,
                                    title: rec.title || '未知',
                                    cleanupType
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
                            if (result.value.cleanupType === 'repeat') {
                                repeatCount++;
                            } else {
                                firstTimeCount++;
                            }
                            cleanedUsers.push(result.value);
                            Logger.info('cleanup_user', {
                                userId: result.value.userId,
                                threadId: result.value.threadId,
                                cleanupType: result.value.cleanupType
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

        let reportText = `✅ 清理完成\n\n`;
        reportText += `📊 统计信息\n`;
        reportText += `- 扫描用户数: ${scannedCount}\n`;
        reportText += `- 已清理用户数: ${cleanedCount}\n`;
        reportText += `  - 首次清理: ${firstTimeCount}\n`;
        reportText += `  - 重复清理: ${repeatCount}\n`;
        reportText += `- 错误数: ${errorCount}\n\n`;

        if (cleanedCount > 0) {
            reportText += `🗑️ 已清理的用户 (话题已删除):\n`;
            const shownUsers = cleanedUsers.slice(0, CONFIG.MAX_CLEANUP_DISPLAY);
            const profiles = await Promise.all(shownUsers.map(async (user) => {
                if (!resolveUserProfileStatus) return { displayName: `用户${user.userId}`, statusLabel: '未知' };
                return await resolveUserProfileStatus(env, user.userId, { name: user.title });
            }));

            for (let i = 0; i < shownUsers.length; i++) {
                const user = shownUsers[i];
                const profile = profiles[i];
                reportText += `· UID: ${user.userId} | 名字: ${profile.displayName}\n`;
                reportText += `  账号状态: ${profile.statusLabel} | 话题: ${user.title}\n`;
                reportText += `  Link: (tg://user?id=${user.userId})\n`;
            }
            if (cleanedUsers.length > CONFIG.MAX_CLEANUP_DISPLAY) {
                reportText += `\n...(还有 ${cleanedUsers.length - CONFIG.MAX_CLEANUP_DISPLAY} 个用户)\n`;
            }
            reportText += `\n💡 这些用户下次发消息时将重新进行人机验证并创建新话题。`;
        } else {
            reportText += `✨ 没有发现需要清理的用户记录。`;
        }

        Logger.info('cleanup_completed', {
            cleanedCount,
            firstTimeCount,
            repeatCount,
            errorCount,
            totalUsers: scannedCount
        });

        await tgCall(env, 'sendMessage', withMessageThreadId({
            chat_id: env.SUPERGROUP_ID,
            text: reportText
        }, threadId));

    } catch (e) {
        Logger.error('cleanup_failed', e, { threadId });
        await tgCall(env, 'sendMessage', withMessageThreadId({
            chat_id: env.SUPERGROUP_ID,
            text: `❌ 清理过程出错\n\n错误信息: ${e.message}`
        }, threadId));
    } finally {
        await env.TOPIC_MAP.delete(lockKey);
    }
}
