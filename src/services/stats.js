const QUEUE_PREFIX = 'queue:';

export async function getBotStatsImpl(env, deps) {
    const { getAllKeys, hasD1, dbCount, safeGetBulk, Logger } = deps;

    try {
        const queueKeys = await getAllKeys(env, QUEUE_PREFIX);
        if (hasD1(env)) {
            const totalUsers = await dbCount(env);
            const verifiedUsers = await dbCount(env, 'WHERE verify_state IS NOT NULL');
            const bannedUsers = await dbCount(env, 'WHERE is_blocked = 1');
            const totalTopics = await dbCount(env, 'WHERE thread_id IS NOT NULL');
            const closedTopics = await dbCount(env, 'WHERE thread_id IS NOT NULL AND closed = 1');

            return {
                totalUsers,
                verifiedUsers,
                bannedUsers,
                totalTopics,
                closedTopics,
                activeTopics: totalTopics - closedTopics,
                queuedMessages: queueKeys.length,
                timestamp: Date.now()
            };
        }

        const userKeys = await getAllKeys(env, 'user:');
        const verifiedKeys = await getAllKeys(env, 'verified:');
        const bannedKeys = await getAllKeys(env, 'banned:');

        const userDataMap = await safeGetBulk(env, userKeys.map(k => k.name));

        let totalTopics = 0;
        let closedTopics = 0;

        for (const [, userData] of userDataMap) {
            if (userData && userData.thread_id) {
                totalTopics++;
                if (userData.closed) closedTopics++;
            }
        }

        return {
            totalUsers: userKeys.length,
            verifiedUsers: verifiedKeys.length,
            bannedUsers: bannedKeys.length,
            totalTopics,
            closedTopics,
            activeTopics: totalTopics - closedTopics,
            queuedMessages: queueKeys.length,
            timestamp: Date.now()
        };
    } catch (e) {
        Logger.error('get_bot_stats_failed', e);
        return null;
    }
}

export async function exportUserDataImpl(env, userIds = null, deps) {
    const { hasD1, getAllKeys, safeGetBulk, getValueWithFullMetadata, Logger } = deps;

    try {
        const exported = [];

        if (hasD1(env)) {
            if (userIds && Array.isArray(userIds) && userIds.length > 0) {
                const placeholders = userIds.map(() => '?').join(',');
                const result = await env.TG_BOT_DB
                    .prepare(`SELECT * FROM users WHERE user_id IN (${placeholders})`)
                    .bind(...userIds.map(String))
                    .all();
                for (const row of result?.results || []) {
                    exported.push({
                        userId: row.user_id,
                        userData: {
                            thread_id: row.thread_id,
                            title: row.title,
                            closed: !!row.closed
                        },
                        verified: !!row.verify_state,
                        banned: !!row.is_blocked,
                        metadata: {},
                        createdAt: row.created_at,
                        updatedAt: row.updated_at
                    });
                }
            } else {
                const result = await env.TG_BOT_DB
                    .prepare('SELECT * FROM users')
                    .all();
                for (const row of result?.results || []) {
                    exported.push({
                        userId: row.user_id,
                        userData: {
                            thread_id: row.thread_id,
                            title: row.title,
                            closed: !!row.closed
                        },
                        verified: !!row.verify_state,
                        banned: !!row.is_blocked,
                        metadata: {},
                        createdAt: row.created_at,
                        updatedAt: row.updated_at
                    });
                }
            }

            Logger.info('user_data_exported', { userCount: exported.length });
            return { userCount: exported.length, data: exported };
        }

        let keysToExport;

        if (userIds && Array.isArray(userIds)) {
            keysToExport = userIds.map(uid => `user:${uid}`);
        } else {
            const allUserKeys = await getAllKeys(env, 'user:');
            keysToExport = allUserKeys.map(k => k.name);
        }

        if (keysToExport.length === 0) {
            return { userCount: 0, data: [] };
        }

        for (let i = 0; i < keysToExport.length; i += 100) {
            const batch = keysToExport.slice(i, i + 100);
            const results = await safeGetBulk(env, batch);

            for (const [key, value] of results) {
                if (value) {
                    const userId = key.replace('user:', '');
                    const fullMetadata = await getValueWithFullMetadata(env, key);

                    exported.push({
                        userId,
                        userData: value,
                        verified: !!await env.TOPIC_MAP.get(`verified:${userId}`),
                        banned: !!await env.TOPIC_MAP.get(`banned:${userId}`),
                        metadata: fullMetadata?.metadata || {},
                        createdAt: fullMetadata?.createdAt,
                        updatedAt: fullMetadata?.updatedAt
                    });
                }
            }
        }

        Logger.info('user_data_exported', { userCount: exported.length });
        return { userCount: exported.length, data: exported };
    } catch (e) {
        Logger.error('export_user_data_failed', e);
        return { userCount: 0, data: [], error: e.message };
    }
}

export async function getUserActivityStatsImpl(env, limit = 50, deps) {
    const { hasD1, getAllKeys, getValueWithFullMetadata, Logger } = deps;

    try {
        if (hasD1(env)) {
            const result = await env.TG_BOT_DB
                .prepare('SELECT user_id, created_at, updated_at FROM users ORDER BY updated_at DESC LIMIT ?')
                .bind(limit)
                .all();
            return (result?.results || []).map(row => ({
                userId: row.user_id,
                createdAt: row.created_at,
                updatedAt: row.updated_at,
                ageSeconds: row.created_at ? Math.floor((Date.now() - row.created_at) / 1000) : null,
                metadata: {}
            }));
        }

        const userKeys = await getAllKeys(env, 'user:');
        const stats = [];

        for (const keyInfo of userKeys.slice(0, limit * 2)) {
            const fullMetadata = await getValueWithFullMetadata(env, keyInfo.name);
            if (fullMetadata) {
                const userId = keyInfo.name.replace('user:', '');
                stats.push({
                    userId,
                    createdAt: fullMetadata.createdAt,
                    updatedAt: fullMetadata.updatedAt,
                    ageSeconds: fullMetadata.ageSeconds,
                    metadata: fullMetadata.metadata
                });
            }
        }

        stats.sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0));

        return stats.slice(0, limit);
    } catch (e) {
        Logger.error('get_activity_stats_failed', e);
        return [];
    }
}
