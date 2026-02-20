import { CONFIG } from '../config/constants.js';
import { Logger } from '../core/logger.js';

const keywordCache = {
    ts: 0,
    list: []
};

export function hasD1(env) {
    return !!env.TG_BOT_DB;
}

function shouldRetryD1Error(error) {
    const message = String(error?.message || error || '');
    const retryable = [
        'Network connection lost',
        'Socket was closed',
        'reset because its code was updated',
        'storage reset because its code was updated'
    ];
    return retryable.some((text) => message.includes(text));
}

async function runD1Write(env, action, fn) {
    let attempt = 0;
    while (true) {
        try {
            return await fn();
        } catch (e) {
            attempt++;
            const shouldRetry = shouldRetryD1Error(e) && attempt < CONFIG.D1_WRITE_MAX_RETRIES;
            if (!shouldRetry) {
                Logger.error('d1_write_failed', e, { action, attempt });
                throw e;
            }
            const base = CONFIG.D1_WRITE_BASE_DELAY_MS;
            const max = CONFIG.D1_WRITE_MAX_DELAY_MS;
            const delay = Math.min(max, base * (2 ** (attempt - 1)) + Math.floor(Math.random() * base));
            Logger.warn('d1_write_retry', { action, attempt, delay });
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
}

function toDbBool(val) {
    return val ? 1 : 0;
}

function normalizeUserRecord(row) {
    if (!row) return null;
    return {
        thread_id: row.thread_id ?? null,
        title: row.title ?? null,
        closed: row.closed ? true : false
    };
}

async function ensureUserRow(env, userId) {
    if (!hasD1(env)) return;
    const now = Date.now();
    await runD1Write(env, 'user_insert', async () => {
        await env.TG_BOT_DB
            .prepare('INSERT OR IGNORE INTO users (user_id, created_at, updated_at) VALUES (?, ?, ?)')
            .bind(String(userId), now, now)
            .run();
    });
}

export async function dbUserGet(env, userId) {
    if (!hasD1(env)) return null;
    const row = await env.TG_BOT_DB
        .prepare('SELECT user_id, thread_id, title, closed FROM users WHERE user_id = ?')
        .bind(String(userId))
        .first();
    return normalizeUserRecord(row);
}

export async function dbUserUpdate(env, userId, data = {}) {
    if (!hasD1(env)) return;
    await ensureUserRow(env, userId);

    const fields = [];
    const values = [];

    if ('thread_id' in data) {
        fields.push('thread_id = ?');
        values.push(data.thread_id !== undefined ? (data.thread_id === null ? null : String(data.thread_id)) : null);
    }
    if ('title' in data) {
        fields.push('title = ?');
        values.push(data.title ?? null);
    }
    if ('closed' in data) {
        fields.push('closed = ?');
        values.push(toDbBool(!!data.closed));
    }
    if ('verify_state' in data) {
        fields.push('verify_state = ?');
        values.push(data.verify_state ?? null);
    }
    if ('verify_expires_at' in data) {
        fields.push('verify_expires_at = ?');
        values.push(data.verify_expires_at ?? null);
    }
    if ('is_blocked' in data) {
        fields.push('is_blocked = ?');
        values.push(toDbBool(!!data.is_blocked));
    }
    if ('user_info_json' in data) {
        fields.push('user_info_json = ?');
        values.push(data.user_info_json ?? null);
    }

    if (fields.length === 0) return;

    const now = Date.now();
    fields.push('updated_at = ?');
    values.push(now);

    await runD1Write(env, 'user_update', async () => {
        await env.TG_BOT_DB
            .prepare(`UPDATE users SET ${fields.join(', ')} WHERE user_id = ?`)
            .bind(...values, String(userId))
            .run();
    });
}

export async function dbGetVerifyState(env, userId) {
    if (!hasD1(env)) return null;
    const row = await env.TG_BOT_DB
        .prepare('SELECT verify_state, verify_expires_at FROM users WHERE user_id = ?')
        .bind(String(userId))
        .first();

    if (!row || !row.verify_state) return null;
    if (row.verify_state === 'trusted') return 'trusted';

    const expiresAt = Number(row.verify_expires_at || 0);
    if (expiresAt && expiresAt < Date.now()) {
        await dbUserUpdate(env, userId, { verify_state: null, verify_expires_at: null });
        return null;
    }

    return row.verify_state;
}

export async function dbSetVerifyState(env, userId, state) {
    if (!hasD1(env)) return;
    if (!state) {
        await dbUserUpdate(env, userId, { verify_state: null, verify_expires_at: null });
        return;
    }
    const now = Date.now();
    const expiresAt = state === 'trusted' ? null : (now + CONFIG.VERIFIED_EXPIRE_SECONDS * 1000);
    await dbUserUpdate(env, userId, { verify_state: state, verify_expires_at: expiresAt });
}

export async function dbIsBanned(env, userId) {
    if (!hasD1(env)) return false;
    const row = await env.TG_BOT_DB
        .prepare('SELECT is_blocked FROM users WHERE user_id = ?')
        .bind(String(userId))
        .first();
    return !!(row && row.is_blocked);
}

export async function dbSetBanned(env, userId, isBanned) {
    if (!hasD1(env)) return;
    await dbUserUpdate(env, userId, { is_blocked: !!isBanned });
}

export async function dbThreadGetUserId(env, threadId) {
    if (!hasD1(env)) return null;
    const row = await env.TG_BOT_DB
        .prepare('SELECT user_id FROM threads WHERE thread_id = ?')
        .bind(String(threadId))
        .first();
    if (row?.user_id) return row.user_id;

    const fallback = await env.TG_BOT_DB
        .prepare('SELECT user_id FROM users WHERE thread_id = ?')
        .bind(String(threadId))
        .first();
    if (fallback?.user_id) {
        await dbThreadPut(env, threadId, fallback.user_id);
        return fallback.user_id;
    }
    return null;
}

export async function dbThreadPut(env, threadId, userId) {
    if (!hasD1(env)) return;
    await runD1Write(env, 'thread_put', async () => {
        await env.TG_BOT_DB
            .prepare('INSERT OR REPLACE INTO threads (thread_id, user_id) VALUES (?, ?)')
            .bind(String(threadId), String(userId))
            .run();
    });
}

export async function dbThreadDelete(env, threadId) {
    if (!hasD1(env)) return;
    await runD1Write(env, 'thread_delete', async () => {
        await env.TG_BOT_DB
            .prepare('DELETE FROM threads WHERE thread_id = ?')
            .bind(String(threadId))
            .run();
    });
}

export async function dbMessageMapPut(env, sourceChatId, sourceMsgId, targetChatId, targetMsgId) {
    if (!hasD1(env)) return;
    const now = Date.now();
    await runD1Write(env, 'message_map_put', async () => {
        await env.TG_BOT_DB
            .prepare(`INSERT OR REPLACE INTO messages
                (source_chat_id, source_msg_id, target_chat_id, target_msg_id, created_at)
                VALUES (?, ?, ?, ?, ?)`)
            .bind(String(sourceChatId), String(sourceMsgId), String(targetChatId), String(targetMsgId), now)
            .run();
    });
}

export async function dbMessageMapGet(env, sourceChatId, sourceMsgId) {
    if (!hasD1(env)) return null;
    const row = await env.TG_BOT_DB
        .prepare(`SELECT target_chat_id, target_msg_id, created_at
                  FROM messages WHERE source_chat_id = ? AND source_msg_id = ?`)
        .bind(String(sourceChatId), String(sourceMsgId))
        .first();
    if (!row) return null;
    return {
        targetChatId: row.target_chat_id,
        targetMsgId: row.target_msg_id,
        createdAt: row.created_at
    };
}

export async function dbCount(env, whereSql = '', params = []) {
    if (!hasD1(env)) return 0;
    const sql = `SELECT COUNT(*) AS count FROM users ${whereSql}`;
    const row = await env.TG_BOT_DB.prepare(sql).bind(...params).first();
    return Number(row?.count || 0);
}

export async function dbListUsers(env, limit, offset) {
    if (!hasD1(env)) return [];
    const result = await env.TG_BOT_DB
        .prepare('SELECT user_id, thread_id, title, closed FROM users LIMIT ? OFFSET ?')
        .bind(limit, offset)
        .all();
    return result?.results || [];
}

export async function dbKeywordList(env) {
    if (!hasD1(env)) return [];
    const result = await env.TG_BOT_DB
        .prepare('SELECT keyword FROM keywords ORDER BY id ASC')
        .all();
    return (result?.results || []).map(row => String(row.keyword)).filter(Boolean);
}

export async function dbKeywordListWithId(env) {
    if (!hasD1(env)) return [];
    const result = await env.TG_BOT_DB
        .prepare('SELECT id, keyword FROM keywords ORDER BY id ASC')
        .all();
    return (result?.results || [])
        .map(row => ({ id: Number(row.id), keyword: String(row.keyword) }))
        .filter(row => row.keyword);
}

export async function dbKeywordAdd(env, keyword) {
    if (!hasD1(env)) return;
    await runD1Write(env, 'keyword_add', async () => {
        await env.TG_BOT_DB
            .prepare('INSERT OR IGNORE INTO keywords (keyword, created_at) VALUES (?, ?)')
            .bind(String(keyword), Date.now())
            .run();
    });
    keywordCache.ts = 0;
}

export async function dbKeywordDelete(env, keyword) {
    if (!hasD1(env)) return 0;
    let changes = 0;
    await runD1Write(env, 'keyword_delete', async () => {
        const result = await env.TG_BOT_DB
            .prepare('DELETE FROM keywords WHERE keyword = ?')
            .bind(String(keyword))
            .run();
        changes = Number(result?.meta?.changes ?? result?.changes ?? 0);
    });
    keywordCache.ts = 0;
    return changes;
}

export async function dbKeywordDeleteById(env, id) {
    if (!hasD1(env)) return 0;
    let changes = 0;
    await runD1Write(env, 'keyword_delete', async () => {
        const result = await env.TG_BOT_DB
            .prepare('DELETE FROM keywords WHERE id = ?')
            .bind(Number(id))
            .run();
        changes = Number(result?.meta?.changes ?? result?.changes ?? 0);
    });
    keywordCache.ts = 0;
    return changes;
}

export async function getKeywordListCached(env) {
    if (!hasD1(env)) return [];
    const now = Date.now();
    if (keywordCache.ts && (now - keywordCache.ts) < 60000 && keywordCache.list.length) {
        return keywordCache.list;
    }
    const list = await dbKeywordList(env);
    keywordCache.ts = now;
    keywordCache.list = list;
    return list;
}
