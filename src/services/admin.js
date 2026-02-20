import { CONFIG } from '../config/constants.js';
import { Logger } from '../core/logger.js';
import { tgCall } from '../adapters/telegram.js';

const adminStatusCache = new Map();

function parseAdminIdAllowlist(env) {
    const raw = (env.ADMIN_IDS || '').toString().trim();
    if (!raw) return new Set();
    return new Set(
        raw
            .split(',')
            .map(s => s.trim())
            .filter(Boolean)
            .filter(v => /^\d+$/.test(v))
    );
}

export async function isAdminUser(env, userId) {
    if (!userId) return false;

    const allowlist = parseAdminIdAllowlist(env);
    if (allowlist.has(String(userId))) {
        return true;
    }

    const cacheKey = String(userId);
    const now = Date.now();
    const cached = adminStatusCache.get(cacheKey);
    if (cached && (now - cached.ts < CONFIG.ADMIN_CACHE_TTL_SECONDS * 1000)) {
        return cached.isAdmin;
    }

    const kvKey = `admin:${userId}`;
    const kvVal = await env.TOPIC_MAP.get(kvKey);
    if (kvVal === '1' || kvVal === '0') {
        const isAdmin = kvVal === '1';
        adminStatusCache.set(cacheKey, { ts: now, isAdmin });
        return isAdmin;
    }

    try {
        const res = await tgCall(env, 'getChatMember', {
            chat_id: env.SUPERGROUP_ID,
            user_id: userId
        });

        const status = res.result?.status;
        const isAdmin = res.ok && (status === 'creator' || status === 'administrator');
        await env.TOPIC_MAP.put(kvKey, isAdmin ? '1' : '0', { expirationTtl: CONFIG.ADMIN_CACHE_TTL_SECONDS });
        adminStatusCache.set(cacheKey, { ts: now, isAdmin });
        return isAdmin;
    } catch (e) {
        Logger.warn('admin_check_failed', { userId });
        return false;
    }
}
