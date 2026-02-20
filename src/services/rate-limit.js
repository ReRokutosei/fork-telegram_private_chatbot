import { Logger } from '../core/logger.js';

export async function checkRateLimit(userId, env, action = 'message', limit = 20, window = 60) {
    if (!env.RATE_LIMIT_DO) {
        Logger.warn('rate_limit_do_not_configured', { userId, action });
        const key = `ratelimit:${action}:${userId}`;
        const countStr = await env.TOPIC_MAP.get(key);
        const count = parseInt(countStr || '0');

        if (count >= limit) {
            return { allowed: false, remaining: 0 };
        }

        await env.TOPIC_MAP.put(key, String(count + 1), { expirationTtl: window });
        return { allowed: true, remaining: limit - count - 1 };
    }

    try {
        const stub = env.RATE_LIMIT_DO.getByName(String(userId));
        const result = await stub.check(`${action}:${userId}`, limit, window);
        return { allowed: result.allowed, remaining: result.remaining };
    } catch (e) {
        if (e.retryable) {
            Logger.warn('rate_limit_do_retryable_error', { userId, action, error: e.message });
        } else if (e.overloaded) {
            Logger.warn('rate_limit_do_overloaded', { userId, action });
        } else {
            Logger.error('rate_limit_do_call_failed', e, { userId, action });
        }

        return { allowed: true, remaining: limit };
    }
}
