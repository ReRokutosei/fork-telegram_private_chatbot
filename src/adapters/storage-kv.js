import { Logger } from '../core/logger.js';

export async function safeGetJSON(env, key, defaultValue = null) {
    try {
        const data = await env.TOPIC_MAP.get(key, { type: 'json' });
        if (data === null || data === undefined) {
            return defaultValue;
        }
        if (typeof data !== 'object') {
            Logger.warn('kv_invalid_type', { key, type: typeof data });
            return defaultValue;
        }
        return data;
    } catch (e) {
        Logger.error('kv_parse_failed', e, { key });
        return defaultValue;
    }
}

export async function safeGetWithMetadata(env, key, defaultValue = null) {
    try {
        const result = await env.TOPIC_MAP.getWithMetadata(key, { type: 'json' });
        if (!result || !result.value) {
            return { value: defaultValue, metadata: null };
        }
        if (typeof result.value !== 'object') {
            Logger.warn('kv_invalid_type', { key, type: typeof result.value });
            return { value: defaultValue, metadata: result.metadata };
        }
        return { value: result.value, metadata: result.metadata };
    } catch (e) {
        Logger.error('kv_get_with_metadata_failed', e, { key });
        return { value: defaultValue, metadata: null };
    }
}

export async function safeGetBulk(env, keys, defaultValue = null) {
    if (!keys || keys.length === 0) return new Map();

    try {
        const results = await env.TOPIC_MAP.get(keys, { type: 'json' });
        if (!(results instanceof Map)) return new Map();

        const validated = new Map();
        for (const [key, value] of results) {
            if (value === null) {
                validated.set(key, defaultValue);
            } else if (typeof value === 'object') {
                validated.set(key, value);
            } else {
                Logger.warn('kv_bulk_invalid_type', { key, type: typeof value });
                validated.set(key, defaultValue);
            }
        }
        return validated;
    } catch (e) {
        Logger.error('kv_bulk_get_failed', e, { keyCount: keys.length });
        return new Map();
    }
}

export async function getAllKeys(env, prefix = '', limit = null) {
    const allKeys = [];
    let cursor = undefined;
    let count = 0;

    do {
        const result = await env.TOPIC_MAP.list({ prefix, cursor });

        for (const key of result.keys) {
            if (limit && count >= limit) break;
            allKeys.push(key);
            count++;
        }

        if (limit && count >= limit) break;
        cursor = result.list_complete ? undefined : result.cursor;
    } while (cursor);

    return allKeys;
}

export async function putWithMetadata(env, key, value, options = {}) {
    const {
        expirationTtl = null,
        metadata = {}
    } = options;

    const finalMetadata = {
        updatedAt: Date.now(),
        ...metadata,
        createdAt: metadata.createdAt || Date.now()
    };

    const putOptions = {
        metadata: finalMetadata
    };

    if (expirationTtl) putOptions.expirationTtl = expirationTtl;

    try {
        await env.TOPIC_MAP.put(key, JSON.stringify(value), putOptions);
    } catch (e) {
        Logger.error('kv_put_with_metadata_failed', e, { key });
        throw e;
    }
}

export async function deleteBulk(env, keys) {
    if (!keys || keys.length === 0) return 0;

    try {
        const deletePromises = keys.map(key =>
            env.TOPIC_MAP.delete(key).catch(e => {
                Logger.warn('kv_delete_failed', { key, error: e.message });
            })
        );

        await Promise.all(deletePromises);
        return keys.length;
    } catch (e) {
        Logger.error('kv_bulk_delete_failed', e, { keyCount: keys.length });
        return 0;
    }
}

export async function getWithCache(env, key, cacheTtl = 60, type = 'json') {
    try {
        return await env.TOPIC_MAP.get(key, {
            type,
            cacheTtl: Math.max(30, cacheTtl)
        });
    } catch (e) {
        Logger.error('kv_get_with_cache_failed', e, { key });
        return null;
    }
}

export async function getValueWithFullMetadata(env, key) {
    try {
        const { value, metadata } = await env.TOPIC_MAP.getWithMetadata(key, { type: 'json' });

        if (!value) return null;

        const createdAt = metadata?.createdAt || Date.now();
        const updatedAt = metadata?.updatedAt || createdAt;
        const now = Date.now();

        return {
            value,
            metadata: metadata || {},
            createdAt,
            updatedAt,
            age: now - createdAt,
            ageSeconds: Math.floor((now - createdAt) / 1000)
        };
    } catch (e) {
        Logger.error('kv_get_full_metadata_failed', e, { key });
        return null;
    }
}
