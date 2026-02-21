import { secureRandomId } from '../core/random.js';

export class UserLockTimeoutError extends Error {
    constructor(message) {
        super(message);
        this.name = 'UserLockTimeoutError';
        this.code = 'USER_LOCK_TIMEOUT';
    }
}

export class UserLockLostError extends Error {
    constructor(message) {
        super(message);
        this.name = 'UserLockLostError';
        this.code = 'USER_LOCK_LOST';
    }
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function normalizeLockConfig(config = {}) {
    return {
        lockKey: config.lockKey || 'private_flow',
        ttlMs: Number(config.ttlMs || 15000),
        acquireTimeoutMs: Number(config.acquireTimeoutMs || 6000),
        retryIntervalMs: Number(config.retryIntervalMs || 120),
        heartbeatIntervalMs: Number(config.heartbeatIntervalMs || 5000),
        logger: config.logger || null
    };
}

/**
 * 获取用户级锁。
 *
 * 优先使用 Durable Object 锁实现跨实例互斥；
 * 若未配置 DO，则降级为 KV 近似锁（非强一致，仅兜底）。
 */
export async function acquireUserLock(env, userId, config = {}) {
    const cfg = normalizeLockConfig(config);
    const ownerToken = secureRandomId(20);
    const startedAt = Date.now();
    const deadline = startedAt + cfg.acquireTimeoutMs;

    while (Date.now() < deadline) {
        if (env.RATE_LIMIT_DO) {
            const stub = env.RATE_LIMIT_DO.getByName(String(userId));
            const result = await stub.acquireLock(cfg.lockKey, ownerToken, cfg.ttlMs);
            if (result?.acquired) {
                return { ownerToken, lockKey: cfg.lockKey, acquiredAt: Date.now(), mode: 'do' };
            }

            const waitMs = Math.max(40, Math.min(cfg.retryIntervalMs, Number(result?.retryAfterMs || cfg.retryIntervalMs)));
            await sleep(waitMs);
            continue;
        }

        const kvKey = `lock:${cfg.lockKey}:${userId}`;
        const existing = await env.TOPIC_MAP.get(kvKey);
        if (!existing) {
            await env.TOPIC_MAP.put(kvKey, ownerToken, {
                expirationTtl: Math.ceil(cfg.ttlMs / 1000)
            });
            const confirmed = await env.TOPIC_MAP.get(kvKey);
            if (confirmed === ownerToken) {
                return { ownerToken, lockKey: cfg.lockKey, acquiredAt: Date.now(), mode: 'kv' };
            }
        }
        await sleep(cfg.retryIntervalMs);
    }

    throw new UserLockTimeoutError(`获取用户锁超时: userId=${userId}`);
}

/**
 * 释放用户级锁。
 */
export async function releaseUserLock(env, userId, lockState) {
    if (!lockState || !lockState.ownerToken) return false;

    if (env.RATE_LIMIT_DO && lockState.mode === 'do') {
        const stub = env.RATE_LIMIT_DO.getByName(String(userId));
        const result = await stub.releaseLock(lockState.lockKey, lockState.ownerToken);
        return !!result?.released;
    }

    const kvKey = `lock:${lockState.lockKey}:${userId}`;
    const existing = await env.TOPIC_MAP.get(kvKey);
    if (existing === lockState.ownerToken) {
        await env.TOPIC_MAP.delete(kvKey);
        return true;
    }
    return false;
}

/**
 * 在用户锁保护下执行函数。
 */
export async function withUserLock(env, userId, fn, config = {}) {
    const cfg = normalizeLockConfig(config);
    const lockState = await acquireUserLock(env, userId, cfg);
    let stopHeartbeat = false;
    let heartbeatError = null;

    const heartbeatPromise = (async () => {
        if (lockState.mode !== 'do' || !env.RATE_LIMIT_DO) return;

        const intervalMs = Math.max(1000, Math.min(cfg.heartbeatIntervalMs, Math.floor(cfg.ttlMs / 2)));
        while (!stopHeartbeat) {
            await sleep(intervalMs);
            if (stopHeartbeat) break;

            try {
                const stub = env.RATE_LIMIT_DO.getByName(String(userId));
                const result = await stub.renewLock(lockState.lockKey, lockState.ownerToken, cfg.ttlMs);
                if (!result?.renewed) {
                    heartbeatError = new UserLockLostError(`用户锁续期失败: userId=${userId}`);
                    stopHeartbeat = true;
                    break;
                }
            } catch (e) {
                cfg.logger?.warn?.('user_lock_renew_failed', { userId, error: String(e?.message || e) });
            }
        }
    })();

    try {
        const result = await fn();
        if (heartbeatError) throw heartbeatError;
        return result;
    } finally {
        stopHeartbeat = true;
        try {
            await heartbeatPromise;
        } catch {
            // 忽略心跳协程退出异常
        }
        await releaseUserLock(env, userId, lockState);
    }
}
