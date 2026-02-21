/**
 * Durable Object: 速率限制服务
 *
 * 设计目标：
 * 1. 通过 Durable Object 的单线程执行模型，提供计数更新的原子性语义。
 * 2. 使用 SQLite 持久化保存限流状态，降低实例被驱逐（eviction）后状态丢失的风险。
 * 3. 支持按 key 维度的独立限流策略（limit / window 由调用方传入）。
 * 4. 通过“内存缓存 + SQLite”组合，兼顾读写性能与数据可靠性。
 *
 * 对外 RPC 方法：
 * - check(key, limit, window) -> { allowed: boolean, remaining: number }
 *   - key：限流键，建议格式为 `${action}:${userId}`，需保证业务侧唯一性。
 *   - limit：窗口期内允许的最大请求次数（正整数）。
 *   - window：窗口期长度（秒，正数）。
 *
 * SQLite 表结构：
 * - rate_limits：按 key 记录窗口期内的计数及过期时间（毫秒时间戳）。
 *   - key：主键。
 *   - count：当前窗口期累计次数。
 *   - expires_at：窗口期结束时间（Unix 时间戳，毫秒）。
 *   - created_at / updated_at：记录创建与最后更新时间（Unix 时间戳，毫秒）。
 */

import { DurableObject } from 'cloudflare:workers';

export class RateLimitDO extends DurableObject {
    constructor(ctx, env) {
        super(ctx, env);

        /**
         * 初始化 SQLite 表结构。
         *
         * 约束说明：
         * - blockConcurrencyWhile 用于在实例对外提供服务前完成一次性初始化，避免并发请求
         *   在表尚未创建时访问导致的错误。
         */
        ctx.blockConcurrencyWhile(async () => {
            this.ctx.storage.sql.exec(`
                CREATE TABLE IF NOT EXISTS rate_limits (
                    key TEXT PRIMARY KEY,
                    count INTEGER NOT NULL DEFAULT 0,
                    expires_at INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );

                /* expires_at 索引用于加速按过期时间的清理与统计查询 */
                CREATE INDEX IF NOT EXISTS idx_expires_at
                ON rate_limits(expires_at);

                CREATE TABLE IF NOT EXISTS user_locks (
                    lock_key TEXT PRIMARY KEY,
                    owner_token TEXT NOT NULL,
                    expires_at INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_user_locks_expires_at
                ON user_locks(expires_at);
            `);
        });

        /**
         * 内存缓存（热点 key 优化）。
         *
         * 说明：
         * - 仅作为性能优化的近端缓存，数据以 SQLite 为最终事实来源（source of truth）。
         * - 缓存项在 expiresAt 过期后应视为无效，需回源 SQLite 冷读。
         *
         * 缓存结构：
         * - Map<key, { count: number, expiresAt: number }>
         */
        this.cache = new Map();
    }

    /**
     * RPC：获取用户锁。
     *
     * 说明：
     * - 同一 DO 实例对应单个 userId，lock_key 用于区分该用户下不同业务锁。
     * - 返回 acquired=false 时，调用方应等待 retryAfterMs 后重试。
     */
    async acquireLock(lockKey, ownerToken, ttlMs = 15000) {
        if (!lockKey || !ownerToken) {
            throw new Error('Missing parameters: lockKey, ownerToken');
        }

        const now = Date.now();
        const ttl = Math.max(1000, Number(ttlMs || 15000));
        const nextExpiresAt = now + ttl;

        const row = this.ctx.storage.sql.exec(
            `SELECT owner_token, expires_at FROM user_locks WHERE lock_key = ?`,
            lockKey
        ).one();

        if (row && row.expires_at > now && row.owner_token !== ownerToken) {
            return {
                acquired: false,
                retryAfterMs: Math.max(50, row.expires_at - now)
            };
        }

        this.ctx.storage.sql.exec(
            `INSERT INTO user_locks (lock_key, owner_token, expires_at, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(lock_key) DO UPDATE SET
             owner_token = excluded.owner_token,
             expires_at = excluded.expires_at,
             updated_at = excluded.updated_at`,
            lockKey, ownerToken, nextExpiresAt, now, now
        );

        return {
            acquired: true,
            ownerToken,
            expiresAt: nextExpiresAt
        };
    }

    /**
     * RPC：释放用户锁。
     *
     * 说明：
     * - 必须携带 ownerToken，避免误释放其他请求持有的锁。
     */
    async releaseLock(lockKey, ownerToken) {
        if (!lockKey || !ownerToken) {
            throw new Error('Missing parameters: lockKey, ownerToken');
        }

        const result = this.ctx.storage.sql.exec(
            `DELETE FROM user_locks WHERE lock_key = ? AND owner_token = ?`,
            lockKey, ownerToken
        );

        return { released: (result.meta.changes || 0) > 0 };
    }

    /**
     * RPC：续期用户锁。
     *
     * 说明：
     * - 仅锁持有者可续期。
     * - 锁已过期时续期失败，调用方应按“锁丢失”处理。
     */
    async renewLock(lockKey, ownerToken, ttlMs = 15000) {
        if (!lockKey || !ownerToken) {
            throw new Error('Missing parameters: lockKey, ownerToken');
        }

        const now = Date.now();
        const ttl = Math.max(1000, Number(ttlMs || 15000));
        const nextExpiresAt = now + ttl;
        const row = this.ctx.storage.sql.exec(
            `SELECT owner_token, expires_at FROM user_locks WHERE lock_key = ?`,
            lockKey
        ).one();

        if (!row || row.owner_token !== ownerToken || row.expires_at <= now) {
            return { renewed: false };
        }

        this.ctx.storage.sql.exec(
            `UPDATE user_locks
             SET expires_at = ?, updated_at = ?
             WHERE lock_key = ? AND owner_token = ?`,
            nextExpiresAt, now, lockKey, ownerToken
        );

        return { renewed: true, expiresAt: nextExpiresAt };
    }

    /**
     * RPC：检查并更新限流计数。
     *
     * 行为定义：
     * - 若当前窗口期内计数未达 limit，则允许请求并将计数 +1。
     * - 若计数已达 limit，则拒绝请求，不再递增计数。
     * - 若记录不存在或已过期，则创建/重置窗口期并将计数置为 1。
     *
     * 注意：
     * - 本实现依赖 Durable Object 的串行执行模型来避免同一对象实例内的并发竞态。
     * - SQLite 写入通过 exec 提交，写操作在 DO 存储层排队执行。
     */
    async check(key, limit, window) {
        /**
         * 参数校验：
         * - key：不能为空。
         * - limit/window：需为可用值；此处沿用原有校验策略（仅判空/判 0），不改变行为。
         */
        if (!key || !limit || !window) {
            throw new Error('Missing parameters: key, limit, window');
        }

        const now = Date.now();
        const expiresAt = now + (window * 1000);

        /**
         * 快路径：命中内存缓存且未过期。
         *
         * 说明：
         * - 命中缓存可减少一次 SQLite 读取。
         * - 写入仍需落库，以保证 DO 被驱逐后状态可恢复。
         */
        const cached = this.cache.get(key);
        if (cached && cached.expiresAt > now) {
            // 窗口期内已达上限：直接拒绝
            if (cached.count >= limit) {
                return { allowed: false, remaining: 0 };
            }

            /**
             * 递增计数并持久化。
             *
             * 说明：
             * - 使用 UPSERT 统一处理“插入或更新”路径。
             * - 该语句在冲突更新时将 count 递增，并更新时间戳字段。
             */
            this.ctx.storage.sql.exec(
                `INSERT INTO rate_limits (key, count, expires_at, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT(key) DO UPDATE SET
                 count = count + 1,
                 updated_at = ?`,
                key, cached.count + 1, expiresAt, now, now, now
            );

            // 更新缓存中的计数与过期时间
            cached.count++;
            this.cache.set(key, { count: cached.count, expiresAt });

            return { allowed: true, remaining: limit - cached.count };
        }

        /**
         * 冷路径：回源 SQLite 查询当前计数与过期时间。
         */
        const result = this.ctx.storage.sql.exec(
            `SELECT count, expires_at FROM rate_limits WHERE key = ?`,
            key
        ).one();

        if (result && result.expires_at > now) {
            // 记录存在且仍在窗口期内
            if (result.count >= limit) {
                /**
                 * 超限结果写入缓存：
                 * - 便于后续同窗口期内的请求直接命中快路径拒绝，减少数据库读取。
                 */
                this.cache.set(key, {
                    count: result.count,
                    expiresAt: result.expires_at
                });
                return { allowed: false, remaining: 0 };
            }

            /**
             * 未达上限：递增计数并更新时间戳。
             *
             * 说明：
             * - UPDATE 语句仅更新计数与 updated_at，不变更 expires_at（窗口期边界保持不变）。
             */
            this.ctx.storage.sql.exec(
                `UPDATE rate_limits SET count = count + 1, updated_at = ? WHERE key = ?`,
                now, key
            );

            const newCount = result.count + 1;
            this.cache.set(key, { count: newCount, expiresAt: result.expires_at });

            return { allowed: true, remaining: limit - newCount };
        }

        /**
         * 记录不存在或已过期：创建/重置窗口期。
         *
         * 行为：
         * - count 重置为 1。
         * - expires_at 设为新的窗口期结束时间。
         * - updated_at 同步更新。
         */
        this.ctx.storage.sql.exec(
            `INSERT INTO rate_limits (key, count, expires_at, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(key) DO UPDATE SET
             count = 1,
             expires_at = ?,
             updated_at = ?`,
            key, 1, expiresAt, now, now, expiresAt, now
        );

        this.cache.set(key, { count: 1, expiresAt });
        return { allowed: true, remaining: limit - 1 };
    }

    /**
     * 清理过期限流记录（建议由 alarm 或外部调度周期触发）。
     *
     * 行为：
     * - 删除 SQLite 中 expires_at < now 的记录。
     * - 同步删除缓存中过期项，避免缓存膨胀。
     *
     * 返回：
     * - deleted：本次删除的记录数量（以 SQLite meta.changes 为准）。
     */
    async cleanupExpired() {
        const now = Date.now();
        const rateLimitResult = this.ctx.storage.sql.exec(
            `DELETE FROM rate_limits WHERE expires_at < ?`,
            now
        );
        const lockResult = this.ctx.storage.sql.exec(
            `DELETE FROM user_locks WHERE expires_at < ?`,
            now
        );

        // 清理缓存中过期项（仅根据缓存自身的 expiresAt 判断）
        for (const [key, value] of this.cache.entries()) {
            if (value.expiresAt < now) {
                this.cache.delete(key);
            }
        }

        return {
            deletedRateLimits: rateLimitResult.meta.changes,
            deletedLocks: lockResult.meta.changes
        };
    }

    /**
     * 获取服务统计信息。
     *
     * 字段说明：
     * - totalRecords：SQLite 表内记录总数（含已过期但未清理的记录）。
     * - activeRecords：未过期记录数（expires_at > now）。
     * - cachedItems：当前内存缓存项数量。
     */
    async getStats() {
        const total = this.ctx.storage.sql.exec(
            `SELECT COUNT(*) as count FROM rate_limits`
        ).one();

        const active = this.ctx.storage.sql.exec(
            `SELECT COUNT(*) as count FROM rate_limits WHERE expires_at > ?`,
            Date.now()
        ).one();
        const activeLocks = this.ctx.storage.sql.exec(
            `SELECT COUNT(*) as count FROM user_locks WHERE expires_at > ?`,
            Date.now()
        ).one();

        return {
            totalRecords: total?.count || 0,
            activeRecords: active?.count || 0,
            activeLocks: activeLocks?.count || 0,
            cachedItems: this.cache.size
        };
    }

    /**
     * 重置指定 key 的限流状态（管理用途）。
     *
     * 行为：
     * - 删除 SQLite 中对应记录。
     * - 删除缓存中的对应项。
     */
    async reset(key) {
        this.ctx.storage.sql.exec(`DELETE FROM rate_limits WHERE key = ?`, key);
        this.cache.delete(key);
        return { success: true };
    }
}
