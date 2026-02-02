/**
 * Durable Object: 速率限制服务
 *
 * 功能：
 * - 提供原子性的速率限制计数
 * - 支持独立的限流策略
 * - 避免并发竞态条件
 *
 * API 端点:
 * POST /check - 检查并更新限流计数
 *
 * 请求参数:
 * {
 *   key: string,      // 限流键（格式: action:userId）
 *   limit: number,    // 限流阈值
 *   window: number    // 时间窗口（秒）
 * }
 *
 * 响应格式:
 * {
 *   allowed: boolean,  // 是否允许
 *   remaining: number  // 剩余配额
 * }
 */

export class RateLimitDO {
    constructor(state, env) {
        this.state = state;
        this.env = env;
        // 内存存储限流计数和过期时间
        this.data = {};
    }

    async fetch(request) {
        if (request.method !== 'POST') {
            return new Response('Method not allowed', { status: 405 });
        }

        const url = new URL(request.url);
        const action = url.pathname.slice(1);

        if (action === 'check') {
            return await this.handleCheck(request);
        }

        return new Response('Not found', { status: 404 });
    }

    /**
     * 处理限流检查和计数更新
     */
    async handleCheck(request) {
        try {
            const body = await request.json();
            const { key, limit, window } = body;

            if (!key || !limit || !window) {
                return new Response(
                    JSON.stringify({ error: 'Missing parameters: key, limit, window' }),
                    { status: 400, headers: { 'content-type': 'application/json' } }
                );
            }

            const now = Date.now();
            const entry = this.data[key];

            // 条目不存在或已过期：重置
            if (!entry || entry.expiresAt < now) {
                this.data[key] = { count: 1, expiresAt: now + window * 1000 };
                return new Response(
                    JSON.stringify({ allowed: true, remaining: limit - 1 }),
                    { headers: { 'content-type': 'application/json' } }
                );
            }

            // 已超限
            if (entry.count >= limit) {
                return new Response(
                    JSON.stringify({ allowed: false, remaining: 0 }),
                    { headers: { 'content-type': 'application/json' } }
                );
            }

            // 增加计数
            entry.count++;
            return new Response(
                JSON.stringify({ allowed: true, remaining: limit - entry.count }),
                { headers: { 'content-type': 'application/json' } }
            );
        } catch (e) {
            console.error('RateLimitDO error:', e);
            return new Response(
                JSON.stringify({ error: e.message }),
                { status: 500, headers: { 'content-type': 'application/json' } }
            );
        }
    }
}
