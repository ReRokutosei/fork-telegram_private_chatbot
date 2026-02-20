import { CONFIG } from '../config/constants.js';
import { Logger } from '../core/logger.js';

export async function tgCall(env, method, body, timeout = CONFIG.API_TIMEOUT_MS) {
    let base = env.API_BASE || 'https://api.telegram.org';

    if (base.startsWith('http://')) {
        Logger.warn('api_http_upgraded', { originalBase: base });
        base = base.replace('http://', 'https://');
    }

    try {
        new URL(`${base}/test`);
    } catch (e) {
        Logger.error('api_base_invalid', e, { base });
        base = 'https://api.telegram.org';
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeout);

    try {
        const resp = await fetch(`${base}/bot${env.BOT_TOKEN}/${method}`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify(body),
            signal: controller.signal
        });

        clearTimeout(timeoutId);

        if (!resp.ok && resp.status >= 500) {
            Logger.warn('telegram_api_server_error', {
                method,
                status: resp.status
            });
        }

        let result;
        try {
            result = await resp.json();
        } catch (parseError) {
            Logger.error('telegram_api_json_parse_failed', parseError, { method, status: resp.status });
            return { ok: false, description: 'Invalid JSON response from Telegram' };
        }

        if (!result.ok && result.description && result.description.includes('Too Many Requests')) {
            const retryAfter = result.parameters?.retry_after || 5;
            Logger.warn('telegram_api_rate_limit', {
                method,
                retryAfter
            });
        }

        return result;
    } catch (e) {
        clearTimeout(timeoutId);

        if (e.name === 'AbortError') {
            Logger.error('telegram_api_timeout', e, { method, timeout });
            return { ok: false, description: 'Request timeout' };
        }

        Logger.error('telegram_api_failed', e, { method });
        return { ok: false, description: String(e.message) };
    }
}
