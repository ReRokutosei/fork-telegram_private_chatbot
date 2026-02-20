export function createWebhookFetchHandler({
    Logger,
    tgCall,
    flushExpiredMediaGroups,
    cleanupExpiredMessageMaps,
    handleEditedMessage,
    handleCallbackQuery,
    handlePrivateMessage,
    updateThreadStatus,
    handleAdminReply
}) {
    const CLEANUP_INTERVAL_MS = 60 * 60 * 1000;
    const CLEANUP_THROTTLE_KEY = 'sys:message_map_cleanup:last';

    async function runPeriodicMessageMapCleanup(env, now) {
        if (!cleanupExpiredMessageMaps) return;
        try {
            const lastRunRaw = await env.TOPIC_MAP.get(CLEANUP_THROTTLE_KEY);
            const lastRun = Number(lastRunRaw || 0);
            if (lastRun && (now - lastRun) < CLEANUP_INTERVAL_MS) return;

            await env.TOPIC_MAP.put(CLEANUP_THROTTLE_KEY, String(now), {
                expirationTtl: Math.ceil((CLEANUP_INTERVAL_MS * 2) / 1000)
            });

            const cleaned = await cleanupExpiredMessageMaps(env);
            if (cleaned > 0) {
                Logger.info('message_map_cleanup_done', { cleaned });
            }
        } catch (e) {
            Logger.warn('message_map_cleanup_failed', { error: String(e?.message || e) });
        }
    }

    return async function fetch(request, env, ctx) {
        if (!env.TOPIC_MAP) return new Response("Error: KV 'TOPIC_MAP' not bound.");
        if (!env.TG_BOT_DB) return new Response("Error: D1 'TG_BOT_DB' not bound.");
        if (!env.BOT_TOKEN) return new Response('Error: BOT_TOKEN not set.');
        if (!env.SUPERGROUP_ID) return new Response('Error: SUPERGROUP_ID not set.');

        const normalizedEnv = {
            ...env,
            SUPERGROUP_ID: String(env.SUPERGROUP_ID),
            BOT_TOKEN: String(env.BOT_TOKEN)
        };

        if (!normalizedEnv.SUPERGROUP_ID.startsWith('-100')) {
            return new Response('Error: SUPERGROUP_ID must start with -100');
        }

        if (request.method !== 'POST') return new Response('OK');

        const contentType = request.headers.get('content-type') || '';
        if (!contentType.includes('application/json')) {
            Logger.warn('invalid_content_type', { contentType });
            return new Response('OK');
        }

        let update;
        try {
            update = await request.json();
            if (!update || typeof update !== 'object') {
                Logger.warn('invalid_json_structure', { update: typeof update });
                return new Response('OK');
            }
        } catch (e) {
            Logger.error('json_parse_failed', e);
            return new Response('OK');
        }

        if (update.edited_message) {
            await handleEditedMessage(update.edited_message, normalizedEnv, ctx);
            return new Response('OK');
        }

        if (update.callback_query) {
            await handleCallbackQuery(update.callback_query, normalizedEnv, ctx);
            return new Response('OK');
        }

        const msg = update.message;
        if (!msg) return new Response('OK');

        const now = Date.now();
        ctx.waitUntil(flushExpiredMediaGroups(normalizedEnv, now));
        ctx.waitUntil(runPeriodicMessageMapCleanup(normalizedEnv, now));

        if (msg.chat && msg.chat.type === 'private') {
            try {
                await handlePrivateMessage(msg, normalizedEnv, ctx);
            } catch (e) {
                const errText = '⚠️ 系统繁忙，请稍后再试。';
                await tgCall(normalizedEnv, 'sendMessage', { chat_id: msg.chat.id, text: errText });
                Logger.error('private_message_failed', e, { userId: msg.chat.id });
            }
            return new Response('OK');
        }

        if (msg.chat && String(msg.chat.id) === normalizedEnv.SUPERGROUP_ID) {
            if (msg.forum_topic_closed && msg.message_thread_id) {
                await updateThreadStatus(msg.message_thread_id, true, normalizedEnv);
                return new Response('OK');
            }
            if (msg.forum_topic_reopened && msg.message_thread_id) {
                await updateThreadStatus(msg.message_thread_id, false, normalizedEnv);
                return new Response('OK');
            }
            const text = (msg.text || '').trim();
            const isCommand = !!text && text.startsWith('/');
            if (msg.message_thread_id || isCommand) {
                await handleAdminReply(msg, normalizedEnv, ctx);
                return new Response('OK');
            }
        }

        return new Response('OK');
    };
}
