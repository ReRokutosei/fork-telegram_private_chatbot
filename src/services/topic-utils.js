import { Logger } from '../core/logger.js';
import { tgCall } from '../adapters/telegram.js';

export function withMessageThreadId(body, threadId) {
    if (threadId === undefined || threadId === null) return body;
    return { ...body, message_thread_id: threadId };
}

export function normalizeTgDescription(description) {
    return (description || '').toString().toLowerCase();
}

export function isTopicMissingOrDeleted(description) {
    const desc = normalizeTgDescription(description);
    return desc.includes('thread not found') ||
           desc.includes('topic not found') ||
           desc.includes('message thread not found') ||
           desc.includes('topic deleted') ||
           desc.includes('thread deleted') ||
           desc.includes('forum topic not found') ||
           desc.includes('topic closed permanently');
}

export function isTestMessageInvalid(description) {
    const desc = normalizeTgDescription(description);
    return desc.includes('message text is empty') ||
           desc.includes('bad request: message text is empty');
}

export async function sendWelcomeCard(env, threadId, userId, userFrom) {
    if (!userFrom) return;

    const firstName = (userFrom.first_name || '').trim();
    const lastName = (userFrom.last_name || '').trim();
    const userNameStr = userFrom.username ? `@${userFrom.username}` : '未设置用户名';
    const fullName = (firstName + (lastName ? ' ' + lastName : '')).trim() || '匿名用户';

    const cardText = `👤 <b>新用户接入</b>
` +
                    `ID: <a href="tg://user?id=${userId}">${userId}</a>
` +
                    `名字: <a href="tg://user?id=${userId}">${fullName}</a>
` +
                    `用户名: ${userNameStr};

    try {
        await tgCall(env, 'sendMessage', {
            chat_id: env.SUPERGROUP_ID,
            message_thread_id: threadId,
            text: cardText,
            parse_mode: 'HTML'
        });

        Logger.info('welcome_card_sent', { userId, threadId });
    } catch (e) {
        Logger.warn('welcome_card_send_failed', { userId, threadId, error: e.message });
    }
}

export async function probeForumThread(env, expectedThreadId, { userId, reason, doubleCheckOnMissingThreadId = true } = {}) {
    const attemptOnce = async () => {
        const res = await tgCall(env, 'sendMessage', {
            chat_id: env.SUPERGROUP_ID,
            message_thread_id: expectedThreadId,
            text: 'probe'
        });

        const actualThreadId = res.result?.message_thread_id;
        const probeMessageId = res.result?.message_id;

        if (res.ok && probeMessageId) {
            try {
                await tgCall(env, 'deleteMessage', {
                    chat_id: env.SUPERGROUP_ID,
                    message_id: probeMessageId
                });
            } catch {
                // ignore
            }
        }

        if (!res.ok) {
            if (isTopicMissingOrDeleted(res.description)) {
                return { status: 'missing', description: res.description };
            }
            if (isTestMessageInvalid(res.description)) {
                return { status: 'probe_invalid', description: res.description };
            }
            return { status: 'unknown_error', description: res.description };
        }

        if (actualThreadId === undefined || actualThreadId === null) {
            return { status: 'missing_thread_id' };
        }

        if (Number(actualThreadId) !== Number(expectedThreadId)) {
            return { status: 'redirected', actualThreadId };
        }

        return { status: 'ok' };
    };

    const first = await attemptOnce();
    if (first.status !== 'missing_thread_id' || !doubleCheckOnMissingThreadId) {
        return first;
    }

    const second = await attemptOnce();
    if (second.status === 'missing_thread_id') {
        Logger.warn('probe_missing_thread_id_confirmed', {
            userId,
            expectedThreadId,
            reason
        });
    }
    return second;
}
