import { Logger } from '../core/logger.js';
import { tgCall } from '../adapters/telegram.js';

const USER_PROFILE_CACHE_TTL_MS = 60000;
const userProfileCache = new Map();

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

function buildDisplayName(firstName, lastName) {
    const first = (firstName || '').trim();
    const last = (lastName || '').trim();
    return (first + (last ? ` ${last}` : '')).trim();
}

function normalizeNameForDeletedCheck(name) {
    return String(name || '')
        .toLowerCase()
        .replace(/^[^\p{L}\p{N}]+/u, '')
        .replace(/\s+/g, ' ')
        .trim();
}

function isLikelyDeletedName(name) {
    const normalized = normalizeNameForDeletedCheck(name);
    if (!normalized) return false;
    return normalized.includes('deleted account') ||
           normalized.includes('已销号') ||
           normalized.includes('已删除账户') ||
           normalized.includes('已销号用户') ||
           normalized.includes('注销账号') ||
           normalized.includes('注销用户');
}

function isUnreachableUserError(description) {
    const desc = normalizeTgDescription(description);
    return desc.includes('chat not found') ||
           desc.includes('user not found') ||
           desc.includes('user is deactivated') ||
           desc.includes('forbidden: bot was blocked by the user') ||
           desc.includes('forbidden: user is deactivated');
}

/**
 * 解析用户资料状态。
 *
 * 说明：
 * - 优先调用 getChat 获取实时名称。
 * - 若无法获取，则回退到调用方提供的兜底名称。
 * - “疑似已销号”仅基于名称弱特征判断，不能作为官方强判定。
 */
export async function resolveUserProfileStatus(env, userId, fallback = {}) {
    const cacheKey = String(userId);
    const now = Date.now();
    const cached = userProfileCache.get(cacheKey);
    if (cached && (now - cached.ts) < USER_PROFILE_CACHE_TTL_MS) {
        return cached.value;
    }

    const fallbackName = String(fallback?.name || '').trim();
    const fallbackUsername = String(fallback?.username || '').trim();

    let displayName = fallbackName || `用户${userId}`;
    let username = fallbackUsername || '';
    let status = 'unknown';
    let statusLabel = '未知';
    let source = 'fallback';

    try {
        const chat = await tgCall(env, 'getChat', { chat_id: userId });
        if (chat?.ok && chat?.result) {
            const nameFromApi = buildDisplayName(chat.result.first_name, chat.result.last_name);
            displayName = nameFromApi || displayName;
            username = chat.result.username || username;
            source = 'telegram_api';

            if (isLikelyDeletedName(displayName)) {
                status = 'suspected_deleted';
                statusLabel = '疑似已销号';
            } else {
                status = 'ok';
                statusLabel = '正常';
            }

            const result = {
                userId,
                displayName,
                username,
                status,
                statusLabel,
                source
            };
            userProfileCache.set(cacheKey, { ts: now, value: result });
            return result;
        }
    } catch (e) {
        if (isUnreachableUserError(e?.message || '')) {
            const result = {
                userId,
                displayName,
                username,
                status: 'unreachable',
                statusLabel: '不可达',
                source,
                reason: String(e?.message || e)
            };
            userProfileCache.set(cacheKey, { ts: now, value: result });
            return result;
        }
    }

    if (isLikelyDeletedName(displayName)) {
        status = 'suspected_deleted';
        statusLabel = '疑似已销号';
    }

    const result = {
        userId,
        displayName,
        username,
        status,
        statusLabel,
        source
    };
    userProfileCache.set(cacheKey, { ts: now, value: result });
    return result;
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
                    `用户名: ${userNameStr}`;

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
