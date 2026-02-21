/**
 * Telegram 双向机器人
 *
 * Cloudflare Worker 脚本，实现用户私聊消息转发至超级群组话题。
 * 包含人机验证、限流、管理员命令、消息编辑同步等功能。
 */

import { RateLimitDO } from './do/rate-limit-do.js';
import { CONFIG, LOCAL_QUESTIONS } from './config/constants.js';
import { Logger } from './core/logger.js';
import { secureRandomId, secureRandomInt } from './core/random.js';
import { tgCall } from './adapters/telegram.js';
import { checkRateLimit } from './services/rate-limit.js';
import { getFilterText, validateKeywordPattern, matchKeyword } from './services/keywords.js';
import { isAdminUser } from './services/admin.js';
import { withMessageThreadId, normalizeTgDescription, isTopicMissingOrDeleted, sendWelcomeCard, probeForumThread } from './services/topic-utils.js';
import { sendVerificationChallengeImpl, handleCallbackQueryImpl } from './services/verification.js';
import { handleMediaGroupImpl, flushExpiredMediaGroupsImpl, delaySendImpl } from './services/media-group.js';
import { handleCleanupCommandImpl } from './services/cleanup.js';
import { getOrCreateUserTopicRecImpl, resetUserVerificationAndRequireReverifyImpl, createTopicImpl, updateThreadStatusImpl, buildTopicTitleImpl } from './services/topic-lifecycle.js';
import { handleEditedMessageImpl } from './services/edit-sync.js';
import { handleAdminReplyImpl } from './services/admin-reply.js';
import { handlePrivateMessageImpl, forwardToTopicImpl } from './services/message-flow.js';
import { withUserLock, UserLockTimeoutError, UserLockLostError } from './services/user-lock.js';
import { safeGetJSON, getAllKeys, putWithMetadata, deleteBulk } from './adapters/storage-kv.js';
import { hasD1, dbUserGet, dbUserUpdate, dbGetVerifyState, dbSetVerifyState, dbIsBanned, dbSetBanned, dbThreadGetUserId, dbThreadPut, dbThreadDelete, dbMessageMapPut, dbMessageMapGet, dbMessageMapCleanupExpired, dbListUsers, dbKeywordListWithId, dbKeywordAdd, dbKeywordDelete, dbKeywordDeleteById } from './adapters/storage-d1.js';
import { createWebhookFetchHandler } from './handlers/webhook.js';

// ============================================================================
// 配置常量
// ============================================================================


// ============================================================================
// 内存缓存（实例级）
// ============================================================================

// 话题健康检查缓存，减少重复探测请求
const threadHealthCache = new Map();

// 并发保护：避免同一用户短时间内重复创建话题
const topicCreateInFlight = new Map();


// ============================================================================
// 本地题库
// ============================================================================


// ============================================================================
// 日志系统
// ============================================================================

/**
 * 结构化日志系统
 * 使用 JSON 格式输出，便于日志聚合和分析
 */

// ============================================================================
// 加密安全工具
// ============================================================================

/**
 * 生成加密安全的随机整数
 */

/**
 * 生成加密安全的随机 ID
 */

// ============================================================================
// KV 存储工具
// ============================================================================


// ============================================================================
// 话题管理
// ============================================================================

/**
 * 获取或创建用户话题记录
 * 使用并发保护避免重复创建
 */
async function getOrCreateUserTopicRec(from, key, env, userId) {
    return getOrCreateUserTopicRecImpl({
        from,
        key,
        env,
        userId,
        hasD1,
        dbUserGet,
        safeGetJSON,
        createTopic,
        topicCreateInFlight
    });
}

/**
 * 重置用户验证并要求重新验证
 */
async function resetUserVerificationAndRequireReverify(env, { userId, userKey, oldThreadId, pendingMsgId, reason, userFrom = null }) {
    return resetUserVerificationAndRequireReverifyImpl({
        env,
        userId,
        userKey,
        oldThreadId,
        pendingMsgId,
        reason,
        hasD1,
        dbUserUpdate,
        dbThreadDelete,
        CONFIG,
        threadHealthCache,
        Logger,
        sendVerificationChallenge
    });
}

// ============================================================================
// 管理员管理
// ============================================================================

/**
 * 解析管理员 ID 白名单
 */
// ============================================================================
// 消息队列系统
// ============================================================================

// ============================================================================
// 工具函数
// ============================================================================

/**
 * Fisher-Yates 洗牌算法
 */
function shuffleArray(arr) {
    const array = [...arr];
    for (let i = array.length - 1; i > 0; i--) {
        const j = secureRandomInt(0, i + 1);
        [array[i], array[j]] = [array[j], array[i]];
    }
    return array;
}

// ============================================================================
// Worker 入口点
// ============================================================================

export { RateLimitDO };

const fetchHandler = createWebhookFetchHandler({
    Logger,
    tgCall,
    flushExpiredMediaGroups,
    cleanupExpiredMessageMaps,
    handleEditedMessage,
    handleCallbackQuery,
    handlePrivateMessage,
    updateThreadStatus,
    handleAdminReply
});

export default {
    fetch: fetchHandler
};

// ============================================================================
// 核心业务逻辑
// ============================================================================

/**
 * 处理私聊消息
 */
async function handlePrivateMessage(msg, env, ctx) {
    return handlePrivateMessageImpl(msg, env, ctx, {
        forwardToTopic,
        withUserLock: runWithUserLock,
        tgCall,
        Logger
    });
}

async function runWithUserLock(env, userId, fn) {
    try {
        return await withUserLock(env, userId, fn, {
            ttlMs: CONFIG.USER_LOCK_TTL_MS,
            acquireTimeoutMs: CONFIG.USER_LOCK_ACQUIRE_TIMEOUT_MS,
            retryIntervalMs: CONFIG.USER_LOCK_RETRY_INTERVAL_MS,
            heartbeatIntervalMs: CONFIG.USER_LOCK_HEARTBEAT_INTERVAL_MS,
            logger: Logger
        });
    } catch (e) {
        if (e instanceof UserLockTimeoutError || e instanceof UserLockLostError) {
            throw e;
        }
        Logger.warn('user_lock_failed_fallback_to_unlocked', { userId, error: e.message });
        return await fn();
    }
}

/**
 * 转发消息到话题
 */
async function forwardToTopic(msg, env, ctx) {
    return forwardToTopicImpl(msg, env, ctx, {
        checkRateLimit,
        CONFIG,
        tgCall,
        hasD1,
        dbIsBanned,
        dbGetVerifyState,
        sendVerificationChallenge,
        getFilterText,
        matchKeyword,
        Logger,
        dbUserGet,
        safeGetJSON,
        getOrCreateUserTopicRec,
        sendWelcomeCard,
        dbThreadGetUserId,
        dbThreadPut,
        threadHealthCache,
        probeForumThread,
        resetUserVerificationAndRequireReverify,
        handleMediaGroup,
        normalizeTgDescription,
        isTopicMissingOrDeleted,
        dbMessageMapPut
    });
}

/**
 * 处理管理员回复
 */
async function handleAdminReply(msg, env, ctx) {
    return handleAdminReplyImpl(msg, env, ctx, {
        isAdminUser,
        hasD1,
        dbKeywordListWithId,
        tgCall,
        dbSetBanned,
        dbThreadGetUserId,
        dbThreadPut,
        getAllKeys,
        safeGetJSON,
        dbKeywordAdd,
        dbKeywordDelete,
        dbKeywordDeleteById,
        validateKeywordPattern,
        CONFIG,
        dbUserUpdate,
        dbSetVerifyState,
        dbUserGet,
        dbGetVerifyState,
        dbIsBanned,
        handleMediaGroup,
        dbMessageMapPut,
        handleCleanupCommand
    });
}

// ============================================================================
// 验证模块
// ============================================================================

/**
 * 发送人机验证挑战
 */
async function sendVerificationChallenge(userId, env, pendingMsgId) {
    return sendVerificationChallengeImpl({
        userId,
        env,
        pendingMsgId,
        safeGetJSON,
        checkRateLimit,
        tgCall,
        Logger,
        CONFIG,
        LOCAL_QUESTIONS,
        secureRandomInt,
        shuffleArray,
        secureRandomId
    });
}

/**
 * 处理验证按钮点击
 */
async function handleCallbackQuery(query, env, ctx) {
    return handleCallbackQueryImpl({
        query,
        env,
        ctx,
        tgCall,
        Logger,
        hasD1,
        dbSetVerifyState,
        CONFIG,
        forwardToTopic
    });
}

// ============================================================================
// 管理命令
// ============================================================================

/**
 * 处理 /cleanup 命令
 * 批量清理已删除话题的用户记录
 */
async function handleCleanupCommand(threadId, env) {
    return handleCleanupCommandImpl({
        threadId,
        env,
        CONFIG,
        hasD1,
        dbListUsers,
        probeForumThread,
        resetUserVerificationAndRequireReverify,
        Logger,
        safeGetJSON,
        deleteBulk,
        tgCall,
        withMessageThreadId
    });
}

// ============================================================================
// 话题创建和管理
// ============================================================================

/**
 * 创建新论坛话题
 */
async function createTopic(from, key, env, userId) {
    return createTopicImpl({
        from,
        key,
        env,
        userId,
        CONFIG,
        tgCall,
        hasD1,
        dbUserUpdate,
        dbThreadPut,
        putWithMetadata,
        buildTopicTitle,
        probeForumThread,
        Logger
    });
}

/**
 * 更新话题状态（关闭/打开）
 */
async function updateThreadStatus(threadId, isClosed, env) {
    return updateThreadStatusImpl({
        threadId,
        isClosed,
        env,
        hasD1,
        dbThreadGetUserId,
        dbUserGet,
        dbUserUpdate,
        dbThreadDelete,
        safeGetJSON,
        getAllKeys,
        Logger
    });
}

/**
 * 构建话题标题
 */
function buildTopicTitle(from) {
    return buildTopicTitleImpl(from, CONFIG);
}

// ============================================================================
// Telegram API
// ============================================================================

// ============================================================================
// 媒体组处理
// ============================================================================

/**
 * 处理媒体组消息
 */
async function handleMediaGroup(msg, env, ctx, { direction, targetChat, threadId }) {
    return handleMediaGroupImpl({
        msg,
        env,
        ctx,
        direction,
        targetChat,
        threadId,
        tgCall,
        withMessageThreadId,
        safeGetJSON,
        delaySend,
        CONFIG
    });
}

/**
 * 清理过期的媒体组
 */
async function flushExpiredMediaGroups(env, now) {
    return flushExpiredMediaGroupsImpl({ env, now, getAllKeys, safeGetJSON, Logger });
}

async function cleanupExpiredMessageMaps(env) {
    return dbMessageMapCleanupExpired(env);
}

/**
 * 延迟发送媒体组
 */
async function delaySend(env, key, ts) {
    return delaySendImpl({
        env,
        key,
        ts,
        CONFIG,
        safeGetJSON,
        Logger,
        tgCall,
        withMessageThreadId
    });
}

// ============================================================================
// 消息编辑同步
// ============================================================================

/**
 * 处理消息编辑
 * 支持用户端和管理员端的编辑同步
 */
async function handleEditedMessage(msg, env, ctx) {
    return handleEditedMessageImpl({
        msg,
        env,
        hasD1,
        dbMessageMapGet,
        safeGetJSON,
        dbUserGet,
        tgCall,
        Logger
    });
}
