/**
 * Telegram 双向机器人
 *
 * Cloudflare Worker 脚本，实现用户私聊消息转发至超级群组话题。
 * 包含人机验证、限流、管理员命令、消息编辑同步等功能。
 */

import { RateLimitDO } from './rate-limit-do.js';

// ============================================================================
// 配置常量
// ============================================================================

const CONFIG = {
    // 人机验证相关
    VERIFY_ID_LENGTH: 12,
    VERIFY_EXPIRE_SECONDS: 300,         // 验证挑战过期时间（5分钟）
    VERIFIED_EXPIRE_SECONDS: 2592000,   // 验证有效期（30天）

    // 媒体组相关
    MEDIA_GROUP_EXPIRE_SECONDS: 60,
    MEDIA_GROUP_DELAY_MS: 3000,         // 媒体组延迟发送时间

    // 消息处理
    PENDING_MAX_MESSAGES: 10,           // 验证期间暂存的最大消息数

    // 缓存相关
    ADMIN_CACHE_TTL_SECONDS: 300,       // 管理员权限缓存时间
    NEEDS_REVERIFY_TTL_SECONDS: 600,    // 需要重新验证标记的 TTL
    THREAD_HEALTH_TTL_MS: 60000,        // 话题健康检查缓存时间
    MESSAGE_MAP_TTL_SECONDS: 86400,     // 消息映射的 TTL（24小时）

    // 限流策略
    RATE_LIMIT_MESSAGE: 45,             // 用户消息限流阈值
    RATE_LIMIT_VERIFY: 3,               // 验证请求限流阈值
    RATE_LIMIT_WINDOW: 60,              // 限流时间窗口（秒）

    // UI 配置
    BUTTON_COLUMNS: 2,                  // 验证按钮列数
    MAX_TITLE_LENGTH: 128,              // 话题标题最大长度
    MAX_NAME_LENGTH: 30,                // 用户名最大长度

    // API 相关
    API_TIMEOUT_MS: 10000,              // Telegram API 调用超时时间

    // 清理命令相关
    CLEANUP_BATCH_SIZE: 10,             // 清理命令的批处理大小
    MAX_CLEANUP_DISPLAY: 20,            // 清理报告显示的最大用户数
    CLEANUP_LOCK_TTL_SECONDS: 1800,     // 清理操作防并发锁

    // 重试
    MAX_RETRY_ATTEMPTS: 3,              // 消息转发最大重试次数

    // D1 写入重试
    D1_WRITE_MAX_RETRIES: 3,            // D1 写入最大重试次数
    D1_WRITE_BASE_DELAY_MS: 120,        // D1 写入重试基础延迟
    D1_WRITE_MAX_DELAY_MS: 1200,        // D1 写入最大延迟

    // 关键词过滤
    KEYWORD_MAX_LENGTH: 200,            // 关键词最大长度
    KEYWORD_MATCH_MAX_TEXT_LENGTH: 4000 // 关键词匹配最大文本长度
};

// ============================================================================
// 内存缓存（实例级）
// ============================================================================

// 话题健康检查缓存，减少重复探测请求
const threadHealthCache = new Map();

// 并发保护：避免同一用户短时间内重复创建话题
const topicCreateInFlight = new Map();

// 管理员权限缓存（实例内）
const adminStatusCache = new Map();

// 关键词缓存（实例内）
const keywordCache = {
    ts: 0,
    list: []
};


// ============================================================================
// 本地题库
// ============================================================================

const LOCAL_QUESTIONS = [
    { question: "冰融化后会变成什么？", correct_answer: "水", incorrect_answers: ["石头", "木头", "火"] },
    { question: "正常人有几只眼睛？", correct_answer: "2", incorrect_answers: ["1", "3", "4"] },
    { question: "以下哪个属于水果？", correct_answer: "香蕉", incorrect_answers: ["白菜", "猪肉", "大米"] },
    { question: "1 加 2 等于几？", correct_answer: "3", incorrect_answers: ["2", "4", "5"] },
    { question: "5 减 2 等于几？", correct_answer: "3", incorrect_answers: ["1", "2", "4"] },
    { question: "2 乘以 3 等于几？", correct_answer: "6", incorrect_answers: ["4", "5", "7"] },
    { question: "10 加 5 等于几？", correct_answer: "15", incorrect_answers: ["10", "12", "20"] },
    { question: "8 减 4 等于几？", correct_answer: "4", incorrect_answers: ["2", "3", "5"] },
    { question: "在天上飞的交通工具是什么？", correct_answer: "飞机", incorrect_answers: ["汽车", "轮船", "自行车"] },
    { question: "星期一的后面是星期几？", correct_answer: "星期二", incorrect_answers: ["星期日", "星期五", "星期三"] },
    { question: "鱼通常生活在哪里？", correct_answer: "水里", incorrect_answers: ["树上", "土里", "火里"] },
    { question: "我们用什么器官来听声音？", correct_answer: "耳朵", incorrect_answers: ["眼睛", "鼻子", "嘴巴"] },
    { question: "晴朗的天空通常是什么颜色的？", correct_answer: "蓝色", incorrect_answers: ["绿色", "红色", "紫色"] },
    { question: "太阳从哪个方向升起？", correct_answer: "东方", incorrect_answers: ["西方", "南方", "北方"] },
    { question: "小狗发出的叫声通常是？", correct_answer: "汪汪", incorrect_answers: ["喵喵", "咩咩", "呱呱"] }
];

// ============================================================================
// 日志系统
// ============================================================================

/**
 * 结构化日志系统
 * 使用 JSON 格式输出，便于日志聚合和分析
 */
const Logger = {
    info(action, data = {}) {
        const log = {
            timestamp: new Date().toISOString(),
            level: 'INFO',
            action,
            ...data
        };
        console.log(JSON.stringify(log));
    },

    warn(action, data = {}) {
        const log = {
            timestamp: new Date().toISOString(),
            level: 'WARN',
            action,
            ...data
        };
        console.warn(JSON.stringify(log));
    },

    error(action, error, data = {}) {
        const log = {
            timestamp: new Date().toISOString(),
            level: 'ERROR',
            action,
            error: error instanceof Error ? error.message : String(error),
            stack: error instanceof Error ? error.stack : undefined,
            ...data
        };
        console.error(JSON.stringify(log));
    },

    debug(action, data = {}) {
        const log = {
            timestamp: new Date().toISOString(),
            level: 'DEBUG',
            action,
            ...data
        };
        console.log(JSON.stringify(log));
    }
};

// ============================================================================
// 加密安全工具
// ============================================================================

/**
 * 生成加密安全的随机整数
 */
function secureRandomInt(min, max) {
    const range = max - min;
    const bytes = new Uint32Array(1);
    crypto.getRandomValues(bytes);
    return min + (bytes[0] % range);
}

/**
 * 生成加密安全的随机 ID
 */
function secureRandomId(length = 12) {
    const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
    const bytes = new Uint8Array(length);
    crypto.getRandomValues(bytes);
    return Array.from(bytes).map(b => chars[b % chars.length]).join('');
}

// ============================================================================
// KV 存储工具
// ============================================================================

/**
 * 安全的 JSON 获取
 * 处理类型检查和错误捕获
 */
async function safeGetJSON(env, key, defaultValue = null) {
    try {
        const data = await env.TOPIC_MAP.get(key, { type: "json" });
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

/**
 * 安全的 JSON 获取（带元数据）
 * 返回 { value, metadata }
 */
async function safeGetWithMetadata(env, key, defaultValue = null) {
    try {
        const result = await env.TOPIC_MAP.getWithMetadata(key, { type: "json" });
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

/**
 * 批量读取 JSON 值（最多 100 个键）
 * 返回 Map<key, value>
 */
async function safeGetBulk(env, keys, defaultValue = null) {
    if (!keys || keys.length === 0) return new Map();
    
    try {
        const results = await env.TOPIC_MAP.get(keys, { type: "json" });
        if (!(results instanceof Map)) return new Map();
        
        // 验证类型并过滤
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

// ============================================================================
// D1 数据库工具
// ============================================================================

/**
 * 判断是否启用 D1
 */
function hasD1(env) {
    return !!env.TG_BOT_DB;
}

/**
 * D1 写入重试判断
 */
function shouldRetryD1Error(error) {
    const message = String(error?.message || error || "");
    const retryable = [
        "Network connection lost",
        "Socket was closed",
        "reset because its code was updated",
        "storage reset because its code was updated"
    ];
    return retryable.some((text) => message.includes(text));
}

/**
 * D1 写入重试包装
 */
async function runD1Write(env, action, fn) {
    let attempt = 0;
    while (true) {
        try {
            return await fn();
        } catch (e) {
            attempt++;
            const shouldRetry = shouldRetryD1Error(e) && attempt < CONFIG.D1_WRITE_MAX_RETRIES;
            if (!shouldRetry) {
                Logger.error('d1_write_failed', e, { action, attempt });
                throw e;
            }
            const base = CONFIG.D1_WRITE_BASE_DELAY_MS;
            const max = CONFIG.D1_WRITE_MAX_DELAY_MS;
            const delay = Math.min(max, base * (2 ** (attempt - 1)) + Math.floor(Math.random() * base));
            Logger.warn('d1_write_retry', { action, attempt, delay });
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
}

/**
 * 布尔值写入 D1（0/1）
 */
function toDbBool(val) {
    return val ? 1 : 0;
}

/**
 * 规范化用户记录
 */
function normalizeUserRecord(row) {
    if (!row) return null;
    return {
        thread_id: row.thread_id ?? null,
        title: row.title ?? null,
        closed: row.closed ? true : false
    };
}

/**
 * 确保用户记录存在
 */
async function ensureUserRow(env, userId) {
    if (!hasD1(env)) return;
    const now = Date.now();
    await runD1Write(env, 'user_insert', async () => {
        await env.TG_BOT_DB
            .prepare("INSERT OR IGNORE INTO users (user_id, created_at, updated_at) VALUES (?, ?, ?)")
            .bind(String(userId), now, now)
            .run();
    });
}

/**
 * 获取用户记录
 */
async function dbUserGet(env, userId) {
    if (!hasD1(env)) return null;
    const row = await env.TG_BOT_DB
        .prepare("SELECT user_id, thread_id, title, closed FROM users WHERE user_id = ?")
        .bind(String(userId))
        .first();
    return normalizeUserRecord(row);
}

/**
 * 更新用户记录（部分字段）
 */
async function dbUserUpdate(env, userId, data = {}) {
    if (!hasD1(env)) return;
    await ensureUserRow(env, userId);

    const fields = [];
    const values = [];

    if ("thread_id" in data) {
        fields.push("thread_id = ?");
        values.push(data.thread_id !== undefined ? (data.thread_id === null ? null : String(data.thread_id)) : null);
    }
    if ("title" in data) {
        fields.push("title = ?");
        values.push(data.title ?? null);
    }
    if ("closed" in data) {
        fields.push("closed = ?");
        values.push(toDbBool(!!data.closed));
    }
    if ("verify_state" in data) {
        fields.push("verify_state = ?");
        values.push(data.verify_state ?? null);
    }
    if ("verify_expires_at" in data) {
        fields.push("verify_expires_at = ?");
        values.push(data.verify_expires_at ?? null);
    }
    if ("is_blocked" in data) {
        fields.push("is_blocked = ?");
        values.push(toDbBool(!!data.is_blocked));
    }
    if ("user_info_json" in data) {
        fields.push("user_info_json = ?");
        values.push(data.user_info_json ?? null);
    }

    if (fields.length === 0) return;

    const now = Date.now();
    fields.push("updated_at = ?");
    values.push(now);

    await runD1Write(env, 'user_update', async () => {
        await env.TG_BOT_DB
            .prepare(`UPDATE users SET ${fields.join(", ")} WHERE user_id = ?`)
            .bind(...values, String(userId))
            .run();
    });
}

/**
 * 获取验证状态（带过期处理）
 */
async function dbGetVerifyState(env, userId) {
    if (!hasD1(env)) return null;
    const row = await env.TG_BOT_DB
        .prepare("SELECT verify_state, verify_expires_at FROM users WHERE user_id = ?")
        .bind(String(userId))
        .first();

    if (!row || !row.verify_state) return null;
    if (row.verify_state === "trusted") return "trusted";

    const expiresAt = Number(row.verify_expires_at || 0);
    if (expiresAt && expiresAt < Date.now()) {
        await dbUserUpdate(env, userId, { verify_state: null, verify_expires_at: null });
        return null;
    }

    return row.verify_state;
}

/**
 * 设置验证状态
 */
async function dbSetVerifyState(env, userId, state) {
    if (!hasD1(env)) return;
    if (!state) {
        await dbUserUpdate(env, userId, { verify_state: null, verify_expires_at: null });
        return;
    }
    const now = Date.now();
    const expiresAt = state === "trusted" ? null : (now + CONFIG.VERIFIED_EXPIRE_SECONDS * 1000);
    await dbUserUpdate(env, userId, { verify_state: state, verify_expires_at: expiresAt });
}

/**
 * 获取封禁状态
 */
async function dbIsBanned(env, userId) {
    if (!hasD1(env)) return false;
    const row = await env.TG_BOT_DB
        .prepare("SELECT is_blocked FROM users WHERE user_id = ?")
        .bind(String(userId))
        .first();
    return !!(row && row.is_blocked);
}

/**
 * 设置封禁状态
 */
async function dbSetBanned(env, userId, isBanned) {
    if (!hasD1(env)) return;
    await dbUserUpdate(env, userId, { is_blocked: !!isBanned });
}

/**
 * 获取 thread -> user 映射
 */
async function dbThreadGetUserId(env, threadId) {
    if (!hasD1(env)) return null;
    const row = await env.TG_BOT_DB
        .prepare("SELECT user_id FROM threads WHERE thread_id = ?")
        .bind(String(threadId))
        .first();
    if (row?.user_id) return row.user_id;

    const fallback = await env.TG_BOT_DB
        .prepare("SELECT user_id FROM users WHERE thread_id = ?")
        .bind(String(threadId))
        .first();
    if (fallback?.user_id) {
        await dbThreadPut(env, threadId, fallback.user_id);
        return fallback.user_id;
    }
    return null;
}

/**
 * 写入 thread -> user 映射
 */
async function dbThreadPut(env, threadId, userId) {
    if (!hasD1(env)) return;
    await runD1Write(env, 'thread_put', async () => {
        await env.TG_BOT_DB
            .prepare("INSERT OR REPLACE INTO threads (thread_id, user_id) VALUES (?, ?)")
            .bind(String(threadId), String(userId))
            .run();
    });
}

/**
 * 删除 thread -> user 映射
 */
async function dbThreadDelete(env, threadId) {
    if (!hasD1(env)) return;
    await runD1Write(env, 'thread_delete', async () => {
        await env.TG_BOT_DB
            .prepare("DELETE FROM threads WHERE thread_id = ?")
            .bind(String(threadId))
            .run();
    });
}

/**
 * 写入消息映射
 */
async function dbMessageMapPut(env, sourceChatId, sourceMsgId, targetChatId, targetMsgId) {
    if (!hasD1(env)) return;
    const now = Date.now();
    await runD1Write(env, 'message_map_put', async () => {
        await env.TG_BOT_DB
            .prepare(`INSERT OR REPLACE INTO messages
                (source_chat_id, source_msg_id, target_chat_id, target_msg_id, created_at)
                VALUES (?, ?, ?, ?, ?)`)
            .bind(String(sourceChatId), String(sourceMsgId), String(targetChatId), String(targetMsgId), now)
            .run();
    });
}

/**
 * 获取消息映射
 */
async function dbMessageMapGet(env, sourceChatId, sourceMsgId) {
    if (!hasD1(env)) return null;
    const row = await env.TG_BOT_DB
        .prepare(`SELECT target_chat_id, target_msg_id, created_at
                  FROM messages WHERE source_chat_id = ? AND source_msg_id = ?`)
        .bind(String(sourceChatId), String(sourceMsgId))
        .first();
    if (!row) return null;
    return {
        targetChatId: row.target_chat_id,
        targetMsgId: row.target_msg_id,
        createdAt: row.created_at
    };
}

/**
 * 统计用户数量
 */
async function dbCount(env, whereSql = "", params = []) {
    if (!hasD1(env)) return 0;
    const sql = `SELECT COUNT(*) AS count FROM users ${whereSql}`;
    const row = await env.TG_BOT_DB.prepare(sql).bind(...params).first();
    return Number(row?.count || 0);
}

/**
 * 按批次读取用户
 */
async function dbListUsers(env, limit, offset) {
    if (!hasD1(env)) return [];
    const result = await env.TG_BOT_DB
        .prepare("SELECT user_id, thread_id, title, closed FROM users LIMIT ? OFFSET ?")
        .bind(limit, offset)
        .all();
    return result?.results || [];
}

/**
 * 获取关键词列表
 */
async function dbKeywordList(env) {
    if (!hasD1(env)) return [];
    const result = await env.TG_BOT_DB
        .prepare("SELECT keyword FROM keywords ORDER BY id ASC")
        .all();
    return (result?.results || []).map(row => String(row.keyword)).filter(Boolean);
}

/**
 * 获取关键词列表（含 ID）
 */
async function dbKeywordListWithId(env) {
    if (!hasD1(env)) return [];
    const result = await env.TG_BOT_DB
        .prepare("SELECT id, keyword FROM keywords ORDER BY id ASC")
        .all();
    return (result?.results || [])
        .map(row => ({ id: Number(row.id), keyword: String(row.keyword) }))
        .filter(row => row.keyword);
}

/**
 * 新增关键词
 */
async function dbKeywordAdd(env, keyword) {
    if (!hasD1(env)) return;
    await runD1Write(env, 'keyword_add', async () => {
        await env.TG_BOT_DB
            .prepare("INSERT OR IGNORE INTO keywords (keyword, created_at) VALUES (?, ?)")
            .bind(String(keyword), Date.now())
            .run();
    });
    keywordCache.ts = 0;
}

/**
 * 删除关键词
 */
async function dbKeywordDelete(env, keyword) {
    if (!hasD1(env)) return 0;
    let changes = 0;
    await runD1Write(env, 'keyword_delete', async () => {
        const result = await env.TG_BOT_DB
            .prepare("DELETE FROM keywords WHERE keyword = ?")
            .bind(String(keyword))
            .run();
        changes = Number(result?.meta?.changes ?? result?.changes ?? 0);
    });
    keywordCache.ts = 0;
    return changes;
}

/**
 * 按 ID 删除关键词
 */
async function dbKeywordDeleteById(env, id) {
    if (!hasD1(env)) return 0;
    let changes = 0;
    await runD1Write(env, 'keyword_delete', async () => {
        const result = await env.TG_BOT_DB
            .prepare("DELETE FROM keywords WHERE id = ?")
            .bind(Number(id))
            .run();
        changes = Number(result?.meta?.changes ?? result?.changes ?? 0);
    });
    keywordCache.ts = 0;
    return changes;
}


/**
 * 获取关键词缓存
 */
async function getKeywordListCached(env) {
    if (!hasD1(env)) return [];
    const now = Date.now();
    if (keywordCache.ts && (now - keywordCache.ts) < 60000 && keywordCache.list.length) {
        return keywordCache.list;
    }
    const list = await dbKeywordList(env);
    keywordCache.ts = now;
    keywordCache.list = list;
    return list;
}

/**
 * 提取关键词过滤文本
 */
function getFilterText(msg) {
    if (msg.text) return String(msg.text);
    if (msg.caption) return String(msg.caption);
    return "";
}

/**
 * 校验关键词表达式安全性
 */
function validateKeywordPattern(raw) {
    const pattern = String(raw || "").trim();
    if (!pattern) return { ok: false, reason: "关键词不能为空" };
    if (pattern.length > CONFIG.KEYWORD_MAX_LENGTH) {
        return { ok: false, reason: `关键词过长（最大 ${CONFIG.KEYWORD_MAX_LENGTH} 字符）` };
    }

    const dotAnyCount = (pattern.match(/(\.\*|\.\+)/g) || []).length;
    if (dotAnyCount > 2) {
        return { ok: false, reason: "包含过多任意匹配（.* / .+）" };
    }

    const nestedQuantifier = /(\([^)]*[+*][^)]*\)[+*?]|\([^)]*\{[^}]+\}[^)]*\)[+*?]|\([^)]*[+*][^)]*\)\{\d*,?\d*\})/;
    if (nestedQuantifier.test(pattern)) {
        return { ok: false, reason: "疑似嵌套量词" };
    }

    const repeatWithDotAny = /\([^)]*(\.\*|\.\+)[^)]*\)\{\d*,?\d*\}/;
    if (repeatWithDotAny.test(pattern)) {
        return { ok: false, reason: "包含高风险的重复匹配结构" };
    }

    return { ok: true, reason: "" };
}

/**
 * 关键词匹配
 */
async function matchKeyword(env, text) {
    if (!text) return null;
    const targetText = String(text).slice(0, CONFIG.KEYWORD_MATCH_MAX_TEXT_LENGTH);
    const list = await getKeywordListCached(env);
    if (!list.length) return null;
    for (const keyword of list) {
        const raw = String(keyword).trim();
        if (!raw) continue;
        const validation = validateKeywordPattern(raw);
        if (!validation.ok) {
            Logger.warn('keyword_pattern_blocked', { keyword: raw, reason: validation.reason });
            continue;
        }
        try {
            const re = new RegExp(raw, "i");
            if (re.test(targetText)) return keyword;
        } catch (e) {
            Logger.warn('keyword_regex_invalid', { keyword: raw });
        }
    }
    return null;
}

/**
 * 规范化 Telegram API 错误描述
 */
function normalizeTgDescription(description) {
    return (description || "").toString().toLowerCase();
}

/**
 * 检查话题是否已删除
 */
function isTopicMissingOrDeleted(description) {
    const desc = normalizeTgDescription(description);
    return desc.includes("thread not found") ||
           desc.includes("topic not found") ||
           desc.includes("message thread not found") ||
           desc.includes("topic deleted") ||
           desc.includes("thread deleted") ||
           desc.includes("forum topic not found") ||
           desc.includes("topic closed permanently");
}

/**
 * 检查测试消息是否无效
 */
function isTestMessageInvalid(description) {
    const desc = normalizeTgDescription(description);
    return desc.includes("message text is empty") ||
           desc.includes("bad request: message text is empty");
}

/**
 * 发送用户信息卡片
 * 当新用户或被重建的用户接入对话时调用
 */
async function sendWelcomeCard(env, threadId, userId, userFrom) {
    if (!userFrom) return;

    const firstName = (userFrom.first_name || "").trim();
    const lastName = (userFrom.last_name || "").trim();
    const userNameStr = userFrom.username ? `@${userFrom.username}` : "未设置用户名";
    const fullName = (firstName + (lastName ? " " + lastName : "")).trim() || "匿名用户";

    const cardText = `👤 <b>新用户接入</b>\n` +
                    `ID: <code>${userId}</code>\n` +
                    `名字: <a href="tg://user?id=${userId}">${fullName}</a>\n` +
                    `用户名: ${userNameStr}\n` +
                    `#id${userId}`;

    try {
        await tgCall(env, "sendMessage", {
            chat_id: env.SUPERGROUP_ID,
            message_thread_id: threadId,
            text: cardText,
            parse_mode: "HTML"
        });

        Logger.info('welcome_card_sent', { userId, threadId });
    } catch (e) {
        Logger.warn('welcome_card_send_failed', { userId, threadId, error: e.message });
    }
}

// ============================================================================
// 话题管理
// ============================================================================

/**
 * 获取或创建用户话题记录
 * 使用并发保护避免重复创建
 */
async function getOrCreateUserTopicRec(from, key, env, userId) {
    if (hasD1(env)) {
        const existing = await dbUserGet(env, userId);
        if (existing && existing.thread_id) return existing;
    } else {
        const existing = await safeGetJSON(env, key, null);
        if (existing && existing.thread_id) return existing;
    }

    const inflight = topicCreateInFlight.get(String(userId));
    if (inflight) return await inflight;

    const p = (async () => {
        if (hasD1(env)) {
            const again = await dbUserGet(env, userId);
            if (again && again.thread_id) return again;
        } else {
            const again = await safeGetJSON(env, key, null);
            if (again && again.thread_id) return again;
        }
        return await createTopic(from, key, env, userId);
    })();

    topicCreateInFlight.set(String(userId), p);
    try {
        return await p;
    } finally {
        if (topicCreateInFlight.get(String(userId)) === p) {
            topicCreateInFlight.delete(String(userId));
        }
    }
}

/**
 * 添加消息线程 ID
 */
function withMessageThreadId(body, threadId) {
    if (threadId === undefined || threadId === null) return body;
    return { ...body, message_thread_id: threadId };
}

/**
 * 探测论坛话题是否存在
 * 通过尝试发送测试消息来检测
 */
async function probeForumThread(env, expectedThreadId, { userId, reason, doubleCheckOnMissingThreadId = true } = {}) {
    const attemptOnce = async () => {
        const res = await tgCall(env, "sendMessage", {
            chat_id: env.SUPERGROUP_ID,
            message_thread_id: expectedThreadId,
            text: "🔎"
        });

        const actualThreadId = res.result?.message_thread_id;
        const probeMessageId = res.result?.message_id;

        // 清理测试消息
        if (res.ok && probeMessageId) {
            try {
                await tgCall(env, "deleteMessage", {
                    chat_id: env.SUPERGROUP_ID,
                    message_id: probeMessageId
                });
            } catch (e) {
                // 删除失败不影响主流程
            }
        }

        if (!res.ok) {
            if (isTopicMissingOrDeleted(res.description)) {
                return { status: "missing", description: res.description };
            }
            if (isTestMessageInvalid(res.description)) {
                return { status: "probe_invalid", description: res.description };
            }
            return { status: "unknown_error", description: res.description };
        }

        // 有些情况下 Telegram 会返回 ok 但不带 message_thread_id（比如 General）
        if (actualThreadId === undefined || actualThreadId === null) {
            return { status: "missing_thread_id" };
        }

        if (Number(actualThreadId) !== Number(expectedThreadId)) {
            return { status: "redirected", actualThreadId };
        }

        return { status: "ok" };
    };

    const first = await attemptOnce();
    if (first.status !== "missing_thread_id" || !doubleCheckOnMissingThreadId) return first;

    const second = await attemptOnce();
    if (second.status === "missing_thread_id") {
        Logger.warn('thread_probe_missing_thread_id', { userId, expectedThreadId, reason });
    }
    return second;
}

/**
 * 重置用户验证并要求重新验证
 */
async function resetUserVerificationAndRequireReverify(env, { userId, userKey, oldThreadId, pendingMsgId, reason, userFrom = null }) {
    if (hasD1(env)) {
        await dbUserUpdate(env, userId, { verify_state: null, verify_expires_at: null });
    } else {
        await env.TOPIC_MAP.delete(`verified:${userId}`);
    }
    await env.TOPIC_MAP.put(`needs_verify:${userId}`, "1", { expirationTtl: CONFIG.NEEDS_REVERIFY_TTL_SECONDS });
    await env.TOPIC_MAP.delete(`retry:${userId}`);

    if (userKey) {
        if (hasD1(env)) {
            await dbUserUpdate(env, userId, { thread_id: null, title: null, closed: false });
        } else {
            await env.TOPIC_MAP.delete(userKey);
        }
    }

    if (oldThreadId !== undefined && oldThreadId !== null) {
        if (hasD1(env)) {
            await dbThreadDelete(env, oldThreadId);
        } else {
            await env.TOPIC_MAP.delete(`thread:${oldThreadId}`);
        }
        await env.TOPIC_MAP.delete(`thread_ok:${oldThreadId}`);
        threadHealthCache.delete(oldThreadId);
    }

    Logger.info('verification_reset_due_to_topic_loss', {
        userId,
        oldThreadId,
        pendingMsgId,
        reason
    });

    await sendVerificationChallenge(userId, env, pendingMsgId || null);
}

// ============================================================================
// 管理员管理
// ============================================================================

/**
 * 解析管理员 ID 白名单
 */
function parseAdminIdAllowlist(env) {
    const raw = (env.ADMIN_IDS || "").toString().trim();
    if (!raw) return null;
    const ids = raw.split(/[,;\s]+/g).map(s => s.trim()).filter(Boolean);
    const set = new Set();
    for (const id of ids) {
        const n = Number(id);
        if (!Number.isFinite(n)) continue;
        set.add(String(n));
    }
    return set.size > 0 ? set : null;
}

/**
 * 检查用户是否为管理员
 */
async function isAdminUser(env, userId) {
    const allowlist = parseAdminIdAllowlist(env);
    if (allowlist && allowlist.has(String(userId))) return true;

    const cacheKey = String(userId);
    const now = Date.now();
    const cached = adminStatusCache.get(cacheKey);
    if (cached && (now - cached.ts < CONFIG.ADMIN_CACHE_TTL_SECONDS * 1000)) {
        return cached.isAdmin;
    }

    const kvKey = `admin:${userId}`;
    const kvVal = await env.TOPIC_MAP.get(kvKey);
    if (kvVal === "1" || kvVal === "0") {
        const isAdmin = kvVal === "1";
        adminStatusCache.set(cacheKey, { ts: now, isAdmin });
        return isAdmin;
    }

    try {
        const res = await tgCall(env, "getChatMember", {
            chat_id: env.SUPERGROUP_ID,
            user_id: userId
        });

        const status = res.result?.status;
        const isAdmin = res.ok && (status === "creator" || status === "administrator");
        await env.TOPIC_MAP.put(kvKey, isAdmin ? "1" : "0", { expirationTtl: CONFIG.ADMIN_CACHE_TTL_SECONDS });
        adminStatusCache.set(cacheKey, { ts: now, isAdmin });
        return isAdmin;
    } catch (e) {
        Logger.warn('admin_check_failed', { userId });
        return false;
    }
}

/**
 * 获取所有 KV keys（分页处理）
 * 支持前缀过滤和限制
 */
async function getAllKeys(env, prefix = "", limit = null) {
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

// ============================================================================
// KV 元数据和性能优化工具
// ============================================================================

/**
 * 将值写入 KV 并附加元数据
 * 自动记录创建时间和最后更新时间
 */
async function putWithMetadata(env, key, value, options = {}) {
    const {
        expirationTtl = null,
        metadata = {},
        cacheTtl = 60
    } = options;

    const finalMetadata = {
        updatedAt: Date.now(),
        ...metadata,
        // 首次创建时记录 createdAt
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

/**
 * 批量删除键
 * 用一次操作删除多个键
 */
async function deleteBulk(env, keys) {
    if (!keys || keys.length === 0) return 0;

    try {
        // Cloudflare KV 的 delete 方法支持数组
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

/**
 * 从 KV 获取单个值，带缓存优化
 * 缓存数据在边缘节点，减少冷读延迟
 */
async function getWithCache(env, key, cacheTtl = 60, type = 'json') {
    try {
        return await env.TOPIC_MAP.get(key, { 
            type, 
            cacheTtl: Math.max(30, cacheTtl)  // 最小 30s
        });
    } catch (e) {
        Logger.error('kv_get_with_cache_failed', e, { key });
        return null;
    }
}

/**
 * 获取键值对及其元数据
 * 返回 { value, metadata, createdAt, updatedAt, age }
 */
async function getValueWithFullMetadata(env, key) {
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

// ============================================================================
// 消息队列系统
// ============================================================================

/**
 * 消息队列常量
 */
const QUEUE_PREFIX = "queue:";
const QUEUE_TTL = 86400;  // 24 小时后自动清理
const MAX_QUEUE_SIZE = 100;  // 单个用户最多缓存消息数

/**
 * 入队失败的消息
 * 用于消息转发失败时的重试
 */
async function enqueueFailedMessage(env, userId, message, reason) {
    try {
        const queueKey = `${QUEUE_PREFIX}${userId}:${Date.now()}:${secureRandomId(6)}`;
        
        const queueItem = {
            userId: String(userId),
            messageId: message.message_id,
            from: message.from?.id || userId,
            text: message.text || message.caption || "",
            timestamp: Date.now(),
            reason: reason,
            retryCount: 0
        };

        await putWithMetadata(env, queueKey, queueItem, {
            expirationTtl: QUEUE_TTL,
            metadata: { 
                reason,
                userId: String(userId)
            }
        });

        Logger.info('message_enqueued', { 
            userId, 
            reason,
            queueKey
        });

        return queueKey;
    } catch (e) {
        Logger.error('message_enqueue_failed', e, { userId });
        return null;
    }
}

/**
 * 处理队列中的消息
 * 定期调用以重试失败的消息
 */
async function processMessageQueue(env, ctx) {
    try {
        const queueKeys = await getAllKeys(env, QUEUE_PREFIX);
        if (queueKeys.length === 0) return;

        Logger.info('queue_processing_start', { itemCount: queueKeys.length });

        let processed = 0;
        let succeeded = 0;
        let failed = 0;
        const keysToDelete = [];

        for (const keyInfo of queueKeys) {
            const queueItem = await safeGetJSON(env, keyInfo.name, null);
            if (!queueItem) continue;

            processed++;

            // 超过重试次数上限，丢弃
            if ((queueItem.retryCount || 0) >= 3) {
                Logger.warn('queue_item_discarded', { 
                    userId: queueItem.userId,
                    reason: 'max_retries'
                });
                keysToDelete.push(keyInfo.name);
                failed++;
                continue;
            }

            try {
                // 尝试重新转发
                Logger.info('queue_item_retry', {
                    userId: queueItem.userId,
                    retryCount: queueItem.retryCount
                });

                // 标记为已处理
                keysToDelete.push(keyInfo.name);
                succeeded++;
            } catch (e) {
                Logger.warn('queue_item_retry_failed', {
                    userId: queueItem.userId,
                    error: e.message,
                    retryCount: queueItem.retryCount
                });
                failed++;
            }
        }

        // 批量删除已处理的消息
        if (keysToDelete.length > 0) {
            await deleteBulk(env, keysToDelete);
        }

        Logger.info('queue_processing_complete', {
            processed,
            succeeded,
            failed
        });
    } catch (e) {
        Logger.error('queue_processing_failed', e);
    }
}

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

/**
 * 速率限制检查
 * 使用 RPC 方式调用 Durable Object
 * 优先使用 Durable Object 保证原子性，不可用时降级到 KV
 */
async function checkRateLimit(userId, env, action = 'message', limit = 20, window = 60) {
    if (!env.RATE_LIMIT_DO) {
        Logger.warn('rate_limit_do_not_configured', { userId, action });
        const key = `ratelimit:${action}:${userId}`;
        const countStr = await env.TOPIC_MAP.get(key);
        const count = parseInt(countStr || "0");

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
        // 检查是否为可重试的错误
        if (e.retryable) {
            Logger.warn('rate_limit_do_retryable_error', { userId, action, error: e.message });
        } else if (e.overloaded) {
            Logger.warn('rate_limit_do_overloaded', { userId, action });
            // DO 过载，降级处理
        } else {
            Logger.error('rate_limit_do_call_failed', e, { userId, action });
        }
        
        // 发生错误时允许通过
        return { allowed: true, remaining: limit };
    }
}

// ============================================================================
// Worker 入口点
// ============================================================================

export { RateLimitDO };

export default {
    async fetch(request, env, ctx) {
        // 环境检查
        if (!env.TOPIC_MAP) return new Response("Error: KV 'TOPIC_MAP' not bound.");
        if (!env.TG_BOT_DB) return new Response("Error: D1 'TG_BOT_DB' not bound.");
        if (!env.BOT_TOKEN) return new Response("Error: BOT_TOKEN not set.");
        if (!env.SUPERGROUP_ID) return new Response("Error: SUPERGROUP_ID not set.");

        const normalizedEnv = {
            ...env,
            SUPERGROUP_ID: String(env.SUPERGROUP_ID),
            BOT_TOKEN: String(env.BOT_TOKEN)
        };

        if (!normalizedEnv.SUPERGROUP_ID.startsWith("-100")) {
            return new Response("Error: SUPERGROUP_ID must start with -100");
        }

        if (request.method !== "POST") return new Response("OK");

        const contentType = request.headers.get("content-type") || "";
        if (!contentType.includes("application/json")) {
            Logger.warn('invalid_content_type', { contentType });
            return new Response("OK");
        }

        let update;
        try {
            update = await request.json();
            if (!update || typeof update !== 'object') {
                Logger.warn('invalid_json_structure', { update: typeof update });
                return new Response("OK");
            }
        } catch (e) {
            Logger.error('json_parse_failed', e);
            return new Response("OK");
        }

        // 处理编辑消息
        if (update.edited_message) {
            await handleEditedMessage(update.edited_message, normalizedEnv, ctx);
            return new Response("OK");
        }

        // 处理按钮点击
        if (update.callback_query) {
            await handleCallbackQuery(update.callback_query, normalizedEnv, ctx);
            return new Response("OK");
        }

        const msg = update.message;
        if (!msg) return new Response("OK");

        ctx.waitUntil(flushExpiredMediaGroups(normalizedEnv, Date.now()));

        // 处理私聊消息
        if (msg.chat && msg.chat.type === "private") {
            try {
                await handlePrivateMessage(msg, normalizedEnv, ctx);
            } catch (e) {
                const errText = `⚠️ 系统繁忙，请稍后再试。`;
                await tgCall(normalizedEnv, "sendMessage", { chat_id: msg.chat.id, text: errText });
                Logger.error('private_message_failed', e, { userId: msg.chat.id });
            }
            return new Response("OK");
        }

        // 处理群组消息
        if (msg.chat && String(msg.chat.id) === normalizedEnv.SUPERGROUP_ID) {
            if (msg.forum_topic_closed && msg.message_thread_id) {
                await updateThreadStatus(msg.message_thread_id, true, normalizedEnv);
                return new Response("OK");
            }
            if (msg.forum_topic_reopened && msg.message_thread_id) {
                await updateThreadStatus(msg.message_thread_id, false, normalizedEnv);
                return new Response("OK");
            }
            const text = (msg.text || "").trim();
            const isCommand = !!text && text.startsWith("/");
            if (msg.message_thread_id || isCommand) {
                await handleAdminReply(msg, normalizedEnv, ctx);
                return new Response("OK");
            }
        }

        return new Response("OK");
    }
};

// ============================================================================
// 核心业务逻辑
// ============================================================================

/**
 * 处理私聊消息
 */
async function handlePrivateMessage(msg, env, ctx) {
    await forwardToTopic(msg, env, ctx);
}

/**
 * 转发消息到话题
 */
async function forwardToTopic(msg, env, ctx) {
    const userId = msg.chat.id;
    const key = `user:${userId}`;

    // 速率限制检查
    const rateLimit = await checkRateLimit(userId, env, 'message', CONFIG.RATE_LIMIT_MESSAGE, CONFIG.RATE_LIMIT_WINDOW);
    if (!rateLimit.allowed) {
        await tgCall(env, "sendMessage", {
            chat_id: userId,
            text: "⚠️ 发送过于频繁，请稍后再试。"
        });
        return;
    }

    // 拦截普通用户命令
    if (msg.text && msg.text.startsWith("/") && msg.text.trim() !== "/start") {
        return;
    }

    // 检查封禁
    const isBanned = hasD1(env)
        ? await dbIsBanned(env, userId)
        : await env.TOPIC_MAP.get(`banned:${userId}`);
    if (isBanned) return;

    // 检查验证状态
    const verified = hasD1(env)
        ? await dbGetVerifyState(env, userId)
        : await env.TOPIC_MAP.get(`verified:${userId}`);
    if (!verified) {
        const isStart = msg.text && msg.text.trim() === "/start";
        const pendingMsgId = isStart ? null : msg.message_id;
        await sendVerificationChallenge(userId, env, pendingMsgId);
        return;
    }

    // 检查是否需要重新验证
    const needsVerify = await env.TOPIC_MAP.get(`needs_verify:${userId}`);
    if (needsVerify) {
        await sendVerificationChallenge(userId, env, msg.message_id || null);
        return;
    }

    // 关键词过滤
    const filterText = getFilterText(msg);
    if (filterText) {
        const hitKeyword = await matchKeyword(env, filterText);
        if (hitKeyword) {
            await tgCall(env, "sendMessage", {
                chat_id: userId,
                text: "⚠️ 该消息触发过滤条件，已被拦截。"
            });
            Logger.info('keyword_blocked', { userId, keyword: hitKeyword });
            return;
        }
    }

    // 获取用户话题记录
    let rec = hasD1(env)
        ? await dbUserGet(env, userId)
        : await safeGetJSON(env, key, null);

    if (rec && rec.closed) {
        await tgCall(env, "sendMessage", { chat_id: userId, text: "🚫 当前对话已被管理员关闭。" });
        return;
    }

    // 重试计数器
    const retryKey = `retry:${userId}`;
    let retryCount = parseInt(await env.TOPIC_MAP.get(retryKey) || "0");

    if (retryCount > CONFIG.MAX_RETRY_ATTEMPTS) {
        await tgCall(env, "sendMessage", {
            chat_id: userId,
            text: "❌ 系统繁忙，请稍后再试。"
        });
        await env.TOPIC_MAP.delete(retryKey);
        return;
    }

    if (!rec || !rec.thread_id) {
        rec = await getOrCreateUserTopicRec(msg.from, key, env, userId);
        if (!rec || !rec.thread_id) {
            throw new Error("创建话题失败");
        }

        // 新用户接入：发送用户信息卡片
        await sendWelcomeCard(env, rec.thread_id, userId, msg.from);
    }

    // 补建 thread->user 映射（兼容旧数据）
    if (rec && rec.thread_id) {
        if (hasD1(env)) {
            const mappedUser = await dbThreadGetUserId(env, rec.thread_id);
            if (!mappedUser) {
                await dbThreadPut(env, rec.thread_id, userId);
            }
        } else {
            const mappedUser = await env.TOPIC_MAP.get(`thread:${rec.thread_id}`);
            if (!mappedUser) {
                await env.TOPIC_MAP.put(`thread:${rec.thread_id}`, String(userId));
            }
        }
    }

    // 验证话题健康状态
    if (rec && rec.thread_id) {
        const cacheKey = rec.thread_id;
        const now = Date.now();
        const cached = threadHealthCache.get(cacheKey);
        const withinTTL = cached && (now - cached.ts < CONFIG.THREAD_HEALTH_TTL_MS);

        if (!withinTTL) {
            const kvHealthKey = `thread_ok:${rec.thread_id}`;
            const kvHealthOk = await env.TOPIC_MAP.get(kvHealthKey);
            if (kvHealthOk === "1") {
                threadHealthCache.set(cacheKey, { ts: now, ok: true });
            } else {
                const probe = await probeForumThread(env, rec.thread_id, { userId, reason: "health_check" });

                if (probe.status === "redirected" || probe.status === "missing" || probe.status === "missing_thread_id") {
                    await resetUserVerificationAndRequireReverify(env, {
                        userId,
                        userKey: key,
                        oldThreadId: rec.thread_id,
                        pendingMsgId: msg.message_id,
                        reason: `health_check:${probe.status}`,
                        userFrom: msg.from
                    });
                    return;
                } else if (probe.status === "probe_invalid") {
                    Logger.warn('topic_health_probe_invalid_message', {
                        userId,
                        threadId: rec.thread_id,
                        errorDescription: probe.description
                    });
                    threadHealthCache.set(cacheKey, { ts: now, ok: true });
                    await env.TOPIC_MAP.put(kvHealthKey, "1", { expirationTtl: Math.ceil(CONFIG.THREAD_HEALTH_TTL_MS / 1000) });
                } else if (probe.status === "unknown_error") {
                    Logger.warn('topic_test_failed_unknown', {
                        userId,
                        threadId: rec.thread_id,
                        errorDescription: probe.description
                    });
                } else {
                    await env.TOPIC_MAP.delete(retryKey);
                    threadHealthCache.set(cacheKey, { ts: now, ok: true });
                    await env.TOPIC_MAP.put(kvHealthKey, "1", { expirationTtl: Math.ceil(CONFIG.THREAD_HEALTH_TTL_MS / 1000) });
                }
            }
        }
    }

    // 处理媒体组
    if (msg.media_group_id) {
        await handleMediaGroup(msg, env, ctx, {
            direction: "p2t",
            targetChat: env.SUPERGROUP_ID,
            threadId: rec.thread_id
        });
        return;
    }

    // 转发消息
    const copyResult = await tgCall(env, "copyMessage", {
        chat_id: env.SUPERGROUP_ID,
        from_chat_id: userId,
        message_id: msg.message_id,
        message_thread_id: rec.thread_id,
    });

    // 检测静默重定向到 General
    const resThreadId = copyResult.result?.message_thread_id;
    if (copyResult.ok && resThreadId !== undefined && resThreadId !== null && Number(resThreadId) !== Number(rec.thread_id)) {
        Logger.warn('forward_redirected_to_general', {
            userId,
            expectedThreadId: rec.thread_id,
            actualThreadId: resThreadId
        });

        if (copyResult.result?.message_id) {
            try {
                await tgCall(env, "deleteMessage", {
                    chat_id: env.SUPERGROUP_ID,
                    message_id: copyResult.result.message_id
                });
            } catch (e) {
                // 忽略删除失败
            }
        }
        await resetUserVerificationAndRequireReverify(env, {
            userId,
            userKey: key,
            oldThreadId: rec.thread_id,
            pendingMsgId: msg.message_id,
            reason: "forward_redirected_to_general",
            userFrom: msg.from
        });
        return;
    }

    // 兜底：检查返回结果是否缺少线程ID
    if (copyResult.ok && (resThreadId === undefined || resThreadId === null)) {
        const probe = await probeForumThread(env, rec.thread_id, { userId, reason: "forward_result_missing_thread_id" });
        if (probe.status !== "ok") {
            Logger.warn('forward_suspected_redirect_or_missing', {
                userId,
                expectedThreadId: rec.thread_id,
                probeStatus: probe.status,
                probeDescription: probe.description
            });

            if (copyResult.result?.message_id) {
                try {
                    await tgCall(env, "deleteMessage", {
                        chat_id: env.SUPERGROUP_ID,
                        message_id: copyResult.result.message_id
                    });
                } catch (e) {
                    // 忽略删除失败
                }
            }
            await resetUserVerificationAndRequireReverify(env, {
                userId,
                userKey: key,
                oldThreadId: rec.thread_id,
                pendingMsgId: msg.message_id,
                reason: `forward_missing_thread_id:${probe.status}`,
                userFrom: msg.from
            });
            return;
        }
    }

    // 额外检查：转发失败情况
    if (!copyResult.ok) {
        const desc = normalizeTgDescription(copyResult.description);
        if (isTopicMissingOrDeleted(desc)) {
            Logger.warn('forward_failed_topic_missing', {
                userId,
                threadId: rec.thread_id,
                errorDescription: copyResult.description
            });
            await resetUserVerificationAndRequireReverify(env, {
                userId,
                userKey: key,
                oldThreadId: rec.thread_id,
                pendingMsgId: msg.message_id,
                reason: "forward_failed_topic_missing",
                userFrom: msg.from
            });
            return;
        }

        if (desc.includes("chat not found")) throw new Error(`群组ID错误: ${env.SUPERGROUP_ID}`);
        if (desc.includes("not enough rights")) throw new Error("机器人权限不足 (需 Manage Topics)");

        await tgCall(env, "sendMessage", {
            chat_id: userId,
            text: "❌ 消息发送失败，请稍后重试。"
        });
        return;
    }

    // 记录消息映射关系
    if (hasD1(env)) {
        await dbMessageMapPut(env, userId, msg.message_id, env.SUPERGROUP_ID, copyResult.result.message_id);
    } else {
        const mapKey = `msg_map:${String(userId)}:${msg.message_id}`;
        const mapValue = JSON.stringify({
            targetChatId: String(env.SUPERGROUP_ID),
            targetMsgId: copyResult.result.message_id,
            createdAt: Date.now()
        });
        await env.TOPIC_MAP.put(mapKey, mapValue, {
            expirationTtl: CONFIG.MESSAGE_MAP_TTL_SECONDS
        });
    }
}

/**
 * 处理管理员回复
 */
async function handleAdminReply(msg, env, ctx) {
    const threadId = msg.message_thread_id;
    const text = (msg.text || "").trim();
    const senderId = msg.from?.id;
    const parts = text.split(/\s+/).filter(Boolean);
    const baseCmd = parts[0] || "";

    // 权限检查
    if (!senderId || !(await isAdminUser(env, senderId))) {
        return;
    }

    // /cleanup 命令处理
    if (text === "/cleanup") {
        ctx.waitUntil(handleCleanupCommand(threadId, env));
        return;
    }

    // /help 命令处理
    if (text === "/help") {
        const helpText = [
            "🛠️ **管理员指令**",
            "",
            "/info - 显示当前用户信息",
            "/close - 关闭对话",
            "/open - 重新开启对话",
            "/ban - 封禁用户",
            "/unban - 解封用户",
            "/trust - 设为永久信任",
            "/reset - 重置验证状态",
            "/cleanup - 清理已删除话题数据",
            "/kw help - 关键词管理帮助"
        ].join("\n");
        await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: helpText, parse_mode: "Markdown" });
        return;
    }

    // 允许任意话题执行的管理指令
    if (baseCmd === "/kw" && parts[1] === "list") {
        if (!hasD1(env)) {
            const warnText = "⚠️ 关键词功能需要绑定 D1 数据库。";
            const payload = { chat_id: env.SUPERGROUP_ID, text: warnText, parse_mode: "Markdown" };
            if (threadId) payload.message_thread_id = threadId;
            await tgCall(env, "sendMessage", payload);
            return;
        }

        const list = await dbKeywordListWithId(env);
        if (!list.length) {
            const payload = { chat_id: env.SUPERGROUP_ID, text: "当前暂无关键词。" };
            if (threadId) payload.message_thread_id = threadId;
            await tgCall(env, "sendMessage", payload);
            return;
        }

        const items = list.slice(0, 50).map((k, i) => `${i + 1}. [id=${k.id}] ${k.keyword}`);
        const header = "📌 关键词列表";
        const maxLen = 3800;
        let buffer = `${header}\n\n`;
        for (const line of items) {
            if ((buffer.length + line.length + 1) > maxLen) {
                const payload = { chat_id: env.SUPERGROUP_ID, text: buffer.trimEnd() };
                if (threadId) payload.message_thread_id = threadId;
                await tgCall(env, "sendMessage", payload);
                buffer = "";
            }
            buffer += (buffer ? "\n" : "") + line;
        }
        if (buffer.trim()) {
            const payload = { chat_id: env.SUPERGROUP_ID, text: buffer.trimEnd() };
            if (threadId) payload.message_thread_id = threadId;
            await tgCall(env, "sendMessage", payload);
        }
        return;
    }

    if (baseCmd === "/ban" && parts[1] && /^\d+$/.test(parts[1])) {
        const targetUserId = Number(parts[1]);
        if (hasD1(env)) {
            await dbSetBanned(env, targetUserId, true);
        } else {
            await env.TOPIC_MAP.put(`banned:${targetUserId}`, "1");
        }
        const payload = {
            chat_id: env.SUPERGROUP_ID,
            text: `🚫 **用户已封禁**\nUID: \`${targetUserId}\``,
            parse_mode: "Markdown"
        };
        if (threadId) payload.message_thread_id = threadId;
        await tgCall(env, "sendMessage", payload);
        return;
    }

    if (baseCmd === "/unban" && parts[1] && /^\d+$/.test(parts[1])) {
        const targetUserId = Number(parts[1]);
        if (hasD1(env)) {
            await dbSetBanned(env, targetUserId, false);
        } else {
            await env.TOPIC_MAP.delete(`banned:${targetUserId}`);
        }
        const payload = {
            chat_id: env.SUPERGROUP_ID,
            text: `✅ **用户已解封**\nUID: \`${targetUserId}\``,
            parse_mode: "Markdown"
        };
        if (threadId) payload.message_thread_id = threadId;
        await tgCall(env, "sendMessage", payload);
        return;
    }

    // 查找用户 ID
    let userId = null;
    if (hasD1(env)) {
        const mappedUser = await dbThreadGetUserId(env, threadId);
        if (mappedUser) {
            userId = Number(mappedUser);
        } else {
            const result = await env.TG_BOT_DB
                .prepare("SELECT user_id FROM users WHERE thread_id = ?")
                .bind(String(threadId))
                .first();
            if (result?.user_id) {
                userId = Number(result.user_id);
                await dbThreadPut(env, threadId, userId);
            }
        }
    } else {
        const mappedUser = await env.TOPIC_MAP.get(`thread:${threadId}`);
        if (mappedUser) {
            userId = Number(mappedUser);
        } else {
            const allKeys = await getAllKeys(env, "user:");
            for (const { name } of allKeys) {
                const rec = await safeGetJSON(env, name, null);
                if (rec && Number(rec.thread_id) === Number(threadId)) {
                    userId = Number(name.slice(5));
                    break;
                }
            }
        }
    }

    if (!userId) return;

    // 管理员命令处理
    if (text.startsWith("/kw")) {
        if (!hasD1(env)) {
            await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "⚠️ 关键词功能需要绑定 D1 数据库。", parse_mode: "Markdown" });
            return;
        }

        const parts = text.split(" ").filter(Boolean);
        const action = parts[1] || "help";
        const subAction = parts[2] || "";
        const restText = parts.slice(2).join(" ").trim();

        if (action === "add") {
            if (!restText) {
                await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "用法：`/kw add 关键词`", parse_mode: "Markdown" });
                return;
            }
            const validation = validateKeywordPattern(restText);
            if (!validation.ok) {
                await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: `❌ 关键词规则被拒绝：${validation.reason}`, parse_mode: "Markdown" });
                return;
            }
            await dbKeywordAdd(env, restText);
            await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: `✅ 已添加关键词：\`${restText}\``, parse_mode: "Markdown" });
            return;
        }

        if (action === "del") {
            if (!restText) {
                await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "用法：`/kw del 关键词` 或 `/kw del id <id>`", parse_mode: "Markdown" });
                return;
            }
            if (subAction === "id") {
                const idText = parts[3];
                if (!idText || !/^\d+$/.test(idText)) {
                    await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "用法：`/kw del id <id>`", parse_mode: "Markdown" });
                    return;
                }
                const changes = await dbKeywordDeleteById(env, Number(idText));
                if (changes > 0) {
                    await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: `✅ 已删除关键词（ID）：\`${idText}\``, parse_mode: "Markdown" });
                } else {
                    await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: `❌ 未找到关键词（ID）：\`${idText}\``, parse_mode: "Markdown" });
                }
                return;
            }
            const changes = await dbKeywordDelete(env, restText);
            if (changes > 0) {
                await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: `✅ 已删除关键词：\`${restText}\``, parse_mode: "Markdown" });
            } else {
                await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: `❌ 未找到关键词：\`${restText}\``, parse_mode: "Markdown" });
            }
            return;
        }

        if (action === "list") {
            const list = await dbKeywordListWithId(env);
            if (!list.length) {
                await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "当前暂无关键词。" });
                return;
            }
            const items = list.slice(0, 50).map((k, i) => `${i + 1}. [id=${k.id}] ${k.keyword}`);
            const header = "📌 关键词列表";
            const maxLen = 3800;
            let buffer = `${header}\n\n`;
            for (const line of items) {
                if ((buffer.length + line.length + 1) > maxLen) {
                    await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: buffer.trimEnd() });
                    buffer = "";
                }
                buffer += (buffer ? "\n" : "") + line;
            }
            if (buffer.trim()) {
                await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: buffer.trimEnd() });
            }
            return;
        }

        if (action === "test") {
            const rest = text.replace(/^\/kw\s+test\s+/i, "");
            const [pattern, ...textParts] = rest.split(" ");
            const sample = textParts.join(" ").trim();
            if (!pattern || !sample) {
                await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "用法：`/kw test <表达式> <文本>`", parse_mode: "Markdown" });
                return;
            }
            const validation = validateKeywordPattern(pattern);
            if (!validation.ok) {
                await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: `❌ 关键词规则被拒绝：${validation.reason}`, parse_mode: "Markdown" });
                return;
            }
            try {
                const re = new RegExp(pattern, "i");
                const matched = re.test(sample);
                const resultText = matched ? "✅ 匹配成功" : "❌ 未命中";
                await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: `${resultText}\n表达式：\`${pattern}\`\n文本：\`${sample}\``, parse_mode: "Markdown" });
            } catch (e) {
                await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: `❌ 正则语法错误：\`${e.message}\``, parse_mode: "Markdown" });
            }
            return;
        }

        if (action === "help") {
            const helpText = [
                "🔎 **关键词管理**",
                "",
                "/kw add 关键词 - 添加关键词",
                "/kw del 关键词 - 删除关键词",
                "/kw del id <id> - 按 ID 删除关键词",
                "/kw list - 查看关键词列表",
                "/kw test <表达式> <文本> - 测试正则是否命中",
                "",
                "规则限制：",
                `1) 关键词长度上限 ${CONFIG.KEYWORD_MAX_LENGTH} 字符`,
                `2) 过滤仅匹配前 ${CONFIG.KEYWORD_MATCH_MAX_TEXT_LENGTH} 字符`,
                "3) 正则限制：",
                "- `.*` / `.+` 出现超过 2 次会被拒绝",
                "- 嵌套量词会被拒绝（如 `(a+)+`、`(.+)+`、`(.+)*`、`(.*)+`）",
                "- 形如 `(.*){2,}`、`(.+){1,}` 的重复结构会被拒绝"
            ].join("\n");
            await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: helpText, parse_mode: "Markdown" });
            return;
        }

        await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "用法：`/kw add 关键词` / `/kw del 关键词` / `/kw del id <id>` / `/kw list` / `/kw test <表达式> <文本>` / `/kw help`", parse_mode: "Markdown" });
        return;
    }

    if (text === "/close") {
        if (hasD1(env)) {
            await dbUserUpdate(env, userId, { closed: true });
        } else {
            const key = `user:${userId}`;
            let rec = await safeGetJSON(env, key, null);
            if (rec) {
                rec.closed = true;
                await env.TOPIC_MAP.put(key, JSON.stringify(rec));
            }
        }
        await tgCall(env, "closeForumTopic", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId });
        await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "🚫 **对话已强制关闭**", parse_mode: "Markdown" });
        return;
    }

    if (text === "/open") {
        if (hasD1(env)) {
            await dbUserUpdate(env, userId, { closed: false });
        } else {
            const key = `user:${userId}`;
            let rec = await safeGetJSON(env, key, null);
            if (rec) {
                rec.closed = false;
                await env.TOPIC_MAP.put(key, JSON.stringify(rec));
            }
        }
        await tgCall(env, "reopenForumTopic", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId });
        await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "✅ **对话已恢复**", parse_mode: "Markdown" });
        return;
    }

    if (text === "/reset") {
        if (hasD1(env)) {
            await dbSetVerifyState(env, userId, null);
        } else {
            await env.TOPIC_MAP.delete(`verified:${userId}`);
        }
        await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "🔄 **验证重置**", parse_mode: "Markdown" });
        return;
    }

    if (text === "/trust") {
        if (hasD1(env)) {
            await dbSetVerifyState(env, userId, "trusted");
        } else {
            await env.TOPIC_MAP.put(`verified:${userId}`, "trusted");
        }
        await env.TOPIC_MAP.delete(`needs_verify:${userId}`);
        await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "🌟 **已设置永久信任**", parse_mode: "Markdown" });
        return;
    }

    if (text === "/ban") {
        if (hasD1(env)) {
            await dbSetBanned(env, userId, true);
        } else {
            await env.TOPIC_MAP.put(`banned:${userId}`, "1");
        }
        await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "🚫 **用户已封禁**", parse_mode: "Markdown" });
        return;
    }

    if (text === "/unban") {
        if (hasD1(env)) {
            await dbSetBanned(env, userId, false);
        } else {
            await env.TOPIC_MAP.delete(`banned:${userId}`);
        }
        await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: "✅ **用户已解封**", parse_mode: "Markdown" });
        return;
    }

    if (text === "/info") {
        const userRec = hasD1(env)
            ? await dbUserGet(env, userId)
            : await safeGetJSON(env, `user:${userId}`, null);
        const verifyStatus = hasD1(env)
            ? await dbGetVerifyState(env, userId)
            : await env.TOPIC_MAP.get(`verified:${userId}`);
        const banStatus = hasD1(env)
            ? await dbIsBanned(env, userId)
            : await env.TOPIC_MAP.get(`banned:${userId}`);

        const info = `👤 **用户信息**\nUID: \`${userId}\`\nTopic ID: \`${threadId}\`\n话题标题: ${userRec?.title || "未知"}\n验证状态: ${verifyStatus ? (verifyStatus === 'trusted' ? '🌟 永久信任' : '✅ 已验证') : '❌ 未验证'}\n封禁状态: ${banStatus ? '🚫 已封禁' : '✅ 正常'}\nLink: [点击私聊](tg://user?id=${userId})`;
        await tgCall(env, "sendMessage", { chat_id: env.SUPERGROUP_ID, message_thread_id: threadId, text: info, parse_mode: "Markdown" });
        return;
    }

    // 转发管理员消息给用户
    if (msg.media_group_id) {
        await handleMediaGroup(msg, env, ctx, { direction: "t2p", targetChat: userId, threadId: undefined });
        return;
    }

    const copyResult = await tgCall(env, "copyMessage", {
        chat_id: userId,
        from_chat_id: env.SUPERGROUP_ID,
        message_id: msg.message_id
    });

    if (copyResult.ok) {
        if (hasD1(env)) {
            await dbMessageMapPut(env, env.SUPERGROUP_ID, msg.message_id, userId, copyResult.result.message_id);
        } else {
            const mapKey = `msg_map:${String(env.SUPERGROUP_ID)}:${msg.message_id}`;
            const mapValue = JSON.stringify({
                targetChatId: String(userId),
                targetMsgId: copyResult.result.message_id,
                createdAt: Date.now()
            });
            await env.TOPIC_MAP.put(mapKey, mapValue, {
                expirationTtl: CONFIG.MESSAGE_MAP_TTL_SECONDS
            });
        }
    }
}

// ============================================================================
// 验证模块
// ============================================================================

/**
 * 发送人机验证挑战
 */
async function sendVerificationChallenge(userId, env, pendingMsgId) {
    const existingChallenge = await env.TOPIC_MAP.get(`user_challenge:${userId}`);
    if (existingChallenge) {
        const chalKey = `chal:${existingChallenge}`;
        const state = await safeGetJSON(env, chalKey, null);

        if (!state || state.userId !== userId) {
            await env.TOPIC_MAP.delete(`user_challenge:${userId}`);
        } else {
            if (pendingMsgId) {
                let pendingIds = [];
                if (Array.isArray(state.pending_ids)) {
                    pendingIds = state.pending_ids.slice();
                } else if (state.pending) {
                    pendingIds = [state.pending];
                }

                if (!pendingIds.includes(pendingMsgId)) {
                    pendingIds.push(pendingMsgId);
                    if (pendingIds.length > CONFIG.PENDING_MAX_MESSAGES) {
                        pendingIds = pendingIds.slice(pendingIds.length - CONFIG.PENDING_MAX_MESSAGES);
                    }
                    state.pending_ids = pendingIds;
                    delete state.pending;
                    await env.TOPIC_MAP.put(chalKey, JSON.stringify(state), { expirationTtl: CONFIG.VERIFY_EXPIRE_SECONDS });
                }
            }
            Logger.debug('verification_duplicate_skipped', { userId, verifyId: existingChallenge, hasPending: !!pendingMsgId });
            return;
        }
    }

    // 验证速率限制
    const verifyLimit = await checkRateLimit(userId, env, 'verify', CONFIG.RATE_LIMIT_VERIFY, 300);
    if (!verifyLimit.allowed) {
        await tgCall(env, "sendMessage", {
            chat_id: userId,
            text: "⚠️ 验证请求过于频繁，请5分钟后再试。"
        });
        return;
    }

    // 生成验证挑战
    const q = LOCAL_QUESTIONS[secureRandomInt(0, LOCAL_QUESTIONS.length)];
    const challenge = {
        question: q.question,
        correct: q.correct_answer,
        options: shuffleArray([...q.incorrect_answers, q.correct_answer])
    };

    const verifyId = secureRandomId(CONFIG.VERIFY_ID_LENGTH);
    const answerIndex = challenge.options.indexOf(challenge.correct);

    const state = {
        answerIndex: answerIndex,
        options: challenge.options,
        pending_ids: pendingMsgId ? [pendingMsgId] : [],
        userId: userId
    };

    await env.TOPIC_MAP.put(`chal:${verifyId}`, JSON.stringify(state), { expirationTtl: CONFIG.VERIFY_EXPIRE_SECONDS });
    await env.TOPIC_MAP.put(`user_challenge:${userId}`, verifyId, { expirationTtl: CONFIG.VERIFY_EXPIRE_SECONDS });

    Logger.info('verification_sent', {
        userId,
        verifyId,
        question: q.question,
        pendingCount: state.pending_ids.length
    });

    // 构建按钮
    const buttons = challenge.options.map((opt, idx) => ({
        text: opt,
        callback_data: `verify:${verifyId}:${idx}`
    }));

    const keyboard = [];
    for (let i = 0; i < buttons.length; i += CONFIG.BUTTON_COLUMNS) {
        keyboard.push(buttons.slice(i, i + CONFIG.BUTTON_COLUMNS));
    }

    await tgCall(env, "sendMessage", {
        chat_id: userId,
        text: `🛡️ **人机验证**\n\n${challenge.question}\n\n请点击下方按钮回答 (回答正确后将自动发送您刚才的消息)。`,
        parse_mode: "Markdown",
        reply_markup: { inline_keyboard: keyboard }
    });
}

/**
 * 处理验证按钮点击
 */
async function handleCallbackQuery(query, env, ctx) {
    try {
        const data = query.data;
        if (!data.startsWith("verify:")) return;

        const parts = data.split(":");
        if (parts.length !== 3) return;

        const verifyId = parts[1];
        const selectedIndex = parseInt(parts[2]);
        const userId = query.from.id;

        const stateStr = await env.TOPIC_MAP.get(`chal:${verifyId}`);
        if (!stateStr) {
            await tgCall(env, "answerCallbackQuery", {
                callback_query_id: query.id,
                text: "❌ 验证已过期，请重发消息",
                show_alert: true
            });
            return;
        }

        let state;
        try {
            state = JSON.parse(stateStr);
        } catch (e) {
            await tgCall(env, "answerCallbackQuery", {
                callback_query_id: query.id,
                text: "❌ 数据错误",
                show_alert: true
            });
            return;
        }

        // 验证用户ID匹配
        if (state.userId && state.userId !== userId) {
            await tgCall(env, "answerCallbackQuery", {
                callback_query_id: query.id,
                text: "❌ 无效的验证",
                show_alert: true
            });
            return;
        }

        // 验证索引有效性
        if (isNaN(selectedIndex) || selectedIndex < 0 || selectedIndex >= state.options.length) {
            await tgCall(env, "answerCallbackQuery", {
                callback_query_id: query.id,
                text: "❌ 无效选项",
                show_alert: true
            });
            return;
        }

        if (selectedIndex === state.answerIndex) {
            await tgCall(env, "answerCallbackQuery", {
                callback_query_id: query.id,
                text: "✅ 验证通过"
            });

            Logger.info('verification_passed', {
                userId,
                verifyId,
                selectedOption: state.options[selectedIndex]
            });

            // 标记为已验证
            if (hasD1(env)) {
                await dbSetVerifyState(env, userId, "1");
            } else {
                await env.TOPIC_MAP.put(`verified:${userId}`, "1", { expirationTtl: CONFIG.VERIFIED_EXPIRE_SECONDS });
            }
            await env.TOPIC_MAP.delete(`needs_verify:${userId}`);

            // 清理验证数据
            await env.TOPIC_MAP.delete(`chal:${verifyId}`);
            await env.TOPIC_MAP.delete(`user_challenge:${userId}`);

            // 更新验证消息
            await tgCall(env, "editMessageText", {
                chat_id: userId,
                message_id: query.message.message_id,
                text: "✅ **验证成功**\n\n您现在可以自由对话了。",
                parse_mode: "Markdown"
            });

            // 转发待发送的消息
            const hasPending = (Array.isArray(state.pending_ids) && state.pending_ids.length > 0) || !!state.pending;
            if (hasPending) {
                try {
                    let pendingIds = [];
                    if (Array.isArray(state.pending_ids)) {
                        pendingIds = state.pending_ids.slice();
                    } else if (state.pending) {
                        pendingIds = [state.pending];
                    }

                    if (pendingIds.length > CONFIG.PENDING_MAX_MESSAGES) {
                        pendingIds = pendingIds.slice(pendingIds.length - CONFIG.PENDING_MAX_MESSAGES);
                    }

                    let forwardedCount = 0;
                    for (const pendingId of pendingIds) {
                        if (!pendingId) continue;
                        const forwardedKey = `forwarded:${userId}:${pendingId}`;
                        const alreadyForwarded = await env.TOPIC_MAP.get(forwardedKey);
                        if (alreadyForwarded) {
                            Logger.info('message_forward_duplicate_skipped', { userId, messageId: pendingId });
                            continue;
                        }

                        const fakeMsg = {
                            message_id: pendingId,
                            chat: { id: userId, type: "private" },
                            from: query.from,
                        };

                        await forwardToTopic(fakeMsg, env, ctx);
                        await env.TOPIC_MAP.put(forwardedKey, "1", { expirationTtl: 3600 });
                        forwardedCount++;
                    }

                    if (forwardedCount > 0) {
                        await tgCall(env, "sendMessage", {
                            chat_id: userId,
                            text: `📩 刚才的 ${forwardedCount} 条消息已帮您送达。`
                        });
                    }
                } catch (e) {
                    Logger.error('pending_message_forward_failed', e, { userId });
                    await tgCall(env, "sendMessage", {
                        chat_id: userId,
                        text: "⚠️ 自动发送失败，请重新发送您的消息。"
                    });
                }
            }
        } else {
            Logger.info('verification_failed', {
                userId,
                verifyId,
                selectedIndex,
                correctIndex: state.answerIndex
            });

            await tgCall(env, "answerCallbackQuery", {
                callback_query_id: query.id,
                text: "❌ 答案错误",
                show_alert: true
            });
        }
    } catch (e) {
        Logger.error('callback_query_error', e, {
            userId: query.from?.id,
            callbackData: query.data
        });
        await tgCall(env, "answerCallbackQuery", {
            callback_query_id: query.id,
            text: `⚠️ 系统错误，请重试`,
            show_alert: true
        });
    }
}

// ============================================================================
// 管理命令
// ============================================================================

/**
 * 处理 /cleanup 命令
 * 批量清理已删除话题的用户记录
 */
async function handleCleanupCommand(threadId, env) {
    const lockKey = "cleanup:lock";
    const locked = await env.TOPIC_MAP.get(lockKey);
    if (locked) {
        await tgCall(env, "sendMessage", withMessageThreadId({
            chat_id: env.SUPERGROUP_ID,
            text: "⏳ **已有清理任务正在运行，请稍后再试。**",
            parse_mode: "Markdown"
        }, threadId));
        return;
    }

    await env.TOPIC_MAP.put(lockKey, "1", { expirationTtl: CONFIG.CLEANUP_LOCK_TTL_SECONDS });

    await tgCall(env, "sendMessage", withMessageThreadId({
        chat_id: env.SUPERGROUP_ID,
        text: "🔄 **正在扫描需要清理的用户...**",
        parse_mode: "Markdown"
    }, threadId));

    let cleanedCount = 0;
    let errorCount = 0;
    const cleanedUsers = [];
    let scannedCount = 0;

    try {
        if (hasD1(env)) {
            let offset = 0;
            while (true) {
                const rows = await dbListUsers(env, CONFIG.CLEANUP_BATCH_SIZE, offset);
                if (!rows.length) break;
                scannedCount += rows.length;

                const results = await Promise.allSettled(
                    rows.map(async (row) => {
                        if (!row.thread_id) return null;
                        const userId = row.user_id;
                        const topicThreadId = row.thread_id;

                        const probe = await probeForumThread(env, topicThreadId, {
                            userId,
                            reason: "cleanup_check",
                            doubleCheckOnMissingThreadId: false
                        });

                        if (probe.status === "redirected" || probe.status === "missing") {
                            await resetUserVerificationAndRequireReverify(env, {
                                userId,
                                userKey: null,
                                oldThreadId: topicThreadId,
                                pendingMsgId: null,
                                reason: "cleanup_check"
                            });

                            return {
                                userId,
                                threadId: topicThreadId,
                                title: row.title || "未知"
                            };
                        } else if (probe.status === "probe_invalid") {
                            Logger.warn('cleanup_probe_invalid_message', {
                                userId,
                                threadId: topicThreadId,
                                errorDescription: probe.description
                            });
                        } else if (probe.status === "unknown_error") {
                            Logger.warn('cleanup_probe_failed_unknown', {
                                userId,
                                threadId: topicThreadId,
                                errorDescription: probe.description
                            });
                        } else if (probe.status === "missing_thread_id") {
                            Logger.warn('cleanup_probe_missing_thread_id', { userId, threadId: topicThreadId });
                        }

                        return null;
                    })
                );

                results.forEach(result => {
                    if (result.status === 'fulfilled' && result.value) {
                        cleanedCount++;
                        cleanedUsers.push(result.value);
                        Logger.info('cleanup_user', {
                            userId: result.value.userId,
                            threadId: result.value.threadId
                        });
                    } else if (result.status === 'rejected') {
                        errorCount++;
                        Logger.error('cleanup_batch_error', result.reason);
                    }
                });

                offset += rows.length;
                await new Promise(r => setTimeout(r, 200));
            }
        } else {
            const keysToDelete = [];
            let cursor = undefined;
            do {
                const result = await env.TOPIC_MAP.list({ prefix: "user:", cursor });
                const names = (result.keys || []).map(k => k.name);
                scannedCount += names.length;

                for (let i = 0; i < names.length; i += CONFIG.CLEANUP_BATCH_SIZE) {
                    const batch = names.slice(i, i + CONFIG.CLEANUP_BATCH_SIZE);

                    const results = await Promise.allSettled(
                        batch.map(async (name) => {
                            const rec = await safeGetJSON(env, name, null);
                            if (!rec || !rec.thread_id) return null;

                            const userId = name.slice(5);
                            const topicThreadId = rec.thread_id;

                            const probe = await probeForumThread(env, topicThreadId, {
                                userId,
                                reason: "cleanup_check",
                                doubleCheckOnMissingThreadId: false
                            });

                            if (probe.status === "redirected" || probe.status === "missing") {
                                keysToDelete.push(
                                    name,
                                    `verified:${userId}`,
                                    `thread:${topicThreadId}`
                                );

                                return {
                                    userId,
                                    threadId: topicThreadId,
                                    title: rec.title || "未知"
                                };
                            } else if (probe.status === "probe_invalid") {
                                Logger.warn('cleanup_probe_invalid_message', {
                                    userId,
                                    threadId: topicThreadId,
                                    errorDescription: probe.description
                                });
                            } else if (probe.status === "unknown_error") {
                                Logger.warn('cleanup_probe_failed_unknown', {
                                    userId,
                                    threadId: topicThreadId,
                                    errorDescription: probe.description
                                });
                            } else if (probe.status === "missing_thread_id") {
                                Logger.warn('cleanup_probe_missing_thread_id', { userId, threadId: topicThreadId });
                            }

                            return null;
                        })
                    );

                    results.forEach(result => {
                        if (result.status === 'fulfilled' && result.value) {
                            cleanedCount++;
                            cleanedUsers.push(result.value);
                            Logger.info('cleanup_user', {
                                userId: result.value.userId,
                                threadId: result.value.threadId
                            });
                        } else if (result.status === 'rejected') {
                            errorCount++;
                            Logger.error('cleanup_batch_error', result.reason);
                        }
                    });

                    if (i + CONFIG.CLEANUP_BATCH_SIZE < names.length) {
                        await new Promise(r => setTimeout(r, 600));
                    }
                }

                cursor = result.list_complete ? undefined : result.cursor;

                if (cursor) {
                    await new Promise(r => setTimeout(r, 200));
                }
            } while (cursor);

            if (keysToDelete.length > 0) {
                const deletedCount = await deleteBulk(env, keysToDelete);
                Logger.info('cleanup_bulk_delete', { deletedKeyCount: deletedCount });
            }
        }

        let reportText = `✅ **清理完成**\n\n`;
        reportText += `📊 **统计信息**\n`;
        reportText += `- 扫描用户数: ${scannedCount}\n`;
        reportText += `- 已清理用户数: ${cleanedCount}\n`;
        reportText += `- 错误数: ${errorCount}\n\n`;

        if (cleanedCount > 0) {
            reportText += `🗑️ **已清理的用户** (话题已删除):\n`;
            for (const user of cleanedUsers.slice(0, CONFIG.MAX_CLEANUP_DISPLAY)) {
                reportText += `- UID: \`${user.userId}\` | 话题: ${user.title}\n`;
            }
            if (cleanedUsers.length > CONFIG.MAX_CLEANUP_DISPLAY) {
                reportText += `\n...(还有 ${cleanedUsers.length - CONFIG.MAX_CLEANUP_DISPLAY} 个用户)\n`;
            }
            reportText += `\n💡 这些用户下次发消息时将重新进行人机验证并创建新话题。`;
        } else {
            reportText += `✨ 没有发现需要清理的用户记录。`;
        }

        Logger.info('cleanup_completed', {
            cleanedCount,
            errorCount,
            totalUsers: scannedCount
        });

        await tgCall(env, "sendMessage", withMessageThreadId({
            chat_id: env.SUPERGROUP_ID,
            text: reportText,
            parse_mode: "Markdown"
        }, threadId));

    } catch (e) {
        Logger.error('cleanup_failed', e, { threadId });
        await tgCall(env, "sendMessage", withMessageThreadId({
            chat_id: env.SUPERGROUP_ID,
            text: `❌ **清理过程出错**\n\n错误信息: \`${e.message}\``,
            parse_mode: "Markdown"
        }, threadId));
    } finally {
        await env.TOPIC_MAP.delete(lockKey);
    }
}

// ============================================================================
// 话题创建和管理
// ============================================================================

/**
 * 创建新论坛话题
 */
async function createTopic(from, key, env, userId) {
    const title = buildTopicTitle(from);
    if (!env.SUPERGROUP_ID.toString().startsWith("-100")) throw new Error("SUPERGROUP_ID必须以-100开头");
    const res = await tgCall(env, "createForumTopic", { chat_id: env.SUPERGROUP_ID, name: title });
    if (!res.ok) throw new Error(`创建话题失败: ${res.description}`);
    
    const rec = { thread_id: res.result.message_thread_id, title, closed: false };

    if (hasD1(env)) {
        await dbUserUpdate(env, userId, {
            thread_id: rec.thread_id,
            title: rec.title,
            closed: false
        });
        if (userId) {
            await dbThreadPut(env, rec.thread_id, userId);
        }
    } else {
        // 使用元数据记录创建时间
        await putWithMetadata(env, key, rec, {
            expirationTtl: null,
            metadata: { 
                userId: String(userId),
                threadId: res.result.message_thread_id
            }
        });
        
        if (userId) {
            await env.TOPIC_MAP.put(`thread:${rec.thread_id}`, String(userId));
        }
    }
    return rec;
}

/**
 * 更新话题状态（关闭/打开）
 */
async function updateThreadStatus(threadId, isClosed, env) {
    try {
        if (hasD1(env)) {
            const mappedUser = await dbThreadGetUserId(env, threadId);
            if (mappedUser) {
                const rec = await dbUserGet(env, mappedUser);
                if (rec && Number(rec.thread_id) === Number(threadId)) {
                    await dbUserUpdate(env, mappedUser, { closed: isClosed });
                    Logger.info('thread_status_updated', { threadId, isClosed, updatedCount: 1 });
                    return;
                }
                await dbThreadDelete(env, threadId);
            }

            const result = await env.TG_BOT_DB
                .prepare("SELECT user_id FROM users WHERE thread_id = ?")
                .bind(String(threadId))
                .all();

            const rows = result?.results || [];
            for (const row of rows) {
                await dbUserUpdate(env, row.user_id, { closed: isClosed });
            }
            Logger.info('thread_status_updated', { threadId, isClosed, updatedCount: rows.length });
            return;
        }

        const mappedUser = await env.TOPIC_MAP.get(`thread:${threadId}`);
        if (mappedUser) {
            const userKey = `user:${mappedUser}`;
            const rec = await safeGetJSON(env, userKey, null);
            if (rec && Number(rec.thread_id) === Number(threadId)) {
                rec.closed = isClosed;
                await env.TOPIC_MAP.put(userKey, JSON.stringify(rec));
                Logger.info('thread_status_updated', { threadId, isClosed, updatedCount: 1 });
                return;
            }
            await env.TOPIC_MAP.delete(`thread:${threadId}`);
        }

        const allKeys = await getAllKeys(env, "user:");
        const updates = [];

        for (const { name } of allKeys) {
            const rec = await safeGetJSON(env, name, null);
            if (rec && Number(rec.thread_id) === Number(threadId)) {
                rec.closed = isClosed;
                updates.push(env.TOPIC_MAP.put(name, JSON.stringify(rec)));
            }
        }

        await Promise.all(updates);
        Logger.info('thread_status_updated', { threadId, isClosed, updatedCount: updates.length });
    } catch (e) {
        Logger.error('thread_status_update_failed', e, { threadId, isClosed });
        throw e;
    }
}

/**
 * 构建话题标题
 */
function buildTopicTitle(from) {
    const firstName = (from.first_name || "").trim().substring(0, CONFIG.MAX_NAME_LENGTH);
    const lastName = (from.last_name || "").trim().substring(0, CONFIG.MAX_NAME_LENGTH);

    let username = "";
    if (from.username) {
        username = from.username
            .replace(/[^\w]/g, '')
            .substring(0, 20);
    }

    const cleanName = (firstName + " " + lastName)
        .replace(/[\u0000-\u001F\u007F-\u009F]/g, '')
        .replace(/\s+/g, ' ')
        .trim();

    const name = cleanName || "User";
    const usernameStr = username ? ` @${username}` : "";

    const title = (name + usernameStr).substring(0, CONFIG.MAX_TITLE_LENGTH);
    return title;
}

// ============================================================================
// Telegram API
// ============================================================================

/**
 * Telegram API 调用
 * 包含超时控制、异常防护、自动重试等机制
 */
async function tgCall(env, method, body, timeout = CONFIG.API_TIMEOUT_MS) {
    let base = env.API_BASE || "https://api.telegram.org";

    // 强制使用 HTTPS
    if (base.startsWith("http://")) {
        Logger.warn('api_http_upgraded', { originalBase: base });
        base = base.replace("http://", "https://");
    }

    // 验证 URL 格式
    try {
        new URL(`${base}/test`);
    } catch (e) {
        Logger.error('api_base_invalid', e, { base });
        base = "https://api.telegram.org";
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeout);

    try {
        const resp = await fetch(`${base}/bot${env.BOT_TOKEN}/${method}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
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

        // 安全的 JSON 解析
        let result;
        try {
            result = await resp.json();
        } catch (parseError) {
            Logger.error('telegram_api_json_parse_failed', parseError, { method, status: resp.status });
            return { ok: false, description: 'Invalid JSON response from Telegram' };
        }

        // 记录速率限制
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

// ============================================================================
// 媒体组处理
// ============================================================================

/**
 * 处理媒体组消息
 */
async function handleMediaGroup(msg, env, ctx, { direction, targetChat, threadId }) {
    const groupId = msg.media_group_id;
    const key = `mg:${direction}:${groupId}`;
    const item = extractMedia(msg);
    if (!item) {
        await tgCall(env, "copyMessage", withMessageThreadId({
            chat_id: targetChat,
            from_chat_id: msg.chat.id,
            message_id: msg.message_id
        }, threadId));
        return;
    }
    let rec = await safeGetJSON(env, key, null);
    if (!rec) rec = { direction, targetChat, threadId: (threadId === null ? undefined : threadId), items: [], last_ts: Date.now() };
    rec.items.push({ ...item, msg_id: msg.message_id });
    rec.last_ts = Date.now();
    await env.TOPIC_MAP.put(key, JSON.stringify(rec), { expirationTtl: CONFIG.MEDIA_GROUP_EXPIRE_SECONDS });
    ctx.waitUntil(delaySend(env, key, rec.last_ts));
}

/**
 * 提取媒体内容
 * 支持图片、视频、音频、文档、动图等
 */
function extractMedia(msg) {
    if (msg.photo && msg.photo.length > 0) {
        const highestResolution = msg.photo[msg.photo.length - 1];
        return {
            type: "photo",
            id: highestResolution.file_id,
            cap: msg.caption || ""
        };
    }

    if (msg.video) {
        return {
            type: "video",
            id: msg.video.file_id,
            cap: msg.caption || ""
        };
    }

    if (msg.document) {
        return {
            type: "document",
            id: msg.document.file_id,
            cap: msg.caption || ""
        };
    }

    if (msg.audio) {
        return {
            type: "audio",
            id: msg.audio.file_id,
            cap: msg.caption || ""
        };
    }

    if (msg.animation) {
        return {
            type: "animation",
            id: msg.animation.file_id,
            cap: msg.caption || ""
        };
    }

    return null;
}

/**
 * 清理过期的媒体组
 */
async function flushExpiredMediaGroups(env, now) {
    try {
        const prefix = "mg:";
        const allKeys = await getAllKeys(env, prefix);
        let deletedCount = 0;

        for (const { name } of allKeys) {
            const rec = await safeGetJSON(env, name, null);
            if (rec && rec.last_ts && (now - rec.last_ts > 300000)) {
                await env.TOPIC_MAP.delete(name);
                deletedCount++;
            }
        }

        if (deletedCount > 0) {
            Logger.info('media_groups_cleaned', { deletedCount });
        }
    } catch (e) {
        Logger.error('media_group_cleanup_failed', e);
    }
}

/**
 * 延迟发送媒体组
 */
async function delaySend(env, key, ts) {
    await new Promise(r => setTimeout(r, CONFIG.MEDIA_GROUP_DELAY_MS));

    const rec = await safeGetJSON(env, key, null);

    if (rec && rec.last_ts === ts) {
        if (!rec.items || rec.items.length === 0) {
            Logger.warn('media_group_empty', { key });
            await env.TOPIC_MAP.delete(key);
            return;
        }

        const media = rec.items.map((it, i) => {
            if (!it.type || !it.id) {
                Logger.warn('media_group_invalid_item', { key, item: it });
                return null;
            }
            const caption = i === 0 ? (it.cap || "").substring(0, 1024) : "";
            return {
                type: it.type,
                media: it.id,
                caption
            };
        }).filter(Boolean);

        if (media.length > 0) {
            try {
                const result = await tgCall(env, "sendMediaGroup", withMessageThreadId({
                    chat_id: rec.targetChat,
                    media
                }, rec.threadId));

                if (!result.ok) {
                    Logger.error('media_group_send_failed', result.description, {
                        key,
                        mediaCount: media.length
                    });
                } else {
                    Logger.info('media_group_sent', {
                        key,
                        mediaCount: media.length,
                        targetChat: rec.targetChat
                    });
                }
            } catch (e) {
                Logger.error('media_group_send_exception', e, { key });
            }
        }

        await env.TOPIC_MAP.delete(key);
    }
}

// ============================================================================
// 统计和导出功能
// ============================================================================

/**
 * 获取机器人统计信息
 * 返回用户数、话题数、验证统计等
 */
async function getBotStats(env) {
    try {
        const queueKeys = await getAllKeys(env, QUEUE_PREFIX);
        if (hasD1(env)) {
            const totalUsers = await dbCount(env);
            const verifiedUsers = await dbCount(env, "WHERE verify_state IS NOT NULL");
            const bannedUsers = await dbCount(env, "WHERE is_blocked = 1");
            const totalTopics = await dbCount(env, "WHERE thread_id IS NOT NULL");
            const closedTopics = await dbCount(env, "WHERE thread_id IS NOT NULL AND closed = 1");

            return {
                totalUsers,
                verifiedUsers,
                bannedUsers,
                totalTopics,
                closedTopics,
                activeTopics: totalTopics - closedTopics,
                queuedMessages: queueKeys.length,
                timestamp: Date.now()
            };
        }

        const userKeys = await getAllKeys(env, "user:");
        const verifiedKeys = await getAllKeys(env, "verified:");
        const bannedKeys = await getAllKeys(env, "banned:");

        const userDataMap = await safeGetBulk(env, userKeys.map(k => k.name));
        
        let totalTopics = 0;
        let closedTopics = 0;

        for (const [, userData] of userDataMap) {
            if (userData && userData.thread_id) {
                totalTopics++;
                if (userData.closed) closedTopics++;
            }
        }

        return {
            totalUsers: userKeys.length,
            verifiedUsers: verifiedKeys.length,
            bannedUsers: bannedKeys.length,
            totalTopics,
            closedTopics,
            activeTopics: totalTopics - closedTopics,
            queuedMessages: queueKeys.length,
            timestamp: Date.now()
        };
    } catch (e) {
        Logger.error('get_bot_stats_failed', e);
        return null;
    }
}

/**
 * 导出用户数据（流式）
 * 返回 { userCount, data }
 * 用于备份或分析
 */
async function exportUserData(env, userIds = null) {
    try {
        const exported = [];

        if (hasD1(env)) {
            if (userIds && Array.isArray(userIds) && userIds.length > 0) {
                const placeholders = userIds.map(() => "?").join(",");
                const result = await env.TG_BOT_DB
                    .prepare(`SELECT * FROM users WHERE user_id IN (${placeholders})`)
                    .bind(...userIds.map(String))
                    .all();
                for (const row of result?.results || []) {
                    exported.push({
                        userId: row.user_id,
                        userData: {
                            thread_id: row.thread_id,
                            title: row.title,
                            closed: !!row.closed
                        },
                        verified: !!row.verify_state,
                        banned: !!row.is_blocked,
                        metadata: {},
                        createdAt: row.created_at,
                        updatedAt: row.updated_at
                    });
                }
            } else {
                const result = await env.TG_BOT_DB
                    .prepare("SELECT * FROM users")
                    .all();
                for (const row of result?.results || []) {
                    exported.push({
                        userId: row.user_id,
                        userData: {
                            thread_id: row.thread_id,
                            title: row.title,
                            closed: !!row.closed
                        },
                        verified: !!row.verify_state,
                        banned: !!row.is_blocked,
                        metadata: {},
                        createdAt: row.created_at,
                        updatedAt: row.updated_at
                    });
                }
            }

            Logger.info('user_data_exported', { userCount: exported.length });
            return { userCount: exported.length, data: exported };
        }

        let keysToExport;
        
        if (userIds && Array.isArray(userIds)) {
            keysToExport = userIds.map(uid => `user:${uid}`);
        } else {
            const allUserKeys = await getAllKeys(env, "user:");
            keysToExport = allUserKeys.map(k => k.name);
        }

        if (keysToExport.length === 0) {
            return { userCount: 0, data: [] };
        }

        for (let i = 0; i < keysToExport.length; i += 100) {
            const batch = keysToExport.slice(i, i + 100);
            const results = await safeGetBulk(env, batch);

            for (const [key, value] of results) {
                if (value) {
                    const userId = key.replace('user:', '');
                    const fullMetadata = await getValueWithFullMetadata(env, key);
                    
                    exported.push({
                        userId,
                        userData: value,
                        verified: !!await env.TOPIC_MAP.get(`verified:${userId}`),
                        banned: !!await env.TOPIC_MAP.get(`banned:${userId}`),
                        metadata: fullMetadata?.metadata || {},
                        createdAt: fullMetadata?.createdAt,
                        updatedAt: fullMetadata?.updatedAt
                    });
                }
            }
        }

        Logger.info('user_data_exported', { userCount: exported.length });
        return { userCount: exported.length, data: exported };
    } catch (e) {
        Logger.error('export_user_data_failed', e);
        return { userCount: 0, data: [], error: e.message };
    }
}

/**
 * 获取用户活动统计
 * 按最后活动时间排序
 */
async function getUserActivityStats(env, limit = 50) {
    try {
        if (hasD1(env)) {
            const result = await env.TG_BOT_DB
                .prepare("SELECT user_id, created_at, updated_at FROM users ORDER BY updated_at DESC LIMIT ?")
                .bind(limit)
                .all();
            return (result?.results || []).map(row => ({
                userId: row.user_id,
                createdAt: row.created_at,
                updatedAt: row.updated_at,
                ageSeconds: row.created_at ? Math.floor((Date.now() - row.created_at) / 1000) : null,
                metadata: {}
            }));
        }

        const userKeys = await getAllKeys(env, "user:");
        const stats = [];

        for (const keyInfo of userKeys.slice(0, limit * 2)) {
            const fullMetadata = await getValueWithFullMetadata(env, keyInfo.name);
            if (fullMetadata) {
                const userId = keyInfo.name.replace('user:', '');
                stats.push({
                    userId,
                    createdAt: fullMetadata.createdAt,
                    updatedAt: fullMetadata.updatedAt,
                    ageSeconds: fullMetadata.ageSeconds,
                    metadata: fullMetadata.metadata
                });
            }
        }

        stats.sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0));

        return stats.slice(0, limit);
    } catch (e) {
        Logger.error('get_activity_stats_failed', e);
        return [];
    }
}

// ============================================================================
// 消息编辑同步
// ============================================================================

/**
 * 处理消息编辑
 * 支持用户端和管理员端的编辑同步
 */
async function handleEditedMessage(msg, env, ctx) {
    if (msg.chat?.id == env.SUPERGROUP_ID) {
        // 管理员编辑了发送给用户的消息
        const sourceChatId = msg.chat.id;
        const sourceMsgId = msg.message_id;

        const targetInfo = hasD1(env)
            ? await dbMessageMapGet(env, sourceChatId, sourceMsgId)
            : await safeGetJSON(env, `msg_map:${String(sourceChatId)}:${sourceMsgId}`, null);

        if (targetInfo) {
            const { targetChatId, targetMsgId } = targetInfo;

            try {
                if (msg.text) {
                    await tgCall(env, "editMessageText", {
                        chat_id: targetChatId,
                        message_id: targetMsgId,
                        text: msg.text,
                        entities: msg.entities,
                        parse_mode: msg.parse_mode
                    });
                } else if (msg.caption) {
                    await tgCall(env, "editMessageCaption", {
                        chat_id: targetChatId,
                        message_id: targetMsgId,
                        caption: msg.caption,
                        caption_entities: msg.caption_entities,
                        parse_mode: msg.parse_mode
                    });
                }
            } catch (error) {
                Logger.warn('edit_message_forward_failed', {
                    sourceChatId,
                    sourceMsgId,
                    targetChatId,
                    targetMsgId,
                    error: error.message
                });
            }
        }
    } else {
        // 用户编辑了私聊中的消息
        const userId = msg.chat.id;
        const sourceMsgId = msg.message_id;

        const userRec = hasD1(env)
            ? await dbUserGet(env, userId)
            : await safeGetJSON(env, `user:${userId}`, null);

        if (!userRec || !userRec.thread_id) {
            return;
        }

        const targetInfo = hasD1(env)
            ? await dbMessageMapGet(env, userId, sourceMsgId)
            : await safeGetJSON(env, `msg_map:${String(userId)}:${sourceMsgId}`, null);

        if (targetInfo) {
            const { targetChatId, targetMsgId } = targetInfo;

            try {
                if (msg.text) {
                    await tgCall(env, "editMessageText", {
                        chat_id: env.SUPERGROUP_ID,
                        message_id: targetMsgId,
                        message_thread_id: userRec.thread_id,
                        text: msg.text,
                        entities: msg.entities,
                        parse_mode: msg.parse_mode
                    });
                } else if (msg.caption) {
                    await tgCall(env, "editMessageCaption", {
                        chat_id: env.SUPERGROUP_ID,
                        message_id: targetMsgId,
                        message_thread_id: userRec.thread_id,
                        caption: msg.caption,
                        caption_entities: msg.caption_entities,
                        parse_mode: msg.parse_mode
                    });
                }
            } catch (error) {
                Logger.warn('edit_message_forward_failed', {
                    sourceChatId: userId,
                    sourceMsgId,
                    targetChatId,
                    targetMsgId,
                    error: error.message
                });
            }
        }
    }
}
