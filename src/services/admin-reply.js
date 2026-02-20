export async function handleAdminReplyImpl(msg, env, ctx, deps) {
    const { isAdminUser, hasD1, dbKeywordListWithId, tgCall, dbSetBanned, dbThreadGetUserId, dbThreadPut, getAllKeys, safeGetJSON, dbKeywordAdd, dbKeywordDelete, dbKeywordDeleteById, validateKeywordPattern, CONFIG, dbUserUpdate, dbSetVerifyState, dbUserGet, dbGetVerifyState, dbIsBanned, handleMediaGroup, dbMessageMapPut, handleCleanupCommand } = deps;

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
