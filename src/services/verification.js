export async function sendVerificationChallengeImpl({
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
}) {
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

    const verifyLimit = await checkRateLimit(userId, env, 'verify', CONFIG.RATE_LIMIT_VERIFY, 300);
    if (!verifyLimit.allowed) {
        await tgCall(env, 'sendMessage', {
            chat_id: userId,
            text: '⚠️ 验证请求过于频繁，请5分钟后再试。'
        });
        return;
    }

    const q = LOCAL_QUESTIONS[secureRandomInt(0, LOCAL_QUESTIONS.length)];
    const challenge = {
        question: q.question,
        correct: q.correct_answer,
        options: shuffleArray([...q.incorrect_answers, q.correct_answer])
    };

    const verifyId = secureRandomId(CONFIG.VERIFY_ID_LENGTH);
    const answerIndex = challenge.options.indexOf(challenge.correct);

    const state = {
        answerIndex,
        options: challenge.options,
        pending_ids: pendingMsgId ? [pendingMsgId] : [],
        userId
    };

    await env.TOPIC_MAP.put(`chal:${verifyId}`, JSON.stringify(state), { expirationTtl: CONFIG.VERIFY_EXPIRE_SECONDS });
    await env.TOPIC_MAP.put(`user_challenge:${userId}`, verifyId, { expirationTtl: CONFIG.VERIFY_EXPIRE_SECONDS });

    Logger.info('verification_sent', {
        userId,
        verifyId,
        question: q.question,
        pendingCount: state.pending_ids.length
    });

    const buttons = challenge.options.map((opt, idx) => ({
        text: opt,
        callback_data: `verify:${verifyId}:${idx}`
    }));

    const keyboard = [];
    for (let i = 0; i < buttons.length; i += CONFIG.BUTTON_COLUMNS) {
        keyboard.push(buttons.slice(i, i + CONFIG.BUTTON_COLUMNS));
    }

    await tgCall(env, 'sendMessage', {
        chat_id: userId,
        text: `🛡️ **人机验证**

${challenge.question}

请点击下方按钮回答 (回答正确后将自动发送您刚才的消息)。`,
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: keyboard }
    });
}

export async function handleCallbackQueryImpl({
    query,
    env,
    ctx,
    tgCall,
    Logger,
    hasD1,
    dbSetVerifyState,
    CONFIG,
    forwardToTopic
}) {
    try {
        const data = query.data;
        if (!data.startsWith('verify:')) return;

        const parts = data.split(':');
        if (parts.length !== 3) return;

        const verifyId = parts[1];
        const selectedIndex = parseInt(parts[2]);
        const userId = query.from.id;

        const stateStr = await env.TOPIC_MAP.get(`chal:${verifyId}`);
        if (!stateStr) {
            await tgCall(env, 'answerCallbackQuery', {
                callback_query_id: query.id,
                text: '❌ 验证已过期，请重发消息',
                show_alert: true
            });
            return;
        }

        let state;
        try {
            state = JSON.parse(stateStr);
        } catch {
            await tgCall(env, 'answerCallbackQuery', {
                callback_query_id: query.id,
                text: '❌ 数据错误',
                show_alert: true
            });
            return;
        }

        if (state.userId && state.userId !== userId) {
            await tgCall(env, 'answerCallbackQuery', {
                callback_query_id: query.id,
                text: '❌ 无效的验证',
                show_alert: true
            });
            return;
        }

        if (isNaN(selectedIndex) || selectedIndex < 0 || selectedIndex >= state.options.length) {
            await tgCall(env, 'answerCallbackQuery', {
                callback_query_id: query.id,
                text: '❌ 无效选项',
                show_alert: true
            });
            return;
        }

        if (selectedIndex === state.answerIndex) {
            await tgCall(env, 'answerCallbackQuery', {
                callback_query_id: query.id,
                text: '✅ 验证通过'
            });

            Logger.info('verification_passed', {
                userId,
                verifyId,
                selectedOption: state.options[selectedIndex]
            });

            if (hasD1(env)) {
                await dbSetVerifyState(env, userId, '1');
            } else {
                await env.TOPIC_MAP.put(`verified:${userId}`, '1', { expirationTtl: CONFIG.VERIFIED_EXPIRE_SECONDS });
            }
            await env.TOPIC_MAP.delete(`needs_verify:${userId}`);

            await env.TOPIC_MAP.delete(`chal:${verifyId}`);
            await env.TOPIC_MAP.delete(`user_challenge:${userId}`);

            await tgCall(env, 'editMessageText', {
                chat_id: userId,
                message_id: query.message.message_id,
                text: `✅ **验证成功**\n\n您现在可以自由对话了。`,
                parse_mode: 'Markdown'
            });

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
                            chat: { id: userId, type: 'private' },
                            from: query.from
                        };

                        await forwardToTopic(fakeMsg, env, ctx);
                        await env.TOPIC_MAP.put(forwardedKey, '1', { expirationTtl: 3600 });
                        forwardedCount++;
                    }

                    if (forwardedCount > 0) {
                        await tgCall(env, 'sendMessage', {
                            chat_id: userId,
                            text: `📩 刚才的 ${forwardedCount} 条消息已帮您送达。`
                        });
                    }
                } catch (e) {
                    Logger.error('pending_message_forward_failed', e, { userId });
                    await tgCall(env, 'sendMessage', {
                        chat_id: userId,
                        text: '⚠️ 自动发送失败，请重新发送您的消息。'
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

            await tgCall(env, 'answerCallbackQuery', {
                callback_query_id: query.id,
                text: '❌ 答案错误',
                show_alert: true
            });
        }
    } catch (e) {
        Logger.error('callback_query_error', e, {
            userId: query.from?.id,
            callbackData: query.data
        });
        await tgCall(env, 'answerCallbackQuery', {
            callback_query_id: query.id,
            text: '⚠️ 系统错误，请重试',
            show_alert: true
        });
    }
}
