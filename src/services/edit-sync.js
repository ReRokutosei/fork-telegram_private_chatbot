export async function handleEditedMessageImpl({
    msg,
    env,
    hasD1,
    dbMessageMapGet,
    safeGetJSON,
    dbUserGet,
    tgCall,
    Logger
}) {
    if (msg.chat?.id == env.SUPERGROUP_ID) {
        const sourceChatId = msg.chat.id;
        const sourceMsgId = msg.message_id;

        const targetInfo = hasD1(env)
            ? await dbMessageMapGet(env, sourceChatId, sourceMsgId)
            : await safeGetJSON(env, `msg_map:${String(sourceChatId)}:${sourceMsgId}`, null);

        if (targetInfo) {
            const { targetChatId, targetMsgId } = targetInfo;

            try {
                if (msg.text) {
                    await tgCall(env, 'editMessageText', {
                        chat_id: targetChatId,
                        message_id: targetMsgId,
                        text: msg.text,
                        entities: msg.entities,
                        parse_mode: msg.parse_mode
                    });
                } else if (msg.caption) {
                    await tgCall(env, 'editMessageCaption', {
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
                    await tgCall(env, 'editMessageText', {
                        chat_id: env.SUPERGROUP_ID,
                        message_id: targetMsgId,
                        message_thread_id: userRec.thread_id,
                        text: msg.text,
                        entities: msg.entities,
                        parse_mode: msg.parse_mode
                    });
                } else if (msg.caption) {
                    await tgCall(env, 'editMessageCaption', {
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
