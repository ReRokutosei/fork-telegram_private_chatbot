const QUEUE_PREFIX = 'queue:';
const QUEUE_TTL = 86400;

export async function enqueueFailedMessageImpl(env, userId, message, reason, deps) {
    const { secureRandomId, putWithMetadata, Logger } = deps;

    try {
        const queueKey = `${QUEUE_PREFIX}${userId}:${Date.now()}:${secureRandomId(6)}`;

        const queueItem = {
            userId: String(userId),
            messageId: message.message_id,
            from: message.from?.id || userId,
            text: message.text || message.caption || '',
            timestamp: Date.now(),
            reason,
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

export async function processMessageQueueImpl(env, ctx, deps) {
    const { getAllKeys, safeGetJSON, deleteBulk, Logger } = deps;

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
                Logger.info('queue_item_retry', {
                    userId: queueItem.userId,
                    retryCount: queueItem.retryCount
                });

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
