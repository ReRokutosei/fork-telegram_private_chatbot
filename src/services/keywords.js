import { CONFIG } from '../config/constants.js';
import { Logger } from '../core/logger.js';
import { getKeywordListCached } from '../adapters/storage-d1.js';

export function getFilterText(msg) {
    if (msg.text) return String(msg.text);
    if (msg.caption) return String(msg.caption);
    return '';
}

export function validateKeywordPattern(raw) {
    const pattern = String(raw || '').trim();
    if (!pattern) return { ok: false, reason: '关键词不能为空' };
    if (pattern.length > CONFIG.KEYWORD_MAX_LENGTH) {
        return { ok: false, reason: `关键词过长（最大 ${CONFIG.KEYWORD_MAX_LENGTH} 字符）` };
    }

    const dotAnyCount = (pattern.match(/(\.\*|\.\+)/g) || []).length;
    if (dotAnyCount > 2) {
        return { ok: false, reason: '包含过多任意匹配（.* / .+）' };
    }

    const nestedQuantifier = /(\([^)]*[+*][^)]*\)[+*?]|\([^)]*\{[^}]+\}[^)]*\)[+*?]|\([^)]*[+*][^)]*\)\{\d*,?\d*\})/;
    if (nestedQuantifier.test(pattern)) {
        return { ok: false, reason: '疑似嵌套量词' };
    }

    const repeatWithDotAny = /\([^)]*(\.\*|\.\+)[^)]*\)\{\d*,?\d*\}/;
    if (repeatWithDotAny.test(pattern)) {
        return { ok: false, reason: '包含高风险的重复匹配结构' };
    }

    return { ok: true, reason: '' };
}

export async function matchKeyword(env, text) {
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
            const re = new RegExp(raw, 'i');
            if (re.test(targetText)) return keyword;
        } catch {
            Logger.warn('keyword_regex_invalid', { keyword: raw });
        }
    }

    return null;
}
