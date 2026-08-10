const AI_API_KEY = process.env.DEEPSEEK_API_KEY || 'sk-59f5047c71e84696b83bff46f92260af';
const AI_BASE_URL = process.env.DEEPSEEK_BASE_URL || 'https://api.deepseek.com';
const AI_MODEL = process.env.DEEPSEEK_MODEL || 'deepseek-chat';
const AI_TIMEOUT_MS = 60000;
const AI_MAX_RETRIES = 2;

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function callLLM({ system, user, temperature = 0.3, maxTokens = 2200 }) {
    if (!AI_API_KEY) {
        throw new Error('未配置 AI_API_KEY');
    }

    const body = {
        model: AI_MODEL,
        messages: [
            { role: 'system', content: system || '' },
            { role: 'user', content: user || '' }
        ],
        temperature,
        max_tokens: maxTokens,
        stream: false
    };

    let lastErr;
    for (let attempt = 0; attempt <= AI_MAX_RETRIES; attempt++) {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), AI_TIMEOUT_MS);
        try {
            const resp = await fetch(`${AI_BASE_URL}/chat/completions`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${AI_API_KEY}`
                },
                body: JSON.stringify(body),
                signal: controller.signal
            });
            clearTimeout(timer);

            if (!resp.ok) {
                const errText = await resp.text().catch(() => '');
                throw new Error(`AI HTTP ${resp.status}: ${errText.slice(0, 300)}`);
            }

            const data = await resp.json();
            const content = data?.choices?.[0]?.message?.content;
            if (!content) {
                throw new Error('AI 返回内容为空');
            }
            return { content };
        } catch (err) {
            clearTimeout(timer);
            lastErr = err;
            const isAbort = err.name === 'AbortError';
            const isRetryable = isAbort || /HTTP 5\d\d|ETIMEDOUT|ECONNRESET|socket hang up/i.test(err.message);
            if (attempt < AI_MAX_RETRIES && isRetryable) {
                await sleep(500 * Math.pow(2, attempt));
                continue;
            }
            break;
        }
    }
    throw lastErr || new Error('AI 调用失败');
}

module.exports = { callLLM };