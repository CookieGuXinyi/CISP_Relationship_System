// ai_client.js — DeepSeek LLM 调用封装
const https = require('https');
const http = require('http');

const AI_CONFIG = {
    apiKey: process.env.AI_API_KEY || 'sk-59f5047c71e84696b83bff46f92260af',
    baseUrl: process.env.AI_BASE_URL || 'https://api.deepseek.com',
    model: process.env.AI_MODEL || 'deepseek-chat',
    timeoutMs: 60000,
    maxRetries: 2
};

function callLLM({ system, user, temperature = 0.3, maxTokens = 2000 }) {
    return new Promise((resolve, reject) => {
        const body = JSON.stringify({
            model: AI_CONFIG.model,
            messages: [
                { role: 'system', content: system },
                { role: 'user', content: user }
            ],
            temperature,
            max_tokens: maxTokens,
            stream: false
        });

        const url = new URL(AI_CONFIG.baseUrl + '/v1/chat/completions');
        const lib = url.protocol === 'https:' ? https : http;
        const options = {
            method: 'POST',
            hostname: url.hostname,
            port: url.port || (url.protocol === 'https:' ? 443 : 80),
            path: url.pathname + url.search,
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${AI_CONFIG.apiKey}`,
                'Content-Length': Buffer.byteLength(body)
            }
        };

        let lastErr = null;
        let attempt = 0;
        const tryOnce = () => {
            attempt++;
            const req = lib.request(options, (resp) => {
                let data = '';
                resp.on('data', (chunk) => { data += chunk; });
                resp.on('end', () => {
                    if (resp.statusCode >= 200 && resp.statusCode < 300) {
                        try {
                            const json = JSON.parse(data);
                            const content = json.choices?.[0]?.message?.content || '';
                            resolve({ content, raw: json });
                        } catch (e) {
                            reject(new Error('LLM 返回非 JSON: ' + data.slice(0, 200)));
                        }
                    } else {
                        lastErr = new Error(`LLM HTTP ${resp.statusCode}: ${data.slice(0, 200)}`);
                        if (attempt <= AI_CONFIG.maxRetries) setTimeout(tryOnce, 1000 * attempt);
                        else reject(lastErr);
                    }
                });
            });
            req.on('error', (e) => {
                lastErr = e;
                if (attempt <= AI_CONFIG.maxRetries) setTimeout(tryOnce, 1000 * attempt);
                else reject(e);
            });
            req.setTimeout(AI_CONFIG.timeoutMs, () => {
                req.destroy(new Error('LLM 请求超时'));
            });
            req.write(body);
            req.end();
        };
        tryOnce();
    });
}

module.exports = { callLLM, AI_CONFIG };