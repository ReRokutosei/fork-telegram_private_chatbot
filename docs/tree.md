# 项目结构说明

本文档用于说明当前代码组织方式，方便后续维护与扩展。

## 根目录

- `main.js`
  - Worker 入口文件，仅负责导入 `src/app.js` 并导出 `fetch` 与 `RateLimitDO`。
- `wrangler.toml`
  - Cloudflare Workers 配置（入口、迁移、绑定说明）。
- `src/`
  - 业务代码主目录。
- `docs/`
  - 项目文档与部署说明。

## src 目录分层

### `src/app.js`
- 应用装配层。
- 负责聚合各模块依赖，并提供少量包装函数连接旧调用路径。
- 不承载大段业务实现。

### `src/handlers/`
- Webhook 路由分发层。
- 当前文件：
  - `webhook.js`：处理请求校验、Update 分发（私聊/群组/回调/编辑）。

### `src/services/`
- 业务服务层（按领域拆分）。
- 当前文件：
  - `message-flow.js`：私聊消息主链路（用户消息 -> 话题）。
  - `admin-reply.js`：管理员回复与命令处理。
  - `verification.js`：人机验证挑战与回调处理。
  - `media-group.js`：媒体组聚合、延迟发送、过期清理。
  - `cleanup.js`：`/cleanup` 清理逻辑。
  - `topic-lifecycle.js`：话题创建、状态更新、重建相关。
  - `topic-utils.js`：话题工具函数（线程参数注入、探测、欢迎卡片等）。
  - `edit-sync.js`：消息编辑同步。
  - `keywords.js`：关键词规则校验与匹配。
  - `admin.js`：管理员身份判定与缓存。
  - `rate-limit.js`：限流调用封装（DO 优先，KV 降级）。
  - `queue.js`：失败消息队列（入队/处理）。
  - `stats.js`：统计、导出、活跃度数据读取。

### `src/adapters/`
- 基础设施适配层（外部系统交互）。
- 当前文件：
  - `telegram.js`：Telegram Bot API 调用封装。
  - `storage-kv.js`：KV 读写、批量、元数据工具。
  - `storage-d1.js`：D1 数据访问与写入重试。

### `src/core/`
- 通用基础能力。
- 当前文件：
  - `logger.js`：结构化日志。
  - `random.js`：安全随机工具。

### `src/config/`
- 配置与静态数据。
- 当前文件：
  - `constants.js`：运行配置、题库数据等。

### `src/do/`
- Durable Object 实现。
- 当前文件：
  - `rate-limit-do.js`：限流 Durable Object。

## 依赖方向约定

建议维持以下依赖方向，避免反向耦合：

- `handlers -> services -> adapters`
- `services/adapters` 可使用 `core` 与 `config`
- `app.js` 只做组装，不写复杂业务逻辑

## 后续拆分建议

- 继续减少 `src/app.js` 包装函数数量，逐步将调用链直接下沉到 `handlers`。
- 对 `admin-reply.js` 进一步拆分为命令子模块（如 `commands/kw`、`commands/user`、`commands/cleanup`）。
- 根据稳定性考虑，为关键 service 增加最小单元测试。
