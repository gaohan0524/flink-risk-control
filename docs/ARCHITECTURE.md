# Flink 规则引擎风控系统 — 架构设计

> 版本：v3.0 ｜ 更新：2026-11-09 ｜ 对应代码：`2.0.1-SNAPSHOT`（包名 `com.qinyadan.risk`）
>
> 本文以**当前代码**为准重写。旧版（v2.1）中的 `com.risk` 包名、`rule_type/rule_name/rule_expression` 列、
> `rule_engine:risk:` Redis 前缀、ProLayout 前端等内容均已失效。
>
> 配套：[README](../README.md) · [部署说明](DEPLOY.md) · [学习总览](LEARNING-GUIDE.md) · [逐行精读](code-reading/02-job.md)

---

## 1. 设计目标

| 目标 | 落地手段 | 代价 |
|------|----------|------|
| 异常识别与处置策略解耦 | CEP 只产出"命中了哪种模式"，处置由 MySQL 中的 Aviator 表达式决定 | 两侧通过 `context` Map 隐式契约耦合 |
| 规则热更新、不重启作业 | `BroadcastState` + 30 秒轮询，每个 subtask 本地持有只读规则副本 | 规则生效有最长 30 秒延迟 |
| 同一事件序列可配多种策略 | 多规则命中"就高不就低" | 只记录 top1，审计信息不完整 |
| 结果多路复用 | 一次计算，Redis/MySQL/Kafka 三种落地 | 三路格式与失败语义不一致 |
| 故障可恢复 | Checkpoint 每 60 秒；规则快照进状态 | 未配状态后端/存储，仅能应对进程级重启 |

**非目标**：多租户、规则版本管理、灰度发布、模型评分、离线回溯。这些均未实现。

---

## 2. 整体架构

```
┌──────────────────────── 接入层 ────────────────────────┐
│  Kafka（KRaft，无 Zookeeper）                          │
│  login-events / order-events / activity-events         │
└───────────────────────────┬────────────────────────────┘
                            │
┌───────────────────────────▼────────────────────────────┐
│              Flink 流处理引擎（flink-risk-job）          │
│                                                        │
│  KafkaSource(p=2) ──▶ keyBy(userId)                     │
│        │                    │                          │
│        │                    ▼                          │
│        │            CEP Pattern × 6（within 30~60s）     │
│        │                    │                          │
│        │                    ▼                          │
│        │            PatternMatchResult                  │
│        │        （patternType + context + details）      │
│        │                    │                          │
│        │                 union                          │
│        │                    ▼                          │
│        │        connect(BroadcastStream)                │
│        │                    │                          │
│  MySQL rule_config ──▶ RuleSourceFunction(p=1)          │
│        │                    │                          │
│        │            RuleEvaluationFunction              │
│        │            （Aviator 求值 + 就高不就低）        │
│        │                    │                          │
│        │                    ▼                          │
│        │               RiskResult                       │
│        ▼                    ▼                          │
│   Redis Sink          MySQL Async Sink   Kafka Sink     │
└────────┬───────────────────┬──────────────────┬────────┘
         ▼                   ▼                  ▼
    Redis(key/TTL)     MySQL risk_result   Kafka risk-results
```

### 2.1 两条独立链路

| 链路 | 触发 | 数据形态 | 关键类 |
|------|------|----------|--------|
| 事件链路 | Kafka 消息 | `LoginEvent`/`OrderEvent`/`ActivityEvent` → `PatternMatchResult` → `RiskResult` | `KafkaSourceFactory`、`PatternFactory`、三个 `*CepJob`、`RuleEvaluationFunction`、三个 Sink |
| 规则链路 | 30 秒定时 | `List<RuleConfig>` 全量快照 | `RuleSourceFunction`、`AviatorRuleEngine`、`RiskActionResolver` |

两条链路唯一的交汇点是 `RuleEvaluationFunction`：事件侧读取广播状态，广播侧写入广播状态并重建引擎。

### 2.2 为什么是两段式而不是一段式

如果把"拦截/放行"写进 CEP 的 `select`，任何策略调整都要改代码、重打包、重启作业。拆开之后：

- CEP 回答"**发生了哪种可疑序列**"（代码能力，变更低频）；
- 规则回答"**这种序列算多严重**"（配置能力，变更高频）。

代价是 `context` 成为隐式契约：CEP 少写一个 key，规则就会取值失败并被静默降级为"未命中"。这是本架构最脆弱的地方，也是所有"规则不生效"问题的根因。

---

## 3. 模块划分

```
flink-risk-control/
├── flink-risk-common/      Java 8   模型 + 规则引擎 + Redis 客户端（16 个类）
├── flink-risk-job/         Java 8   Flink 作业（12 个类）
├── flink-risk-web/         Java 17  Spring Boot 管理后端（12 个类 + 1 测试）
├── frontend/               React 18 管理界面
├── sql/init.sql            建表 + 15 条预置规则
├── docker/docker-compose.yml
├── scripts/                docker-start.sh、event-simulator.sh
└── docs/                   本文档 + 部署 + 学习与精读文档
```

### 3.1 依赖方向

```
flink-risk-common ◀──────── flink-risk-job
        ▲
        └────────────────── flink-risk-web（声明了依赖，但当前未 import 任何 common 类）

flink-risk-job  ──写/读──▶  MySQL  ◀──读/写── flink-risk-web
```

关键约束：**job 与 web 之间没有代码依赖**，它们只共享 MySQL 表结构。字段名、枚举大小写、库名必须严格一致，改表要三处同步（`sql/init.sql`、`RuleSourceFunction` + `RuleConfig`、web 的 `Rule` 实体）。

### 3.2 分层职责

| 模块 | 分层 | 职责 |
|------|------|------|
| common | `event` | 事件 POJO（`Event` 抽象 + 3 子类） |
| | `kafka` | 4 个 `DeserializationSchema` |
| | `rule` | `RuleConfig`、`RiskLevel`、`AviatorRuleEngine`、`RiskActionResolver`、`RuleLoader`（未用） |
| | `redis` | `RiskResult`、`RedisClient`、`RedisResultWriter`（未用） |
| job | `cep` | `PatternFactory`、3 个 `*CepJob`、`PatternMatchResult`、`RuleSourceFunction`、`RuleEvaluationFunction` |
| | `kafka` | `KafkaSourceFactory` |
| | `sink` | Redis / MySQL / Kafka 三个 Sink |
| web | `controller` | 3 个 REST 控制器 |
| | `service` | 2 个空壳 `ServiceImpl` |
| | `mapper` | 2 个 `BaseMapper` |
| | `entity` | `Rule`、`RiskResult` |
| | `config` | 分页插件 + CORS、全局异常 |

---

## 4. 运行时拓扑

### 4.1 并行度

| 算子 | 并行度 | 说明 |
|------|--------|------|
| `mysql-rule-source` | 1 | 必须为 1，避免多实例查库/广播冲突 |
| 三个 Kafka Source | 2 | 受 topic 分区数限制（compose 默认 3） |
| 三个 CEP 算子 | 4 | 全局默认 |
| `rule-evaluation` | 4 | 每个 subtask 各持一份规则 |
| 三个 Sink | 4 | 各自独立 |

### 4.2 分区与状态

```
Kafka partition ──▶ Source subtask ──keyBy(userId) hash──▶ CEP keyed state
                                                              │
                                                   每用户一份部分匹配
```

- `keyBy(userId)` 保证同一用户的事件进入同一 CEP 实例，模式状态按用户隔离；
- CEP 状态由 `within` 到期后清理，**没有额外 TTL**；
- `union` 之后流不再带 key，广播评估按到达顺序逐条处理。

### 4.3 事件时间

```
event.getTimestamp() ──▶ Watermark(maxTs - 5s) ──▶ within() 窗口进度
```

- 三路 Source 均使用 `forBoundedOutOfOrderness(Duration.ofSeconds(5))` + `withTimestampAssigner`；
- `timestamp` 必须是**毫秒**；秒级会导致 Watermark 停在 1970 年，CEP 永不输出；
- 乱序超过 5 秒的事件会被当作迟到数据丢弃（`AllowedLateness=0`）。

---

## 5. 领域模型

### 5.1 事件（`flink-risk-common/event`）

| 类 | 关键字段 | `eventType` |
|----|----------|-------------|
| `Event`（抽象） | `userId` `eventId` `timestamp` `eventType` `attributes` | — |
| `LoginEvent` | `ip` `deviceId` `success` `failureReason` `location` `browser` | `LOGIN` |
| `OrderEvent` | `orderId` `amount(BigDecimal)` `productId` `productName` `quantity` `paymentMethod` `deliveryAddress` `processTime` | `ORDER` |
| `ActivityEvent` | `activityId` `activityName` `couponCode` `couponValue` `actionType` `participationCount` `channel` `source` | `ACTIVITY` |

全部实现 `Serializable`：会进入网络 shuffle 与 CEP 状态。

### 5.2 `PatternMatchResult`（CEP 输出）

| 字段 | 含义 |
|------|------|
| `userId` / `eventType` | 事件维度 |
| `eventId` | **每次命中新生成的 UUID**（原始 eventId 不保留） |
| `patternType` | 6 种模式之一，是 CEP 与规则之间的枚举契约 |
| `context` | 供 Aviator 求值的变量表 |
| `details` | 供审计/展示的业务详情 |

### 5.3 `RuleConfig`（规则）

`id(String)` `name` `expression` `riskLevel(RiskLevel)` `action(String)` `weight` `enabled` `description` `createTime` `updateTime`

`RiskLevel`：`LOW(10)` / `MEDIUM(50)` / `HIGH(80)` / `CRITICAL(100)`。

### 5.4 `RiskResult`（输出）

`userId` `eventId` `eventType` `riskLevel` `riskScore` `action` `details(Map)` `timestamp` `ttl`

Redis key：`risk:{eventType小写}:{userId}:{eventId}`，TTL 默认 3600 秒。

---

## 6. CEP 设计

### 6.1 六个模式

| 方法 | 事件 | 条件 | 元素数 | 窗口 | patternType |
|------|------|------|--------|------|-------------|
| `createLoginFailurePattern` | LOGIN | `!success` | 3 | 60s | `LOGIN_FAILURE` |
| `createLoginLocationChangePattern` | LOGIN | `success` | 2 | 60s | `LOCATION_CHANGE` |
| `createOrderFrequentPattern` | ORDER | `amount > 0` | 3 | 60s | `ORDER_FREQUENT` |
| `createOrderRapidPattern` | ORDER | `amount > 100` | 2 | 30s | `ORDER_RAPID` |
| `createActivityFrequentPattern` | ACTIVITY | `actionType == PARTICIPATE` | 4 | 60s | `ACTIVITY_FREQUENT` |
| `createCouponRepeatPattern` | ACTIVITY | `actionType == CLAIM_COUPON` | 3 | 60s | `COUPON_REPEAT` |

### 6.2 量词选择

全部使用 `followedBy`（宽松连续）：两次匹配事件之间允许无关事件。相比 `next`（严格连续），它更贴近真实攻击行为（"失败 2 次 → 成功 1 次 → 再失败"仍然可疑），但会产生重叠匹配，同一用户可能收到多条结果。

### 6.3 模式 → 规则上下文的映射

| patternType | 特有变量 |
|-------------|----------|
| `LOGIN_FAILURE` | `failCount = 3` |
| `LOCATION_CHANGE` | `prevIp` `currIp`（`failCount = 1`） |
| `ORDER_FREQUENT` | `orderCount = 3`、`totalAmount` |
| `ORDER_RAPID` | `intervalMs`（`orderCount = 1`） |
| `ACTIVITY_FREQUENT` | `participationCount = 4` |
| `COUPON_REPEAT` | `participationCount = 3` |

**同名变量在不同模式下语义不同**，例如 `orderCount` 在 `ORDER_RAPID` 下恒为 1。这是规则编写最易踩的坑。

### 6.4 已知缺陷

- `LOCATION_CHANGE` 只判断"两次成功登录"，**不比较 IP**，误报率高；
- 所有模式都在 `userId` 维度聚合，无法覆盖"同设备多账号"；
- `ORDER_RAPID` 的金额门槛（100）写死在代码里，规则无法调整。

---

## 7. 规则引擎设计

### 7.1 结构

```
RuleSourceFunction (p=1, 30s 轮询 MySQL)
        │ collect(List<RuleConfig>)
        ▼
broadcast(RULE_STATE_DESCRIPTOR)      Map<Void, List<RuleConfig>>，null 为唯一 key
        │
        ├──────────────┬──────────────┐
        ▼              ▼              ▼
   subtask-0      subtask-1      subtask-2 ...
   BroadcastState 全量覆盖
   new AviatorRuleEngine + registerRules
   volatile 替换 ruleEngine / resolver
```

### 7.2 引擎能力

| 能力 | 实现 |
|------|------|
| 注册校验 | `registerRule` 先 `AviatorEvaluator.compile`，失败跳过该条 |
| 编译缓存 | Guava Cache（`maximumSize=1000`，`expireAfterAccess=3600s`） |
| 求值 | 遍历全部规则，表达式返回严格 `Boolean.TRUE` 才算命中 |
| 异常降级 | 任何求值异常 → 记日志 → 视为未命中（不中断数据流） |
| 线程安全 | 内部无锁；规则更新时"新建引擎 + volatile 发布" |

### 7.3 决策口径

```java
matchedRules.stream().max(
    Comparator.comparingInt(r -> r.getRiskLevel().getScore())
              .thenComparingInt(RuleConfig::getWeight)
)
```

- 先比等级分（10/50/80/100），同分比 `weight`；
- 最终 `riskScore` = 选中规则的等级分（**不是**加权平均、不是权重）；
- 未命中 → `LOW` / `0` / `ALLOW`；
- 规则未加载 → `LOW` / `0` / `ALLOW`（fail-open，避免启动瞬间误伤全量用户）。

**风险**：只记录 top1 规则（`matchedRuleId` / `matchedRuleName` / `matchedRuleCount`），不记录全部命中项；平局时依赖 `HashMap` 遍历顺序，结果不稳定。

### 7.4 热更新时序

```
t0  Web PUT /api/rules/6  ──▶ UPDATE rule_config SET enabled=0
t1  RuleSourceFunction 轮询（≤30s）
      SELECT ... WHERE enabled=1
t2  ctx.collect(新快照)
t3  broadcast 到 4 个 subtask
t4  每个 subtask：
      broadcastState.put(null, rules)
      newEngine = new AviatorRuleEngine(...)
      newEngine.registerRules(rules)
      this.ruleEngine = newEngine      // volatile 写
t5  下一条事件使用新引擎
```

规则删除/禁用无需特殊消息：全量快照天然覆盖。规则**新增**从下一轮轮询开始生效。

### 7.5 规则表达式的边界

| 写法 | 结果 |
|------|------|
| `failCount >= 3` | ✅ |
| `eventType == "LOGIN" && patternType == "LOGIN_FAILURE"` | ✅ |
| `deviceId =~ /abnormal/` | ✅（正则字面量） |
| `deviceId =~ "abnormal"` | ❌ 运行期异常 → 规则静默失效 |
| `string.contains(source, "脚本")` | ✅ |
| `unknownVar > 1` | ⚠️ 取决于 Aviator 对未定义变量的处理，通常为 false 或异常 |

`sql/init.sql` 中 4 条规则使用了错误语法（规则 3/8/9/12），需按上表修正。

---

## 8. 输出设计

### 8.1 三路对比

| | Redis | MySQL | Kafka |
|---|-------|-------|-------|
| API | `Sink` / `SinkWriter` | `RichAsyncFunction` + `AsyncDataStream.orderedWait` | `KafkaSink` |
| 阻塞 | 同步 SETEX | 异步（线程池，60s 超时，100 并发） | 异步 |
| 序列化 | `RiskResult.toJsonString()`（手写） | `ObjectMapper.writeValueAsString(details)` | `ObjectMapper.writeValueAsBytes(整个对象)` |
| 失败 | 吞异常，作业继续 | `completeExceptionally` → 可能 failover | Flink 重试 / failover |
| 幂等 | key 覆盖 | 无唯一键，可能重复 | 生产者幂等（`acks=all`） |
| 用途 | 实时查询 | 审计、后台查询 | 下游告警/数仓 |

### 8.2 格式不一致

同一个 `RiskResult`，三路产出的 JSON 不同：

- Redis：`"riskLevel":"RiskLevel{score=80, description='高风险'}"`（枚举 `toString()`），且无 `details`；
- MySQL：`risk_level` 列存 `HIGH`，`details` 列存 JSON 文本；
- Kafka：`"riskLevel":"HIGH"`，含全部字段。

下游若同时消费 Redis 与 Kafka，必须分别适配。

---

## 9. 存储设计

### 9.1 `rule_config`

```sql
CREATE TABLE rule_config (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(100)  NOT NULL,
    expression  TEXT          NOT NULL,
    risk_level  VARCHAR(20)   NOT NULL,      -- LOW/MEDIUM/HIGH/CRITICAL，必须大写
    action      VARCHAR(20)   NOT NULL,      -- ALLOW/VERIFY/REVIEW/WARN/BLOCK...
    weight      INT           DEFAULT 1,
    enabled     TINYINT(1)    DEFAULT 1,
    description VARCHAR(500),
    create_time DATETIME      DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_name (name),
    INDEX idx_enabled (enabled)
);
```

- Job 只查 `enabled=1`；
- 没有 `rule_type` 列 → 规则的场景分类只能靠 `name` 关键词推断（web 统计就是这么做的）；
- `risk_level` 大小写敏感，写错会导致**整轮规则加载失败**（`RiskLevel.valueOf` 抛异常）。

### 9.2 `risk_result`

```sql
CREATE TABLE risk_result (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id     VARCHAR(50) NOT NULL,
    event_id    VARCHAR(50) NOT NULL,
    event_type  VARCHAR(20) NOT NULL,
    risk_level  VARCHAR(20) NOT NULL,
    risk_score  INT,
    action      VARCHAR(20),
    details     JSON,
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id),
    INDEX idx_event_type (event_type),
    INDEX idx_create_time (create_time)
);
```

问题：

- 无唯一键 → checkpoint 重放会重复插入；
- `risk_level` 无索引，但列表页/统计频繁按它过滤；
- `user_id` 查询用 `LIKE '%x%'`，索引失效。

### 9.3 读写矩阵

| 表 | 写入方 | 读取方 |
|----|--------|--------|
| `rule_config` | web（CRUD/toggle/batch） | job（30s 轮询）、web（列表/统计） |
| `risk_result` | job（MySQL Sink） | web（列表/统计） |

---

## 10. Web 层设计

### 10.1 分层

```
Controller（参数绑定 + 条件构造 + 字段同步）
    └── Service extends ServiceImpl<Mapper, Entity>（空壳，提供 CRUD 能力）
            └── Mapper extends BaseMapper<Entity>（MyBatis 动态代理）
```

业务逻辑集中在 Controller，Service 只承担泛型绑定。分页依赖 `WebConfig` 中的 `PaginationInnerInterceptor`。

### 10.2 字段别名机制

前端使用 `ruleName` / `ruleExpression`，数据库列是 `name` / `expression`。`Rule` 实体用两个 `@TableField(exist=false)` 影子字段承载别名，由 `syncFromFrontend()` / `syncToFrontend()` 手工搬运：

```
前端 JSON ──syncFromFrontend──▶ DB 字段
DB 字段   ──syncToFrontend────▶ 前端 JSON
```

注意：`ruleType` 也是影子字段，但**没有任何搬运逻辑，也没有数据库列**，全链路无效。

### 10.3 统计口径

| 指标 | 计算方式 | 缺陷 |
|------|----------|------|
| 规则总数 / 启用数 | `count()` | 无 |
| 今日风险 / 今日高危 | `count(create_time >= 今日0点)` | 时区取 JVM 默认 |
| 规则类型分布 | 按 `name` 关键词分组 | 名字改名即变；全大写 `LOGIN` 漏判 |
| 风险等级分布 | 今日记录内存分组，预置 4 个 0 | 全量 list，数据量大时 OOM |
| 近 7 天趋势 | 循环 7 天 × 2 次 count（14 条 SQL） | N+1 |
| 规则命中排行 | 解析 `details.matchedRuleName` 计数 | 依赖字段名，改动即失效 |

---

## 11. API 设计

| 分组 | 方法 | 路径 | 说明 |
|------|------|------|------|
| 规则 | GET | `/api/rules` | 分页，参数 `page/pageSize/name/riskLevel/action/enabled` |
| | GET | `/api/rules/enabled` | 启用规则数组 |
| | GET | `/api/rules/{id}` | 单条 |
| | POST | `/api/rules` | 新建 |
| | PUT | `/api/rules/{id}` | 更新（只更新非 null 字段） |
| | DELETE | `/api/rules/{id}` | 删除 |
| | PUT | `/api/rules/{id}/toggle` | 启停切换 |
| | PUT | `/api/rules/batch/enable` | 批量启用（body 为 id 数组） |
| | PUT | `/api/rules/batch/disable` | 批量停用 |
| 风险 | GET | `/api/risks` | 分页，参数 `userId/eventType/riskLevel/action` |
| | GET | `/api/risks/{id}` | 单条 |
| 统计 | GET | `/api/stats/overview` | 总览 |
| | GET | `/api/stats/trend` | 近 7 天 |
| | GET | `/api/stats/top-rules` | 今日 Top10 |

响应约定：

```json
// 分页
{ "data": [...], "total": 15, "page": 1, "pageSize": 10 }

// 异常（注意：HTTP 状态码仍为 200）
{ "status": 500, "error": "DataIntegrityViolationException", "message": "...", "stackTrace": "..." }
```

没有统一的成功包装（无 `code/message`），也没有鉴权。

---

## 12. 配置设计

### 12.1 真实生效的配置（命令行参数）

| 参数 | 默认值 |
|------|--------|
| `--kafka.bootstrap.servers` | `localhost:9092` |
| `--mysql.url` | `jdbc:mysql://localhost:3306/flink_risk?...` |
| `--mysql.username` / `--mysql.password` | `root` / `root123123` |
| `--redis.host` / `--redis.port` / `--redis.db` | `localhost` / `6379` / `0` |
| `--rule.poll.interval` | `30000` |

### 12.2 声明了但不生效的配置

`flink-risk-job/src/main/resources/application.properties` 中的全部内容（`flink.parallelism`、`flink.state.backend`、`kafka.*`、`window.*`、`rule.hot.reload.*`）**没有任何代码读取**。它是一份设计意图说明，改动不会影响运行。

### 12.3 Web 配置

`flink-risk-web/src/main/resources/application.yml`：端口 `SERVER_PORT`、数据源 `MYSQL_*`（库名硬编码 `risk_control`）、Redis `REDIS_*`（未使用）、Jackson 时区、MyBatis-Plus 驼峰映射。

---

## 13. 容错与一致性

| 机制 | 现状 | 评价 |
|------|------|------|
| Checkpoint | 每 60s，EXACTLY_ONCE，默认内存状态后端与 JobManager 存储 | 仅适合演示 |
| Kafka offset | 随 checkpoint 提交 | 故障恢复会重放最近 ≤60s 数据 |
| 规则状态 | 进广播状态，参与 checkpoint | 恢复后无需等轮询 |
| MySQL 写入 | 无幂等键 | 重放导致重复 |
| Redis 写入 | key 覆盖 | 天然幂等 |
| Kafka 写入 | 幂等生产者 | 单分区不重复 |
| 失败策略 | Redis 吞异常；MySQL 抛异常；Kafka 交给 Flink | 语义不统一 |
| 算子 uid | 无 | 无法稳定从 savepoint 恢复 |

---

## 14. 扩展设计

### 14.1 新增一个 CEP 模式

1. `PatternFactory` 增加 `createXxxPattern()`；
2. 对应 `XxxCepJob.process()` 中 `CEP.pattern(...).select(...)`，组装 `context`/`details`；
3. `sql/init.sql` 增加规则（`patternType == "XXX"`）；
4. 前端 `Rule/List.tsx` 的 `EXPRESSION_DOC.patterns` 同步；
5. 更新本文档第 6 章。

### 14.2 新增事件类型（如支付）

新增 POJO + 反序列化器 + `PaymentCepJob`，在 `RiskControlApplication` 接线；`risk_result.event_type` 与前端 `EVENT_TYPE_CONFIG` 同步。

### 14.3 规则规模变大

当前 `executeRules` 是 O(规则数) 全量遍历。建议：

- 规则表加 `event_type` 列，引擎按事件类型建索引；
- 广播前做内容哈希，只在下发内容变化时重建引擎；
- 或引入规则分片（按 `patternType` 分组求值）。

### 14.4 规则实时推送

30 秒轮询的延迟可用 CDC（Debezium → Kafka → Flink）替代，把 `rule_config` 变更变成事件流，广播延迟降到秒级。

### 14.5 水平扩展

- 加 TaskManager / 提高并行度，上限受 `keyBy(userId)` 的 key 分布影响（热点用户会成为单点）；
- Kafka topic 增加分区（需开启 `partition.discovery.interval.ms`，当前未配置）；
- 规则源并行度保持 1。

---

## 15. 架构债务清单

| # | 债务 | 影响 | 优先级 |
|---|------|------|--------|
| 1 | 无 `.uid()`、无状态后端/checkpoint 目录 | 无法可靠从 savepoint 升级 | P0 |
| 2 | Redis 客户端不支持密码 | 无法接生产 Redis | P0 |
| 3 | 数据库名不统一（`flink_risk` vs `risk_control`） | 开箱跑不通 | P0 |
| 4 | MySQL Sink 无连接池、异常触发重启 | 稳定性 | P0 |
| 5 | 规则表达式无入库校验（`=~` 错误语法） | 规则静默失效 | P1 |
| 6 | Redis 值格式错误（枚举 toString、无 details） | 下游不可用 | P1 |
| 7 | `risk_result` 无唯一键 | 重复数据 | P1 |
| 8 | 统计接口 N+1 / 全量 list | 性能 | P1 |
| 9 | `context` 隐式契约无校验 | 规则不生效难排查 | P1 |
| 10 | `ruleType` 全链路无效 | 分类统计靠猜 | P2 |
| 11 | 未使用的类/配置（`RuleLoader`、`RedisResultWriter`、`KafkaEventDeserializer`、`application.properties`、web 的 common 依赖） | 认知负担 | P2 |
| 12 | Web 无鉴权、异常返回堆栈且 HTTP 200 | 安全 | P2 |

---

## 16. 演进记录

| 版本 | 变更 |
|------|------|
| v1.0 | 初版架构（Flink 1.x 风格，Redis 单路输出） |
| v2.1 | 三路输出、广播规则、Web 管理端（文档中的包名 `com.risk` 即此阶段） |
| v2.3 | 修复 CEP 不触发（`noWatermarks` → `forBoundedOutOfOrderness(5s)`），`next` → `followedBy`，窗口 5s → 30~60s |
| v2.4 | 包名 `com.risk` → `com.qinyadan.risk` |
| **v3.0（本文）** | 按代码重写：真实类表、真实 DDL、真实 API、三 Sink 语义、热更新时序、架构债务 |

---

*文档版本：v3.0 ｜ 更新日期：2026-11-09*
