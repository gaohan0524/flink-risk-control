# Flink 风控系统 · 学习总览

> 面向"想系统读懂这个项目"的读者。本文负责讲清**全局图景、运行链路、数据契约和关键设计决策**；
> 具体每一行代码的精读见 `docs/deep-dive/` 下的四个模块文档。
>
> 文档基于仓库当前代码（`com.qinyadan.risk` 包名，2.0.1-SNAPSHOT）整理，凡与 `README.md` / `docs/ARCHITECTURE.md` 冲突之处，以本系列文档（即代码事实）为准，冲突点集中记录在[第 9 章](#9-已知问题与代码痛点)。

---

## 目录

1. [这套系统到底在做什么](#1-这套系统到底在做什么)
2. [技术栈与硬性约束](#2-技术栈与硬性约束)
3. [模块地图与依赖方向](#3-模块地图与依赖方向)
4. [端到端数据流](#4-端到端数据流)
5. [四个必须理解的设计决策](#5-四个必须理解的设计决策)
6. [数据契约](#6-数据契约)
7. [本地跑通的最小闭环](#7-本地跑通的最小闭环)
8. [调试与观测手段](#8-调试与观测手段)
9. [已知问题与代码痛点](#9-已知问题与代码痛点)
10. [精读路线](#10-精读路线)
11. [模块文档索引](#11-模块文档索引)

---

## 1. 这套系统到底在做什么

一句话：**把"用户行为事件流"变成"风险处置结果"**。

```
原始行为事件                     命中的异常模式                    最终处置
login/order/activity  ──CEP──▶  LOGIN_FAILURE / ORDER_RAPID ──规则引擎──▶  BLOCK / VERIFY / ALLOW …
```

它解决的是两类问题：

1. **时序模式识别**：单条事件看不出问题，"5 秒内连续 3 次登录失败"才有问题 → 交给 Flink CEP。
2. **策略与代码解耦**：什么样的模式算多严重的风险、该拦截还是该验证，不应写死在 Java 里 → 存 MySQL 的 Aviator 表达式，热更新。

所以整个项目本质上是一条**两段式流水线**：

| 阶段 | 输入 | 输出 | 谁负责 | 变更频率 |
|------|------|------|--------|----------|
| 模式识别 | 原始事件 | `PatternMatchResult`（含 `patternType` + 上下文 `context`） | CEP（代码） | 低，改代码重启 |
| 风险决策 | `PatternMatchResult` + 规则集 | `RiskResult`（level/score/action） | Aviator（配置） | 高，改库 30 秒生效 |

理解这个"两段式"是本项目的**第一把钥匙**：`LoginCepJob` 里没有一行"拦截/放行"的判断逻辑，`AviatorRuleEngine` 里也没有一行"什么算连续失败"。两者只通过 `context` 这个 `Map<String,Object>` 通信。

---

## 2. 技术栈与硬性约束

| 层 | 技术 | 版本 | 备注 |
|----|------|------|------|
| 流处理 | Apache Flink | 2.2.1 | 新 Source/Sink API（`fromSource` / `sinkTo`） |
| CEP | flink-cep | 2.2.1 | 集群需手动放进 `lib/` |
| Kafka 连接器 | flink-connector-kafka | **5.0.0-2.2** | 独立版本号，与 Flink 版本不是一一对应 |
| 规则表达式 | Aviator | 5.4.3 | 注意 `=~` 必须配正则字面量 |
| 消息队列 | Kafka（KRaft） | cp-kafka 8.1.4 | 无 Zookeeper |
| 缓存 | Redis / Jedis | 8.8 / 5.2.0 | 容器默认带 `requirepass` |
| 数据库 | MySQL | 8.0 | 两张表：`rule_config`、`risk_result` |
| 管理后端 | Spring Boot | 3.2.0 | **Java 17** |
| ORM | MyBatis-Plus | 3.5.5 | 无 XML mapper，全注解/条件构造器 |
| 前端 | React + antd + ProComponents | 18.3 / 5.22 / 2.8 | Vite 5，TypeScript |

**Java 版本是分模块的**：`flink-risk-common` 与 `flink-risk-job` 编译到 Java 8（父 POM `maven.compiler.target=8`），`flink-risk-web` 覆盖为 Java 17（Boot 3 要求）。CI 用 JDK 21 编译全部。

Flink 2.x 相对 1.x 的 API 变化，本项目实际踩到的几条：

| 旧写法 | 本项目写法 |
|--------|-----------|
| `env.addSource(...)` | `env.fromSource(source, watermark, name)`（Kafka）；规则源仍用 legacy `SourceFunction` + `addSource` |
| `addSink(...)` | `sinkTo(...)`；Redis 走 `Sink`/`SinkWriter` |
| `Time.seconds()` | `java.time.Duration` |
| `RichFunction.open(Configuration)` | `open(OpenContext)` |
| `RichSinkFunction` | `RichAsyncFunction`（MySQL）或新 Sink API |

---

## 3. 模块地图与依赖方向

```
flink-risk-control (parent pom)
├── flink-risk-common   Java 8   事件模型 / 反序列化 / Aviator 引擎 / Redis 客户端
├── flink-risk-job      Java 8   Flink 作业：Source → CEP → 广播规则评估 → 3×Sink
├── flink-risk-web      Java 17  Spring Boot 管理后端（CRUD + 统计）
├── frontend            TS/React 管理界面
├── sql/init.sql                 建表 + 15 条预置规则
├── docker/docker-compose.yml    MySQL/Redis/Kafka/Kafka-UI/Flink/Adminer
├── scripts/                     docker-start.sh、event-simulator.sh
└── docs/                        架构/部署/本系列学习文档
```

```
        ┌──────────────────┐
        │ flink-risk-common│◀──────────┐
        └────────┬─────────┘           │
                 │ 依赖                │ 依赖
        ┌────────▼─────────┐   ┌───────┴────────┐
        │ flink-risk-job   │   │ flink-risk-web │
        └────────┬─────────┘   └───────┬────────┘
                 │                     │ HTTP(/api)
                 │ 只通过 MySQL         │
                 ▼                     ▼
        ┌────────────────────────────────────┐
        │  MySQL rule_config / risk_result   │◀── frontend
        └────────────────────────────────────┘
```

关键点：**job 与 web 之间没有任何代码依赖**。它们唯一的耦合是 MySQL 里的 `rule_config` 表——web 写，job 每 30 秒读。这既是最优雅的地方（可以独立部署/重启），也是最容易出问题的地方（字段名、库名、`enabled` 语义必须严格一致，见第 9 章）。

### 各模块一句话职责

| 模块 | 一句话 | 精读文档 |
|------|--------|----------|
| common | 定义"事件长什么样、规则怎么算、结果怎么写" | [01-flink-risk-common](deep-dive/01-flink-risk-common.md) |
| job | 把上面这些零件组装成一条 Flink DAG 并跑起来 | [02-flink-risk-job](deep-dive/02-flink-risk-job.md) |
| web | 规则的增删改查 + 风险结果查询 + 看板统计 | [03-flink-risk-web](deep-dive/03-flink-risk-web.md) |
| frontend | 上面三个能力的可视化界面 | [04-frontend](deep-dive/04-frontend.md) |

---

## 4. 端到端数据流

### 4.1 主链路（一次风险命中要走完的 10 步）

```
① 事件生产              Kafka topic: login-events / order-events / activity-events
                              │
② 反序列化              LoginEventDeserializer 等（Jackson → POJO）
                              │
③ 事件时间              forBoundedOutOfOrderness(5s) + event.getTimestamp()
                              │
④ 按用户分组            keyBy(e -> e.getUserId())
                              │
⑤ CEP 匹配              PatternFactory 的 6 个 Pattern，within(30~60s)
                              │  命中 → PatternSelectFunction
⑥ 组装上下文            buildXxxContext() → Map{patternType, failCount, amount, …}
                              │  → PatternMatchResult(userId, eventType, patternType, context, details)
⑦ 三流合并              loginMatches.union(orderMatches, activityMatches)
                              │
⑧ 连接规则广播流        .connect(ruleBroadcastStream) → RuleEvaluationFunction
                              │  从 ReadOnlyBroadcastState 取最新 List<RuleConfig>
⑨ Aviator 评估          AviatorRuleEngine.executeRules(context) → 命中规则集合
                              │  RiskActionResolver 取"最高危"的一条 → RiskResult
⑩ 三路输出              Redis（实时 key）/ MySQL（审计表）/ Kafka（risk-results）
```

### 4.2 规则侧链路（为什么改库能立刻生效）

```
Web POST /api/rules ──▶ MySQL rule_config
                              │
                    RuleSourceFunction（parallelism=1，每 30s 轮询）
                              │ collect(List<RuleConfig> 全量快照)
                    broadcast(RULE_STATE_DESCRIPTOR)
                              │
         ┌────────────────────┼────────────────────┐
         ▼                    ▼                    ▼
    subtask-0            subtask-1            subtask-2 …
   全量覆盖 BroadcastState + new AviatorRuleEngine() + registerRules()
         │
   后续每条 PatternMatchResult 都用新引擎评估
```

**这里有个容易忽视的点**：广播的是"全量快照"而不是增量事件。因此删除规则、禁用规则都天然生效，不需要额外的 DELETE 消息类型；但也意味着每 30 秒每个 subtask 都要重建一次引擎（规则多时有编译开销，靠 Aviator 的编译缓存和 Guava Cache 兜底）。

### 4.3 两条链路的交汇点：`context`

CEP 侧写进 `context` 的 key，就是规则表达式能用的变量名。**这是全项目最脆弱的契约**：

- 少写一个 key → 表达式取值失败 → `evaluateExpression` 捕获异常返回 `false` → 规则**静默失效**，日志里只有一行 error。
- 改了 key 名（比如 `orderCount` → `count`）→ 规则不报错但永不命中。
- 多写没关系，Aviator 不关心未使用的变量。

完整变量表见 [6.2](#62-规则上下文变量context-契约)。

---

## 5. 四个必须理解的设计决策

### 5.1 为什么 CEP 与规则引擎要拆开

如果把"拦截还是放行"写进 `PatternSelectFunction`，每加一条策略都要改代码、重新打包、重启作业。拆开之后：

- CEP 只负责"**发生了一种可疑的事件序列**"（这是代码能力）；
- 规则负责"**这种序列在你的业务里算多严重**"（这是运营能力）。

代价是两条链路的上下文必须对齐（第 4.3 节），以及多了一次广播状态的查表开销（本地内存，可忽略）。

### 5.2 为什么用 BroadcastState 而不是每个 subtask 自己查库

`RuleSourceFunction` 的并行度被钉死为 1，查一次库、发一份快照，Flink 负责把它广播到所有并行实例并参与 Checkpoint。好处：

- 数据库压力恒定（与作业并行度解耦）；
- 规则在 checkpoint 里持久化，故障恢复后不用等下一轮轮询；
- 主数据流侧拿到的是 `ReadOnlyBroadcastState`，无法误改。

代价：**广播状态没有 TTL**，规则快照会一直留在状态后端里（体量很小，可接受）。

### 5.3 "失败开放"与"就高不就低"

两个策略性选择，都写死在代码里，改策略需要改代码：

| 场景 | 行为 | 位置 | 理由 |
|------|------|------|------|
| 规则还没加载出来 | 输出 `LOW` / `ALLOW` / score 0 | `RuleEvaluationFunction.processElement` | 宁可漏判，不可在启动瞬间把所有用户全拦掉 |
| 没有规则命中 | `LOW` / `ALLOW` / score 0 | `RiskActionResolver.resolve` | 同上 |
| 多条规则命中 | 取 `RiskLevel.score` 最大者；同分取 `weight` 大者 | `RiskActionResolver.selectTopPriorityRule` | 风控"就高不就低" |

注意命中后的 `riskScore` **直接取等级分**（LOW=10 / MEDIUM=50 / HIGH=80 / CRITICAL=100），不是加权平均。`AviatorRuleEngine.calculateRiskScore()` 里确实实现了加权平均，但主链路没调用它——读代码时不要被这个"孤岛方法"误导。

### 5.4 三路 Sink 的语义差异（不是简单重复写三次）

| Sink | 实现方式 | 失败行为 | 用途 |
|------|----------|----------|------|
| Redis | `Sink`/`SinkWriter`，Jedis 同步 `SETEX` | 异常被 catch 并记日志，**不影响作业** | 实时查询，TTL 1h |
| MySQL | `RichAsyncFunction` + 线程池，`orderedWait` | `completeExceptionally` → **可能触发作业失败/重启** | 审计、Web 后台查询 |
| Kafka | `KafkaSink`，`acks=all` + 幂等 + lz4 | 由 Flink 的至少一次/幂等保证 | 下游告警/数仓消费 |

"Redis 吞异常、MySQL 抛异常"不是随意的：Redis 是旁路缓存，丢了可以重算；MySQL 是审计账本，写失败应该让人知道。但当前 MySQL 的失败处理会连累整个作业，属于待改进项。

---

## 6. 数据契约

### 6.1 Kafka 事件 JSON

三路 topic 的字段与 POJO 一一对应（Jackson 反序列化，**字段名必须完全一致**）：

| Topic | 关键字段 | 对应类 |
|-------|----------|--------|
| `login-events` | `userId` `eventId` `timestamp` `eventType` `ip` `deviceId` `success` `failureReason` `location` `browser` | `LoginEvent` |
| `order-events` | `userId` `eventId` `timestamp` `eventType` `orderId` `amount` `productId` `productName` `quantity` `paymentMethod` `deliveryAddress` `processTime` | `OrderEvent` |
| `activity-events` | `userId` `eventId` `timestamp` `eventType` `activityId` `activityName` `couponCode` `couponValue` `actionType` `participationCount` `channel` `source` | `ActivityEvent` |

`timestamp` 必须是**毫秒时间戳**：它既作事件时间（Watermark），又用于计算"快速下单"的间隔。

### 6.2 规则上下文变量（context 契约）

| patternType | 触发条件（当前代码） | 必有的 context 变量 |
|-------------|----------------------|---------------------|
| `LOGIN_FAILURE` | 60s 内 3 次 `success=false` | `eventType=LOGIN` `patternType` `userId` `failCount` `ip` `deviceId` `success` `location` `browser` |
| `LOCATION_CHANGE` | 60s 内 2 次 `success=true` | 同上（由第二次登录构造，`failCount=1`）+ `prevIp` `currIp` |
| `ORDER_FREQUENT` | 60s 内 3 笔 `amount>0` | `eventType=ORDER` `patternType` `userId` `orderCount` `amount` `productId` `paymentMethod` + `totalAmount` |
| `ORDER_RAPID` | 30s 内 2 笔 `amount>100` | 同上（`orderCount=1`）+ `intervalMs` |
| `ACTIVITY_FREQUENT` | 60s 内 4 次 `actionType=PARTICIPATE` | `eventType=ACTIVITY` `patternType` `userId` `participationCount` `activityId` `activityName` `actionType` `channel` `source` |
| `COUPON_REPEAT` | 60s 内 3 次 `actionType=CLAIM_COUPON` | 同上 |

> 注意 `LOCATION_CHANGE` 名字有误导性：代码只匹配"两次成功登录"，并没有比较 IP/地点。真正的 IP 对比在规则表达式层（`prevIp != currIp`），或者干脆没做。

### 6.3 MySQL 表

| 表 | 谁写 | 谁读 | 关键列 |
|----|------|------|--------|
| `rule_config` | web（POST/PUT/toggle/batch） | job 的 `RuleSourceFunction`；web 查询 | `id` `name` `expression` `risk_level` `action` `weight` `enabled` `description` |
| `risk_result` | job 的 `MysqlSinkFunction` | web 的风险查询/统计 | `user_id` `event_id` `event_type` `risk_level` `risk_score` `action` `details(JSON)` `create_time` |

`details` 是 JSON 字符串，里面至少有 `matchedRuleId` / `matchedRuleName` / `matchedRuleCount` + CEP 侧塞进来的业务详情（`failCount`、`intervalMs` 等）。**看板的"规则命中排行"就是解析这个字段的 `matchedRuleName`。**

### 6.4 REST API（以代码为准）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/rules?page&pageSize&name&riskLevel&action&enabled` | 分页；`name` 是模糊匹配 |
| GET | `/api/rules/enabled` | 全部启用规则（供外部拉取） |
| GET | `/api/rules/{id}` | 单条 |
| POST | `/api/rules` | 新建 |
| PUT | `/api/rules/{id}` | 更新 |
| DELETE | `/api/rules/{id}` | 删除 |
| PUT | `/api/rules/{id}/toggle` | 取反 `enabled` |
| PUT | `/api/rules/batch/enable`、`/batch/disable` | 请求体为 `number[]` |
| GET | `/api/risks?page&pageSize&userId&eventType&riskLevel&action` | 风险记录分页 |
| GET | `/api/risks/{id}` | 单条 |
| GET | `/api/stats/overview` | 总览（规则数/今日风险/分布） |
| GET | `/api/stats/trend` | 近 7 天趋势 |
| GET | `/api/stats/top-rules` | 今日规则命中 Top 10 |

分页统一返回 `{data, total, page, pageSize}`。**`README.md` 里写的 `/api/risk-results` 是错的，实际是 `/api/risks`。**

---

## 7. 本地跑通的最小闭环

```bash
# 0. 依赖组件（MySQL/Redis/Kafka/Flink/Adminer）
cd docker && docker compose up -d

# 1. 建表 + 预置规则（compose 里的 init.sql 挂载路径有误，建议手工导入）
mysql -h 127.0.0.1 -u root -proot123 flink_risk < sql/init.sql

# 2. 后端（Java 17）
mvn clean install -DskipTests        # 先让 common 进本地仓库
cd flink-risk-web && mvn spring-boot:run     # :8080

# 3. 前端（Node 20）
cd frontend && npm install && npm run dev    # :3000，/api 代理到 :8080

# 4. Flink Job（Java 8 编译，IDEA 直接跑 main 最省事）
cd flink-risk-job
mvn exec:java -Dexec.mainClass=com.qinyadan.risk.RiskControlApplication
```

跑通判据（按顺序看）：

1. Job 控制台出现 `Broadcast rules snapshot: 15 rules`；
2. 造数：`cd scripts && ./event-simulator.sh login_failure user_001`；
3. Job 日志依次出现 `[SOURCE]` → `[CEP] Pattern matched` → `[EVAL]` → `[RULE] RiskResult` → `[SINK-*]`；
4. `risk_result` 表有新行；`/api/risks` 页面能看到记录。

**四个最常见的启动失败原因**（详见第 9 章）：

- web 连 `risk_control`、job 连 `flink_risk`，库名不一致 → 表不存在；
- Redis 容器有密码而客户端不带密码 → `NOAUTH`，Redis 一路静默失败；
- 忘记把 `flink-cep` / `flink-connector-kafka` 放进集群 `lib/` → `ClassNotFoundException`；
- CEP 用了 `noWatermarks()` → 模式永不触发（CHANGELOG v2.3 修过一次）。

---

## 8. 调试与观测手段

| 手段 | 怎么用 | 看什么 |
|------|--------|--------|
| 日志标记 | `grep -E "\[SOURCE\]\|\[CEP\]\|\[EVAL\]\|\[RULE\]\|\[SINK"` | 判断卡在第几步 |
| 日志文件 | `logs/flink-risk-job.log`（Log4j2 按天滚动） | 历史回溯 |
| Flink UI | http://localhost:8081 | 反压、checkpoint 大小、算子并行度 |
| Kafka UI | http://localhost:9090 | topic 是否有数据、消费组 lag |
| Adminer | http://localhost:8082 | `rule_config` 是否被 CEP 读到、`risk_result` 是否写入 |
| Redis | `docker exec -it flink-risk-redis redis-cli -a redis123 KEYS 'risk:*'` | 实时结果 key 是否存在 |
| 造数 | `./event-simulator.sh login_failure user_001` | 单场景复现 |

`event-simulator.sh` 的场景与 CEP 模式是一一对应的（`login_failure` / `order_frequent` / `activity_frequent` / `coupon_repeat` / `all` / `batch N` / `stress`），造数时会按模式要求连续发多条事件，因此在控制台会看到 `[CEP]` 在最后一条事件之后才出现——这不是延迟问题，是 CEP 的匹配语义。

---

## 9. 已知问题与代码痛点

按"影响面"排序，前四条会直接导致跑不通或结果错。

| # | 现象 | 根因（代码位置） | 建议 |
|---|------|------------------|------|
| 1 | Redis 里查不到结果，日志刷 `NOAUTH` | `RedisClient` 创建 `JedisPool` 时没有 password 参数（`flink-risk-common/.../redis/RedisClient.java`），而 compose 的 Redis 带 `requirepass redis123`；`RiskControlApplication` 也没解析 `--redis.password` | 给 `RedisClient` 加 password 重载，并在入口解析该参数 |
| 2 | Redis 里 `riskLevel` 是无法解析的长字符串，且丢了 `details` | `RiskResult.toJsonString()` 手写字符串拼接，`riskLevel` 是枚举对象，`append` 走了 `toString()`，产出 `"riskLevel":"RiskLevel{score=80, description='高风险'}"` | 改用 Jackson/Fastjson 序列化；`details` 也一并写入 |
| 3 | web 起不来 / 报表不存在 | job 默认 `flink_risk`，web 默认 `risk_control`（`RiskControlApplication` 与 `application.yml`） | 统一库名，或明确用 `MYSQL_*` 环境变量 |
| 4 | 部分预置规则永不命中 | `sql/init.sql` 里 `deviceId =~ "abnormal"`、`source =~ "脚本"` 等写法：Aviator 的 `=~` 右侧必须是**正则字面量** `/pattern/`，字符串字面量运行期抛异常并被吞掉 | 改成 `=~ /脚本/` 或 `string.contains(source,"脚本")`；引擎已专门打日志提示 |
| 5 | 评论说"不重试仅记录日志"，实际作业会重启 | `MysqlSinkFunction.asyncInvoke` 失败时 `completeExceptionally` | 改成 `complete(emptyList())` + 告警，或明确接受失败重启 |
| 6 | 宣称"连接池"，实际每次新建连接 | `MysqlSinkFunction` 在 `asyncInvoke` 里 `DriverManager.getConnection(...)` | 引入 HikariCP 或复用连接 |
| 7 | 高并发下 MySQL 写丢失且无声 | 线程池队列满时 `DiscardOldestPolicy` | 换成 `CallerRunsPolicy` 或直接依赖 Flink 反压 |
| 8 | 改 `application.properties` 不生效 | `RiskControlApplication` 全部用 `getArg(args, key, default)`，从未读取 properties 文件 | 要么删文件，要么真用 `ParameterTool` 加载 |
| 9 | `KafkaEventDeserializer` 遇到未知 `eventType` 会抛异常 | 默认分支 `readValue(message, Event.class)`，而 `Event` 是抽象类 | 返回 null 跳过，或定义一个 `UnknownEvent` |
| 10 | 告警/巡检看到 `stackTrace` 外泄 | `GlobalExceptionHandler` 把完整堆栈塞进响应体，且 HTTP 状态仍是 200 | 只在日志里记堆栈，响应体返回错误码 |
| 11 | 前端表达式文档教错语法 | `frontend/src/pages/Rule/List.tsx` 中 `EXPRESSION_DOC` 示例为 `productId=~'ABNORMAL.*'` | 改为 `/ABNORMAL.*/` |
| 12 | 前端文档说"5s 窗口"，代码是 30~60s | 同文件 `patterns` 数组 | 以前端展示为准还是以代码为准，需统一 |
| 13 | `docs/ARCHITECTURE.md` 多处过时 | 旧包名 `com.risk`、Redis 前缀 `rule_engine:risk:`、三列表结构、ProLayout | 以 `docs/deep-dive/` 系列为准 |
| 14 | 规则类型 `ruleType` 存不进库 | `Rule` 的 `ruleType` 是 `@TableField(exist=false)`，`syncFromFrontend()` 也不拷贝它 | 加列或删除该字段，避免前端误以为已保存 |
| 15 | `/api/stats/trend` 慢 | 7 天 × 2 次 count，共 14 条 SQL | 改一次 `GROUP BY date` 聚合查询 |

另外两个"看着奇怪但没错"的地方，避免误改：

- `RuleLoader` + `rules.yaml` 是残留的 YAML 规则加载能力，**当前无人调用**，删除不影响运行；
- `AviatorRuleEngine.calculateRiskScore()` / `getRiskLevel()` 是加权平均实现，**主链路未使用**，`RiskActionResolver` 用的是"取最高危规则"。

---

## 10. 精读路线

按依赖顺序读，每读完一个模块做对应练习（练习写在模块文档末尾）：

```
第 0 步  跑通最小闭环（第 7 章）—— 不跑通就别读代码，很多设计只有跑起来才看得懂
   │
第 1 步  common：数据模型 + 规则引擎            ← 无 Flink 依赖，最容易读
   │     产出认知：context 里有什么、规则怎么算出一个 RiskResult
   │
第 2 步  job：DAG 组装 + CEP + 广播             ← 项目核心，占全部难度的 70%
   │     产出认知：为什么必须 keyBy、为什么必须 Watermark、广播状态怎么替换
   │
第 3 步  web：CRUD + 统计                       ← 最简单的一层，注意字段别名
   │
第 4 步  frontend：页面 + 请求层                 ← 关注与后端的字段契约
   │
第 5 步  回到第 9 章，挑 1~2 个问题动手修         ← 检验是否真的读懂
```

建议的"读懂标准"：

- 能不看代码画出第 4 章的 10 步流程图；
- 能解释"把 `within(60s)` 改成 `within(5s)` 会发生什么"；
- 能解释"新增一个 `ORDER_OVERNIGHT` 模式需要改哪几个文件、改哪几行类型的东西"；
- 能说出"为什么规则改了 30 秒后才生效，而不是立刻"。

---

## 11. 模块文档索引

| 文档 | 内容 | 适合谁 |
|------|------|--------|
| **本文** | 全局图景、数据契约、坑位清单 | 所有人，先读 |
| [01 · flink-risk-common](deep-dive/01-flink-risk-common.md) | 事件模型、Kafka 反序列化、Aviator 引擎、`RiskActionResolver`、`RiskResult`、Redis 客户端 | 想搞懂"规则到底怎么算出来的" |
| [02 · flink-risk-job](deep-dive/02-flink-risk-job.md) | `RiskControlApplication` DAG 逐段、`PatternFactory` 六个模式、三个 CepJob、广播函数、三个 Sink | 想搞懂 Flink 侧全部逻辑 |
| [03 · flink-risk-web](deep-dive/03-flink-risk-web.md) | Spring Boot 分层、实体别名技巧、三个 Controller、统计实现 | 想改接口或加管理功能 |
| [04 · frontend](deep-dive/04-frontend.md) | Vite 工程、路由布局、请求层、四个页面、字段契约 | 想改界面或排查前后端联调 |

配套参考：`README.md`（快速启动/截图）、`docs/DEPLOY.md`（集群 lib 依赖）、`docs/ARCHITECTURE.md`（初版设计，部分过时）、`CHANGELOG.md`（v2.2~v2.4 的踩坑记录）、`CODEBUDDY.md`（给 AI 助手的仓库速览）。
