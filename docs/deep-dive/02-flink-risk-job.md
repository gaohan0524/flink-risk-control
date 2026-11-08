# 模块精读 02 · flink-risk-job

> Flink 作业模块，整个项目的**核心与主要难点**。它只做一件事：把
> `Kafka 事件 → CEP 模式 → 规则评估 → 三个 Sink` 组装成一条 DAG。
>
> 读完本文应能独立修改 CEP 模式、增加一种事件类型、调整并行度与容错策略。
>
> 返回：[学习总览](../LEARNING-GUIDE.md) · 上一篇：[01 · common](01-flink-risk-common.md) · 下一篇：[03 · web](03-flink-risk-web.md)

---

## 目录

1. [文件清单与职责](#1-文件清单与职责)
2. [作业骨架：`RiskControlApplication.main`](#2-作业骨架riskcontrolapplicationmain)
3. [参数与执行环境](#3-参数与执行环境)
4. [规则广播流](#4-规则广播流)
5. [Kafka 三源接入](#5-kafka-三源接入)
6. [`PatternFactory`：六个 CEP 模式](#6-patternfactory六个-cep-模式)
7. [三个 CepJob 与上下文构造](#7-三个-cepjob-与上下文构造)
8. [合并与广播评估](#8-合并与广播评估)
9. [三路 Sink](#9-三路-sink)
10. [并行度、状态与容错](#10-并行度状态与容错)
11. [故障排查表](#11-故障排查表)
12. [读完后应该能回答的问题](#12-读完后应该能回答的问题)

---

## 1. 文件清单与职责

```
flink-risk-job/src/main/java/com/qinyadan/risk/
├── RiskControlApplication.java       入口：组装 DAG（唯一有 main 的类）
├── kafka/KafkaSourceFactory.java     三个 KafkaSource + 一个通用 Source
├── cep/
│   ├── PatternFactory.java           6 个 CEP Pattern 的唯一来源
│   ├── LoginCepJob.java              登录：失败风暴、异地登录
│   ├── OrderCepJob.java              订单：频繁下单、快速下单
│   ├── ActivityCepJob.java           活动：频繁参与、重复领券
│   ├── PatternMatchResult.java       CEP 输出中间结果（+ context/details）
│   ├── RuleSourceFunction.java       MySQL 规则轮询源（parallelism=1）
│   └── RuleEvaluationFunction.java   广播规则评估（BroadcastProcessFunction）
└── sink/
    ├── RedisSinkFunction.java        Flink 2.x Sink/SinkWriter API
    ├── MysqlSinkFunction.java        RichAsyncFunction（异步 JDBC）
    └── KafkaSinkFactory.java         KafkaSink + JSON 序列化
```

整体 DAG（与代码一一对应）：

```
RuleSourceFunction ──broadcast──┐
                                ▼
login-events   ─keyBy──▶ LoginCepJob    ─┐
order-events   ─keyBy──▶ OrderCepJob    ─┼─union──▶ RuleEvaluationFunction ─┬─▶ RedisSink
activity-events─keyBy──▶ ActivityCepJob ─┘                                 ├─▶ AsyncMySQLSink
                                                                           └─▶ KafkaSink
```

---

## 2. 作业骨架：`RiskControlApplication.main`

`main` 方法被切成 7 段，每段都有注释编号，逐段读即可。

### 2.1 第 3 段：规则广播流

```java
SingleOutputStreamOperator<List<RuleConfig>> ruleStream = env
        .addSource(new RuleSourceFunction(mysqlUrl, mysqlUsername, mysqlPassword, rulePollInterval))
        .setParallelism(1)                 // 只查一次库
        .name("mysql-rule-source");

BroadcastStream<List<RuleConfig>> ruleBroadcastStream = ruleStream
        .broadcast(RuleEvaluationFunction.RULE_STATE_DESCRIPTOR);
```

两个细节：

- 这里用的是**旧版 `addSource`**（`RuleSourceFunction` 实现 legacy `SourceFunction`），与 Kafka 的 `fromSource` 不是一套 API。Flink 2.x 仍保留了 legacy 适配层，但这是明确的待迁移点。
- `setParallelism(1)` 是硬性要求：并行度 >1 会有多个实例各自查库、各自广播冲突的快照。

### 2.2 第 4 段：三个 Kafka 源

以登录为例：

```java
DataStream<LoginEvent> loginStream = env
        .fromSource(
                KafkaSourceFactory.createLoginSource(kafkaBootstrapServers),
                WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, ts) -> event.getTimestamp()),
                "kafka-login-source")
        .name("login-events")
        .setParallelism(2)
        .map(event -> { System.out.println("[SOURCE] LoginEvent received: " + ...); return event; });
```

三源的写法完全对称（`LoginEvent`/`OrderEvent`/`ActivityEvent`），最后的 `map` 纯粹是打日志的旁路算子，没有业务含义。

**`WatermarkStrategy` 是整个作业的生死线**：

- CEP 的 `within(Duration)` 用**事件时间**判断窗口；
- 没有 Watermark（比如误用 `WatermarkStrategy.noWatermarks()`），窗口永远不闭合，模式永远不输出；
- `forBoundedOutOfOrderness(5s)` 表示容忍 5 秒乱序，Watermark = maxEventTime - 5s；
- `withTimestampAssigner` 必须用 `event.getTimestamp()`，不能用 `System.currentTimeMillis()`，否则乱序/回放数据会被算错。

`CHANGELOG.md` v2.3 记录过这个坑：早期用 `noWatermarks()`，CEP 完全不触发。

### 2.3 第 5 段：keyBy + CEP + union

```java
DataStream<PatternMatchResult> loginMatches = LoginCepJob.process(
        loginStream.keyBy((KeySelector<LoginEvent, String>) event -> event.getUserId()));
// order / activity 同理

DataStream<PatternMatchResult> allMatches = loginMatches
        .union(orderMatches, activityMatches)
        .map(result -> { System.out.println("[CEP] Pattern matched: ..."); return result; });
```

- **`keyBy(userId)` 不可省**：CEP 的模式状态是按 key 隔离的（每个用户自己的"连续 3 次失败"），没有 key 就只能全局匹配，语义完全错。
- 这里为了适配 Flink 2.x 的函数式接口，显式把 lambda 强转成 `KeySelector<XxxEvent, String>`，否则与 `ProcessFunction` 的重载会冲突。
- `union` 要求三个流元素类型一致（都是 `PatternMatchResult`），产出的流**不再有 key**（union 会丢 keyBy 的分组），所以后面是 `connect` 广播流而不是 `keyBy().connect()`。

### 2.4 第 6 段：连接广播流

```java
DataStream<RiskResult> riskResults = allMatches
        .connect(ruleBroadcastStream)
        .process(new RuleEvaluationFunction())
        .name("rule-evaluation")
        .map(result -> { System.out.println("[RULE] RiskResult: ..."); return result; });
```

`BroadcastProcessFunction` 的双输入特性：

- 主输入（`allMatches`）只能**读**广播状态（`ReadOnlyBroadcastState`）；
- 广播输入（规则）才能**写**广播状态。

### 2.5 第 7 段：三路 Sink

```java
// 7a Redis：新 Sink API，逐条同步写
riskResults.map(...日志...).sinkTo(new RedisSinkFunction(redisHost, redisPort, redisDb)).name("redis-sink");

// 7b MySQL：异步 IO，保持输入顺序，60s 超时，最多 100 并发
AsyncDataStream.orderedWait(
        riskResults.map(...日志...),
        new MysqlSinkFunction(mysqlUrl, mysqlUsername, mysqlPassword),
        60, TimeUnit.SECONDS, 100)
    .name("mysql-async-sink");

// 7c Kafka：写 risk-results topic
riskResults.map(...日志...).sinkTo(KafkaSinkFactory.createRiskResultSink(kafkaBootstrapServers))
    .name("kafka-risk-result-sink");
```

同一个 `riskResults` 被消费三次，Flink 会把它扇出到三个下游算子，不需要重复计算上游。三处 `.map(...日志...)` 同样只是观测点。

---

## 3. 参数与执行环境

```java
String kafkaBootstrapServers = getArg(args, "--kafka.bootstrap.servers", "localhost:9092");
String mysqlUrl = getArg(args, "--mysql.url", "jdbc:mysql://localhost:3306/flink_risk?...");
String mysqlUsername = getArg(args, "--mysql.username", "root");
String mysqlPassword = getArg(args, "--mysql.password", "root123123");
String redisHost = getArg(args, "--redis.host", "localhost");
int redisPort = Integer.parseInt(getArg(args, "--redis.port", "6379"));
int redisDb = Integer.parseInt(getArg(args, "--redis.db", "0"));
long rulePollInterval = Long.parseLong(getArg(args, "--rule.poll.interval", "30000"));
```

`getArg` 是手写的 `--key value` 线性扫描（要求成对出现，找不到就返回默认值）。

**必须知道的三件事**：

1. `application.properties` **完全没被读取**。文件里写的 `flink.state.backend=rocksdb`、`window.login.size=300000`、`redis.password=` 全是"文档性配置"，改了不会生效。真正的默认值都在上面的 `getArg` 里。
2. **没有 `--redis.password` 参数**，`RedisClient` 也不支持密码 → 对带 `requirepass` 的 Redis 会 `NOAUTH`。
3. 默认库名是 `flink_risk`，而 web 端默认 `risk_control`，两者不一致。

执行环境：

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(4);          // 全局默认 4
env.enableCheckpointing(60000); // 60s 一次 checkpoint
```

没有设置 state backend、没有设置 `restartStrategy`、没有给算子设置 `.uid()`。前两项用 Flink 默认值（哈希 Map 状态 + 固定延迟重启），第三项意味着**从 savepoint 恢复时算子状态可能对不上**——生产化时必须补 `uid()`。

---

## 4. 规则广播流

### 4.1 `RuleSourceFunction`

一个 legacy `SourceFunction<List<RuleConfig>>`：

```java
public void run(SourceContext<List<RuleConfig>> ctx) throws Exception {
    while (running) {
        try {
            List<RuleConfig> rules = fetchRulesFromDB();
            ruleSnapshot.set(rules);
            ctx.collect(rules);                       // 发一份完整快照
            logger.info("Broadcast rules snapshot: {} rules", rules.size());
        } catch (Exception e) {
            logger.error("Failed to fetch rules from MySQL, will retry in {}ms", intervalMs, e);
        }
        Thread.sleep(intervalMs);                     // 30s
    }
}
```

设计意图与代价：

| 设计 | 好处 | 代价 |
|------|------|------|
| 启动立即拉一次 + 定时轮询 | 简单、无需 binlog/CDC | 规则生效有最长 30s 延迟 |
| 每次发全量 `List` | 天然支持删/禁用，无需消息类型 | 每条消息都重新广播整份规则 |
| 异常只记日志不退出 | 数据库抖动不会杀作业 | 长期失败时规则一直是旧的，只有日志能发现 |
| `cancel()` 置 `running=false` | 能优雅退出 | `Thread.sleep` 期间 cancel 需等最多 30s |

查询 SQL 直接映射到 `RuleConfig`：

```java
String sql = "SELECT id, name, expression, risk_level, action, weight, enabled, description " +
             "FROM rule_config WHERE enabled = 1";
```

`RiskLevel.valueOf(rs.getString("risk_level"))` —— 如果库里写了小写 `high` 或 `P0` 之类，会抛 `IllegalArgumentException`，被外层 catch 后**这一轮规则整体加载失败**（不是跳过单条）。改库时注意枚举值必须大写。

### 4.2 广播状态描述符

```java
public static final MapStateDescriptor<Void, List<RuleConfig>> RULE_STATE_DESCRIPTOR =
        new MapStateDescriptor<>(
                "rule-config-state",
                BasicTypeInfo.VOID_TYPE_INFO,                          // key: Void
                TypeInformation.of(new TypeHint<List<RuleConfig>>() {}) // value: List<RuleConfig>
        );
```

为什么 key 是 `Void`：广播状态本质是 `Map`，但这里只需要存"一份快照"，于是用 `null` 作唯一 key（`Void` 无法实例化）。这是 Flink 社区常见写法。

`TypeHint` 匿名子类是为了保留泛型信息——`List<RuleConfig>` 这种嵌套泛型在类型擦除后无法推断，Flink 需要它来选择序列化器。

---

## 5. Kafka 三源接入

`KafkaSourceFactory` 五个常量 + 四个工厂方法：

```java
private static final String LOGIN_TOPIC = "login-events";
private static final String ORDER_TOPIC = "order-events";
private static final String ACTIVITY_TOPIC = "activity-events";
private static final String GROUP_ID = "flink-risk-control-group";   // 三源共用
```

```java
KafkaSource.<LoginEvent>builder()
    .setBootstrapServers(bootstrapServers)
    .setTopics(LOGIN_TOPIC)
    .setGroupId(GROUP_ID)
    .setValueOnlyDeserializer(new LoginEventDeserializer())
    .setStartingOffsets(OffsetsInitializer.earliest())
    .build();
```

要点：

- `earliest`：首次启动从头消费（便于本地调试与回放）；生产通常改 `committed`/`latest`。
- 三个源**共用同一个 group id**：因为订阅的 topic 不同，分区分配与 offset 互不影响；但如果你在外部用同一个 group 消费其中某个 topic，会与作业抢分区。
- `setValueOnlyDeserializer` 意味着忽略消息 key。
- 没有配置 `isolation.level`、`commit.offsets.on.checkpoint`。Flink Kafka Source 默认在 checkpoint 完成时提交 offset，与 `enableCheckpointing(60000)` 配合，最坏情况下故障恢复会重放最近一分钟的数据——这正是 `MysqlSinkFunction` 里"插入"可能重复的原因（表上没有唯一键去重）。

---

## 6. `PatternFactory`：六个 CEP 模式

全部用 `Pattern.begin(...).where(SimpleCondition).followedBy(...).within(Duration)` 构造。

### 6.1 六个模式一览

| 方法 | 事件 | 条件 | 元素数 | 窗口 |
|------|------|------|--------|------|
| `createLoginFailurePattern` | LOGIN | `!event.isSuccess()` | 3 | 60s |
| `createLoginLocationChangePattern` | LOGIN | `event.isSuccess()` | 2 | 60s |
| `createOrderFrequentPattern` | ORDER | `amount != null && amount > 0` | 3 | 60s |
| `createOrderRapidPattern` | ORDER | `amount > 100` | 2 | 30s |
| `createActivityFrequentPattern` | ACTIVITY | `"PARTICIPATE".equals(actionType)` | 4 | 60s |
| `createCouponRepeatPattern` | ACTIVITY | `"CLAIM_COUPON".equals(actionType)` | 3 | 60s |

### 6.2 `followedBy` 与 `next` 的区别（读代码时最容易忽略）

```java
Pattern.<LoginEvent>begin("first")
    .where(...)
    .followedBy("second")     // 宽松连续：两次失败之间允许插入"成功登录"等其它事件
    .where(...)
    .followedBy("third")
    .where(...)
    .within(Duration.ofSeconds(60));
```

| 量词 | 语义 |
|------|------|
| `next` | 严格连续，中间不能有任何不匹配的事件 |
| `followedBy` | 宽松连续，中间允许不匹配的事件 |
| `followedByAny` | 宽松连续且不消耗事件，可产生重叠匹配 |

`CHANGELOG.md` v2.3 把所有 `next` 改成了 `followedBy`，理由是"更符合实际场景"——用户三次失败之间夹一次成功登录，也应该算异常。**但这也意味着更容易产生重复匹配**：5 次失败可能匹配出多组"3 连"，同一用户短时间内收到多条 `RiskResult`。

### 6.3 为什么条件里要判 `amount != null`

`OrderEvent.amount` 是 `BigDecimal`，JSON 里没带该字段时为 `null`。`null.compareTo(...)` 会 NPE，而 `SimpleCondition.filter` 抛异常会导致**整个作业失败**（不是跳过）。所以每个条件都写了 null 判断，属于必须保留的防御。

### 6.4 `within` 的语义与陷阱

- `within(60s)` 表示**从第一个元素到最后一个元素的时间跨度**不超过 60 秒，比较的是事件时间（Watermark 推进依赖它）。
- 窗口过小 → 正常场景也匹配不到；过大 → 状态膨胀、命中滞后。
- 当前值（30~60s）与 `README`、前端文档里写的"5s"不一致，前端展示的是旧值。

---

## 7. 三个 CepJob 与上下文构造

三个类的结构完全一致：`process(KeyedStream<X, String>)` 内部用 `CEP.pattern(input, pattern).select(...)` 注册 N 个 `PatternSelectFunction`，最后 `union`。

```java
public static DataStream<PatternMatchResult> process(KeyedStream<LoginEvent, String> input) {
    Pattern<LoginEvent, ?> failurePattern = PatternFactory.createLoginFailurePattern();
    DataStream<PatternMatchResult> failureRisks = CEP.pattern(input, failurePattern)
            .select(new PatternSelectFunction<LoginEvent, PatternMatchResult>() {
                @Override
                public PatternMatchResult select(Map<String, List<LoginEvent>> pattern) { ... }
            });
    ...
    return failureRisks.union(locationRisks);
}
```

**为什么用匿名内部类而不是 lambda**：`PatternSelectFunction` 在 Flink 2.x 里与其它函数式接口重载冲突，lambda 需要显式类型标注，早期代码用 lambda 强转踩过坑（CHANGELOG v2.3）。匿名内部类最稳。

### 7.1 `Map<String, List<Event>>` 的取值方式

```java
List<LoginEvent> allEvents = new ArrayList<>();
allEvents.addAll(pattern.getOrDefault("first",  Collections.emptyList()));
allEvents.addAll(pattern.getOrDefault("second", Collections.emptyList()));
allEvents.addAll(pattern.getOrDefault("third",  Collections.emptyList()));
```

要点：

- 每个模式名对应的 value 是 **List**（因为量词可以产生多个元素），即使当前每个阶段只有一个元素也必须按 List 处理；
- 用 `getOrDefault` 而不是 `get` + 判空，避免 NPE；
- 顺序：按模式名声明顺序拼接，所以 `allEvents.get(0)` 是最早的事件；
- `CHANGELOG` v2.3 提到曾只取 `pattern.get()` 的部分事件导致数量不对，这里的写法是修复后的版本。

### 7.2 三种事件的 context / details 差异（对照表）

| patternType | context（表达式可用变量） | details（落库/展示） |
|-------------|---------------------------|----------------------|
| `LOGIN_FAILURE` | `eventType` `patternType` `userId` `failCount`(=命中数) `ip` `deviceId` `success` `location` `browser` | `failCount` `lastIp` `lastDeviceId` |
| `LOCATION_CHANGE` | 同上（用第二次登录构造，`failCount=1`）+ `prevIp` `currIp` | `prevIp` `currIp` `prevLocation` `currLocation` |
| `ORDER_FREQUENT` | `eventType` `patternType` `userId` `orderCount`(=命中数) `amount`(最后一笔) `productId` `paymentMethod` + `totalAmount` | `orderCount` `totalAmount(BigDecimal)` |
| `ORDER_RAPID` | 同上（用第二笔构造，`orderCount=1`）+ `intervalMs` | `reason` `intervalMs` `firstOrderId` `secondOrderId` |
| `ACTIVITY_FREQUENT` | `eventType` `patternType` `userId` `participationCount`(命中数) `activityId` `activityName` `actionType` `channel` `source` | `participationCount` `reason` |
| `COUPON_REPEAT` | 同上 | `reason` `couponClaimCount` `couponCode` |

三个必须注意的"反直觉"点：

1. **`LOCATION_CHANGE` 并没有比较 IP**。它只匹配"两次成功登录"，`prevIp/currIp` 只是放进 context 供规则使用。模式名有误导性。
2. **`ORDER_RAPID` 的 `orderCount` 是 1，不是 2**。因为它用 `buildOrderContext(secondList, ...)`——只传了第二个阶段的事件列表。规则里若写 `orderCount >= 2` 会永远不命中。
3. **`context.put("amount", ...)` 用最后一笔**，而 `totalAmount` 才是汇总。规则里 `amount > 10000` 判的是单笔而非总额。

### 7.3 `buildXxxContext` 的统一套路

```java
private static Map<String, Object> buildOrderContext(List<OrderEvent> events, String patternType) {
    Map<String, Object> context = new HashMap<>();
    if (events != null && !events.isEmpty()) {
        OrderEvent latest = events.get(events.size() - 1);       // 取最新一条
        context.put("eventType", "ORDER");
        context.put("patternType", patternType);
        context.put("userId", latest.getUserId());
        context.put("orderCount", events.size());
        context.put("amount", latest.getAmount() != null ? latest.getAmount().doubleValue() : 0);
        context.put("productId", latest.getProductId());
        context.put("paymentMethod", latest.getPaymentMethod());
    }
    return context;
}
```

**每次改动 Pattern 或 context 都要同步两处**：`sql/init.sql` 的规则表达式、前端 `Rule/List.tsx` 的 `EXPRESSION_DOC`。这是本项目最容易产生"规则不生效"的地方。

### 7.4 `PatternMatchResult`

```java
public class PatternMatchResult implements Serializable {
    private String userId;
    private String eventId;        // 每次命中新生成 UUID
    private String eventType;      // LOGIN / ORDER / ACTIVITY
    private String patternType;    // LOGIN_FAILURE / ORDER_RAPID / …
    private Map<String, Object> context;   // 给 Aviator 用
    private Map<String, Object> details;   // 给审计/展示用
}
```

`context` 与 `details` 分离是刻意的：`context` 是"求值输入"（值必须简单、可比较），`details` 是"人看的信息"（可以放 BigDecimal、描述文本）。`RuleEvaluationFunction` 会把两者最终合并进 `RiskResult.details`。

### 7.5 防御式 `return null`

`OrderCepJob` / `ActivityCepJob` / `LoginCepJob` 在 `pattern` 取不到元素时返回 `null`：

```java
if (allEvents.isEmpty()) return null;
```

理论上匹配成功时不会走到这里（模式至少要求每个阶段一个元素），属于兜底。但**返回 null 是有风险的**：`Collector.collect(null)` 在部分 Flink 版本会抛 "Output may not be null"。若要做纯防御，应改成抛一个受控异常或构造空结果对象。

---

## 8. 合并与广播评估

### 8.1 `RuleEvaluationFunction` 结构

```java
public class RuleEvaluationFunction
        extends BroadcastProcessFunction<PatternMatchResult, List<RuleConfig>, RiskResult> {

    private transient volatile AviatorRuleEngine ruleEngine;
    private transient volatile RiskActionResolver resolver;

    @Override public void open(OpenContext ctx) {
        this.ruleEngine = new AviatorRuleEngine(1000, 3600);   // 空引擎
        this.resolver = new RiskActionResolver(ruleEngine);
    }
    @Override public void processElement(PatternMatchResult r, ReadOnlyContext ctx, Collector<RiskResult> out) { ... }
    @Override public void processBroadcastElement(List<RuleConfig> rules, Context ctx, Collector<RiskResult> out) { ... }
}
```

三个泛型依次是：主输入类型、广播输入类型、输出类型。

`volatile` + `transient` 的组合含义：

- `transient`：不参与算子序列化（`AviatorRuleEngine` 里有 `Cache`，无法序列化）；
- `volatile`：广播线程（`processBroadcastElement`）换引擎后，处理线程能立刻看到新引用。

### 8.2 `processElement`：读快照 → 评估，或失败开放

```java
ReadOnlyBroadcastState<Void, List<RuleConfig>> broadcastState = ctx.getBroadcastState(RULE_STATE_DESCRIPTOR);
List<RuleConfig> rules = broadcastState.get(null);      // null 是唯一 key

if (rules == null || rules.isEmpty()) {
    RiskResult result = new RiskResult(r.getUserId(), r.getEventId(), r.getEventType());
    result.setRiskLevel(RiskLevel.LOW);
    result.setRiskScore(0);
    result.setAction("ALLOW");
    result.getDetails().put("reason", "No rules loaded yet");
    result.getDetails().putAll(r.getDetails());
    out.collect(result);
    logger.warn("[EVAL] No rules in broadcast state, output ALLOW for user: {}", r.getUserId());
    return;
}

RiskResult result = resolver.resolve(r.getContext(), r.getUserId(), r.getEventId(), r.getEventType(), r.getDetails());
out.collect(result);
```

"失败开放"（fail-open）的意义：作业刚启动、MySQL 还没返回时，不要把所有命中都当高风险拦掉。代价是**这段窗口期是风控空窗**，需要靠告警补足。

### 8.3 `processBroadcastElement`：全量替换 + 重建引擎

```java
BroadcastState<Void, List<RuleConfig>> broadcastState = ctx.getBroadcastState(RULE_STATE_DESCRIPTOR);
broadcastState.put(null, rules);                        // 全量覆盖

AviatorRuleEngine newEngine = new AviatorRuleEngine(1000, 3600);
newEngine.registerRules(rules);                         // 逐条编译校验
this.ruleEngine = newEngine;                            // 发布新实例
this.resolver = new RiskActionResolver(newEngine);
logger.info("Rule engine refreshed in this task: {} rules loaded", newEngine.getRuleCount());
```

为什么"新建 + 替换"而不是增量更新：

- `AviatorRuleEngine` 内部无锁，增量修改会与求值线程竞争；
- 规则删除/禁用天然生效；
- 广播状态参与 checkpoint，恢复后规则不丢。

代价：每次广播都在每个 subtask 重新编译全部表达式（15 条规则可忽略；上千条时每次 30s 一轮会明显吃 CPU），这是最该优化的点。

---

## 9. 三路 Sink

### 9.1 Redis（Flink 2.x 新 Sink API）

```java
public class RedisSinkFunction implements Sink<RiskResult> {
    @Override
    public SinkWriter<RiskResult> createWriter(WriterInitContext context) throws IOException {
        return new RedisSinkWriter(redisHost, redisPort, redisDatabase, timeout);
    }
}
```

`SinkWriter` 里：

```java
public void write(RiskResult value, Context context) {
    try {
        String key = value.getRedisKey();                 // risk:{eventType}:{userId}:{eventId}
        String jsonValue = value.toJsonString();           // ⚠ 手拼 JSON，见 01 文档
        redisClient.setWithTTL(key, jsonValue, value.getTtl());
    } catch (Exception e) {
        logger.error("Error writing risk result to Redis: ...", e);   // 吞掉，不影响作业
    }
}
```

- `flush()` 空实现（Redis 逐条 SETEX，无缓冲）；
- `close()` 关闭 Jedis 池；
- 异常吞掉 → 写入失败时**作业不会重启**，但也没有重试和告警。

### 9.2 MySQL（异步 IO）

`MysqlSinkFunction extends RichAsyncFunction<RiskResult, Void>`，在 `open()` 里建线程池：

```java
this.executorService = new ThreadPoolExecutor(
        4, 4, 60L, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(10000),
        new ThreadFactoryWithName("mysql-sink-"),
        new ThreadPoolExecutor.DiscardOldestPolicy());     // 队列满时丢最老的任务
```

`asyncInvoke` 每次新建连接：

```java
CompletableFuture.runAsync(() -> {
    try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
         PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
        ps.setString(1, result.getUserId());
        ...
        ps.setString(7, objectMapper.writeValueAsString(result.getDetails()));
        ps.executeUpdate();
        resultFuture.complete(Collections.emptyList());
    } catch (SQLException e) {
        resultFuture.completeExceptionally(e);            // ⚠ 会让作业重启
    }
}, executorService);
```

三个与注释不符的地方（读代码要以代码为准）：

| 注释说的 | 实际做的 |
|----------|----------|
| "内部维护连接池（单连接复用）" | 每次 `DriverManager.getConnection`，**无连接复用** |
| "写入失败不重试，仅记录日志" | `completeExceptionally` 会让异步算子抛异常 → 作业 failover → 从 checkpoint 重放 → 可能重复写 |
| "支持 Exactly-Once" | 插入无唯一键，重放会重复 |

`orderedWait(..., 60s, 100)` 的含义：最多 100 个在途请求，输出保持输入顺序；超时回调 `timeout()` 只记日志并正常完成，不会中断作业。

### 9.3 Kafka（下游）

```java
ProducerConfig.ACKS_CONFIG = "all"
ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG = "true"
ProducerConfig.COMPRESSION_TYPE_CONFIG = "lz4"
ProducerConfig.LINGER_MS_CONFIG = "5"

KafkaSink.<RiskResult>builder()
    .setRecordSerializer(KafkaRecordSerializationSchema.<RiskResult>builder()
        .setTopic("risk-results")
        .setKeySerializationSchema(value -> value.getUserId().getBytes(UTF_8))   // 同用户同分区
        .setValueSerializationSchema(new RiskResultSerializationSchema())        // Jackson
        .build())
    .setKafkaProducerConfig(producerProps)
    .build();
```

用 `userId` 做 key 保证同一用户的结果有序；`acks=all + 幂等` 提供无重复投递保证。

### 9.4 三路对比

| | Redis | MySQL | Kafka |
|---|-------|-------|-------|
| API | `Sink`/`SinkWriter` | `RichAsyncFunction` | `KafkaSink` |
| 阻塞 | 同步（每条一次 SETEX） | 异步（线程池 + `orderedWait`） | 异步（Flink 内部） |
| 失败影响 | 吞掉，作业继续 | 抛异常，作业重启 | 触发 Flink 重试/failover |
| 幂等 | key 相同则覆盖 | **无，可能重复** | 生产者幂等 |
| 用途 | 实时查询 | 审计/后台 | 下游消费 |

---

## 10. 并行度、状态与容错

| 项目 | 值 | 位置 | 说明 |
|------|----|------|------|
| 全局并行度 | 4 | `env.setParallelism(4)` | Kafka 源单独设为 2 |
| Kafka 源并行度 | 2 | `.setParallelism(2)` | 受 topic 分区数限制（compose 默认 3） |
| 规则源并行度 | 1 | `.setParallelism(1)` | 必须 |
| Checkpoint 间隔 | 60s | `env.enableCheckpointing(60000)` | 无超时/最小间隔配置 |
| 状态后端 | 默认（内存哈希） | 未设置 | `application.properties` 里的 `rocksdb` 无效 |
| 精确一次 | 部分具备 | — | Kafka→Flink→Kafka 可靠；MySQL 插入不幂等 |
| 重启策略 | 默认 | 未设置 | 生产建议固定延迟 + 最大次数 |

CEP 状态说明：`CEP.pattern(keyedStream, pattern)` 会在 keyed state 里保存每个用户的部分匹配。`within` 到期后 Flink 会清理过期状态，但如果某用户事件量极大且窗口很长，状态会明显增长——这是 CEP 作业最常见的稳定性风险。

---

## 11. 故障排查表

| 现象 | 先查什么 | 常见根因 |
|------|----------|----------|
| 作业起来但 `[CEP]` 永不出现 | 日志有无 `[SOURCE]`；Flink UI 的 watermark | 用了 `noWatermarks`；事件 `timestamp` 是秒级或 0；`within` 太小 |
| `[CEP]` 有但 `[RULE]` 没有 | `Broadcast rules snapshot: N rules` | 规则库为空；`enabled=0`；`risk_level` 大小写不匹配导致整轮加载失败 |
| `[RULE]` 一直 `ALLOW/LOW` | `reason` 字段 | 规则表达式语法/语义错（`=~` 用了字符串）；context 变量名对不上 |
| Redis 查不到 key | 日志 `NOAUTH` | RedisClient 不支持密码 |
| MySQL 无记录 | 日志 `Failed to write risk result` | 库名/表名不对；`details` JSON 列写入失败；异步超时 |
| 提交集群报 `ClassNotFoundException: KafkaSource`/`CepOperator` | 容器 `lib/` 清单 | 缺 `flink-cep`、`flink-connector-kafka`、`kafka-clients`（见 `docs/DEPLOY.md`） |
| 作业频繁重启 | checkpoint 目录、MySQL 连接数 | 异步 sink 抛异常；MySQL 连接被单条创建耗尽 |
| 同一风险重复记录 | `risk_result` 无唯一键 | checkpoint 重放 + 插入不幂等 |

---

## 12. 读完后应该能回答的问题

1. 为什么 `keyBy(userId)` 不能省？省了会发生什么？
2. 为什么必须配 `WatermarkStrategy`，而 `noWatermarks()` 会让 CEP 完全不工作？
3. `followedBy` 和 `next` 对"5 次连续失败"的匹配结果有什么不同？
4. 规则广播为什么用 `setParallelism(1)`？广播状态为什么用 `null` 作 key？
5. `RuleEvaluationFunction` 里两个字段为什么同时 `transient` 和 `volatile`？
6. `ORDER_RAPID` 的 `orderCount` 为什么是 1 而不是 2？这会影响哪些规则？
7. 三路 Sink 中哪一路失败会导致作业重启？为什么？
8. 如果要新增"凌晨大额下单"模式，需要改哪些文件？（`PatternFactory` → `OrderCepJob` → `sql/init.sql` → 前端文档）

### 动手练习

- **练习 1**：把 `createLoginFailurePattern` 的 `within` 从 60s 改成 5s，用 `event-simulator.sh login_failure` 观察是否还能命中，理解 Watermark 与事件间隔的关系。
- **练习 2**：新增 `ORDER_OVERNIGHT` 模式（同一用户 2 笔订单，第二笔 `timestamp` 在 0~6 点），补齐 context（`orderCount`、`hour`），并在 `sql/init.sql` 加一条对应规则，跑通端到端。
- **练习 3**：给 `RuleSourceFunction` 加"规则条数变化才广播"的判重逻辑，观察日志里 `Broadcast rules snapshot` 的频率变化。
- **练习 4**：把 MySQL sink 改为失败不传播（`complete(emptyList())` + 告警日志），对比作业重启次数。
- **练习 5**：为所有算子加 `.uid("xxx")`，从 savepoint 恢复一次，理解为什么生产必须加。

---

下一个模块：[03 · flink-risk-web](03-flink-risk-web.md)
