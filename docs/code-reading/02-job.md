# 代码精读（详解版）· flink-risk-job

> 本文件逐类、逐方法精读 Flink 作业模块。阅读前提：已读完
> [common 详解](01-common.md)（事件模型、规则引擎、RiskResult）。
>
> 全模块 12 个 Java 文件，拆成 **7 层、22 讲**。顺序遵循数据流动方向：
> 入口 → Source → Pattern → CEP 算子 → 广播评估 → Sink → 端到端复盘。
>
> 相关文档：[common 详解](01-common.md) · [web 详解](03-web.md) · [学习总览](../LEARNING-GUIDE.md)

---

## 阅读地图

```
第 1 层  入口与执行环境     RiskControlApplication.main（参数、env、并行度、checkpoint）
              │
第 2 层  Source 层          KafkaSourceFactory · 三个反序列化器 · RuleSourceFunction
              │
第 3 层  Pattern 层         PatternFactory（6 个模式 + CEP 语义）
              │
第 4 层  CEP 算子层         LoginCepJob · OrderCepJob · ActivityCepJob · PatternMatchResult
              │
第 5 层  评估层             RuleEvaluationFunction（BroadcastProcessFunction）
              │
第 6 层  Sink 层            RedisSinkFunction · MysqlSinkFunction · KafkaSinkFactory
              │
第 7 层  复盘               端到端日志轨迹 · 调优 · 排错 · 练习
```

### 层划分与顺序理由

| 层 | 讲号 | 文件 | 为什么这个顺序 |
|----|------|------|----------------|
| 1 入口 | 1-4 | `RiskControlApplication` | 先看 DAG 全貌，知道每个类被挂在哪 |
| 2 Source | 5-7 | `KafkaSourceFactory`、`RuleSourceFunction` | 数据入口；规则源与事件源要分开理解 |
| 3 Pattern | 8-10 | `PatternFactory` | 模式定义决定了后面所有 context 的形态 |
| 4 CEP 算子 | 11-14 | 三个 `XxxCepJob`、`PatternMatchResult` | 模式如何被消费、上下文如何组装 |
| 5 评估 | 15-17 | `RuleEvaluationFunction` | 两条流在此汇合，是作业的"大脑" |
| 6 Sink | 18-20 | 三个 Sink | 结果如何落地 |
| 7 复盘 | 21-22 | — | 串成一次完整运行，检验理解 |

### 先记住三句话

1. **CEP 只判断"事件序列像不像异常"，规则决定"算多严重、怎么处置"。**
2. **`keyBy(userId)` 决定 CEP 状态按用户隔离；`within()` 决定状态何时清理；Watermark 决定窗口何时闭合。**
3. **规则走广播状态（全量快照 + 整体替换引擎），事件走正常数据流（逐条处理）。**

---

# 第 1 层 · 入口与执行环境（讲 1-4）

## 讲 1 · `main` 的 7 段结构与 DAG 全貌

文件：`flink-risk-job/src/main/java/com/qinyadan/risk/RiskControlApplication.java`

`main` 被注释显式切成 7 段：

| 段 | 代码位置 | 产物 | 输出类型 |
|----|----------|------|----------|
| 1 读取配置 | 方法开头 | 8 个变量 | — |
| 2 创建环境 | — | `StreamExecutionEnvironment` | — |
| 3 规则广播流 | — | `BroadcastStream<List<RuleConfig>>` | 规则 |
| 4 Kafka 三源 | — | `DataStream<LoginEvent>` 等 | 事件 |
| 5 CEP 匹配 | — | `allMatches` | `PatternMatchResult` |
| 6 广播评估 | — | `riskResults` | `RiskResult` |
| 7 三路输出 | — | 三个 Sink 算子 | — |

完整的算子拓扑（名字与 `.name()` 对应）：

```
              ┌──────────────────────┐
              │ mysql-rule-source    │  parallelism=1, legacy addSource
              └──────────┬───────────┘
                         │ broadcast(RULE_STATE_DESCRIPTOR)
┌──────────────────┐     │
│ kafka-login-     │     │
│ source (p=2)     │     │
└────────┬─────────┘     │
         │ keyBy(userId) │
         ▼               │
   ┌───────────┐         │
   │ LoginCep  │──┐      │
   └───────────┘  │      │
┌──────────────────┐     │
│ kafka-order-     │     │
│ source (p=2)     │     │
└────────┬─────────┘     │
         ▼               │
   ┌───────────┐         │
   │ OrderCep  │──┼──union──▶ ┌──────────────────┐
   └───────────┘  │          │ rule-evaluation  │
┌──────────────────┐        │ (BroadcastProcess)│
│ kafka-activity-  │        └────────┬─────────┘
│ source (p=2)     │                 │
└────────┬─────────┘                 │
         ▼                           ▼
   ┌────────────┐          ┌────────────────────┐
   │ ActivityCep│─────────▶│ 三个 Sink 并行扇出  │
   └────────────┘          └────────────────────┘
```

关键结构认知：

- **两个"流"**：事件流（无界、逐条）与规则流（周期、全量快照）；
- **一次汇合**：`connect` 把两者接起来，只有广播侧能写规则状态；
- **一次扇出**：`riskResults` 被三个 Sink 同时消费，上游不重复计算。

## 讲 2 · 参数解析 `getArg`

```java
private static String getArg(String[] args, String key, String defaultValue) {
    for (int i = 0; i < args.length - 1; i++) {     // ①
        if (args[i].equals(key)) {
            return args[i + 1];
        }
    }
    return defaultValue;
}
```

逐行：

- **① `i < args.length - 1`**：保证 `args[i+1]` 不越界。因此**最后一个参数如果只有 key 没有 value，会被忽略**。
- 返回**第一个**匹配项，同名参数无法覆盖。
- 完全没做类型校验：`Integer.parseInt(getArg(...))` 遇到 `--redis.port abc` 会抛 `NumberFormatException`，作业启动即失败（可接受）。

调用汇总与默认值：

| 参数 | 默认值 | 去向 |
|------|--------|------|
| `--kafka.bootstrap.servers` | `localhost:9092` | 三源 + Kafka Sink |
| `--mysql.url` | `jdbc:mysql://localhost:3306/flink_risk?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true` | 规则源 + MySQL Sink |
| `--mysql.username` | `root` | 同上 |
| `--mysql.password` | `root123123` | 同上 |
| `--redis.host` | `localhost` | Redis Sink |
| `--redis.port` | `6379` | Redis Sink |
| `--redis.db` | `0` | Redis Sink |
| `--rule.poll.interval` | `30000` | 规则源 |

**缺失的参数**：`--redis.password`（`RedisClient` 也不支持）、`--kafka.group.id`（写死在工厂里）、`--flink.parallelism`。

**不要被 `resources/application.properties` 误导**：那个文件里的 `flink.parallelism=4`、`kafka.bootstrap.servers=127.0.0.1:29092`、`rule.hot.reload.interval=60000` 等**一行都不会被读取**——`main` 里没有 `ParameterTool.fromPropertiesFile` 之类的调用。它只是一份"配置意图说明"。

## 讲 3 · 执行环境：并行度与 Checkpoint

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(4);
env.enableCheckpointing(60000);
```

### 3.1 `getExecutionEnvironment()` 的三种行为

| 运行方式 | 返回 |
|----------|------|
| IDEA 直接跑 main | 本地环境（多线程模拟并行） |
| `flink run` 提交 | 远程环境（连 JobManager） |
| 单测 `StreamExecutionEnvironment.getExecutionEnvironment()` | 本地环境 |

### 3.2 `setParallelism(4)` 的生效范围

这是**算子默认并行度**，会被后续 `.setParallelism(n)` 覆盖：

| 算子 | 实际并行度 | 原因 |
|------|-----------|------|
| `mysql-rule-source` | 1 | 显式设置 |
| 三个 Kafka Source | 2 | 显式设置 |
| 三个 Source 后的 `map`（日志） | 2 | 继承上游 |
| CEP 算子 | 4 | 全局默认 |
| `union` 后的 `map` | 4 | 全局默认 |
| `rule-evaluation` | 4 | 全局默认 |
| 三个 Sink | 4 | 全局默认 |

**并行度不匹配会怎样**：Flink 会自动做 rebalance（`keyBy` 用 hash，普通边用轮询），不会报错。

**规则源的并行度必须是 1**：如果并行度 >1，每个实例会各自查库、各自发快照、各自广播，`broadcast` 之后每个下游会收到 N 份重复规则（最后一份生效，但白白浪费一次全量替换与引擎重建）。

### 3.3 `enableCheckpointing(60000)`

- 每 60 秒触发一次 checkpoint；
- **没有**指定 `CheckpointingMode`（默认 EXACTLY_ONCE）；
- **没有**配置 checkpoint 存储目录（默认用 JobManager 内存，重启后状态丢失）；
- **没有**配置 `minPauseBetweenCheckpoints`、`checkpointTimeout`、`tolerableCheckpointFailureNumber`；
- **没有**设置 state backend（默认 HashMapStateBackend，状态在 TaskManager 内存）。

结论：**这套配置只能支撑本地演示**。生产至少要补：

```java
env.setStateBackend(new EmbeddedRocksDBStateBackend());
env.getCheckpointConfig().setCheckpointStorage("hdfs:///flink/checkpoints");
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000);
env.getCheckpointConfig().setCheckpointTimeout(10 * 60_000);
env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
```

## 讲 4 · 算子命名与一句 `execute`

```java
env.execute("flink-risk-control-job");
```

- 参数是**作业名**，会显示在 Flink UI 与 YARN/K8s 应用名里；
- `execute` 是阻塞调用（本地环境阻塞到作业结束）；
- 三个 Sink 都只是"接线"，真正的执行从这里开始。

### 4.1 `.name()` 与 `.uid()` 的区别（本项目只有前者）

| 方法 | 作用 | 本项目 |
|------|------|--------|
| `.name("xxx")` | 给算子起显示名 | ✅ 每个关键算子都有 |
| `.uid("xxx")` | 给算子固定 ID，**savepoint 恢复依赖它** | ❌ 一个都没有 |

没有 `uid` 时，Flink 用"算子拓扑位置 + hash"生成 ID。**只要改动 DAG 结构（哪怕只是加一个 map），从旧 savepoint 恢复就会失败或状态错位**。这是本项目最重要的生产化缺口之一。

### 4.2 日志旁路算子

三处 `.map(...)` 只做 `System.out.println`：

```java
.map(event -> {
    System.out.println("[SOURCE] LoginEvent received: user=" + event.getUserId() + ...);
    return event;
})
```

评价：

- 优点：不改变数据类型，是最廉价的观测点；
- 缺点：**用 `System.out` 而非 logger**，绕过了 Log4j2（无法滚动、无法按级别过滤、容器里可能进不了日志文件）；且 `map` 会引入一次无谓的序列化边界（虽然 Flink 会做算子链优化，通常能 chain 掉）。
- 规范做法：`logger.info(...)`，或者用 Flink 的 metrics 计数器。

---

# 第 2 层 · Source 层（讲 5-7）

## 讲 5 · `KafkaSourceFactory` 五个常量与四个方法

文件：`kafka/KafkaSourceFactory.java`

```java
private static final String LOGIN_TOPIC = "login-events";
private static final String ORDER_TOPIC = "order-events";
private static final String ACTIVITY_TOPIC = "activity-events";
private static final String GROUP_ID = "flink-risk-control-group";
```

三个专用工厂方法完全同构，以登录为例：

```java
public static KafkaSource<LoginEvent> createLoginSource(String bootstrapServers) {
    return KafkaSource.<LoginEvent>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(LOGIN_TOPIC)
            .setGroupId(GROUP_ID)
            .setValueOnlyDeserializer(new LoginEventDeserializer())
            .setStartingOffsets(OffsetsInitializer.earliest())
            .build();
}
```

### 5.1 每个 setter 的含义与影响

| 配置 | 值 | 影响 |
|------|----|------|
| `setBootstrapServers` | 启动参数 | 连接地址 |
| `setTopics` | 单 topic | 一个 Source 只订阅一个 topic |
| `setGroupId` | 三源共用 | 消费组；外部同组会抢分区 |
| `setValueOnlyDeserializer` | 专用反序列化器 | **忽略消息 key**，只用 value |
| `setStartingOffsets` | `earliest` | 首次启动从头消费；有 committed offset 时优先用 committed |

**没有配置**：

| 缺失项 | 默认行为 | 生产建议 |
|--------|----------|----------|
| `setProperty("partition.discovery.interval.ms", ...)` | 不自动发现新分区 | 显式开启，否则扩分区后新分区不被消费 |
| `setProperty("isolation.level")` | Kafka 消费者默认 `read_uncommitted` | 事务型生产端必须设为 `read_committed`，否则会读到未提交消息 |
| `setProperty("commit.offsets.on.checkpoint")` | `true` | 与 checkpoint 绑定，可接受 |
| `setBounded(...)` | 无界 | — |
| `setStartingOffsets(OffsetsInitializer.committedOffsets(...))` | 用 `earliest` | 生产更常用 `committedOffsets(OffsetResetStrategy.LATEST)` |

### 5.2 为什么用 `KafkaSource` 而不是 `FlinkKafkaConsumer`

Flink 2.x 已移除 `FlinkKafkaConsumer`，`KafkaSource` 是新 API：

```java
env.fromSource(source, watermarkStrategy, "source-name")    // 新
env.addSource(new FlinkKafkaConsumer<>(...))                // 旧，已不可用
```

`fromSource` 必须传 `WatermarkStrategy`（可以传 `noWatermarks()`，但 CEP 会废掉，见讲 10）。

## 讲 6 · 三个反序列化器如何被调用

调用链：

```
KafkaSource.createLoginSource(...)
   └─ setValueOnlyDeserializer(new LoginEventDeserializer())
         │
         └─ 每个 Kafka 分区消息 → deserialize(byte[]) → LoginEvent
```

要点：

1. **实例化时机**：`new LoginEventDeserializer()` 发生在**作业构建期**（`main` 里），对象会被序列化进算子；
2. **调用时机**：运行期每个 subtask、每条消息调用一次 `deserialize`；
3. **线程安全**：每个 subtask 单线程消费自己的分区，`ObjectMapper` 无竞争；
4. **异常语义**：`deserialize` 抛异常 → Source 失败 → 作业按重启策略重启（默认固定延迟 1s，无限次）。

**反序列化失败的一个典型场景**：生产端发了一条空消息（Kafka tombstone），`readValue(new byte[0], ...)` 抛 `MismatchedInputException`，作业挂。需要的话可在 `deserialize` 里判空返回 null，并在下游 `filter(Objects::nonNull)`。

## 讲 7 · `RuleSourceFunction` 精读

文件：`cep/RuleSourceFunction.java`

```java
public class RuleSourceFunction implements SourceFunction<List<RuleConfig>> {
```

注意 import：

```java
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
```

这是 Flink 2.x 里的 **legacy 包**（1.x 时代在 `...functions.source.SourceFunction`）。所以 `RiskControlApplication` 用 `env.addSource(...)` 而不是 `fromSource(...)`。

### 7.1 字段

```java
private final AtomicReference<List<RuleConfig>> ruleSnapshot = new AtomicReference<>(new ArrayList<>());
private final String jdbcUrl;
private final String username;
private final String password;
private final long intervalMs;
private volatile boolean running = true;
```

| 字段 | 为什么 | 问题 |
|------|--------|------|
| `ruleSnapshot` | 提供 `getRuleSnapshot()` 给外部读 | 实际无调用方（广播流才是真数据源） |
| `jdbcUrl/username/password` | JDBC 连接三件套 | 明文存于算子闭包，会出现在 Flink UI 的算子配置里 |
| `intervalMs` | 轮询间隔 | — |
| `running` | `cancel()` 用它停循环 | `Thread.sleep` 期间 cancel 需等待最多 intervalMs |

### 7.2 `run` 方法逐行

```java
@Override
public void run(SourceContext<List<RuleConfig>> ctx) throws Exception {
    while (running) {                                    // ①
        try {
            List<RuleConfig> rules = fetchRulesFromDB(); // ②
            ruleSnapshot.set(rules);
            ctx.collect(rules);                          // ③
            logger.info("Broadcast rules snapshot: {} rules", rules.size());
        } catch (Exception e) {
            logger.error("Failed to fetch rules from MySQL, will retry in {}ms", intervalMs, e);  // ④
        }
        Thread.sleep(intervalMs);                        // ⑤ 在 try 之外
    }
}
```

逐行：

- **① `while (running)`**：`running` 是 `volatile`，`cancel()` 线程写入后本线程能立刻看到（可见性保证）。
- **② `fetchRulesFromDB()`**：每次新建 JDBC 连接（见 7.3）。
- **③ `ctx.collect(rules)`**：把整份列表作为**一个元素**发出。下游 `broadcast` 后再展开给所有 subtask。
- **④ catch 包住整轮**：数据库抖动只记 error，循环继续。**但注意**：如果 `RiskLevel.valueOf` 因大小写不匹配抛异常，这一轮**所有规则**都丢弃（不是跳过单条），要等下一轮修复后才有规则。
- **⑤ `Thread.sleep` 在 try 之外**：即使本轮异常，也会等待 intervalMs 再重试，避免故障时疯狂重连打爆数据库。合理。
- **`Thread.sleep` 不可中断**：`cancel()` 只置 flag，主线程要睡满 30 秒才退出。`SourceFunction` 的契约不要求立即返回，但优雅停机时会被拖慢。

### 7.3 `fetchRulesFromDB` 逐行

```java
private List<RuleConfig> fetchRulesFromDB() throws SQLException {
    List<RuleConfig> rules = new ArrayList<>();

    String sql = "SELECT id, name, expression, risk_level, action, weight, enabled, description " +
                 "FROM rule_config WHERE enabled = 1";

    try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

        while (rs.next()) {
            RuleConfig config = new RuleConfig();
            config.setId(String.valueOf(rs.getLong("id")));
            config.setName(rs.getString("name"));
            config.setExpression(rs.getString("expression"));
            config.setRiskLevel(RiskLevel.valueOf(rs.getString("risk_level")));
            config.setAction(rs.getString("action"));
            config.setWeight(rs.getInt("weight"));
            config.setEnabled(rs.getBoolean("enabled"));
            config.setDescription(rs.getString("description"));
            rules.add(config);
        }
    }
    return rules;
}
```

细节分析：

| 行 | 说明 |
|----|------|
| `DriverManager.getConnection` | **每次轮询新建连接**，30 秒一次可接受；没有连接池，也没有 `connectTimeout` 参数 |
| `createStatement()` + 字符串 SQL | 没有用户输入拼接，无注入风险；用 `PreparedStatement` 也带不来收益 |
| `WHERE enabled = 1` | 与 `registerRule` 的 enabled 检查构成双保险 |
| `rs.getLong("id")` → `String.valueOf` | 呼应 `RuleConfig.id` 是 String |
| `RiskLevel.valueOf(...)` | **大小写敏感**，失败抛 `IllegalArgumentException` → 整轮失败 |
| `getBoolean("enabled")` | MySQL `TINYINT(1)` → true/false |
| 未读取 `create_time`/`update_time` | `RuleConfig` 的这两个字段永远是构造时刻 |
| 未处理 `expression` 为 NULL | `registerRule` 里 `compile(null)` 行为未定义 |

### 7.4 `cancel` 与 `getRuleSnapshot`

```java
@Override
public void cancel() { running = false; }

public List<RuleConfig> getRuleSnapshot() { return ruleSnapshot.get(); }
```

`getRuleSnapshot()` 是给"非广播场景"准备的（例如某个算子直接查最新规则），当前无调用方。

### 7.5 这段代码的三个改进点

1. **用 `RichSourceFunction` + 连接池**（HikariCP），避免每 30 秒建连；
2. **单条规则容错**：`try { valueOf } catch { log + continue }`，避免一条脏数据毁掉整轮；
3. **变更检测**：对 `List<RuleConfig>` 做内容哈希，只有变化时才 `collect`，减少下游重建引擎次数。

---

# 第 3 层 · Pattern 层（讲 8-10）

> 读这一层前先建立 CEP 的三个基本概念：**模式（Pattern）= 状态机**、**量词（Quantifier）= 状态转移条件**、**窗口（within）= 状态存活时间**。

## 讲 8 · CEP API 与 `Pattern` 的构造

一个 Pattern 的最小构成：

```java
Pattern.<LoginEvent>begin("first")        // ① 起始状态，命名 "first"
    .where(condition)                     // ② 该状态的过滤条件
    .followedBy("second")                 // ③ 下一个状态 + 量词
    .where(condition)
    .within(Duration.ofSeconds(60));      // ④ 整个模式的窗口
```

逐项：

| 元素 | 作用 | 本项目取值 |
|------|------|-----------|
| `begin(name)` | 定义第一个状态并命名 | `"first"` |
| `where(condition)` | 状态过滤，返回 `boolean` | `SimpleCondition` 匿名类 |
| `followedBy(name)` | **宽松连续**：中间可有其它事件 | 全部用它 |
| `next(name)` | **严格连续**：中间不能有别的事件 | 未使用 |
| `within(Duration)` | 从首元素到末元素的时间跨度上限 | 30~60s |

### 8.1 `followedBy` / `next` / `followedByAny` 的精确差异

假设事件序列：`fail fail success fail`（A、B 为失败，C 为成功，D 为失败）

| 量词 | 能否匹配 `fail fail fail`（A、B、D） | 说明 |
|------|--------------------------------------|------|
| `next` | ❌ | 因为 B 与 D 之间夹了 C（成功），严格连续被破坏 |
| `followedBy` | ✅ | 允许中间插入不匹配事件 |
| `followedByAny` | ✅ 且可能产生多组 | 不消耗事件，可重叠匹配 |

`CHANGELOG.md` v2.3 把所有 `next` 改成 `followedBy`，说明早期版本会漏掉"中间夹一次成功登录"的真实攻击场景。**代价**：宽松连续更容易产生"滑动窗口式"的重复匹配——5 次失败可能匹配出 3 组 3 连（位置 123、234、345），同一用户会收到多条 `RiskResult`。

### 8.2 为什么用匿名类而不是 lambda

```java
.where(new SimpleCondition<LoginEvent>() {
    @Override
    public boolean filter(LoginEvent event) {
        return !event.isSuccess();
    }
})
```

`SimpleCondition<T>` 是抽象类（不是函数式接口），Flink 2.x 中它与其他重载结合时 lambda 推断容易失败，因此项目统一用匿名类。CHANGELOG v2.3 记录了"lambda 强转"踩坑。

### 8.3 条件抛异常的后果

`filter` 内抛异常会**直接让作业失败**（CEP 算子不捕获条件异常）。所以所有涉及可能为 null 的字段都必须判空：

```java
return event.getAmount() != null && event.getAmount().compareTo(BigDecimal.ZERO) > 0;
```

缺少 `!= null` 就是一颗定时炸弹——一条没有 `amount` 的订单消息足以打挂作业。

## 讲 9 · 六个 Pattern 逐条拆解

文件：`cep/PatternFactory.java`

### 9.1 `createLoginFailurePattern`（60s / 3 次失败）

```java
return Pattern.<LoginEvent>begin("first")
        .where(e -> !e.isSuccess())      // 实际是匿名类
        .followedBy("second").where(同一个条件)
        .followedBy("third").where(同一个条件)
        .within(Duration.ofSeconds(60));
```

| 属性 | 值 |
|------|----|
| 状态名 | `first` / `second` / `third` |
| 条件 | `!event.isSuccess()` |
| 语义 | 60 秒内出现 3 次失败（中间可有成功） |
| 输出 | `LOGIN_FAILURE`，`failCount = 命中事件数 = 3` |

**为什么不用 `times(3)`**：`Pattern.times(3)` 会生成 `times` 状态，`select` 里取到的 key 不是 `first/second/third`，取值逻辑要重写。手写三次虽然啰嗦，但 `pattern.get("first")` 等取值方式直观。

**条件重复了三次**：三个 `SimpleCondition` 是三个独立对象（各 new 一次）。提取成一个静态常量会更省内存，但 CEP 会序列化 Pattern，共享实例没有副作用。

### 9.2 `createLoginLocationChangePattern`（60s / 2 次成功）

```java
.where(e -> e.isSuccess()).followedBy("second").where(e -> e.isSuccess())
.within(Duration.ofSeconds(60));
```

**模式名骗人**：它只判断"两次成功登录"，完全不比较 IP/地点。真正的"异地"判断只能放在规则表达式里（`prevIp != currIp`），而 `prevIp/currIp` 是在 `LoginCepJob` 里塞进 context 的。

副作用：**任何用户在 60 秒内登录两次成功**都会命中 `LOCATION_CHANGE`，然后被规则 2（`patternType == "LOCATION_CHANGE"` → MEDIUM/VERIFY）判定为中风险。这是一个明显的误报源。正确做法是给 Pattern 加一个 `IterativeCondition` 比较前后事件的 IP：

```java
.followedBy("second").where(new IterativeCondition<LoginEvent>() {
    @Override
    public boolean filter(LoginEvent curr, Context<LoginEvent> ctx) throws Exception {
        for (LoginEvent prev : ctx.getEventsForPattern("first")) {
            if (prev.getIp() != null && !prev.getIp().equals(curr.getIp())) return true;
        }
        return false;
    }
})
```

### 9.3 `createOrderFrequentPattern`（60s / 3 笔金额 > 0）

```java
.where(e -> e.getAmount() != null && e.getAmount().compareTo(BigDecimal.ZERO) > 0)
```

- 条件宽松到几乎没有过滤作用（任何正金额订单都算）；
- 真正的"频繁"阈值留给规则（`orderCount >= 3`）；
- `BigDecimal.ZERO` 比较避免 `double` 精度问题。

### 9.4 `createOrderRapidPattern`（30s / 2 笔金额 > 100）

```java
.where(amount > 100).followedBy("second").where(amount > 100).within(Duration.ofSeconds(30));
```

- 窗口 30 秒（其余都是 60 秒）；
- **金额门槛写死在代码里**，规则无法调整（规则只能看 `intervalMs`）。要改成可配置，需要把门槛移到规则里，或从配置注入。

### 9.5 `createActivityFrequentPattern`（60s / 4 次 PARTICIPATE）

```java
.where("PARTICIPATE".equals(e.getActionType())) × 4 个状态（first~fourth）
.within(Duration.ofSeconds(60));
```

注意 `"PARTICIPATE".equals(...)` 把常量写在前面，避免 `actionType` 为 null 时 NPE。这是正确的写法。

### 9.6 `createCouponRepeatPattern`（60s / 3 次 CLAIM_COUPON）

```java
.where("CLAIM_COUPON".equals(e.getActionType())) × 3
.within(Duration.ofSeconds(60));
```

### 9.7 六个模式的横向对比

| 方法 | 事件 | 状态数 | 窗口 | 条件严格度 | 业务名 |
|------|------|--------|------|-----------|--------|
| `createLoginFailurePattern` | LOGIN | 3 | 60s | 中（只看 success） | 登录失败风暴 |
| `createLoginLocationChangePattern` | LOGIN | 2 | 60s | 极松（任何两次成功） | 异地登录 |
| `createOrderFrequentPattern` | ORDER | 3 | 60s | 松（正金额） | 频繁下单 |
| `createOrderRapidPattern` | ORDER | 2 | 30s | 中（金额 >100） | 快速下单 |
| `createActivityFrequentPattern` | ACTIVITY | 4 | 60s | 中（actionType） | 频繁参与 |
| `createCouponRepeatPattern` | ACTIVITY | 3 | 60s | 中（actionType） | 重复领券 |

**共同缺陷**：`customerId`/`deviceId`/`activityId` 维度都没有参与 Pattern——所有模式都只在 `userId` 维度聚合（因为上游 `keyBy(userId)`）。"同设备多账号薅羊毛"这类场景覆盖不到。

## 讲 10 · `within` 与 Watermark 的因果关系

这是全项目最容易"看起来没 bug 但就是不工作"的地方。

### 10.1 CEP 为什么需要时间

`within(60s)` 要回答："从第一个匹配元素到现在，是否超时？"——超时就要丢弃这个部分匹配，否则状态永久累积。判断"现在"依赖**事件时间**，而事件时间靠 Watermark 推进。

```
事件时间 ──▶ Watermark（maxEventTime - 5s 乱序容忍）──▶ 窗口闭合/超时清理
```

### 10.2 三种 Watermark 策略对本项目的影响

| 策略 | 结果 |
|------|------|
| `WatermarkStrategy.noWatermarks()` | Watermark 永远 `Long.MIN_VALUE`，`within` 永不超时 → **状态只增不减，且匹配结果永远不输出**（CEP 需要在时间推进时确认"不再有新元素"） |
| `forMonotonousTimestamps()` | 假设事件时间严格递增，乱序数据会被丢弃 |
| `forBoundedOutOfOrderness(5s)` ✅ | 容忍 5 秒乱序，Watermark = maxTs - 5s |

CHANGELOG v2.3 的 bug 就是第一种：CEP 完全不触发。

### 10.3 `withTimestampAssigner` 的必要性

```java
.withTimestampAssigner((event, ts) -> event.getTimestamp())
```

如果省略，Flink 用记录的 Kafka 时间戳（broker 写入时间）作为事件时间。回放历史数据时，所有事件时间都是"现在"，`within(60s)` 会把几分钟前的历史事件当成"同一时刻"，产生大量误匹配。**必须用消息体里的业务时间。**

### 10.4 数值不匹配的风险

| 情况 | 表现 |
|------|------|
| `timestamp` 是秒（10 位） | 事件时间落到 1970 年，Watermark 极低，窗口永不闭合 → CEP 不触发 |
| `timestamp` 是毫秒（13 位） ✅ | 正常 |
| `timestamp` 为 0/缺失 | 同上，所有事件堆积在时间 0 |
| 乱序超过 5 秒 | 迟到事件被丢弃（默认 `AllowedLateness=0`），该事件不参与匹配 |

### 10.5 窗口值的选择权衡

| 窗口 | 优点 | 缺点 |
|------|------|------|
| 5s（早期版本） | 状态小、命中快 | 人工操作很难在 5 秒内连点 3 次，几乎不触发 |
| 30~60s（当前） | 符合真实行为 | 状态更大、命中更滞后 |
| 5min+ | 覆盖慢速攻击 | CEP 状态可能膨胀到 GB 级 |

**状态量估算**：每个 userId 保存一个部分匹配（最多 3~4 个事件对象）。100 万活跃用户 × 4 事件 × ~500 字节 ≈ 2GB。这就是为什么生产环境建议用 RocksDB + 合理窗口。

---

# 第 4 层 · CEP 算子层（讲 11-14）

## 讲 11 · 三个 CepJob 的统一骨架

文件：`cep/LoginCepJob.java`、`OrderCepJob.java`、`ActivityCepJob.java`

```java
public static DataStream<PatternMatchResult> process(KeyedStream<LoginEvent, String> input) {
    Pattern<LoginEvent, ?> p1 = PatternFactory.createXxxPattern();
    DataStream<PatternMatchResult> r1 = CEP.pattern(input, p1).select(new PatternSelectFunction<...>() {
        @Override
        public PatternMatchResult select(Map<String, List<LoginEvent>> pattern) { ... }
    });

    Pattern<LoginEvent, ?> p2 = PatternFactory.createYyyPattern();
    DataStream<PatternMatchResult> r2 = CEP.pattern(input, p2).select(...);

    return r1.union(r2);
}
```

三个设计要点：

1. **入参是 `KeyedStream` 而不是 `DataStream`**：`CEP.pattern` 的签名要求 keyed 流，类型层面强制你 `keyBy`。
2. **同一个 `input` 被使用两次**：Flink 会把同一个上游扇出给两个 CEP 算子，事件被复制两份（各自维护状态）。
3. **`union` 要求元素类型相同**：两个流都是 `PatternMatchResult`，所以能 union。union 后**丢失 key 信息**（不再分区），这对下游广播函数无影响。

### 11.1 为什么拆成三个类而不是一个

| 方案 | 优点 | 缺点 |
|------|------|------|
| 一个类里注册 6 个 Pattern | 少两个类 | 一个文件 300+ 行，且不同事件类型混在一起 |
| 三个类（当前） | 按事件类型内聚，加新模式只改一个文件 | 类之间高度重复 |

如果要做"配置化 CEP"（从库里读模式定义），这三个类会被一个通用工厂取代。

## 讲 12 · `LOGIN_FAILURE` 的 select 回调逐行

```java
.select(new PatternSelectFunction<LoginEvent, PatternMatchResult>() {
    @Override
    public PatternMatchResult select(Map<String, List<LoginEvent>> pattern) {
        List<LoginEvent> allEvents = new ArrayList<>();
        allEvents.addAll(pattern.getOrDefault("first", Collections.emptyList()));
        allEvents.addAll(pattern.getOrDefault("second", Collections.emptyList()));
        allEvents.addAll(pattern.getOrDefault("third", Collections.emptyList()));

        LoginEvent firstEvent = allEvents.get(0);

        PatternMatchResult result = new PatternMatchResult(
                firstEvent.getUserId(), "LOGIN", "LOGIN_FAILURE");
        result.setEventId(UUID.randomUUID().toString());
        result.setContext(buildLoginContext(allEvents, "LOGIN_FAILURE"));
        result.setDetails(buildFailureDetails(allEvents));
        return result;
    }
});
```

逐行：

| 行 | 说明 |
|----|------|
| `pattern.getOrDefault("first", emptyList())` | 模式名必须与 `PatternFactory` 里的完全一致，写错就是空列表 |
| 按 `first → second → third` 顺序拼接 | 保证 `allEvents.get(0)` 是**最早**的事件 |
| `firstEvent.getUserId()` | 用最早事件的 userId（同一用户，无差别） |
| `"LOGIN"` / `"LOGIN_FAILURE"` | 硬编码字符串，与规则里的 `patternType == "LOGIN_FAILURE"` 是**字符串契约** |
| `UUID.randomUUID()` | 每次命中生成新 eventId；**原始消息的 eventId 丢失** |
| `buildLoginContext(allEvents, ...)` | 生成规则可用变量 |
| `buildFailureDetails(allEvents)` | 生成审计信息 |

### 12.1 `pattern` Map 的结构

```java
{
  "first":  [LoginEvent],     // 一定是 1 个（当前模式每个状态无循环量词）
  "second": [LoginEvent],
  "third":  [LoginEvent]
}
```

虽然每个状态只有一个元素，但**类型必须是 List**——这是 CEP API 的约定（量词可能产生多个）。所以不能写 `pattern.get("first").get(0)` 之外的花样。

### 12.2 `buildLoginContext` 逐行

```java
private static Map<String, Object> buildLoginContext(List<LoginEvent> events, String patternType) {
    Map<String, Object> context = new HashMap<>();
    if (events != null && !events.isEmpty()) {
        LoginEvent latest = events.get(events.size() - 1);     // ①

        context.put("eventType", "LOGIN");
        context.put("patternType", patternType);
        context.put("userId", latest.getUserId());
        context.put("failCount", events.size());               // ②
        context.put("ip", latest.getIp());
        context.put("deviceId", latest.getDeviceId());
        context.put("success", latest.isSuccess());
        context.put("location", latest.getLocation());
        context.put("browser", latest.getBrowser());
    }
    return context;
}
```

- **① 取最新一条**：IP/设备/浏览器用"最后一次尝试"的值，符合直觉；
- **② `failCount = events.size()`**：对 `LOGIN_FAILURE` 恒等于 3；对 `LOCATION_CHANGE` 恒等于 1（只传了 `secondList`）。**同一变量在不同模式下语义不同**，规则写 `failCount >= 3` 时要想清楚作用在哪个模式上。

### 12.3 `buildFailureDetails`

```java
details.put("failCount", events.size());
details.put("lastIp", events.get(events.size() - 1).getIp());
details.put("lastDeviceId", events.get(events.size() - 1).getDeviceId());
```

注意 `details.failCount` 是 `int`，`context.failCount` 也是 `int`，但两者用途不同（一个给规则算，一个给审计看）。最终 `RiskActionResolver` 会把 context 之外的所有 details 合并进 `RiskResult.details`，落库时键名相同——**如果 details 和最终 RiskResult 里有同名 key，后者会覆盖前者**（`putAll` 顺序决定）。

## 讲 13 · `LOCATION_CHANGE` 的 select 回调

```java
List<LoginEvent> firstList = pattern.getOrDefault("first", Collections.emptyList());
List<LoginEvent> secondList = pattern.getOrDefault("second", Collections.emptyList());

if (firstList.isEmpty() || secondList.isEmpty()) return null;

LoginEvent prevEvent = firstList.get(firstList.size() - 1);
LoginEvent currEvent = secondList.get(0);

PatternMatchResult result = new PatternMatchResult(
        currEvent.getUserId(), "LOGIN", "LOCATION_CHANGE");
result.setEventId(UUID.randomUUID().toString());

Map<String, Object> context = buildLoginContext(secondList, "LOCATION_CHANGE");
context.put("prevIp", prevEvent.getIp());
context.put("currIp", currEvent.getIp());
result.setContext(context);

Map<String, Object> details = new HashMap<>();
details.put("prevIp", prevEvent.getIp());
details.put("currIp", currEvent.getIp());
details.put("prevLocation", prevEvent.getLocation());
details.put("currLocation", currEvent.getLocation());
result.setDetails(details);
```

细节：

| 点 | 说明 |
|----|------|
| `prevEvent = firstList.get(size-1)` | 如果 `first` 有多个元素（理论上），取最后一个作为"前一次" |
| `currEvent = secondList.get(0)` | 取第一个作为"当前" |
| `userId` 用 `currEvent` | 与 `LOGIN_FAILURE` 用 `firstEvent` 不一致，但同一用户无影响 |
| context 只传 `secondList` | 导致 `failCount=1`、`success=true` |
| 额外 context `prevIp/currIp` | 供规则做 IP 比较（当前 `init.sql` 里没有这样的规则） |
| `return null` | 空列表兜底，见讲 14.2 |

**规则可用的写法**（当前库里没有）：

```
patternType == "LOCATION_CHANGE" && prevIp != currIp
```

## 讲 14 · 订单与活动 CEP 的差异点

### 14.1 `ORDER_FREQUENT`：汇总金额

```java
BigDecimal totalAmount = BigDecimal.ZERO;
for (OrderEvent event : allEvents) {
    if (event.getAmount() != null) totalAmount = totalAmount.add(event.getAmount());
}

Map<String, Object> context = buildOrderContext(allEvents, "ORDER_FREQUENT");
context.put("totalAmount", totalAmount.doubleValue());     // 转 double 给 Aviator

Map<String, Object> details = new HashMap<>();
details.put("orderCount", allEvents.size());
details.put("totalAmount", totalAmount);                   // 保留 BigDecimal 给审计
```

- 同一个"总金额"以两种类型存在：context 里是 `double`（规则用），details 里是 `BigDecimal`（落库用 Jackson 序列化，不丢精度）；
- `buildOrderContext` 里的 `amount` 是**最后一笔**的金额，不是总额；
- `orderCount = 3`（命中条数）。

### 14.2 `ORDER_RAPID`：时间间隔 + 一个隐蔽的 count=1

```java
long interval = secondEvent.getTimestamp() - firstEvent.getTimestamp();

Map<String, Object> context = buildOrderContext(secondList, "ORDER_RAPID");   // ← 只传第二笔
context.put("intervalMs", (double) interval);
```

**`buildOrderContext(secondList, ...)` 导致 `orderCount = 1`**。如果规则写：

```
patternType == "ORDER_RAPID" && orderCount >= 2      // ❌ 永远不命中
```

正确写法只能依赖 `patternType` 本身或 `intervalMs`：

```
patternType == "ORDER_RAPID" && intervalMs < 30000   // ✅
```

这是一处**必须记住的陷阱**：同名字段在不同 pattern 下取值含义不同。

### 14.3 `ACTIVITY_FREQUENT`

```java
List<ActivityEvent> allEvents = first + second + third + fourth;   // 4 个状态
if (allEvents.isEmpty()) return null;
Map<String, Object> context = buildActivityContext(allEvents, "ACTIVITY_FREQUENT");  // participationCount=4
details.put("participationCount", allEvents.size());
details.put("reason", "Abnormal participation pattern detected");
```

### 14.4 `COUPON_REPEAT`

```java
List<ActivityEvent> allEvents = first + second + third;            // 3 个状态
Map<String, Object> context = buildActivityContext(allEvents, "COUPON_REPEAT");
details.put("reason", "Repeated coupon claim detected");
details.put("couponClaimCount", allEvents.size());
details.put("couponCode", allEvents.get(0).getCouponCode());       // 取第一张券
```

注意 `couponCode` 取的是**第一张**券的 code，而不是最后一笔——如果用户领了 3 张不同的券，审计信息会显示第一张。这是有意的（"用户重复领取了什么"），但没有注释说明。

### 14.5 `return null` 的真实风险

三个类都在空列表时 `return null`。`PatternSelectFunction` 返回 null 后，Flink CEP 会 `collector.collect(null)`，下游 `map` 里 `result.getUserId()` 直接 NPE；部分版本会抛 "Output may not be null"。

由于模式匹配成功时各状态至少一个元素，这个分支实际不可达。**但它属于"错误处理写成隐藏故障"**。更安全的写法：

```java
if (allEvents.isEmpty()) {
    logger.warn("[CEP] Empty pattern match, skip. pattern={}", pattern.keySet());
    return null;      // 至少配合下游 filter
}
```

并在 `RiskControlApplication` 的 `union` 后加 `.filter(Objects::nonNull)`。

### 14.6 `PatternMatchResult` 字段语义复述

```java
private String userId;
private String eventId;        // 每次命中新 UUID
private String eventType;      // LOGIN / ORDER / ACTIVITY
private String patternType;    // LOGIN_FAILURE / LOCATION_CHANGE / ORDER_FREQUENT
                               // ORDER_RAPID / ACTIVITY_FREQUENT / COUPON_REPEAT
private Map<String, Object> context;
private Map<String, Object> details;
```

**`patternType` 是 CEP 与规则之间唯一的"枚举契约"**。改这个名字要同步：`PatternFactory`（无）、三个 CepJob、`sql/init.sql` 的 6 条规则、前端 `EXPRESSION_DOC`。

---

# 第 5 层 · 评估层（讲 15-17）

## 讲 15 · 广播状态描述符

文件：`cep/RuleEvaluationFunction.java`

```java
public static final String RULE_STATE_NAME = "rule-config-state";

public static final MapStateDescriptor<Void, List<RuleConfig>> RULE_STATE_DESCRIPTOR =
        new MapStateDescriptor<>(
                RULE_STATE_NAME,
                BasicTypeInfo.VOID_TYPE_INFO,
                TypeInformation.of(new TypeHint<List<RuleConfig>>() {})
        );
```

### 15.1 为什么 key 是 `Void`

广播状态本质是 `Map<K,V>`，本场景只需要"一份快照"，最自然的是用一个固定的 key。`Void` 类型不能有实例，只能用 `null` 作 key：

```java
broadcastState.put(null, rules);
List<RuleConfig> rules = broadcastState.get(null);
```

为什么不用 `String` + `"all"`：省一点存储、也避免"哪个字符串才是约定的 key"的歧义。代价是代码看起来有点怪，且 `get(null)` 容易让人以为忘了传 key。

### 15.2 为什么 value 是 `List` 而不是 `Map<String, RuleConfig>`

- 语义上"一次广播 = 一份完整快照"，`List` 足够；
- 用 `Map` 也可以（引擎内部本来就是 Map），但要额外处理 Flink 对 `Map` 泛型序列化的类型推断；
- `List` + `TypeHint` 是社区最常见写法。

### 15.3 `TypeHint` 的作用

`List<RuleConfig>` 在 Java 里类型擦除后只剩 `List`。Flink 需要知道元素类型才能选择序列化器（POJO 序列化 or Kryo）。`new TypeHint<List<RuleConfig>>() {}` 是一个匿名子类，其泛型签名在 class 文件里保留，Flink 通过反射读取。

如果写成 `TypeInformation.of(List.class)`，Flink 会用 Kryo 序列化整个列表——能跑，但更慢、且字段增删不兼容。

### 15.4 广播状态与 checkpoint

广播状态**参与 checkpoint**，但**不参与 keyed 分区**（每个 subtask 都存一份完整副本）。恢复时每个 subtask 从自己的状态恢复。所以：

- 规则不会因作业重启而丢失；
- 恢复后**不需要**等下一轮 MySQL 轮询就有规则（前提是 checkpoint 里有）。

## 讲 16 · `open` 与 `processElement`

### 16.1 `open`

```java
@Override
public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    this.ruleEngine = new AviatorRuleEngine(1000, 3600);
    this.resolver = new RiskActionResolver(ruleEngine);
    logger.info("RuleEvaluationFunction opened, waiting for rule broadcast...");
}
```

| 点 | 说明 |
|----|------|
| 参数是 `OpenContext` | Flink 2.x 的新签名（1.x 是 `Configuration`） |
| 先调用 `super.open` | 保持父类状态初始化（`RichFunction` 里可能初始化 runtime context） |
| 建**空引擎** | 不阻塞启动；真正规则等第一次广播 |
| 日志明确说"waiting" | 便于运维确认启动顺序 |

**为什么不在 `open` 里查库**：查库属于 Source 的职责；算子内查库会导致每个 subtask 各查一次，且无法参与 checkpoint。

### 16.2 `processElement` 逐行

```java
@Override
public void processElement(PatternMatchResult matchResult,
                           ReadOnlyContext ctx,
                           Collector<RiskResult> out) throws Exception {

    logger.info("[EVAL] Processing match: user={}, eventType={}, pattern={}, context={}",
            matchResult.getUserId(), matchResult.getEventType(),
            matchResult.getPatternType(), matchResult.getContext());

    ReadOnlyBroadcastState<Void, List<RuleConfig>> broadcastState =
            ctx.getBroadcastState(RULE_STATE_DESCRIPTOR);

    List<RuleConfig> rules = broadcastState.get(null);

    if (rules == null || rules.isEmpty()) {
        RiskResult result = new RiskResult(
                matchResult.getUserId(), matchResult.getEventId(), matchResult.getEventType());
        result.setRiskLevel(RiskLevel.LOW);
        result.setRiskScore(0);
        result.setAction("ALLOW");
        result.getDetails().put("reason", "No rules loaded yet");
        result.getDetails().putAll(matchResult.getDetails());
        out.collect(result);
        logger.warn("[EVAL] No rules in broadcast state, output ALLOW for user: {}", matchResult.getUserId());
        return;
    }

    logger.info("[EVAL] {} rules available, evaluating context against rules...", rules.size());

    RiskResult result = resolver.resolve(
            matchResult.getContext(),
            matchResult.getUserId(),
            matchResult.getEventId(),
            matchResult.getEventType(),
            matchResult.getDetails()
    );

    logger.info("[EVAL] Result: user={}, action={}, level={}, score={}, details={}",
            result.getUserId(), result.getAction(),
            result.getRiskLevel(), result.getRiskScore(), result.getDetails());

    out.collect(result);
}
```

逐段：

| 段 | 说明 |
|----|------|
| 第一条 `logger.info` | **每条命中都打一次**，包含完整 context。高 QPS 下日志量巨大，生产要降级为 debug |
| `ctx.getBroadcastState(...)` | `ReadOnlyContext` → `ReadOnlyBroadcastState`，**编译期就禁止 put** |
| `broadcastState.get(null)` | 取出整份快照 |
| `rules == null` | 该 subtask 从未收到广播（刚启动） |
| `rules.isEmpty()` | 收到过，但库里没有规则 |
| fail-open 分支 | `LOW/0/ALLOW`，**并合并 CEP details** |
| 正常分支 | 交给 `resolver.resolve` |

**fail-open 的业务含义**：宁可漏判，不可在规则加载前把所有用户全拦。代价是启动窗口期风控失效，需要有外部告警（"作业启动 60 秒内出现 EVAL No rules"）。

### 16.3 为什么不能用 `getBroadcastState` 做主数据流缓存

广播状态是**只读视图**，且 Flink 不保证它在本地的强一致快照（可能落后于广播流）。想缓存"最新规则"到本地变量是可以的（本项目就是通过 `ruleEngine` 字段实现），但不要自己 `Map` 存一份并期望与 checkpoint 一致。

## 讲 17 · `processBroadcastElement`

```java
@Override
public void processBroadcastElement(List<RuleConfig> rules,
                                    Context ctx,
                                    Collector<RiskResult> out) throws Exception {

    BroadcastState<Void, List<RuleConfig>> broadcastState =
            ctx.getBroadcastState(RULE_STATE_DESCRIPTOR);
    broadcastState.put(null, rules);

    AviatorRuleEngine newEngine = new AviatorRuleEngine(1000, 3600);
    newEngine.registerRules(rules);

    this.ruleEngine = newEngine;
    this.resolver = new RiskActionResolver(newEngine);

    logger.info("Rule engine refreshed in this task: {} rules loaded", newEngine.getRuleCount());
}
```

### 17.1 执行顺序的重要性

1. **先 `put` 状态**：保证 checkpoint 里是最新快照；
2. **再建引擎**：编译 + 注册；
3. **最后发布引用**（volatile 写）。

如果反过来（先发布引擎后 put），在极端时序下 checkpoint 可能记录了旧状态而内存里是新引擎，恢复后规则回退——不过 30 秒后又会纠正，影响有限。

### 17.2 为什么"整体替换"而不是"增量更新"

| 方案 | 并发安全 | 规则删除 | 编译开销 |
|------|----------|----------|----------|
| 增量 `removeRule` + `registerRule` | ❌ 需要锁 | 需要显式处理 | 只编译新增 |
| 整体替换（当前） | ✅ 无锁 | 天然生效 | 每次全量编译 |

在"15 条规则、30 秒一次"的场景下，全量编译开销可忽略（毫秒级）。规则上千后需要改成"按内容哈希判断是否需要重建"。

### 17.3 每个 subtask 都会执行

`processBroadcastElement` 在**每个并行子任务**（本项目 4 个）各执行一次，日志会出现 4 行 `Rule engine refreshed`。这是正常的，不是重复消费。

### 17.4 广播流的并行度

`broadcast()` 之后，下游 `process` 的并行度由下游决定（4）。广播流本身（`RuleSourceFunction`，p=1）会把同一份数据复制给 4 个下游实例。

### 17.5 没有处理的异常

`newEngine.registerRules(rules)` 内部已吞掉单条编译失败。但如果 `rules` 为 null（`RuleSourceFunction` 不会发 null），`registerRules` 会 NPE → 作业失败。属于防御缺失，实际不会触发。

---

# 第 6 层 · Sink 层（讲 18-20）

## 讲 18 · `RedisSinkFunction`：Flink 2.x 新 Sink API

文件：`sink/RedisSinkFunction.java`

```java
public class RedisSinkFunction implements Sink<RiskResult> {

    private final String redisHost;
    private final int redisPort;
    private final int redisDatabase;
    private final int timeout;

    @Override
    public SinkWriter<RiskResult> createWriter(WriterInitContext context) throws IOException {
        return new RedisSinkWriter(redisHost, redisPort, redisDatabase, timeout);
    }
}
```

### 18.1 新 Sink API 的两层结构

| 层 | 职责 | 调用次数 |
|----|------|----------|
| `Sink<T>` | 工厂：创建 Writer | 作业构建/恢复时每个 subtask 一次 |
| `SinkWriter<T>` | 实际写入：`write` / `flush` / `close` | `write` 每条一次 |

对比 1.x 的 `RichSinkFunction.invoke()`：新 API 把"初始化"和"写入"彻底分开，便于 Flink 管理生命周期与提交语义。

### 18.2 `RedisSinkWriter` 逐方法

```java
static class RedisSinkWriter implements SinkWriter<RiskResult> {
    private final RedisClient redisClient;

    RedisSinkWriter(String host, int port, int db, int timeout) {
        this.redisClient = new RedisClient(host, port, db, timeout);
    }

    @Override
    public void write(RiskResult value, Context context) throws IOException {
        try {
            String key = value.getRedisKey();
            String jsonValue = value.toJsonString();
            redisClient.setWithTTL(key, jsonValue, value.getTtl());
        } catch (Exception e) {
            logger.error("Error writing risk result to Redis: user={}, event={}",
                    value.getUserId(), value.getEventType(), e);
        }
    }

    @Override
    public void flush(boolean endOfInput) { /* Redis 逐条写入，无需 flush */ }

    @Override
    public void close() throws Exception {
        if (redisClient != null) redisClient.close();
    }
}
```

要点：

- **同步写入**：每条 `SETEX` 一个网络往返。QPS 高时会成为瓶颈（Flink 会在 `write` 处反压）；
- **异常被吞**：作业不会因 Redis 故障重启，但也没有重试、没有降级队列 → 数据静默丢失；
- **`close` 释放连接池**：正确的资源管理；
- **`static class`**：非静态内部类会隐式持有外部类引用（`RedisSinkFunction`），序列化时出问题；静态类是正确写法。

### 18.3 两条隐性依赖

```
RedisSinkFunction
   └─ RedisClient（common 模块）
        └─ RiskResult.getRedisKey() / toJsonString()
```

改 Redis key 格式要改 common 的 `RiskResult`；改 value 格式要改 `toJsonString`（它目前有 bug，见 common 详解讲 20）。

## 讲 19 · `MysqlSinkFunction`：异步 IO 与线程池

文件：`sink/MysqlSinkFunction.java`

```java
public class MysqlSinkFunction extends RichAsyncFunction<RiskResult, Void> {

    private static final String INSERT_SQL =
            "INSERT INTO risk_result (user_id, event_id, event_type, risk_level, risk_score, action, details, create_time) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, NOW())";

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final int maxPoolSize;

    private transient ExecutorService executorService;
    private transient ObjectMapper objectMapper;
```

### 19.1 为什么用 `RichAsyncFunction`

同步 JDBC 写入会阻塞算子线程，吞吐受制于数据库往返（~1-5ms/条）。异步 IO 把写操作提交到线程池，算子线程立即返回，等结果回调再继续——**在等待期间可以处理其它记录**。

`AsyncDataStream.orderedWait(stream, function, 60, SECONDS, 100)` 的三个参数：

| 参数 | 值 | 含义 |
|------|----|------|
| timeout | 60s | 超过 60 秒未完成触发 `timeout()` |
| capacity | 100 | 每个 subtask 最多 100 个在途请求 |
| 模式 | `orderedWait` | 输出顺序与输入一致（`unorderedWait` 吞吐更高但乱序） |

**capacity 的影响**：100 个在途 × 4 并行度 = 400 并发写 MySQL。如果数据库连接数不够，会看到大量连接失败。

### 19.2 `open`：线程池与一次性的 anti-pattern

```java
public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    this.objectMapper = new ObjectMapper();

    this.executorService = new ThreadPoolExecutor(
            maxPoolSize, maxPoolSize,          // core = max = 4
            60L, TimeUnit.SECONDS,             // 空闲线程存活
            new LinkedBlockingQueue<>(10000),  // 有界队列
            new ThreadFactoryWithName("mysql-sink-"),
            new ThreadPoolExecutor.DiscardOldestPolicy());   // 队列满时丢最老任务
    ...
}
```

参数评价：

| 参数 | 值 | 评价 |
|------|----|------|
| 核心/最大线程数 | 4 | 与 `orderedWait` 的 capacity=100 不匹配：100 个在途只有 4 个线程在写，其余排队 |
| 队列 | 10000 有界 | 防 OOM，好事 |
| 拒绝策略 | `DiscardOldestPolicy` | **静默丢弃最老任务**，等于丢风险记录且无日志 |
| 线程名 | `mysql-sink-N` | 便于 jstack 定位，好实践 |
| 守护线程 | `t.setDaemon(true)` | 不阻止 JVM 退出，但可能导致未完成任务丢失 |

**更合理的配置**：线程数 8~16、队列 1000、拒绝策略 `CallerRunsPolicy`（反压给 Flink，而不是丢数据）。

### 19.3 `close`

```java
public void close() throws Exception {
    if (executorService != null) {
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }
    logger.info("MysqlSinkFunction closed");
}
```

`shutdown()` 不再接受新任务，`awaitTermination(10s)` 等已提交任务完成。超过 10 秒的写入会被丢弃（线程池仍会在后台跑，因为守护线程 + JVM 不退出）。

**没有检查 `awaitTermination` 返回值**：超时时不会打日志。

### 19.4 `asyncInvoke` 逐行

```java
@Override
public void asyncInvoke(RiskResult result, ResultFuture<Void> resultFuture) {
    CompletableFuture.runAsync(() -> {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {

            ps.setString(1, result.getUserId());
            ps.setString(2, result.getEventId());
            ps.setString(3, result.getEventType());
            ps.setString(4, result.getRiskLevel().name());
            ps.setInt(5, result.getRiskScore());
            ps.setString(6, result.getAction());
            ps.setString(7, objectMapper.writeValueAsString(result.getDetails()));

            ps.executeUpdate();
            resultFuture.complete(Collections.emptyList());

        } catch (SQLException e) {
            logger.error("Failed to write risk result to MySQL: user={}, event={}",
                    result.getUserId(), result.getEventType(), e);
            resultFuture.completeExceptionally(e);
        } catch (Exception e) {
            logger.error("Unexpected error in MysqlSinkFunction", e);
            resultFuture.completeExceptionally(e);
        }
    }, executorService);
}
```

逐行关键点：

| 点 | 说明 |
|----|------|
| `DriverManager.getConnection` **每条记录一次** | 没有连接池。MySQL 建连成本 ~1-3ms，是最大瓶颈 |
| `try-with-resources` | 正确关闭 Connection/PreparedStatement |
| `riskLevel.name()` | 枚举 → 大写字符串，与表字段契约 |
| `objectMapper.writeValueAsString(details)` | Map → JSON 字符串，写入 JSON 列 |
| `resultFuture.complete(emptyList())` | 通知 Flink 该条完成 |
| `completeExceptionally(e)` | **让 Flink 算子抛异常 → 作业 failover**，与类注释"不重试，仅记录日志"矛盾 |
| 无幂等键 | checkpoint 重放会重复插入 |

### 19.5 `timeout`

```java
@Override
public void timeout(RiskResult input, ResultFuture<Void> resultFuture) {
    logger.warn("MySQL sink timeout for user={}, event={}", input.getUserId(), input.getEventType());
    resultFuture.complete(Collections.emptyList());     // 正常完成，不失败
}
```

**不一致**：超时（60s）→ 视为成功；SQL 异常（毫秒级）→ 视为失败并重启作业。从"数据可靠性"角度看反了：超时更可能意味着数据没写进去。

### 19.6 三个必须知道的缺陷

1. **没有连接池**：每条记录建连，高 QPS 下 MySQL `max_connections` 很快耗尽；
2. **异常导致作业重启**：一条 `details` 超长（JSON 列默认最大 1GB，但 `max_allowed_packet` 可能是 4MB）就能让作业进入重启循环；
3. **可能重复写**：无唯一键 + checkpoint 重放。

## 讲 20 · `KafkaSinkFactory`

文件：`sink/KafkaSinkFactory.java`

```java
private static final String RISK_RESULT_TOPIC = "risk-results";
private static final ObjectMapper objectMapper = new ObjectMapper();   // 静态，未被使用！

public static KafkaSink<RiskResult> createRiskResultSink(String bootstrapServers) {
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    producerProps.put(ProducerConfig.RETRIES_CONFIG, "3");
    producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
    producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "5");

    return KafkaSink.<RiskResult>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(
                    KafkaRecordSerializationSchema.<RiskResult>builder()
                            .setTopic(RISK_RESULT_TOPIC)
                            .setKeySerializationSchema(
                                    (SerializationSchema<RiskResult>) value ->
                                        (value.getUserId() != null ? value.getUserId() : "").getBytes(UTF_8))
                            .setValueSerializationSchema(new RiskResultSerializationSchema())
                            .build())
            .setKafkaProducerConfig(producerProps)
            .build();
}
```

### 20.1 producer 参数逐项

| 参数 | 值 | 作用 |
|------|----|------|
| `acks=all` | 所有 ISR 确认 | 最强持久性，延迟最高 |
| `retries=3` | 重试 3 次 | 与幂等一起使用时实际由幂等保证 |
| `enable.idempotence=true` | 幂等生产者 | 单分区内不重复，需 `acks=all` |
| `compression.type=lz4` | 压缩 | 降低网络与存储 |
| `batch.size=16384` | 16KB 批次 | 吞吐与延迟平衡 |
| `linger.ms=5` | 等 5ms 攒批 | 略微增加延迟，明显提升吞吐 |

**注意**：`setBootstrapServers` 与 `producerProps` 里的 `BOOTSTRAP_SERVERS_CONFIG` 重复设置，后者优先/等价，无害但冗余。

### 20.2 key 序列化：lambda 强制转型

```java
.setKeySerializationSchema(
    (SerializationSchema<RiskResult>) value ->
        (value.getUserId() != null ? value.getUserId() : "").getBytes(StandardCharsets.UTF_8))
```

- `KafkaRecordSerializationSchema` 需要 `SerializationSchema<T>`（有 `open` 默认方法），lambda 无法直接匹配，所以强转；
- key = `userId` → **同一用户的结果落在同一分区**，保证用户维度有序；
- `userId` 为 null 时用空字符串（避免 NPE）。

### 20.3 value 序列化

```java
public static class RiskResultSerializationSchema implements SerializationSchema<RiskResult> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(RiskResult element) {
        try {
            return mapper.writeValueAsBytes(element);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize RiskResult", e);
        }
    }

    @Override
    public void open(InitializationContext context) { /* no-op */ }
}
```

- **Jackson 全字段序列化**：`riskLevel` 枚举默认序列化为 `"HIGH"`（`name()`），与 Redis 的手写 JSON 形成鲜明对比——同一个对象两条输出路径，格式不一致；
- `details` 是 `Map<String,Object>`，会被递归序列化；
- 序列化失败抛 `RuntimeException` → 作业失败（比静默丢数据更安全）。

### 20.4 静态 `objectMapper` 未被使用

`KafkaSinkFactory` 顶部的 `private static final ObjectMapper objectMapper` 在类里没有任何引用（内部类有自己的 `mapper`）。属于死代码，可删。

### 20.5 三路 Sink 的横向对比（重要）

| | Redis | MySQL | Kafka |
|---|-------|-------|-------|
| API | `Sink`/`SinkWriter` | `RichAsyncFunction` | `KafkaSink` |
| 阻塞性 | 同步 | 异步（线程池 + orderedWait） | 异步（Flink 内部） |
| 序列化 | `RiskResult.toJsonString()`（有 bug） | `ObjectMapper.writeValueAsString(details)` | `ObjectMapper.writeValueAsBytes(整个对象)` |
| 失败行为 | 吞掉，作业继续 | `completeExceptionally` → 作业重启 | Flink 重试/failover |
| 幂等 | key 覆盖，天然幂等 | 无唯一键，可能重复 | 生产者幂等 |
| 输出格式 | `riskLevel` 是枚举 toString | `riskLevel` 是 name() | `riskLevel` 是 name() |

**结论**：同一个 `RiskResult`，Redis 与 Kafka 的 JSON 格式**不一致**（`riskLevel` 一个是 `"RiskLevel{score=...}"`，一个是 `"HIGH"`）。如果下游同时消费两路，必须做兼容。

---

# 第 7 层 · 端到端复盘与改造（讲 21-22）

## 讲 21 · 一次完整运行的日志轨迹

用 `./scripts/event-simulator.sh login_failure user_001` 造数，正常应看到：

```
# 1) 规则源启动
Broadcast rules snapshot: 15 rules
Rule registered: 1 - 登录失败次数过多
...
# 2) 每个 subtask 各打印一次（4 行）
Rule engine refreshed in this task: 15 rules loaded

# 3) 三条事件到达
[SOURCE] LoginEvent received: user=user_001, success=false, ip=..., timestamp=...
[SOURCE] LoginEvent received: user=user_001, success=false, ip=..., timestamp=...
[SOURCE] LoginEvent received: user=user_001, success=false, ip=..., timestamp=...

# 4) 第三条之后 CEP 命中（注意是最后一条之后才出现）
[CEP] Pattern matched: user=user_001, eventType=LOGIN, pattern=LOGIN_FAILURE, context={...}
[CEP-MATCH] Login failure pattern detected: user=user_001, failCount=3, events=3

# 5) 广播评估
[EVAL] Processing match: user=user_001, eventType=LOGIN, pattern=LOGIN_FAILURE, context={...}
[EVAL] 15 rules available, evaluating context against rules...
Rule engine resolved action for user: user_001 event: LOGIN -> rule: 登录失败次数过多 action: BLOCK level: HIGH
[EVAL] Result: user=user_001, action=BLOCK, level=HIGH, score=80, details={...}

# 6) 三路输出
[RULE] RiskResult: user=user_001, event=LOGIN, level=HIGH, action=BLOCK, score=80, details={...}
[SINK-REDIS] Writing to Redis: user=user_001, key=risk:login:user_001:<uuid>
[SINK-MYSQL] Writing to MySQL: user=user_001, eventId=<uuid>
[SINK-KAFKA] Writing to Kafka: user=user_001, level=HIGH, action=BLOCK
```

### 21.1 日志顺序的"为什么"

- `[CEP]` 一定在**最后一条事件**之后出现：CEP 必须看到第 3 个元素才知道匹配成功；
- `[CEP-MATCH]` 在 `[CEP]` 之后：前者是 `select` 回调内部打的，后者是 `union` 后 `map` 打的；
- `[EVAL]` 可能出现 **4 次/秒**：广播日志在每个 subtask 各一次；
- `[SINK-REDIS]`/`[SINK-MYSQL]`/`[SINK-KAFKA]` 的顺序**不保证**：三条并行分支，调度决定。

### 21.2 断点定位表

| 最后出现的日志 | 卡在哪 | 下一步查 |
|----------------|--------|----------|
| 无任何日志 | 作业没起来 | Flink UI、启动异常栈 |
| 只有 `Broadcast rules snapshot` | Kafka 没数据 / 消费不到 | Kafka UI 的 topic、消费组 lag |
| 有 `[SOURCE]` 无 `[CEP]` | CEP 不触发 | Watermark、timestamp 位数、`within` 是否过短 |
| 有 `[CEP]` 无 `[EVAL]` | union/connect 问题 | 算子拓扑、异常 |
| 有 `[EVAL]` 但 `ALLOW` | 规则未命中 | context 变量名、表达式语法、规则是否 enabled |
| 有 `[RULE]` 无 `[SINK-*]` | Sink 接线问题 | 算子是否被优化掉、异常 |
| 有 `[SINK]` 但库里没数据 | Sink 内部失败 | Redis `NOAUTH`、MySQL 连接/字段、Kafka 权限 |

## 讲 22 · 调优与改造清单

### 22.1 稳定性（按优先级）

| 优先级 | 改造 | 位置 | 收益 |
|--------|------|------|------|
| P0 | 给所有算子加 `.uid()` | `RiskControlApplication` | 支持 savepoint 恢复 |
| P0 | 配置 state backend + checkpoint 目录 | 同上 | 状态不丢 |
| P0 | MySQL Sink 改连接池 | `MysqlSinkFunction` | 不再每条建连 |
| P0 | MySQL 失败不传播（或明确接受重启） | 同上 | 避免小故障引发重启风暴 |
| P1 | Redis 支持密码 | `RedisClient` + `RedisSinkFunction` | 能连生产 Redis |
| P1 | 修复 `RiskResult.toJsonString` | common | Redis 数据可用 |
| P1 | 规则源单条容错 | `RuleSourceFunction` | 一条脏规则不再毁全量 |
| P2 | 重启策略显式配置 | 入口 | 可控的故障恢复 |

### 22.2 性能（按收益）

| 改造 | 收益 | 成本 |
|------|------|------|
| 规则按 `eventType` 索引 | 表达式求值 O(N) → O(k) | 需要规则表加列 |
| 广播前做内容哈希去重 | 减少引擎重建/编译 | 小 |
| `unorderedWait` 替代 `orderedWait` | 吞吐提升 | 输出乱序 |
| 提高 MySQL 线程数 + 降 capacity | 减少排队 | 需要 DB 连接预算 |
| 日志降级（`[EVAL]` per-record → debug） | 减少 IO | 排障变难 |

### 22.3 功能扩展

| 需求 | 改动点 |
|------|--------|
| 新增 CEP 模式 | `PatternFactory` + 对应 `XxxCepJob.select` + `sql/init.sql` + 前端文档 |
| 新增事件类型（如支付） | 新 POJO + 新反序列化器 + 新 `PaymentCepJob` + 入口接线 + 表 `event_type` 取值 |
| 规则支持"按 eventType 过滤" | 规则表加 `event_type` 列 + 引擎索引 |
| 多规则审计 | `RiskActionResolver` 记录全部命中规则 |
| 规则变更实时推送 | 用 CDC（Debezium）替代 30s 轮询 |
| 状态 TTL 与清理 | CEP `within` 已隐式清理；额外状态需注册 `StateTtlConfig` |

---

## 附录 A · 类清单与生产状态

| 类 | 状态 | 关键风险 |
|----|------|----------|
| `RiskControlApplication` | 生产 | 参数硬编码、无 uid、无状态后端 |
| `KafkaSourceFactory` | 生产 | 共用 group id、earliest、无分区发现 |
| `PatternFactory` | 生产 | 异地登录条件过松、金额门槛写死 |
| `LoginCepJob` / `OrderCepJob` / `ActivityCepJob` | 生产 | `return null`、context 语义不一致、模式名契约 |
| `PatternMatchResult` | 生产 | UUID 覆盖原始 eventId |
| `RuleSourceFunction` | 生产 | legacy API、整轮失败、无连接池 |
| `RuleEvaluationFunction` | 生产 | fail-open、per-record info 日志 |
| `RedisSinkFunction` | 生产 | 吞异常、无密码、序列化 bug |
| `MysqlSinkFunction` | 生产 | 无连接池、异常重启、丢任务 |
| `KafkaSinkFactory` | 生产 | 静态 mapper 死代码、格式与前两者不一致 |

## 附录 B · 读懂标准

能不看代码回答：

1. `main` 里 7 段各产出什么类型？哪一段是"两条流的汇合点"？
2. 为什么规则源并行度必须是 1？Kafka 源为什么是 2？
3. `followedBy` 与 `next` 对"5 次失败"的匹配数量有何不同？
4. `Watermark` 不推进时，`within` 会发生什么？为什么 CHANGELOG 里那次 bug 表现为"完全不触发"？
5. `ORDER_RAPID` 的 `orderCount` 为什么是 1？写规则时该怎么写？
6. `LOCATION_CHANGE` 为什么是误报源？怎么用 `IterativeCondition` 修？
7. 广播状态为什么用 `null` 作 key？`TypeHint` 解决什么问题？
8. 规则热更新时，为什么要"新建引擎 + volatile 替换"而不是 `removeRule`？
9. 三路 Sink 谁的失败会导致作业重启？为什么 Redis 的失败不会？
10. 同一份 `RiskResult` 在 Redis 与 Kafka 里的 `riskLevel` 为什么不同？

### 动手练习

1. **Watermark 实验**：把 `forBoundedOutOfOrderness(5s)` 改成 `noWatermarks()`，跑 `login_failure`，观察 `[CEP]` 是否消失，理解因果。
2. **窗口实验**：把登录失败窗口从 60s 改到 3s，用脚本连发三条（间隔 1 秒），观察是否命中；再改成 10s 对比。
3. **新增模式**：实现 `ORDER_OVERNIGHT`（2 笔订单，第二笔时间在 0~6 点），补齐 `hour` 变量与规则，端到端验证。
4. **规则去重广播**：给 `RuleSourceFunction` 加内容哈希，只有变化才 `collect`，统计日志频率变化。
5. **Sink 改造**：把 `MysqlSinkFunction` 改为 HikariCP + 失败不传播，压测对比作业重启次数与写入成功率。
6. **uid 实验**：给所有算子加 `uid`，打一次 savepoint，故意改 DAG（加一个 map）后恢复，观察失败原因。

---

下一篇：[03 · flink-risk-web 详解](03-web.md)
