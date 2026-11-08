# 模块精读 01 · flink-risk-common

> 公共模块：**事件模型、Kafka 反序列化、Aviator 规则引擎、风险结果与 Redis 客户端**。
> 它不依赖任何 Flink 运行时算子（`flink-core`/`flink-streaming-java` 是 `provided`），
> 所以这里是整个项目**最容易单独读、也最值得先读**的一层。
>
> 返回：[学习总览](../LEARNING-GUIDE.md) · 下一篇：[02 · flink-risk-job](02-flink-risk-job.md)

---

## 目录

1. [模块边界与包结构](#1-模块边界与包结构)
2. [事件模型 `event/`](#2-事件模型-event)
3. [Kafka 反序列化 `kafka/`](#3-kafka-反序列化-kafka)
4. [规则模型与等级 `rule/`](#4-规则模型与等级-rule)
5. [AviatorRuleEngine 精读](#5-aviatorruleengine-精读)
6. [RiskActionResolver 精读](#6-riskactionresolver-精读)
7. [RiskResult 与 Redis `redis/`](#7-riskresult-与-redis-redis)
8. [RuleLoader：未被使用的 YAML 加载](#8-ruleloader未被使用的-yaml-加载)
9. [对象在 Flink 中的生命周期](#9-对象在-flink-中的生命周期)
10. [读完后应该能回答的问题](#10-读完后应该能回答的问题)

---

## 1. 模块边界与包结构

```
flink-risk-common/src/main/java/com/qinyadan/risk/
├── event/     Event(抽象) · LoginEvent · OrderEvent · ActivityEvent
├── kafka/     LoginEventDeserializer · OrderEventDeserializer
│              ActivityEventDeserializer · KafkaEventDeserializer(通用)
├── rule/      RuleConfig · RiskLevel · AviatorRuleEngine
│              RiskActionResolver · RuleLoader
└── redis/     RiskResult · RedisClient · RedisResultWriter
```

依赖：`aviator`、`jedis`、`fastjson2`、`gson`、`jackson-databind`、`snakeyaml`、`guava`、`commons-lang3`、`slf4j`。
注意 `flink-core`、`flink-streaming-java` 是 `provided`：模块里只有 `DeserializationSchema` 这种**接口**引用，不需要真正打包 Flink。

这个模块被两个下游同时使用：

- `flink-risk-job` 用它做反序列化、规则计算、结果写入；
- `flink-risk-web` 只用到它的 `RuleConfig` / `RiskLevel` 等模型（web 的实体是另一套 `Rule`，不要混淆）。

---

## 2. 事件模型 `event/`

### 2.1 `Event`：抽象基类

```java
public abstract class Event implements Serializable {
    protected String userId;
    protected String eventId;
    protected long timestamp;      // 毫秒
    protected String eventType;    // LOGIN / ORDER / ACTIVITY
    protected Map<String, Object> attributes = new HashMap<>();
}
```

`implements Serializable` 是**硬性要求**：这些对象会进入 Flink 的 keyBy 网络 shuffle 和 CEP 状态后端（RocksDB），必须可序列化。`serialVersionUID = 1L` 同理。

两个设计细节：

1. `timestamp` 默认取 `System.currentTimeMillis()`，但 **Kafka 反序列化时必须由消息体覆盖**，否则 Watermark 用的是处理时间，CEP 的 `within()` 语义就错了。
2. `attributes` 这个自由扩展 Map 在整个项目里**没人用**——CEP 上下文是另外构造的 `Map`（见 02 文档）。读代码时不必为它分心。

### 2.2 三个子类

| 类 | 专属字段 | `eventType` 常量 |
|----|----------|------------------|
| `LoginEvent` | `ip` `deviceId` `success(boolean)` `failureReason` `location` `browser` | `"LOGIN"` |
| `OrderEvent` | `orderId` `amount(BigDecimal)` `productId` `productName` `quantity` `paymentMethod` `deliveryAddress` `processTime` | `"ORDER"` |
| `ActivityEvent` | `activityId` `activityName` `couponCode` `couponValue(BigDecimal)` `actionType` `participationCount` `channel` `source` | `"ACTIVITY"` |

三个子类的**无参构造**都会主动设置 `eventType`：

```java
public LoginEvent() {
    super();
    this.eventType = "LOGIN";   // 保证即使 JSON 没带 eventType 也不会是 null
}
```

这一点很关键：`KafkaEventDeserializer` 依赖 `eventType` 做路由，而具体反序列化器又依赖 `eventType` 在 CEP 里被下游使用。Jackson 反序列化时若 JSON 里带了 `eventType`，会用 setter 覆盖构造器里的默认值。

**金额为什么用 `BigDecimal`？** 风控里"金额 > 10000"是阈值判断，`double` 的精度误差会导致边界值（10000.00）判定不稳。CEP 条件里用的是 `BigDecimal.compareTo`，只有在写入 `context` 时才转成 `double` 给 Aviator（`buildOrderContext` 里 `getAmount().doubleValue()`）——因为 Aviator 对 `BigDecimal` 的比较支持不如原生数值直接。

### 2.3 Jackson 反序列化的两个坑

1. **未知字段会直接抛异常**。反序列化器用的是裸 `new ObjectMapper()`，没关 `FAIL_ON_UNKNOWN_PROPERTIES`。生产端只要多发一个字段（比如加了 `traceId`），反序列化就失败。想兼容，需要 `configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)`。
2. **字段名必须完全一致**。POJO 没有 `@JsonProperty`，所以 Java 的 `deviceId` 只能匹配 JSON 的 `deviceId`，`device_id` 匹配不上。

---

## 3. Kafka 反序列化 `kafka/`

四个类，形态高度一致，都实现 Flink 的 `DeserializationSchema<T>`：

```java
public class LoginEventDeserializer implements DeserializationSchema<LoginEvent> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public LoginEvent deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, LoginEvent.class);
    }
    public boolean isEndOfStream(LoginEvent next) { return false; }
    public TypeInformation<LoginEvent> getProducedType() { return TypeInformation.of(LoginEvent.class); }
}
```

三个方法各自的含义：

| 方法 | 作用 | 本项目取值 |
|------|------|-----------|
| `deserialize(byte[])` | 字节 → 对象，也是唯一被高频调用的方法 | Jackson 直接映射 |
| `isEndOfStream(T)` | 流是否结束（流式场景永不结束） | 恒 `false` |
| `getProducedType()` | 告诉 Flink 元素类型，用于类型推断/序列化器选择 | `TypeInformation.of(XxxEvent.class)` |

`KafkaSourceFactory` 用 `setValueOnlyDeserializer(...)` 注入，意味着消息的 **key 被忽略**，只用 value。

### 3.1 `KafkaEventDeserializer`（通用版，当前未接入主链路）

设计意图：一个 Source 订阅三个 topic，按 JSON 里的 `eventType` 分派到具体子类。

```java
switch (eventType.toUpperCase()) {
    case "LOGIN":    return objectMapper.readValue(message, LoginEvent.class);
    case "ORDER":    return objectMapper.readValue(message, OrderEvent.class);
    case "ACTIVITY": return objectMapper.readValue(message, ActivityEvent.class);
    default:         return objectMapper.readValue(message, Event.class);   // ← 问题在这
}
```

**缺陷**：`Event` 是抽象类，Jackson 在 `default` 分支会抛 `InvalidDefinitionException: Cannot construct instance of abstract type`。也就是说，一条 `eventType` 拼错或缺失的消息会直接让反序列化抛异常（默认策略下会终止任务）。

正确写法应该是 `return null;`（下游 filter 掉）或定义一个 `UnknownEvent` 子类。当前主链路用的是三个专用反序列化器，所以这个 bug 不会暴露——属于"备用路径上的雷"。

---

## 4. 规则模型与等级 `rule/`

### 4.1 `RuleConfig`

字段与 `rule_config` 表一一对应：

| 字段 | 类型 | 表列 | 说明 |
|------|------|------|------|
| `id` | `String` | `id BIGINT` | 用 String 是为了兼容 YAML 配置（那里 id 可能是 `"1"`） |
| `name` | `String` | `name` | 规则名，会写进 `RiskResult.details.matchedRuleName` |
| `expression` | `String` | `expression` | Aviator 表达式 |
| `riskLevel` | `RiskLevel` | `risk_level` | 枚举，读取时 `RiskLevel.valueOf(...)` |
| `action` | `String` | `action` | `ALLOW/VERIFY/REVIEW/WARN/BLOCK…`（自由字符串，不是枚举） |
| `weight` | `int` | `weight` | 同等级时的优先级 |
| `enabled` | `boolean` | `enabled` | 只加载 `enabled=1` |
| `description` | `String` | `description` | |
| `createTime`/`updateTime` | `long` | `DATETIME` | 仅在构造器里填了 `System.currentTimeMillis()`；`RuleSourceFunction` 不查这两列，所以 job 侧看到的永远是"加载时刻"而非库里的真实时间 |

要实现 `Serializable`：它会被放进 `List<RuleConfig>`，而该列表是广播状态和算子闭包的一部分。

### 4.2 `RiskLevel`

```java
LOW(10), MEDIUM(50), HIGH(80), CRITICAL(100)
```

两个用途要分清：

1. **`getScore()`**：作为该等级的代表分，直接写进 `RiskResult.riskScore`；多规则命中时用它比大小。
2. **`fromScore(int)`**：分数阈值映射（`>=80 HIGH`、`>=50 MEDIUM`、`>=10 LOW`、否则 LOW）。只有 `AviatorRuleEngine.getRiskLevel()` 用它，而该方法主链路未调用。

所以 `RiskLevel` 在本项目里更像"四档枚举 + 每档一个代表分"，而不是连续分数空间。

---

## 5. AviatorRuleEngine 精读

这个类是"规则引擎"，但代码只有 200 多行，核心就四件事：**注册（含编译校验）→ 执行 → 异常兜底 → 缓存**。

### 5.1 结构

```java
private final Cache<String, Expression> expressionCache;  // key: 表达式原文, value: 编译产物
private final Map<String, RuleConfig> ruleConfigs;        // key: 规则ID

public AviatorRuleEngine(int cacheSize, int cacheTTLSeconds) {
    this.expressionCache = CacheBuilder.newBuilder()
            .maximumSize(cacheSize)                              // 1000
            .expireAfterAccess(cacheTTLSeconds, TimeUnit.SECONDS) // 3600，访问即续期
            .build();
    this.ruleConfigs = new HashMap<>();
}
```

双层缓存的意图：

- Aviator 自己内部也有编译缓存（`AviatorEvaluator.compile` 默认带缓存）；
- 这里再套一层 Guava Cache，是为了**限制条目数**（规则被反复改错时不会无界膨胀）并提供 TTL。

### 5.2 注册：编译期拦截坏规则

```java
public void registerRule(RuleConfig ruleConfig) {
    if (!ruleConfig.isEnabled()) return;                 // 禁用直接忽略
    try {
        AviatorEvaluator.compile(ruleConfig.getExpression());   // 试编译
    } catch (Exception e) {
        logger.error("Rule expression compilation failed, SKIP register: ...");
        return;                                          // 语法错误 → 跳过，不影响其它规则
    }
    ruleConfigs.put(ruleConfig.getId(), ruleConfig);
}
```

要点：

- **语法错误在注册期就被拦掉**，不会在每条事件上重复报错；
- 单条规则失败不中断批量注册（`registerRules` 就是 for 循环）；
- 但**语义错误拦不住**（比如 `=~ "字符串"` 语法正确、运行期才炸）；
- `ruleConfigs` 用 `HashMap`，`getId()` 为 null 的规则会在 `put` 时抛 NPE——`RuleLoader` 里有校验，`RuleSourceFunction` 里 `id` 一定非 null。

### 5.3 执行：遍历 + 单条隔离

```java
public List<RuleConfig> executeRules(Map<String, Object> context) {
    List<RuleConfig> matchedRules = new ArrayList<>();
    for (RuleConfig rule : ruleConfigs.values()) {
        if (!rule.isEnabled()) continue;
        try {
            if (evaluateExpression(rule.getExpression(), context)) matchedRules.add(rule);
        } catch (Exception e) {                 // 理论上 evaluateExpression 不会抛
            logger.error("Error executing rule: {}", rule.getId(), e);
        }
    }
    return matchedRules;
}
```

**时间复杂度 O(规则数)**，每条事件都要把所有规则跑一遍。预置 15 条没问题；上千条时需要按 `eventType`/`patternType` 建索引做预筛，这是最自然的优化点。

### 5.4 表达式求值：只认 Boolean，异常全吞

```java
public boolean evaluateExpression(String expression, Map<String, Object> context) {
    try {
        Expression compiled = getOrCompileExpression(expression);
        Object result = compiled.execute(context);
        return result instanceof Boolean ? (Boolean) result : false;   // 非布尔 → 未命中
    } catch (ExpressionRuntimeException e) {
        logger.error("Expression runtime error (check =~ syntax, use /pattern/ not \"pattern\"): ...");
        return false;
    } catch (Exception e) {
        logger.error("Error evaluating expression: ...");
        return false;
    }
}
```

三条行为约定，改代码时必须保留：

1. **不抛异常**：任何求值错误都降级为"未命中"，保证一条坏规则不会拖垮整条数据流。代价是**规则失效是静默的**——线上排查时只能靠 grep error 日志。
2. **只认 `Boolean`**：表达式写成 `1 == 1 ? 1 : 0` 会被判为未命中。
3. **`=~` 必须配正则字面量**：Aviator 的 `=~` 右侧要求 `/pattern/`，写成 `"pattern"` 会抛 `ExpressionRuntimeException`。`sql/init.sql` 里有多条规则踩了这个坑（见总览第 9 章第 4 条）。

### 5.5 编译缓存取用

```java
private Expression getOrCompileExpression(String expression) {
    return expressionCache.get(expression, () -> AviatorEvaluator.compile(expression));
}
```

`Cache.get(key, Callable)` 语义：命中直接返回；未命中调用 loader 编译并写回。loader 抛异常会被包成 `ExecutionException`，这里又包成 `RuntimeException` 抛给上层去吞。

### 5.6 加权评分（未接入主链路）

```java
public int calculateRiskScore(Map<String, Object> context) {
    int totalScore = 0, totalWeight = 0;
    for (RuleConfig rule : executeRules(context)) {
        totalScore += rule.getRiskLevel().getScore() * rule.getWeight();
        totalWeight += rule.getWeight();
    }
    return totalWeight == 0 ? 0 : totalScore / totalWeight;
}
```

这是教科书式的加权平均。但 `RiskActionResolver` 完全没用它，而是"取最高危规则 + 直接用等级分"。**两份逻辑并存**，读的时候要明确哪条在主链路上，否则会误以为 `riskScore` 是平均分。

> 若要做"多规则累计评分"，把这里接进 `RiskActionResolver` 即可，但要注意：加权平均会把 CRITICAL 规则稀释成 MEDIUM，风控语境下通常不想要这个效果。

### 5.7 线程模型（重要）

类注释写得很清楚："本类内部未做同步，设计为单线程持有"。安全性的来源不是锁，而是**整体替换**：

```java
// RuleEvaluationFunction.processBroadcastElement
AviatorRuleEngine newEngine = new AviatorRuleEngine(1000, 3600);
newEngine.registerRules(rules);
this.ruleEngine = newEngine;     // volatile 写，发布新实例
```

所以：**永远不要在运行中的引擎实例上 `removeRule` / `clearRules`**（虽然 API 提供了），否则会绕过这个约定。

---

## 6. RiskActionResolver 精读

职责：把"命中了哪些规则"翻译成"一个 `RiskResult`"。

```java
RiskResult result = new RiskResult(userId, eventId, eventType);
List<RuleConfig> matchedRules = ruleEngine.executeRules(context);

if (matchedRules.isEmpty()) {
    result.setRiskLevel(RiskLevel.LOW);
    result.setRiskScore(0);
    result.setAction("ALLOW");
    result.getDetails().put("reason", "No rule matched");
    // ⚠ 注意：这里没有 putAll(details)
} else {
    RuleConfig topRule = selectTopPriorityRule(matchedRules);
    result.setRiskLevel(topRule.getRiskLevel());
    result.setRiskScore(topRule.getRiskLevel().getScore());
    result.setAction(topRule.getAction());
    result.getDetails().put("matchedRuleId", topRule.getId());
    result.getDetails().put("matchedRuleName", topRule.getName());
    result.getDetails().put("matchedRuleCount", matchedRules.size());
    if (details != null) result.getDetails().putAll(details);   // 合并 CEP 详情
}
```

### 6.1 "就高不就低"的选择器

```java
matchedRules.stream().max((a, b) -> {
    int scoreCompare = Integer.compare(a.getRiskLevel().getScore(), b.getRiskLevel().getScore());
    if (scoreCompare != 0) return scoreCompare;
    return Integer.compare(a.getWeight(), b.getWeight());      // 等级相同比权重
}).orElse(matchedRules.get(0));
```

决策顺序：**等级分降序 → 权重降序 → 先遇到者胜**。所以 `weight` 只有在同等级时才起作用，不能用于"让低等级规则压过高等级规则"。

### 6.2 一个不对称的地方

未命中时**不合并** `details`，命中时才合并。这意味着：

- 未命中 → `risk_result.details` 里只有 `{"reason":"No rule matched"}`，CEP 侧的 `failCount`、`intervalMs` 等**丢失**；
- 命中 → 业务详情完整。

而 `RuleEvaluationFunction` 的"规则未加载"分支却是 `putAll(matchResult.getDetails())`。三处行为不一致。如果希望审计表里永远能看到 CEP 上下文，把未命中分支也加上 `putAll(details)` 即可。

---

## 7. RiskResult 与 Redis `redis/`

### 7.1 `RiskResult`：三路 Sink 的统一载体

| 字段 | 用途 |
|------|------|
| `userId` / `eventId` / `eventType` | 标识 |
| `riskLevel(RiskLevel)` | 写 MySQL 时 `name()`，写 Redis 时被 `toString()`（见下） |
| `riskScore(int)` | 0~100 |
| `action(String)` | `ALLOW/VERIFY/REVIEW/BLOCK…` |
| `details(Map)` | 命中规则 + CEP 业务详情 |
| `timestamp` | 构造时 `now()` |
| `ttl` | 默认 3600 秒，Redis 过期时间 |

```java
public String getRedisKey() {
    return String.format("risk:%s:%s:%s", eventType.toLowerCase(), userId, eventId);
}
```

即 `risk:login:user_001:<uuid>`。注意 `docs/ARCHITECTURE.md` 里写的 `rule_engine:risk:` 前缀是错的。

### 7.2 `toJsonString()` 的 bug（`riskLevel` 序列化成了枚举的 `toString()`）

```java
sb.append("\"riskLevel\":\"").append(riskLevel).append("\",");   // riskLevel 是枚举！
```

`append(Object)` 会调 `riskLevel.toString()`，而 `RiskLevel.toString()` 是：

```java
"RiskLevel{score=80, description='高风险'}"
```

拼接结果（仍是合法 JSON，因为内层用的是单引号）：

```json
{"userId":"user_001","eventId":"...","eventType":"LOGIN","riskLevel":"RiskLevel{score=80, description='高风险'}","riskScore":80,"action":"BLOCK","timestamp":1710000000000}
```

两个问题：

- `riskLevel` 字段值变成了无意义的长字符串，任何按 `HIGH`/`BLOCK` 解析的下游都会失败；
- `details` 完全没被写入，Redis 里看不到命中规则和业务上下文。

修法：直接用 Jackson 序列化（`MysqlSinkFunction` 和 `KafkaSinkFactory` 都这么做了），别手拼字符串。

### 7.3 `RedisClient`：Jedis 池 + 无密码

```java
JedisPoolConfig config = new JedisPoolConfig();
config.setMaxTotal(10); config.setMaxIdle(5); config.setMinIdle(2);
config.setTestOnBorrow(true); config.setTestOnReturn(true);
this.jedisPool = new JedisPool(config, host, port, timeout);   // 没有 password 参数
```

每次操作都 `jedis.select(database)`：

```java
try (Jedis jedis = jedisPool.getResource()) {
    jedis.select(database);
    jedis.setex(key, ttlSeconds, value);
}
```

- `try-with-resources` 归还连接到池，正确；
- 每次 `select` 多一次往返（可忽略，但更优的写法是在池化配置里指定 db）；
- **没有 password**：`docker-compose.yml` 的 Redis 带 `requirepass redis123`，所以写入会 `NOAUTH`，被 catch 后只留一行 error 日志。这是"Redis 里查不到数据"的头号原因。

`set/get/delete/exists/getTTL/increment` 都是同样的模式：捕获异常 → 记日志 → 返回安全默认值（`null`/`false`/-1）。**没有重试**。

### 7.4 `RedisResultWriter`：备用实现

它把 `RedisClient` 的创建推迟到第一次 `write()`（懒加载），并暴露 `open()`/`write()`/`close()` 三个方法，本意是配合 `DataStream.foreach()` 使用。

主链路**没有使用它**：`RedisSinkFunction` 直接 new `RedisClient`。所以这是一个残留类，改动时别以为它管着 Redis 写入。

---

## 8. RuleLoader：未被使用的 YAML 加载

```java
List<RuleConfig> rules = RuleLoader.loadFromYaml("rules.yaml");
RuleLoader.loadAndRegister(engine, "rules.yaml");
```

用 SnakeYAML 读 `rules:` 列表，逐条解析 `id/name/expression/risk_level/action/weight/enabled/description`，`RiskLevel.valueOf(risk_level)` 转换，缺 `id` 或 `expression` 的跳过。

`flink-risk-job/src/main/resources/rules.yaml` 存在，但**全仓库没有任何调用方**（`grep RuleLoader` 只有它自己）。规则真源是 MySQL。保留它不影响运行，但会误导读者以为有两套规则来源。

---

## 9. 对象在 Flink 中的生命周期

| 对象 | 何时创建 | 是否序列化 | 注意 |
|------|----------|-----------|------|
| `LoginEvent` 等 | 每条消息反序列化时 | 是（shuffle/状态） | 字段变化要考虑兼容 |
| `PatternMatchResult` | CEP 命中时 | 是 | `context`/`details` 是 `HashMap`，值类型必须可 Kryo 序列化 |
| `List<RuleConfig>` | 每次轮询 | 是（广播状态） | 放状态后端，故障恢复保留 |
| `AviatorRuleEngine` | `open()` 一次 + 每次广播 | **否** | 含 `Cache`，不可序列化；只能作为算子内字段 |
| `RiskResult` | 每条命中 | 是 | `ttl`/`timestamp` 随对象走 |
| `RedisClient` | `SinkWriter` 构造 | **否** | `jedisPool` 是 `transient` |
| `MysqlSinkFunction` 的 `ExecutorService` | `open()` | **否** | `transient`，关闭时 `shutdown()` |

通用规则：**Flink 算子里的 `transient` 字段，一律在 `open()`/构造 Writer 时重建**。这套代码基本遵守了，唯一的坑是 `RedisClient` 在构造器里就 `initPool()`——如果它被当作算子字段被序列化再反序列化，池会丢；当前它是 `SinkWriter` 内部字段，所以没事。

---

## 10. 读完后应该能回答的问题

1. 一条 `login-events` 消息从头到尾经过了哪些类？（反序列化 → CEP → context → 引擎 → RiskResult → 3 Sink）
2. `AviatorRuleEngine` 里三层保护分别防什么？（注册期编译校验、执行期异常吞、结果类型校验）
3. 为什么规则注册失败不影响其它规则？为什么运行期规则失效是"静默"的？
4. 多规则命中时最终 `riskScore` 是多少？为什么不是平均分？
5. Redis 里的结果为什么可能不是合法 JSON？为什么 `NOAUTH` 时作业不报错？
6. `context` 少一个 key 会发生什么？日志里能看到什么？
7. 如果要把"命中详情"也写进 Redis，改哪个类？（`RiskResult.toJsonString`）

### 动手练习

- **练习 1**：把 `RiskResult.toJsonString()` 改成 Jackson 序列化，并把 `details` 带上；验证 Redis 里的值能被 `jq` 解析。
- **练习 2**：给 `RedisClient` 增加 `password` 构造重载，并在 `RedisSinkFunction` 中传入；验证 `redis-cli -a redis123 KEYS 'risk:*'` 有数据。
- **练习 3**：修复 `KafkaEventDeserializer` 的 `default` 分支，让未知 `eventType` 被安全跳过，而不是抛出抽象类异常。
- **练习 4**：写一个单元测试：构造 `context = {patternType: "LOGIN_FAILURE", failCount: 3}`，注册 `failCount >= 3` 的规则，断言 `resolve()` 得到 `HIGH/BLOCK/80`。
