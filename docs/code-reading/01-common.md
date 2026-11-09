# 代码精读（详解版）· flink-risk-common

> 本文件面向"逐类、逐方法读懂"的目标。与 `docs/deep-dive/01-flink-risk-common.md`（概览版）的区别：
> 概览版讲**设计意图**，本文件讲**每一行代码在做什么、为什么这样写、改它会牵连谁**。
>
> 全模块 16 个 Java 文件，拆成 **7 个部分、20 讲**。请严格按讲号顺序读：
> 前一部分的输出是后一部分的输入（模型 → 反序列化 → 规则数据 → 引擎 → 解析 → 结果输出）。
>
> 相关文档：[job 详解](02-job.md) · [web 详解](03-web.md) · [学习总览](../LEARNING-GUIDE.md)

---

## 阅读地图

```
第 1 部分  事件模型        event/Event → LoginEvent / OrderEvent / ActivityEvent
                │  （先知道"数据长什么样"，后面所有代码都在处理它）
第 2 部分  反序列化        kafka/*Deserializer
                │  （字节流 → 上面那些对象）
第 3 部分  规则数据结构    rule/RuleConfig · RiskLevel · RuleLoader
                │  （规则从库里读出来长什么样）
第 4 部分  规则引擎        rule/AviatorRuleEngine
                │  （规则怎么被求值）
第 5 部分  结果解析        rule/RiskActionResolver
                │  （命中集合 → 一个风险结论）
第 6 部分  结果与输出      redis/RiskResult · RedisClient · RedisResultWriter
                │  （结论怎么变成可存储/可传输的形态）
第 7 部分  串读与验证      一次完整调用链 + 练习
```

### 部分划分与顺序理由

| 部分 | 包含文件 | 讲号 | 为什么排在这里 |
|------|----------|------|----------------|
| 1 事件模型 | `Event`、`LoginEvent`、`OrderEvent`、`ActivityEvent` | 1-3 | 一切数据的载体；不懂字段就读不懂 CEP 条件与 context |
| 2 反序列化 | 4 个 `*Deserializer` | 4-6 | 紧接模型层，解释"JSON 如何变对象"，并暴露 Jackson 配置缺失 |
| 3 规则数据 | `RuleConfig`、`RiskLevel`、`RuleLoader` | 7-9 | 引擎的输入；先认识数据结构，再读算法 |
| 4 规则引擎 | `AviatorRuleEngine` | 10-15 | 本模块唯一的算法核心，占全部难度的 50% |
| 5 结果解析 | `RiskActionResolver` | 16-18 | 引擎输出的"命中集合"需要翻译成业务结论 |
| 6 结果与输出 | `RiskResult`、`RedisClient`、`RedisResultWriter` | 19-22 | 结论的最终形态；含两个真实 bug |
| 7 串读 | — | 23-24 | 把 1-22 讲串成一次调用，检验理解 |

### 前置准备（5 分钟）

```bash
# 只编译本模块，不碰 Flink
mvn -q -pl flink-risk-common -am compile
# 看依赖树，确认 flink 是 provided
mvn -pl flink-risk-common dependency:tree
```

读到引擎部分时，建议在 IDE 里对 `AviatorRuleEngineTest`（需自己新建）写几个断言，边读边验证——本模块**没有任何现成测试**。

---

# 第 1 部分 · 事件模型（讲 1-3）

> 目标：能画出 `Event` 的继承结构，说出每个字段谁写入、谁读取、在 Flink 里会不会被序列化。

## 讲 1 · `Event` 抽象基类

文件：`flink-risk-common/src/main/java/com/qinyadan/risk/event/Event.java`

```java
public abstract class Event implements Serializable {
    private static final long serialVersionUID = 1L;

    protected String userId;                  // 用户ID
    protected String eventId;                 // 事件ID
    protected long timestamp;                 // 时间戳（毫秒）
    protected String eventType;               // LOGIN / ORDER / ACTIVITY
    protected Map<String, Object> attributes; // 事件属性（自由扩展）
```

### 1.1 逐字段

| 字段 | 谁写 | 谁读 | 说明 |
|------|------|------|------|
| `userId` | Jackson 反序列化 | `keyBy` 分组、CEP 上下文、Redis key、MySQL 列 | 风控的核心维度。为 null 时 `keyBy` 会抛异常 |
| `eventId` | Jackson | `PatternMatchResult.eventId`（被覆盖）、Redis key | 注意：**CEP 命中后会用 UUID 替换它**，原始 eventId 不保留 |
| `timestamp` | 构造器给 `now()`，Jackson 覆盖 | Watermark 分配器、`ORDER_RAPID` 的 `intervalMs` 计算 | 必须是毫秒；秒级会退化成 1970 年附近，CEP 永不触发 |
| `eventType` | 子类构造器常量 + Jackson | 反序列化路由、context、MySQL 列 | 三个子类各自设为 `LOGIN/ORDER/ACTIVITY` |
| `attributes` | 无人写 | 无人读 | 预留扩展位，当前是死字段（见 1.4） |

### 1.2 两个构造器

```java
public Event() {
    this.attributes = new HashMap<>();
    this.timestamp = System.currentTimeMillis();   // 默认值，等 Jackson 覆盖
}

public Event(String userId, String eventId, String eventType) {
    this();                                        // 先初始化默认值
    this.userId = userId;
    this.eventId = eventId;
    this.eventType = eventType;
}
```

**为什么无参构造里要 `new HashMap<>()`**：Jackson 反序列化时会先调无参构造。如果不初始化，`attributes` 为 null，任何 `setAttribute` 调用都会 NPE。同理 `PatternMatchResult` 也在无参构造里初始化两个 Map——这是本项目的统一防御手法。

**为什么 `timestamp` 默认给当前时间**：手工 `new LoginEvent("u1","e1")` 时能有一个合理的事件时间，方便写单测；但如果忘了设置，数据会表现为"永远是最新事件"，掩盖问题。生产路径必须由消息体提供。

### 1.3 方法族

```java
public Map<String, Object> getAttributes() { return attributes; }

public void setAttribute(String key, Object value) { attributes.put(key, value); }
public Object getAttribute(String key) { return attributes.get(key); }
```

注意**没有** `setAttributes(Map)`。这是 Jackson 的一个隐性行为触发点：如果 JSON 里带 `"attributes": {...}`，Jackson 默认开启 `USE_GETTERS_AS_SETTERS`，会把 getter 返回的 Map 当作可写容器，把键值合并进去——恰好能工作，但依赖默认配置，属于"能跑但别依赖"的写法。

### 1.4 `implements Serializable` 的真实含义

`Event` 会出现在三个地方，都必须可序列化：

1. **网络 shuffle**：`keyBy(userId)` 之后，事件可能发往另一台 TaskManager，需要序列化；
2. **CEP 状态**：模式的部分匹配结果存在 keyed state（RocksDB/内存），需要序列化；
3. **闭包**：如果事件被捕获进 lambda，闭包也要序列化。

`serialVersionUID = 1L` 固定下来，是为了**升级类结构后仍能从 checkpoint 恢复**。但这只保证"不因 UID 不匹配报错"，字段增删仍可能导致反序列化得到 null——生产上改 `Event` 字段要考虑 savepoint 兼容。

---

## 讲 2 · 三个子类的字段与构造器

文件：`event/LoginEvent.java`、`OrderEvent.java`、`ActivityEvent.java`

### 2.1 字段对照

| | LoginEvent | OrderEvent | ActivityEvent |
|---|---|---|---|
| 数值 | — | `amount`（BigDecimal）、`quantity`（int） | `couponValue`（BigDecimal）、`participationCount`（int） |
| 标识 | `deviceId` | `orderId`、`productId` | `activityId`、`couponCode` |
| 描述 | `ip`、`location`、`browser`、`failureReason` | `productName`、`paymentMethod`、`deliveryAddress` | `activityName`、`channel`、`source` |
| 布尔 | `success` | — | — |
| 其它 | — | `processTime`（long，未被使用） | — |
| `eventType` | `"LOGIN"` | `"ORDER"` | `"ACTIVITY"` |

### 2.2 构造器三件套

每个类都提供三个构造器，用途不同：

```java
// ① 无参：Jackson 用；同时把 eventType 设成常量
public LoginEvent() {
    super();
    this.eventType = "LOGIN";
}

// ② 最小标识：单测/手工造数用
public LoginEvent(String userId, String eventId) {
    super(userId, eventId, "LOGIN");
}

// ③ 业务快捷构造：只覆盖最常用字段
public LoginEvent(String userId, String eventId, String ip, String deviceId, boolean success) { ... }
```

`OrderEvent` 的 ③ 是 `(userId, eventId, orderId, BigDecimal amount)`；`ActivityEvent` 的 ③ 是 `(userId, eventId, activityId, actionType)`。

**注意 ① 与 ② 的关系**：`super(userId, eventId, "LOGIN")` 走的是 `Event` 的三参构造，最终 `eventType` 也是 `"LOGIN"`。两处硬编码同一个字符串，改常量时要改两处——这是小型重复，但不会有实际风险（`eventType` 在业务上就是这几个字面量）。

### 2.3 为什么金额用 `BigDecimal`，但 context 里是 `double`

- CEP 条件比较：`event.getAmount().compareTo(BigDecimal.ZERO) > 0`——精确比较，避免 `0.1+0.2` 类误差；
- 写入 Aviator 上下文：`latest.getAmount().doubleValue()`——Aviator 对 `BigDecimal` 的比较运算支持不如原生数值直接，且规则作者习惯写 `amount > 10000`。

代价是**精度在进入规则引擎时丢失**。对风控阈值（万元级、两位小数）来说影响可忽略；若要做分账级别的精确计算，应让 Aviator 直接接收 `BigDecimal`（Aviator 支持 `decimal` 类型，但需要显式语法）。

### 2.4 `quantity`、`participationCount`、`processTime` 的真相

| 字段 | 是否被 CEP/规则使用 |
|------|---------------------|
| `OrderEvent.quantity` | 否。规则用的是 `orderCount`（CEP 匹配到的事件条数），不是单笔数量 |
| `ActivityEvent.participationCount` | **否**。context 里的 `participationCount` 是 `events.size()`，不是事件里的这个字段 |
| `OrderEvent.processTime` | 否。完全未使用，属于预留 |

**这是读代码最容易搞混的点**：`context.put("participationCount", events.size())` 会覆盖语义——事件 JSON 里带的 `participationCount` 字段根本不会进入规则上下文。写规则时以为在比较"业务参与次数"，实际比较的是"命中的事件条数"。

---

## 讲 3 · `toString` 与调试约定

三个子类都重写了 `toString()`，但**输出的字段不同**：

| 类 | 输出字段 |
|----|----------|
| `Event` | userId、eventId、timestamp、eventType、attributes |
| `LoginEvent` | userId、eventId、timestamp、ip、deviceId、success、failureReason、location、browser（**没有 eventType**） |
| `OrderEvent` | userId、eventId、timestamp、orderId、amount、productId、quantity、paymentMethod（没有 productName/deliveryAddress） |
| `ActivityEvent` | userId、eventId、timestamp、activityId、activityName、actionType（没有 channel/source/coupon） |

用途：日志里直接 `logger.info("event={}", event)`。不完整是有意的（避免日志过长），但排查"某字段为什么没进规则"时不要指望 `toString` 能看到全部。

> 实测建议：排查时打印 `objectMapper.writeValueAsString(event)`，而不是 `toString()`。

---

# 第 2 部分 · Kafka 反序列化（讲 4-6）

> 目标：说清 `DeserializationSchema` 三个方法各自何时被调用，以及为什么通用反序列化器有个隐藏 bug。

## 讲 4 · `DeserializationSchema<T>` 的接口契约

三个方法：

```java
T deserialize(byte[] message) throws IOException;   // 每条消息调用一次
boolean isEndOfStream(T nextElement);               // Flink 问"流结束了吗"
TypeInformation<T> getProducedType();               // Flink 问"元素类型是什么"
```

| 方法 | 调用时机 | 本项目实现 | 如果写错 |
|------|----------|-----------|----------|
| `deserialize` | 每收到一条 Kafka 消息 | Jackson 映射 | 抛异常 → 默认策略下作业失败 |
| `isEndOfStream` | 每条消息之后 | `return false` | 返回 true 会立刻结束流（无界流必须 false） |
| `getProducedType` | 作业构建 DAG 时一次 | `TypeInformation.of(XxxEvent.class)` | 返回错类型 → 类型不匹配/序列化异常 |

`TypeInformation.of(Class)` 会为 POJO 生成 `PojoTypeInfo`，Flink 据此选择序列化器。POJO 的条件是：public 类、public 无参构造、字段有 getter/setter。本项目的 `Event` 是 `protected` 字段 + getter/setter，符合条件（`Event` 本身是抽象类，但具体类型是 POJO）。

## 讲 5 · 三个专用反序列化器

以 `LoginEventDeserializer` 为例，另外两个只有泛型不同：

```java
public class LoginEventDeserializer implements DeserializationSchema<LoginEvent> {
    private static final long serialVersionUID = 1L;
    private final ObjectMapper objectMapper = new ObjectMapper();   // ①

    @Override
    public LoginEvent deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, LoginEvent.class);   // ②
    }

    @Override
    public boolean isEndOfStream(LoginEvent nextElement) { return false; }

    @Override
    public TypeInformation<LoginEvent> getProducedType() {
        return TypeInformation.of(LoginEvent.class);
    }
}
```

逐行：

- **① `new ObjectMapper()` 作为字段**：`ObjectMapper` 是线程安全的，一个实例可复用；每个反序列化器实例（每个 source 并行子任务一个）持有自己的 mapper，开销可接受。
- **② 直接 `readValue(byte[], Class)`**：字节数组由 Flink 传入，Jackson 内部按 UTF-8 解析。没有指定编码，若生产端用 GBK 写中文，会出现乱码（`location` 字段）。
- **没有配置** `FAIL_ON_UNKNOWN_PROPERTIES=false`：这是最重要的隐性约束（见下）。

### 5.1 未知字段会炸

```java
ObjectMapper mapper = new ObjectMapper();   // FAIL_ON_UNKNOWN_PROPERTIES 默认 true
mapper.readValue("{\"userId\":\"u1\",\"traceId\":\"t1\"}", LoginEvent.class);
// → UnrecognizedPropertyException: Unrecognized field "traceId"
```

后果：生产端加一个埋点字段，整个作业就挂。修复只需一行：

```java
objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
```

**但要注意业务影响**：关掉之后，字段名拼错（`userid` vs `userId`）也会被静默忽略，得到 `userId=null`，后续 `keyBy` 直接抛 NPE。折中方案是保留严格模式 + 在 CI 里用样例 JSON 做契约测试。

### 5.2 三个反序列化器的差异

| 类 | 目标类型 | 代码 |
|----|----------|------|
| `LoginEventDeserializer` | `LoginEvent` | `readValue(message, LoginEvent.class)` |
| `OrderEventDeserializer` | `OrderEvent` | `readValue(message, OrderEvent.class)` |
| `ActivityEventDeserializer` | `ActivityEvent` | `readValue(message, ActivityEvent.class)` |

完全同构。**这是可以消除的重复**：可以写一个 `JsonDeserializer<T>` 泛型类，构造时传 `Class<T>`。当前写法冗余但零风险，改不改都行。

## 讲 6 · 通用反序列化器与它的 bug

文件：`kafka/KafkaEventDeserializer.java`

```java
public Event deserialize(byte[] message) throws IOException {
    JsonNode root = objectMapper.readTree(message);                 // ① 先解析成树
    String eventType = root.has("eventType") ? root.get("eventType").asText() : "";

    switch (eventType.toUpperCase()) {                              // ② 按类型分派
        case "LOGIN":    return objectMapper.readValue(message, LoginEvent.class);
        case "ORDER":    return objectMapper.readValue(message, OrderEvent.class);
        case "ACTIVITY": return objectMapper.readValue(message, ActivityEvent.class);
        default:         return objectMapper.readValue(message, Event.class);   // ③ 抽象类！
    }
}
```

逐行分析：

- **① `readTree` 后再 `readValue(bytes)`**：同一条消息被解析两次，CPU 开销翻倍。更优写法是 `objectMapper.treeToValue(root, XxxEvent.class)`，复用已解析的树。
- **② `eventType.toUpperCase()`**：`eventType` 为 null 时 `asText()` 返回 `""`，`"".toUpperCase()` 安全，不会 NPE。
- **③ 致命 bug**：`Event` 是 `abstract class`，Jackson 无法实例化，必然抛：

```
InvalidDefinitionException: Cannot construct instance of
`com.qinyadan.risk.event.Event` (no Creators, like default constructor, exist):
abstract types either need to be mapped to concrete types,
have custom deserializer, or contain additional type information
```

由于该方法在 `deserialize` 里直接抛出，异常会上抛给 Kafka Source。Flink Kafka Source 对反序列化异常**没有内置跳过策略**，默认让作业失败。

**为什么线上没炸**：`KafkaSourceFactory` 的三个工厂方法都用专用反序列化器，`createUnifiedSource()`（唯一使用通用反序列化器的方法）在主链路里没被调用。

正确改法（三选一）：

```java
// 方案 A：返回 null，下游 filter 掉
default: return null;

// 方案 B：定义 UnknownEvent 子类承载原始 JSON
default: {
    Event e = new UnknownEvent();
    e.setEventType(eventType);
    e.setEventId(root.path("eventId").asText(null));
    e.setUserId(root.path("userId").asText(null));
    return e;
}

// 方案 C：显式抛业务异常，让监控能定位
default: throw new IOException("Unsupported eventType: " + eventType);
```

---

# 第 3 部分 · 规则数据结构（讲 7-9）

> 目标：能在不看代码的情况下写出 `rule_config` 一行数据 → `RuleConfig` 对象的完整映射关系。

## 讲 7 · `RuleConfig` 字段与默认值

文件：`rule/RuleConfig.java`

```java
public class RuleConfig implements Serializable {
    private String id;              // 注意是 String，不是 Long
    private String name;
    private String expression;
    private RiskLevel riskLevel;    // 枚举
    private String action;          // 字符串，不是枚举
    private int weight;
    private boolean enabled;
    private String description;
    private long createTime;
    private long updateTime;

    public RuleConfig() {
        this.createTime = System.currentTimeMillis();
        this.updateTime = System.currentTimeMillis();
        this.enabled = true;        // 默认启用
        this.weight = 1;            // 默认权重 1
    }
}
```

### 7.1 逐字段决策

| 字段 | 决策 | 理由与影响 |
|------|------|-----------|
| `id` 用 `String` | 为了兼容 YAML（id 可能是字符串） | 从 MySQL 读取时用 `String.valueOf(rs.getLong("id"))` 转换；`AviatorRuleEngine` 用 `Map<String,RuleConfig>` 存 |
| `riskLevel` 用枚举 | 保证取值受控 | 从库里读时 `RiskLevel.valueOf(...)`，**大小写敏感** |
| `action` 用 String | 允许自由扩展（ALLOW/VERIFY/REVIEW/WARN/BLOCK...） | 前端 `ACTION_CONFIG` 也有一份对应表，两边要手工保持一致 |
| `weight` 默认 1 | 没填权重时不影响"就高不就低"的等级比较 | 只在同等级命中时作为 tie-breaker |
| `createTime/updateTime` 用 long | 方便序列化 | 但 `RuleSourceFunction` 根本不查这两列（见 job 详解第 2 层），所以值永远是"对象创建时刻" |

### 7.2 为什么没有 `equals/hashCode`

`RuleConfig` 在 `executeRules` 里只是被遍历、被 `List.add`，从不用作 Map 的 key 或去重。缺省的对象身份比较足够。**但如果将来要做"规则去重/对比变更"，需要补 `equals`**（按 id 即可）。

### 7.3 它会被放进哪三种容器

```
Map<String, RuleConfig> ruleConfigs            （引擎内部）
List<RuleConfig>        广播快照               （Flink 状态）
List<RuleConfig>        RuleSourceFunction 的输出
```

三种容器都要求可序列化，所以 `implements Serializable` + 固定 `serialVersionUID` 是必须的。

## 讲 8 · `RiskLevel`：分数体系与一个盲区

文件：`rule/RiskLevel.java`

```java
public enum RiskLevel {
    LOW(10, "低风险"),
    MEDIUM(50, "中风险"),
    HIGH(80, "高风险"),
    CRITICAL(100, "严重风险");
```

### 8.1 `getScore()` 的两种用法

| 用法 | 位置 | 语义 |
|------|------|------|
| 作为最终风险分 | `RiskActionResolver.resolve`：`result.setRiskScore(topRule.getRiskLevel().getScore())` | 命中规则的等级分直接当结果分 |
| 作为比较依据 | `selectTopPriorityRule`：`Integer.compare(a.getRiskLevel().getScore(), ...)` | 等级的数值化排序 |
| 作为加权项 | `AviatorRuleEngine.calculateRiskScore`：`score * weight` | 未接入主链路 |

所以 `10/50/80/100` 不是"概率"，而是"等级代表分"。改这四个数字会直接影响 `risk_result.risk_score` 和看板统计口径。

### 8.2 `fromScore` 的 CRITICAL 盲区

```java
public static RiskLevel fromScore(int score) {
    if (score >= 80)  return HIGH;     // ← 100 也走这里
    else if (score >= 50) return MEDIUM;
    else if (score >= 10) return LOW;
    return LOW;
}
```

**这个方法永远返回不了 `CRITICAL`**：`score >= 80` 的分支先命中，100 分也返回 `HIGH`。也就是说 `fromScore(100) == HIGH`。

影响范围：只有 `AviatorRuleEngine.getRiskLevel()` 调用它，而该方法主链路未使用——所以当前**没有实际 bug 表现**。但一旦有人把 `getRiskLevel()` 接进主链路，就会出现"严重风险被降级为高风险"。

正确实现应改为：

```java
if (score >= 100) return CRITICAL;
if (score >= 80)  return HIGH;
if (score >= 50)  return MEDIUM;
if (score >= 10)  return LOW;
return LOW;
```

### 8.3 枚举与数据库字符串的契约

| 层 | 表示 |
|----|------|
| MySQL `risk_level` | `'LOW' / 'MEDIUM' / 'HIGH' / 'CRITICAL'`（大写） |
| Java | `RiskLevel.LOW` 等 |
| `RiskResult.riskLevel`（写入 MySQL） | `result.getRiskLevel().name()` |
| 前端 | 字符串 `'LOW'` 等 + `RISK_LEVEL_CONFIG` 映射 |

**任何一层出现小写或拼写差异，都会在 `RiskLevel.valueOf` 处抛 `IllegalArgumentException`**。而 `RuleSourceFunction` 的这个异常被外层 catch 后，会导致**整轮规则加载失败**（不是跳过单条）。这是"改库把风控搞挂"的最短路径。

## 讲 9 · `RuleLoader`：YAML 加载（未被使用）

文件：`rule/RuleLoader.java`

```java
public static List<RuleConfig> loadFromYaml(String classpathResource) {
    try (InputStream is = RuleLoader.class.getClassLoader().getResourceAsStream(classpathResource)) {
        if (is == null) { logger.warn(...); return rules; }

        Yaml yaml = new Yaml();
        Map<String, Object> root = yaml.load(is);                     // ①
        List<Map<String, Object>> ruleList = (List<Map<String, Object>>) root.get("rules");   // ②

        for (Map<String, Object> item : ruleList) {
            RuleConfig config = parseRuleConfig(item);
            if (config != null) rules.add(config);
        }
    } catch (Exception e) {
        logger.error("Failed to load rules from: {}", classpathResource, e);
    }
    return rules;
}
```

逐行：

- **① `new Yaml().load(is)`**：SnakeYAML 解析整个文件为一个嵌套 `Map/List` 结构；**没有使用 `SafeConstructor`**，理论上可被恶意 YAML 触发任意类实例化（YAML 反序列化漏洞）。因为只加载 classpath 内的可信文件，风险可控，但规范写法应是 `new Yaml(new SafeConstructor(new LoaderOptions()))`。
- **② `root` 为 null 的风险**：空 YAML 文件会让 `yaml.load` 返回 null，随后 `root.get("rules")` 抛 NPE，被外层 catch 捕获后返回空列表——行为可接受（会记一条 error 日志）。
- **③ `ruleList` 的 null 检查是有的**：`if (ruleList == null) { warn; return rules; }`，所以只有 `rules:` 缺省不会炸；真正没防住的是 `root` 为 null（见 ②）。

`parseRuleConfig` 的字段映射：

```java
String id = (String) item.get("id");
String name = (String) item.get("name");
String expression = (String) item.get("expression");
String riskLevelStr = (String) item.get("risk_level");     // 蛇形命名
String action = (String) item.get("action");
String description = (String) item.get("description");
boolean enabled = item.get("enabled") == null || (boolean) item.get("enabled");   // 默认 true
int weight = item.get("weight") != null ? ((Number) item.get("weight")).intValue() : 1;
```

三个细节：

1. **`risk_level` 用下划线**，与数据库列名一致，但与 Java 驼峰不一致——这是一种"配置贴合 DDL"的取舍。
2. **`weight` 先转 `Number` 再 `intValue()`**：YAML 里写 `weight: 5` 会被解析成 `Integer`，写 `5.0` 会被解析成 `Double`，统一走 `Number` 才不会 `ClassCastException`。
3. **`id` 或 `expression` 为空时返回 null**（跳过该条），并且 `RiskLevel.valueOf` 抛异常时被 `parseRuleConfig` 内部的 catch 捕获、返回 null——**单条坏规则不会中断整份文件加载**，这一点比 `RuleSourceFunction` 的"整轮失败"更健壮。

`loadAndRegister(engine, resource)` 只是 `loadFromYaml` + `registerRules` 的组合。

**现状**：全仓库没有调用方（`grep -r RuleLoader` 只有它自己），`flink-risk-job/src/main/resources/rules.yaml` 是配套样例。它的存在会误导"规则有两套来源"。若要保留，建议加 `@Deprecated` 或删除；若要启用，需在 `RiskControlApplication` 里合并 MySQL 与 YAML 两份规则（并解决 id 冲突）。

---

# 第 4 部分 · AviatorRuleEngine（讲 10-15）

> 本模块核心。读的时候建议手边放一张纸，画出"规则注册 → 缓存 → 求值 → 异常兜底"的路径。

## 讲 10 · 字段与构造器：两级缓存

```java
private final Cache<String, Expression> expressionCache;   // 表达式原文 → 编译产物
private final Map<String, RuleConfig> ruleConfigs;         // 规则ID → 规则

public AviatorRuleEngine(int cacheSize, int cacheTTLSeconds) {
    this.expressionCache = CacheBuilder.newBuilder()
            .maximumSize(cacheSize)
            .expireAfterAccess(cacheTTLSeconds, TimeUnit.SECONDS)
            .build();
    this.ruleConfigs = new HashMap<>();
}
```

### 10.1 为什么需要 Guava Cache（Aviator 自己不是有缓存吗）

Aviator 的 `AviatorEvaluator.compile(expression)` 内部确实有编译缓存，但它有两个问题对 Flink 场景不友好：

1. **无界**：缓存 entry 数没有上限，规则被反复改动（每次广播都是新引擎，但 Aviator 的缓存在 JVM 级静态）时可能持续增长；
2. **不可控过期**：没有 TTL 概念。

外面这层 Guava Cache 提供了 `maximumSize=1000` 与 `expireAfterAccess=3600s` 两个可调旋钮。代价是**同一表达式可能被编译两次**（Guava 未命中 → 调 Aviator → Aviator 也可能未命中），但这个开销只在规则首次生效时发生。

### 10.2 参数选择

调用方固定传 `new AviatorRuleEngine(1000, 3600)`：

- `1000`：远大于当前 15 条规则，留足扩展空间；超过后 LRU 淘汰；
- `3600`：1 小时空闲过期。规则每 30 秒广播一次、每次新建引擎，理论上新引擎缓存是空的——**TTL 在这里其实没机会生效**，因为引擎实例自身的生命周期取决于下一次广播。这也说明"每次广播新建引擎"让缓存的价值大打折扣（见讲 15）。

### 10.3 `ruleConfigs` 为什么用 `HashMap` 而不是 `ConcurrentHashMap`

设计前提是"单线程持有 + 整体替换"（讲 15），不需要并发容器。用 `HashMap` 换取更低的读写开销与更简单的语义。

**但如果有人违反约定、在运行中调 `removeRule`**，就会遇到 `ConcurrentModificationException`（`executeRules` 正在遍历 `values()` 时结构被修改）。这是本项目最隐蔽的并发雷。

## 讲 11 · `registerRule`：编译期拦截

```java
public void registerRule(RuleConfig ruleConfig) {
    if (!ruleConfig.isEnabled()) {
        return;
    }
    try {
        AviatorEvaluator.compile(ruleConfig.getExpression());
    } catch (Exception e) {
        logger.error("Rule expression compilation failed, SKIP register: {} - {}, expression: [{}], error: {}",
                ruleConfig.getId(), ruleConfig.getName(), ruleConfig.getExpression(), e.getMessage());
        return;
    }
    ruleConfigs.put(ruleConfig.getId(), ruleConfig);
    logger.info("Rule registered: {} - {}", ruleConfig.getId(), ruleConfig.getName());
}
```

### 11.1 执行顺序的三层过滤

| 层 | 条件 | 效果 |
|----|------|------|
| ① 启用检查 | `!enabled` → return | 库里 `enabled=0` 的规则不进引擎（`RuleSourceFunction` 也已过滤，双保险） |
| ② 编译校验 | 编译抛异常 → return | 语法错误规则不注册，避免运行期反复报错 |
| ③ 注册 | `ruleConfigs.put(id, rule)` | 同 id 会覆盖（后到的赢） |

### 11.2 编译校验能拦住什么、拦不住什么

| 问题类型 | 例子 | 编译期能拦住？ |
|----------|------|----------------|
| 语法错误 | `failCount >= ` | ✅ |
| 未知函数 | `unknownFunc(1)` | ✅（Aviator 编译期检查函数是否存在） |
| 变量名拼错 | `failcount >= 3` | ❌（Aviator 把未知变量当 nil，运行期才炸或返回 false） |
| `=~` 右侧用字符串 | `deviceId =~ "abnormal"` | ❌（语法合法，运行期抛 `ExpressionRuntimeException`） |
| 类型不匹配 | `failCount + "abc"` | ❌（运行期） |
| 空表达式 | `expression = null` | ⚠️ `compile(null)` 行为未定义，可能 NPE 被 catch |

结论：**注册期只能保证"语法合法"，不能保证"语义正确"**。所以线上排查"规则为什么没命中"，第一步永远是看 `[EVAL]` 附近的 error 日志（讲 13 有专门的日志文本）。

### 11.3 id 为 null 的隐患

`ruleConfigs.put(null, rule)` 对 `HashMap` 是合法的（允许 null key），规则能被注册、能被遍历执行。但 `removeRule(null)` 之类会变得难以推理。当前两个来源（MySQL、YAML）都保证 id 非空，暂时安全。

## 讲 12 · 批量注册与增删查

```java
public void registerRules(List<RuleConfig> rules) {
    for (RuleConfig rule : rules) {
        registerRule(rule);
    }
}

public void clearRules() { ruleConfigs.clear(); logger.info("All rules cleared"); }

public void removeRule(String ruleId) { ruleConfigs.remove(ruleId); logger.info("Rule removed: {}", ruleId); }

public int getRuleCount() { return ruleConfigs.size(); }

public Collection<RuleConfig> getAllRules() { return ruleConfigs.values(); }
```

### 12.1 `registerRules` 的失败语义

逐条调用，单条编译失败只跳过它自己。**没有返回值告诉调用方"成功了几条"**——调用方（`RuleEvaluationFunction`）只能事后用 `getRuleCount()` 拿到总数，通过日志对比"广播了 N 条 / 注册了 M 条"：

```
Broadcast rules snapshot: 15 rules
Rule registered: ... （15 行）
Rule engine refreshed in this task: 15 rules loaded
```

如果日志里出现 `Rule expression compilation failed` 且最终 `loaded` 数偏小，就说明有规则被丢弃。

### 12.2 `getAllRules()` 返回的是视图

`ruleConfigs.values()` 是 `HashMap` 的**实时视图**，不是快照。调用方遍历时如果引擎被替换，拿到的是旧引擎的视图（安全）；但如果有人对同一个引擎实例增删规则，会 OOM/CME。当前无调用方，属于 API 设计缺陷。

### 12.3 `clearRules` / `removeRule` 与热更新约定冲突

类注释明确"单线程持有、运行中不修改"，但这两个方法为运行中修改提供了入口。**正确用法只有**：在构造新引擎的初始化阶段使用（此时还没发布出去）。

## 讲 13 · `executeRules`：遍历与异常隔离

```java
public List<RuleConfig> executeRules(Map<String, Object> context) {
    List<RuleConfig> matchedRules = new ArrayList<>();

    for (RuleConfig rule : ruleConfigs.values()) {
        if (!rule.isEnabled()) {
            continue;
        }
        try {
            boolean result = evaluateExpression(rule.getExpression(), context);
            if (result) {
                matchedRules.add(rule);
                logger.debug("Rule matched: {}", rule.getId());
            }
        } catch (Exception e) {
            logger.error("Error executing rule: {}", rule.getId(), e);
        }
    }
    return matchedRules;
}
```

### 13.1 三重保险

| 保险 | 位置 | 作用 |
|------|------|------|
| 跳过禁用 | `if (!rule.isEnabled()) continue` | 注册时已过滤，此处防"运行中被禁用"（尽管当前不会发生） |
| 双层 try | 这里 + `evaluateExpression` 内部 | `evaluateExpression` 已保证不抛，这里防未来改动引入异常 |
| 单条隔离 | catch 包住单条 | 一条规则异常不影响其它规则 |

### 13.2 遍历顺序不确定

`HashMap.values()` 的顺序取决于 key 的哈希与容量，**不保证稳定**。这带来一个微妙后果：

```java
matchedRules.stream().max(comparator)   // 平局时返回"先遇到"的元素（max 的约定是遇到相等返回前者）
```

当两条规则**等级分相同、权重也相同**时，最终选谁取决于 `HashMap` 遍历顺序——**同一个用户在不同 JVM/不同时刻可能得到不同的 action**。虽然只影响"平局"，但在风控里应当确定。

修法：`selectTopPriorityRule` 加第三个比较维度（id 或 name），或在 `executeRules` 里对结果排序。

### 13.3 O(N) 复杂度

每条 CEP 命中都要把全部规则跑一遍。当前 15 条无感；1000 条 × 每秒数千命中 = 每秒百万次表达式求值，会成为瓶颈。优化方向：

```java
// 注册时按 eventType/patternType 建二级索引，执行时先缩小候选集
Map<String, List<RuleConfig>> byEventType;
```

前提是规则表达式能被解析出"它关心哪种事件"——可以在规则表加一列 `event_type`，比解析表达式更可靠。

## 讲 14 · `evaluateExpression`：三层异常与类型判定

```java
public boolean evaluateExpression(String expression, Map<String, Object> context) {
    try {
        Expression compiledExpression = getOrCompileExpression(expression);
        Object result = compiledExpression.execute(context);
        return result instanceof Boolean ? (Boolean) result : false;
    } catch (com.googlecode.aviator.exception.ExpressionRuntimeException e) {
        logger.error("Expression runtime error (check =~ syntax, use /pattern/ not \"pattern\"): {}, contextKeys={}, tip: use 'source =~ /脚本/' or 'string.contains(source,\"脚本\")'",
                expression, context.keySet());
        return false;
    } catch (Exception e) {
        logger.error("Error evaluating expression: {}, error: {}, contextKeys={}", expression, e.getMessage(), context.keySet());
        return false;
    }
}
```

### 14.1 逐行

- **`getOrCompileExpression`**：取缓存或编译；编译失败会抛 `RuntimeException`（被第二层 catch 接住）；
- **`execute(context)`**：Aviator 用 `Map` 作为变量表，`context` 的 key 就是表达式里能用的变量名；
- **`instanceof Boolean`**：Aviator 表达式可能返回 `Long/Double/String/null`。只有严格 `Boolean.TRUE` 才算命中，`null` 视为未命中。
- **专门的 `ExpressionRuntimeException` 分支**：这是个"经验补丁"，说明作者被 `=~` 坑过，特意把提示写进日志。

### 14.2 `=~` 的完整规则

Aviator 的正则匹配运算符：

```
"abc123" =~ /abc/          // ✅ 右侧是正则字面量
"abc123" =~ "abc"          // ❌ 运行期抛 ExpressionRuntimeException
string.contains("abc123", "abc")   // ✅ 退而求其次
```

`sql/init.sql` 里有 4 条规则用了错误写法：

| 规则 | 表达式片段 | 结果 |
|------|-----------|------|
| 可疑设备登录 | `deviceId =~ "abnormal"` | 永远不命中 |
| 异常商品下单 | `productId =~ "ABNORMAL" \|\| productName =~ "异常"` | 永远不命中 |
| 可疑支付方式 | `paymentMethod =~ "可疑"` | 永远不命中 |
| 异常渠道参与 | `source =~ "脚本"` | 永远不命中（`channel == "bot"` 那半边仍可能命中） |

### 14.3 "失败开放"的代价

任何求值错误都返回 `false`（未命中）→ 该规则被跳过 → 如果没有其它规则命中，最终结果是 `LOW/ALLOW`。也就是说：**规则写错 = 风控静默失效**。

补偿手段只能靠外部监控：

```
告警规则：5 分钟内 "Expression runtime error" 日志 > 0 即告警
```

### 14.4 性能细节

- 每次 `executeRules` 都调用 `evaluateExpression`，每次都要 `Cache.get` 一次（ConcurrentHashMap 读，开销很小）；
- `context.keySet()` 只在异常分支求值，正常路径无额外开销；
- 表达式执行本身是 AST 解释执行，比原生 Java 慢约 10~100 倍，但远快于一次 IO。

## 讲 15 · 编译缓存、加权评分与线程模型

### 15.1 `getOrCompileExpression`

```java
private Expression getOrCompileExpression(String expression) {
    try {
        return expressionCache.get(expression, () -> AviatorEvaluator.compile(expression));
    } catch (ExecutionException e) {
        logger.error("Failed to compile expression: {}", expression, e);
        throw new RuntimeException("Expression compilation failed", e);
    }
}
```

`Cache.get(key, Callable)` 的语义：

1. 命中 → 返回；
2. 未命中 → 调 `Callable.call()` 编译，成功则写入缓存并返回；
3. loader 抛异常 → 包成 `ExecutionException` 抛出（这里再包一层 `RuntimeException`）。

注意 `AviatorEvaluator.compile(expression)` 使用**默认编译选项**。如果规则要用自定义函数（如 `string.contains`），需要提前 `AviatorEvaluator.addFunction` 注册——当前项目依赖 Aviator 内置函数（`string.contains` 是内置的）。

### 15.2 `calculateRiskScore` 与 `getRiskLevel`

```java
public int calculateRiskScore(Map<String, Object> context) {
    List<RuleConfig> matchedRules = executeRules(context);
    int totalScore = 0, totalWeight = 0;
    for (RuleConfig rule : matchedRules) {
        totalScore += rule.getRiskLevel().getScore() * rule.getWeight();
        totalWeight += rule.getWeight();
    }
    if (totalWeight == 0) return 0;
    return totalScore / totalWeight;      // 整数除法，向下取整
}

public RiskLevel getRiskLevel(Map<String, Object> context) {
    int score = calculateRiskScore(context);
    return RiskLevel.fromScore(score);
}
```

三个要点：

1. **重复执行**：`calculateRiskScore` 内部又调 `executeRules`，如果外部已经调过一次，等于同一批规则跑了两遍。方法注释也提示了这一点。
2. **整数除法**：`(80*10 + 50*5) / 15 = 1050/15 = 70`，落在 `MEDIUM` 区间，**CRITICAL 规则被稀释成 MEDIUM**。这是加权平均的固有特性，风控通常不想要。
3. **与主链路冲突**：`RiskActionResolver` 用的是"取最高危规则"，不是这个评分函数。**两套口径并存**，读代码时必须认清哪套在跑。

### 15.3 线程模型：volatile 发布

跨模块的关键约定（`RuleEvaluationFunction` 侧）：

```java
private transient volatile AviatorRuleEngine ruleEngine;

// open()：先给个空引擎，避免 null
this.ruleEngine = new AviatorRuleEngine(1000, 3600);

// 每次规则广播：新建 → 完整注册 → 一次性替换引用
AviatorRuleEngine newEngine = new AviatorRuleEngine(1000, 3600);
newEngine.registerRules(rules);
this.ruleEngine = newEngine;                    // volatile 写，安全发布
this.resolver = new RiskActionResolver(newEngine);
```

**为什么这样是线程安全的**（Java 内存模型）：

1. `newEngine` 的所有构造/注册动作都发生在写 `volatile` 之前（happens-before）；
2. 处理线程读 `volatile ruleEngine` 时会看到写线程在写之前的全部动作，因此能看到**完全初始化**的引擎；
3. 旧引擎对象仍被正在执行的方法栈引用，不会被立刻回收，也不会有"半个引擎"的状态。

**代价**：每次广播都重新编译全部表达式（1000 条规则 × 每个 subtask × 每 30 秒）。优化思路：只在规则内容哈希变化时重建；或让引擎支持"原子替换内部 Map"而不是整体重建。

---

# 第 5 部分 · RiskActionResolver（讲 16-18）

> 目标：能把"命中 3 条规则"手工推导出最终的 level/score/action/details。

## 讲 16 · `resolve` 的签名与前置

```java
public RiskResult resolve(Map<String, Object> context,
                          String userId,
                          String eventId,
                          String eventType,
                          Map<String, Object> details) {

    RiskResult result = new RiskResult(userId, eventId, eventType);
    List<RuleConfig> matchedRules = ruleEngine.executeRules(context);
    ...
}
```

参数来源（`RuleEvaluationFunction.processElement`）：

| 参数 | 来源 |
|------|------|
| `context` | `PatternMatchResult.getContext()`（CEP 侧的 `buildXxxContext`） |
| `userId` / `eventId` / `eventType` | `PatternMatchResult` 的同名字段 |
| `details` | `PatternMatchResult.getDetails()`（CEP 侧的审计信息） |

注意 `eventId` 已经是 **UUID**（CEP 命中时生成），不是原始 Kafka 消息的 eventId。

## 讲 17 · 未命中分支

```java
if (matchedRules.isEmpty()) {
    result.setRiskLevel(RiskLevel.LOW);
    result.setRiskScore(0);
    result.setAction("ALLOW");
    result.getDetails().put("reason", "No rule matched");
    logger.debug("No rule matched for user: {} event: {}", userId, eventType);
}
```

行为：

- 等级 `LOW`，分数 `0`（注意：`LOW` 的等级分是 10，这里却写 0——**分数与等级不完全自洽**，看板上"低风险"事件的 score 会是 0）；
- action 固定 `ALLOW`；
- **没有合并 `details`**（讲 18 详述）。

这个分支对应业务语义："命中了异常模式，但没有匹配到任何风控规则"——通常意味着规则配置缺失，应当监控。但当前只打 `debug` 日志，线上 INFO 级别看不到。

## 讲 18 · 命中分支与比较器逐行

```java
RuleConfig topRule = selectTopPriorityRule(matchedRules);

result.setRiskLevel(topRule.getRiskLevel());
result.setRiskScore(topRule.getRiskLevel().getScore());
result.setAction(topRule.getAction());

result.getDetails().put("matchedRuleId", topRule.getId());
result.getDetails().put("matchedRuleName", topRule.getName());
result.getDetails().put("matchedRuleCount", matchedRules.size());

if (details != null) {
    result.getDetails().putAll(details);
}
```

### 18.1 逐字段

| 结果字段 | 取值 | 说明 |
|----------|------|------|
| `riskLevel` | 最高危规则的等级 | 枚举 |
| `riskScore` | 该等级的固定分（10/50/80/100） | **不是**规则的 `weight`，也不是加权平均 |
| `action` | 该规则的 action | 自由字符串，前端映射成中文 |
| `details.matchedRuleId` | 规则 id（String） | |
| `details.matchedRuleName` | 规则名 | **看板"规则命中排行"依赖这个字段** |
| `details.matchedRuleCount` | 命中条数 | 只告诉你有几条，不告诉你是哪几条 |
| `details.*` | CEP 的 details | `failCount`、`intervalMs`、`couponCode` 等 |

**只记录 topRule，不记录全部命中规则**。若想审计"为什么选了这条"，需要额外加 `matchedRuleIds` 列表。这是最值得补的审计字段。

### 18.2 `selectTopPriorityRule`

```java
private RuleConfig selectTopPriorityRule(List<RuleConfig> matchedRules) {
    return matchedRules.stream()
            .max((a, b) -> {
                int scoreCompare = Integer.compare(
                        a.getRiskLevel().getScore(), b.getRiskLevel().getScore());
                if (scoreCompare != 0) return scoreCompare;
                return Integer.compare(a.getWeight(), b.getWeight());
            })
            .orElse(matchedRules.get(0));
}
```

比较器即"排序规则"：

```
等级分高者胜 → 等级相同看权重高者 → 都相同看遍历顺序（HashMap 顺序，不确定）
```

`Stream.max(comparator)` 的细节：它要求比较器是"全序"；当比较器返回 0 时，`max` 返回**先遇到**的元素（`reduce` 语义）。所以平局结果依赖 `matchedRules` 的顺序，而该顺序来自 `HashMap` 遍历——不确定。

### 18.3 `details` 合并的不对称（真实缺陷）

| 分支 | 是否合并 CEP details | 后果 |
|------|---------------------|------|
| 未命中 | ❌ | `risk_result.details` 只有 `{"reason":"No rule matched"}`，看不到 `failCount` 等上下文 |
| 命中 | ✅ | 详情完整 |
| 规则未加载（`RuleEvaluationFunction` 的 fail-open） | ✅ | 反而是完整的 |

三处行为不一致。若希望审计表里**永远**能看到 CEP 上下文，把未命中分支也加上：

```java
if (details != null) result.getDetails().putAll(details);
```

---

# 第 6 部分 · RiskResult 与 Redis（讲 19-22）

## 讲 19 · `RiskResult` 字段与 Redis Key

```java
public class RiskResult implements Serializable {
    private String userId;
    private String eventId;
    private String eventType;
    private RiskLevel riskLevel;
    private int riskScore;
    private String action;
    private Map<String, Object> details;
    private long timestamp;
    private long ttl;
```

构造器：

```java
public RiskResult() {
    this.details = new HashMap<>();
    this.timestamp = System.currentTimeMillis();
    this.ttl = 3600;              // 默认 1 小时
}
```

三个 Sink 各自取用不同字段：

| Sink | 用到的字段 |
|------|-----------|
| Redis | 全部（但 `toJsonString` 只序列化一部分，见讲 20） |
| MySQL | userId/eventId/eventType/riskLevel/riskScore/action/details |
| Kafka | 全部（Jackson 全字段序列化） |

`ttl` 是**秒**，只在 Redis 侧使用（`SETEX`）。MySQL/Kafka 忽略它。

### 19.1 `getRedisKey`

```java
public String getRedisKey() {
    return String.format("risk:%s:%s:%s", eventType.toLowerCase(), userId, eventId);
}
```

例：`risk:login:user_001:8f14e45f-ea0c-4f2a-...`

为什么把 `eventType` 放最前：方便按类型扫描（`SCAN MATCH risk:order:*`）。为什么带 `eventId` 而不是 `userId` 单键：一次命中一条记录，不覆盖历史（`userId` 作 key 会互相覆盖，只剩最后一条）。

**注意**：`docs/ARCHITECTURE.md` 写的 `rule_engine:risk:` 前缀是错的。

### 19.2 `eventType` 为 null 时会 NPE

`eventType.toLowerCase()` 对 null 抛 NPE。`RiskResult` 的三个来源（Resolver / fail-open 分支）都保证非空，但如果将来有新的构造路径，会在这里炸。

## 讲 20 · `toJsonString` 逐字符剖析

```java
public String toJsonString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("\"userId\":\"").append(userId).append("\",");
    sb.append("\"eventId\":\"").append(eventId).append("\",");
    sb.append("\"eventType\":\"").append(eventType).append("\",");
    sb.append("\"riskLevel\":\"").append(riskLevel).append("\",");     // ①
    sb.append("\"riskScore\":").append(riskScore).append(",");
    sb.append("\"action\":\"").append(action).append("\",");
    sb.append("\"timestamp\":").append(timestamp);
    sb.append("}");
    return sb.toString();
}
```

### 20.1 缺陷清单

| # | 缺陷 | 后果 |
|---|------|------|
| ① | `riskLevel` 是枚举，`append(Object)` 调 `toString()` | 产出 `"RiskLevel{score=80, description='高风险'}"` |
| ② | `details` 完全没有写入 | Redis 里看不到命中规则与业务上下文 |
| ③ | 字符串值未做 JSON 转义 | `userId` 含 `"` 或 `\` 会生成非法 JSON（UUID 场景不会，但 userId 来自外部输入） |
| ④ | 手写拼接性能并不比 Jackson 好 | 维护成本高、易错 |
| ⑤ | `action` 为 null 时输出 `"action":"null"` | 字符串 `"null"` 而非 JSON `null` |

### 20.2 实际产出示例

```json
{"userId":"user_001","eventId":"8f14e45f-...","eventType":"LOGIN","riskLevel":"RiskLevel{score=80, description='高风险'}","riskScore":80,"action":"BLOCK","timestamp":1710000000000}
```

它**是合法 JSON**（内层是单引号），但 `riskLevel` 字段值对任何消费者都无意义。

### 20.3 正确实现

```java
private static final ObjectMapper MAPPER = new ObjectMapper();

public String toJsonString() {
    try {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("userId", userId);
        m.put("eventId", eventId);
        m.put("eventType", eventType);
        m.put("riskLevel", riskLevel == null ? null : riskLevel.name());   // 用 name()
        m.put("riskScore", riskScore);
        m.put("action", action);
        m.put("details", details);
        m.put("timestamp", timestamp);
        return MAPPER.writeValueAsString(m);
    } catch (JsonProcessingException e) {
        throw new RuntimeException("Failed to serialize RiskResult", e);
    }
}
```

## 讲 21 · `RedisClient`：池配置与每个方法

```java
public class RedisClient implements Serializable {
    private transient JedisPool jedisPool;
    private final String host;
    private final int port;
    private final int database;
    private final int timeout;          // 连接超时（毫秒）

    public RedisClient(String host, int port, int database, int timeout) {
        ...
        initPool();                     // 构造即建池
    }
```

### 21.1 池配置逐项

```java
JedisPoolConfig config = new JedisPoolConfig();
config.setMaxTotal(10);       // 最大连接数
config.setMaxIdle(5);         // 最大空闲
config.setMinIdle(2);         // 最小空闲（预热）
config.setTestOnBorrow(true); // 借出前 ping，避免拿到死连接
config.setTestOnReturn(true);// 归还前 ping

this.jedisPool = new JedisPool(config, host, port, timeout);
```

参数选择评价：

- `maxTotal=10` 对单个 SinkWriter 足够（写入是同步的，并发度受 Flink 算子线程数限制）；
- `testOnBorrow/testOnReturn` 双开会在每次读写多一次 `PING` 往返，吞吐敏感场景通常只开 `testOnBorrow`；
- **没有传 password**，`new JedisPool(config, host, port, timeout)` 的签名里没有密码位。compose 里 Redis 带 `requirepass redis123` → 所有命令返回 `NOAUTH`。

### 21.2 为什么构造器里就 `initPool()`

`RedisClient` 在 `RedisSinkWriter` 的构造器里被 `new`，而 `SinkWriter` 由 Flink 在每个并行子任务里创建一次——先建池再写数据，符合"算子生命周期"。但如果 `RedisClient` 被当作算子字段（而不是 Writer 内部字段），序列化时 `jedisPool` 是 `transient` 会丢，反序列化后为 null → 所有操作 NPE。当前用法安全。

### 21.3 每个方法的统一模式

```java
public void setWithTTL(String key, String value, long ttlSeconds) {
    try (Jedis jedis = jedisPool.getResource()) {
        jedis.select(database);
        jedis.setex(key, ttlSeconds, value);
    } catch (Exception e) {
        logger.error("Error setting key with TTL: {}", key, e);
    }
}
```

| 方法 | 命令 | 失败返回 |
|------|------|----------|
| `set` | `SET` | 无（void） |
| `setWithTTL` | `SETEX key ttl value` | 无 |
| `get` | `GET` | `null` |
| `delete` | `DEL` | 无 |
| `exists` | `EXISTS` | `false` |
| `getTTL` | `TTL` | `-1` |
| `increment` | `INCRBY` | 无 |
| `close` | 关闭池 | — |

三个共同问题：

1. **每次 `select(database)`**：多一次往返，且连接池里的连接 db 状态会互相影响（虽然每次重置，但语义上不如在池配置里指定 db）；
2. **异常只记日志**：调用方无法感知失败（`RedisSinkFunction` 也不检查返回值）；
3. **`exists` 返回 `false` 时无法区分"不存在"与"出错"**。

### 21.4 `implements Serializable` 的意义

`RedisClient` 本身实现了 `Serializable`，但字段 `jedisPool` 是 `transient`。这意味着**序列化后的对象是不可用的**（池为 null），必须在反序列化后手动 `initPool()`。当前代码没有 `readObject` 钩子，所以"可序列化"是个危险承诺。更干净的写法是 `implements Closeable` 而不实现 `Serializable`。

## 讲 22 · `RedisResultWriter`：懒加载的备用实现

```java
public class RedisResultWriter {
    private transient RedisClient redisClient;

    public void open() {
        redisClient = new RedisClient(redisHost, redisPort, redisDatabase, timeout);
        logger.info("Redis writer opened");
    }

    public void write(RiskResult value) {
        try {
            if (redisClient == null) open();        // 懒加载
            redisClient.setWithTTL(value.getRedisKey(), value.toJsonString(), value.getTtl());
        } catch (Exception e) {
            logger.error("Error writing risk result to Redis", e);
        }
    }

    public void close() { if (redisClient != null) redisClient.close(); }
}
```

设计意图：配合 `DataStream.foreach(new RedisResultWriter())` 使用（Flink 1.x 风格）。在 Flink 2.x 里 `foreach` 已被 `sinkTo` 取代。

**当前状态**：主链路用的是 `RedisSinkFunction`，本类无调用方。两个实现的关系：

```
RedisSinkFunction（在用） ──内部 new──▶ RedisClient
RedisResultWriter（未用） ──懒加载──▶ 同一个 RedisClient
```

改 Redis 行为（比如加密码）时，**两处都要改**，否则备用实现会悄悄失效。

---

# 第 7 部分 · 串读与验证（讲 23-24）

## 讲 23 · 一次 `LOGIN_FAILURE` 的完整对象流转

假设 Kafka 收到第 3 条失败登录（`user_001`）：

```
① LoginEventDeserializer.deserialize(bytes)
      → LoginEvent{userId=user_001, timestamp=1710000000000, success=false, ip=..., deviceId=...}

② keyBy(userId=user_001) → 进入 LoginCepJob 的 CEP 算子

③ PatternFactory.createLoginFailurePattern() 的状态机累积到第 3 个元素
      → 触发 PatternSelectFunction.select(pattern)

④ select 内部：
      allEvents = [e1, e2, e3]
      PatternMatchResult{
          userId="user_001", eventId=UUID, eventType="LOGIN", patternType="LOGIN_FAILURE",
          context={eventType:"LOGIN", patternType:"LOGIN_FAILURE", userId:"user_001",
                   failCount:3, ip:..., deviceId:..., success:false, location:..., browser:...},
          details={failCount:3, lastIp:..., lastDeviceId:...}
      }

⑤ LoginCepJob 内部 union(locationRisks) → RiskControlApplication 再 union 三个 CepJob 的输出

⑥ RuleEvaluationFunction.processElement(matchResult):
      rules = broadcastState.get(null)          → 15 条 RuleConfig
      resolver.resolve(context, userId, eventId, eventType, details)

⑦ RiskActionResolver:
      matchedRules = engine.executeRules(context)
          "failCount >= 3"        → true   (HIGH/BLOCK)
          "failCount >= 5"        → false
          "patternType == \"LOCATION_CHANGE\"" → false
          ... 其余 12 条 false
      topRule = 登录失败次数过多（HIGH, weight 10）
      RiskResult{
          userId="user_001", eventId=UUID, eventType="LOGIN",
          riskLevel=HIGH, riskScore=80, action="BLOCK", ttl=3600,
          details={matchedRuleId:"1", matchedRuleName:"登录失败次数过多",
                   matchedRuleCount:1, failCount:3, lastIp:..., lastDeviceId:...}
      }

⑧ 三路 Sink:
      Redis: SETEX risk:login:user_001:<uuid> 3600 "<toJsonString 的输出>"
      MySQL: INSERT INTO risk_result(...) VALUES(..., '{"matchedRuleId":"1",...}')
      Kafka: produce key=user_001, value=<Jackson 全字段 JSON>
```

**读代码时请对照这张图**：任何一步的字段名、类型、null 处理对不上，就是 bug 的源头。

## 讲 24 · 练习题与验收标准

### 练习 1（模型层）
给 `Event` 增加 `traceId` 字段（含 getter/setter），并在 `LoginEvent.toString` 中输出。验收：`mvn -pl flink-risk-common compile` 通过；手工 `new LoginEvent()` 后 `toString` 包含空 traceId 不抛 NPE。

### 练习 2（反序列化层）
写一个 JUnit 测试：用 `LoginEventDeserializer` 反序列化带 `traceId` 的 JSON，断言抛 `UnrecognizedPropertyException`；然后关掉 `FAIL_ON_UNKNOWN_PROPERTIES` 再断言成功。验收：能解释"严格模式是保护还是负担"。

### 练习 3（引擎层）
写测试覆盖以下场景，并记录实际返回值：

| 输入表达式 | context | 期望 | 实际 |
|-----------|---------|------|------|
| `failCount >= 3` | `{failCount:3}` | true | |
| `deviceId =~ "abnormal"` | `{deviceId:"abnormal_01"}` | ? | |
| `deviceId =~ /abnormal/` | `{deviceId:"abnormal_01"}` | true | |
| `1 + 1` | `{}` | false（非布尔） | |
| `unknownVar > 1` | `{}` | ? | |
| `failCount >= 3` | `{}`（缺变量） | ? | |

最后两行是重点：理解"缺变量"和"未知函数"的差异。

### 练习 4（决策层）
构造 `matchedRules` = 3 条（CRITICAL/weight1、HIGH/weight9、HIGH/weight1），断言 `selectTopPriorityRule` 返回 CRITICAL 那条，并解释为什么 weight=9 的高风险规则没有胜出。

### 练习 5（输出层）
修复 `RiskResult.toJsonString()`（用 Jackson + `riskLevel.name()` + details），写测试断言输出能被 `ObjectMapper.readTree()` 解析且 `riskLevel` 字段等于 `"HIGH"`。

### 读懂标准

能不看代码回答：

1. `Event.attributes` 为什么是死字段？`participationCount` 到底来自哪里？
2. `KafkaEventDeserializer` 的 `default` 分支为什么必然失败？为什么线上没暴露？
3. `fromScore(100)` 返回什么？为什么这可能是个坑？
4. `registerRule` 能拦住哪类错误、拦不住哪类？
5. 为什么 `executeRules` 的遍历顺序会影响最终 action？怎么消除？
6. 为什么 `RiskActionResolver` 未命中时不合并 `details`？有什么业务后果？
7. Redis 里 `riskLevel` 的值是什么？为什么？
8. 为什么 `AviatorRuleEngine` 不需要加锁？

---

## 附录 A · 全模块类清单与状态

| 类 | 行数量级 | 状态 | 被谁使用 |
|----|---------|------|----------|
| `Event` / 三个子类 | 80~110 | 生产 | job 反序列化、CEP |
| `LoginEventDeserializer` 等 3 个 | 30 | 生产 | `KafkaSourceFactory` |
| `KafkaEventDeserializer` | 60 | **未接入**（有 bug） | 无 |
| `RuleConfig` | 130 | 生产 | 广播状态、引擎 |
| `RiskLevel` | 45 | 生产 | 决策、前端 |
| `RuleLoader` | 100 | **未接入** | 无 |
| `AviatorRuleEngine` | 250 | 生产 | `RiskActionResolver` |
| `RiskActionResolver` | 120 | 生产 | `RuleEvaluationFunction` |
| `RiskResult` | 140 | 生产 | 三路 Sink |
| `RedisClient` | 145 | 生产（无密码） | `RedisSinkFunction` |
| `RedisResultWriter` | 50 | **未接入** | 无 |

## 附录 B · 改动影响面速查

| 你想改什么 | 必须同步改的地方 |
|-----------|------------------|
| 加事件字段 | POJO + 反序列化（未知字段策略）+ 生产端 JSON |
| 加 context 变量 | 对应 `XxxCepJob.buildXxxContext` + `sql/init.sql` 规则 + 前端 `EXPRESSION_DOC` |
| 加风险等级 | `RiskLevel` + `fromScore` + 前端 `RISK_LEVEL_CONFIG` + 库里数据 |
| 加 action 取值 | 无需改代码（String），但前端 `ACTION_CONFIG` 要加 |
| 改 Redis key 结构 | `RiskResult.getRedisKey` + 所有消费 Redis 的下游 |
| 改评分口径 | `RiskActionResolver`（主链路）与 `AviatorRuleEngine.calculateRiskScore`（未用）二选一并统一 |

---

下一篇：[02 · flink-risk-job 详解](02-job.md)
