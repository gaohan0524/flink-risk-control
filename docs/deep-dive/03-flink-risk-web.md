# 模块精读 03 · flink-risk-web

> Spring Boot 管理后端：**规则的增删改查、风险记录查询、看板统计**。
> 技术含量低于 Flink 侧，但它是"运营改一条规则 → 30 秒后作业生效"这条闭环的起点，
> 且有几处**字段别名技巧**和**配置不一致**必须理解。
>
> 返回：[学习总览](../LEARNING-GUIDE.md) · 上一篇：[02 · job](02-flink-risk-job.md) · 下一篇：[04 · frontend](04-frontend.md)

---

## 目录

1. [模块定位与启动](#1-模块定位与启动)
2. [POM 里的三个关键约束](#2-pom-里的三个关键约束)
3. [配置：`application.yml`](#3-配置applicationyml)
4. [分层结构与依赖注入](#4-分层结构与依赖注入)
5. [实体与"字段别名"技巧](#5-实体与字段别名技巧)
6. [`RuleController` 逐端点精读](#6-rulecontroller-逐端点精读)
7. [`RiskResultController`](#7-riskresultcontroller)
8. [`StatsController`：三个统计接口怎么做出来的](#8-statscontroller三个统计接口怎么做出来的)
9. [基础设施：分页、CORS、异常处理、日志](#9-基础设施分页cors异常处理日志)
10. [与 Flink 侧的协作边界](#10-与-flink-侧的协作边界)
11. [测试现状](#11-测试现状)
12. [已知问题与练习](#12-已知问题与练习)

---

## 1. 模块定位与启动

```
flink-risk-web/src/main/
├── java/com/qinyadan/risk/
│   ├── RiskWebApplication.java            @SpringBootApplication
│   └── web/
│       ├── config/    WebConfig · GlobalExceptionHandler
│       ├── controller/ RuleController · RiskResultController · StatsController
│       ├── entity/    Rule · RiskResult
│       ├── mapper/    RuleMapper · RiskResultMapper
│       └── service/   RuleService · RiskResultService
└── resources/
    ├── application.yml
    └── log4j2-spring.xml
```

启动：

```bash
mvn clean install -DskipTests        # 先装 common 到本地仓库
cd flink-risk-web
mvn spring-boot:run                  # http://localhost:8080
# 冒烟：curl http://localhost:8080/api/rules
```

依赖 `flink-risk-common`（为了复用 `RuleConfig`/`RiskLevel` 类型，虽然 Controller 层其实没直接用）。

---

## 2. POM 里的三个关键约束

### 2.1 Java 17

父 POM 把编译级别定为 Java 8，本模块 `<java.version>17</java.version>` 覆盖：

```xml
<maven.compiler.source>${java.version}</maven.compiler.source>
<maven.compiler.target>${java.version}</maven.compiler.target>
```

Spring Boot 3.2 最低要求 Java 17，所以**不能把整个项目的编译级别统一降到 8**。

### 2.2 排除 Logback，统一 Log4j2

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-web</artifactId>
    <exclusions>
        <exclusion>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-logging</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

三处（web / redis / mybatis-plus）都做了同样的排除，否则 classpath 里 Logback 与 Log4j2 同时存在，启动会打印 `SLF4J: Class path contains multiple SLF4J bindings` 并出现"日志只出一部分"的怪现象。

### 2.3 MyBatis-Plus 用 Boot3 专用 starter

```xml
<artifactId>mybatis-plus-spring-boot3-starter</artifactId>
```

用旧的 `mybatis-plus-boot-starter` 会因 Jakarta EE 命名空间（`javax` → `jakarta`）启动失败。同时保留了 `spring-boot-starter-jdbc`（MyBatis-Plus 需要 `DataSource`）。

---

## 3. 配置：`application.yml`

```yaml
server:
  port: ${SERVER_PORT:8080}

spring:
  datasource:
    url: jdbc:mysql://${MYSQL_HOST:localhost}:${MYSQL_PORT:3306}/risk_control?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true
    username: ${MYSQL_USER:root}
    password: ${MYSQL_PASSWORD:root123123}
  data:
    redis:
      host: ${REDIS_HOST:localhost}
      password: ${REDIS_PASSWORD:}
mybatis-plus:
  configuration:
    map-underscore-to-camel-case: true
  global-config:
    db-config:
      id-type: auto
```

逐项说明：

| 配置 | 作用 | 注意 |
|------|------|------|
| `MYSQL_HOST/PORT/USER/PASSWORD` | 环境变量覆盖，默认本地 | **默认库名是 `risk_control`**，与 job 的 `flink_risk` 不一致 |
| `MYSQL_PASSWORD` 默认 `root123123` | 与 compose 的 `root123` 不同 | docker profile 下由 compose 注入 `root123` |
| `useSSL=false&serverTimezone=Asia/Shanghai` | MySQL 8 兼容 | `serverTimezone` 影响日期序列化 |
| `map-underscore-to-camel-case` | `risk_level` → `riskLevel` | 缺了它，所有驼峰字段都是 null |
| `id-type: auto` | 依赖数据库自增 | 与 `@TableId(type=IdType.AUTO)` 一致 |
| `mapper-locations: classpath*:/mapper/**/*.xml` | 预留 XML mapper 目录 | 仓库里**没有任何 XML**，全靠条件构造器 |
| `jackson.date-format/time-zone` | 统一日期输出 | `yyyy-MM-dd HH:mm:ss` |

Redis 配置（`spring.data.redis.*`）虽然配了，但**当前没有任何代码使用**——风险数据是 job 直接写 Redis，web 只查 MySQL。属于预留。

---

## 4. 分层结构与依赖注入

```
Controller  ──@Autowired──▶ Service (extends ServiceImpl<Mapper, Entity>) ──▶ Mapper (extends BaseMapper)
```

两个 Service 都是**空壳**：

```java
@Service
public class RuleService extends ServiceImpl<RuleMapper, Rule> implements IService<Rule> {
}
```

`ServiceImpl` 已经提供了 `page/list/getById/save/updateById/removeById/count/update()` 等全部方法，所以业务代码全在 Controller 里，Service 只承担"泛型绑定"。

这种写法的取舍：

- 优点：代码量极少，CRUD 零样板；
- 缺点：业务逻辑（过滤条件、字段转换）散落在 Controller，测试和复用都不便。若规则数变多、要加校验/审计，应把逻辑下沉到 Service。

Mapper 同样空壳：

```java
@Mapper
public interface RuleMapper extends BaseMapper<Rule> {
}
```

---

## 5. 实体与"字段别名"技巧

这是本模块**唯一需要动脑**的地方。

### 5.1 `Rule`：一套字段，两个名字

```java
@TableName("rule_config")
public class Rule {
    private Long id;
    private String name;              // ← 数据库列 name
    private String expression;        // ← 数据库列 expression
    private String riskLevel;
    private String action;
    private Integer weight;
    private Boolean enabled;
    private String description;
    private Date createTime;
    private Date updateTime;

    @TableField(exist = false) private String ruleType;        // 前端展示用
    @TableField(exist = false) private String ruleExpression;  // 前端展示用
    @TableField(exist = false) private String ruleName;        // 前端展示用
```

前端（以及旧的 ARCHITECTURE 文档）用的是 `ruleName` / `ruleExpression`，数据库列却是 `name` / `expression`。项目没有用 `@JsonProperty` 或 DTO 做转换，而是在实体里加三个 `@TableField(exist = false)` 的"影子字段"，再靠两个方法手工同步：

```java
public void syncFromFrontend() {      // 写入前：前端 → DB 字段
    if (ruleName != null) this.name = ruleName;
    if (ruleExpression != null) this.expression = ruleExpression;
}

public void syncToFrontend() {        // 读取后：DB → 前端字段
    this.ruleName = this.name;
    this.ruleExpression = this.expression;
}
```

调用点必须记清：

| 方法 | 调用位置 |
|------|----------|
| `syncFromFrontend()` | `createRule`、`updateRule`（写库之前） |
| `syncToFrontend()` | `getRules`（每条 record）、`getEnabledRules`、`getRule`、`createRule`/`updateRule` 返回值、`toggleRule` |

**踩坑点**：

- `ruleType` 既没有数据库列，`syncFromFrontend()` 也不拷贝它 → 前端传什么都不会保存，列表里永远是 `undefined`。
- `updateRule` 用的是 `updateById`（MyBatis-Plus 默认忽略 null 字段），所以**只想改一个字段时其它字段不会被清空**，但也意味着"把描述清空"传 `null` 是无效的。
- 若前端只传 `ruleName` 而不传 `expression`，`syncFromFrontend` 后 `expression` 为 null，插入时撞 `expression NOT NULL` 约束 → 500。

### 5.2 `RiskResult`：`details` 是 String

```java
@TableName("risk_result")
public class RiskResult {
    private Long id;
    private String userId;
    private String eventId;
    private String eventType;
    private String riskLevel;
    private Integer riskScore;
    private String action;
    private String details;      // MySQL JSON 列，但 Java 侧按 String 处理
    private Date createTime;
}
```

为什么用 `String` 而不映射成对象：job 写入的 `details` 结构随 `patternType` 变化（有时有 `intervalMs`，有时有 `couponCode`），强类型化收益低。代价是前端拿到的是一段 JSON 字符串，需要自己解析（当前前端并未展示它）。

注意：这是**与 `flink-risk-common` 里 `RiskResult` 完全不同的两个类**（同名不同包），web 的对应数据库表，common 的是流处理载体。读代码时极易混淆。

---

## 6. `RuleController` 逐端点精读

基路径 `/api/rules`，返回原始对象或 `Map`，**没有统一的 `{code,message,data}` 包装**。

### 6.1 分页查询

```java
@GetMapping
public Map<String, Object> getRules(
        @RequestParam(defaultValue = "1") int page,
        @RequestParam(defaultValue = "10") int pageSize,
        @RequestParam(required = false) String name,
        @RequestParam(required = false) String riskLevel,
        @RequestParam(required = false) String action,
        @RequestParam(required = false) Boolean enabled) {

    LambdaQueryWrapper<Rule> wrapper = new LambdaQueryWrapper<>();
    if (name != null && !name.isEmpty())        wrapper.like(Rule::getName, name);
    if (riskLevel != null && !riskLevel.isEmpty()) wrapper.eq(Rule::getRiskLevel, riskLevel);
    if (action != null && !action.isEmpty())    wrapper.eq(Rule::getAction, action);
    if (enabled != null)                        wrapper.eq(Rule::getEnabled, enabled);
    wrapper.orderByDesc(Rule::getUpdateTime);

    Page<Rule> result = ruleService.page(new Page<>(page, pageSize), wrapper);
    result.getRecords().forEach(Rule::syncToFrontend);

    Map<String, Object> response = new HashMap<>();
    response.put("data", result.getRecords());
    response.put("total", result.getTotal());
    response.put("page", result.getCurrent());
    response.put("pageSize", result.getSize());
    return response;
}
```

要点：

- `name` 是 `like`（模糊），其余是 `eq`（精确）；
- `orderByDesc(updateTime)`：最近改过的规则排前面；
- 分页生效的前提是 `WebConfig` 里注册了 `PaginationInnerInterceptor`（见第 9 章），否则 `page()` 会退化成全表查询；
- **前端搜索字段名叫 `ruleName`，后端参数名叫 `name`**，映射在 `ProTable` 的 `request` 里手工完成（`name: params.ruleName`）。

### 6.2 启用规则列表

```java
@GetMapping("/enabled")
public List<Rule> getEnabledRules() {
    LambdaQueryWrapper<Rule> wrapper = new LambdaQueryWrapper<>();
    wrapper.eq(Rule::getEnabled, true);
    wrapper.orderByDesc(Rule::getUpdateTime);
    List<Rule> rules = ruleService.list(wrapper);
    rules.forEach(Rule::syncToFrontend);
    return rules;
}
```

注释说"供 Flink Job 拉取"，但实际 job 是**直连 MySQL 查 `rule_config`**，不走这个接口（见 02 文档）。这个接口目前是给外部消费者预留的。

### 6.3 新建 / 更新

```java
@PostMapping
public Rule createRule(@RequestBody Rule rule) {
    rule.syncFromFrontend();
    ruleService.save(rule);
    rule.syncToFrontend();
    return rule;
}

@PutMapping("/{id}")
public Rule updateRule(@PathVariable Long id, @RequestBody Rule rule) {
    rule.setId(id);
    rule.syncFromFrontend();
    ruleService.updateById(rule);
    rule.syncToFrontend();
    return rule;
}
```

**没有做这些事**（都是可补的改进点）：

| 缺失 | 后果 |
|------|------|
| 表达式编译校验（`AviatorEvaluator.compile`） | 语法错误要等 job 拉取时才发现（job 会跳过该规则并打 error 日志） |
| `riskLevel` / `action` 枚举校验 | 写进 `P0` 之类的值会让 job 整轮加载失败（`RiskLevel.valueOf` 抛异常） |
| `@Valid` + Bean Validation | 空 `name` / 空 `expression` 直接 500 |
| 唯一性校验 | 可重复创建同名规则 |

### 6.4 删除 / 启停 / 批量

```java
@DeleteMapping("/{id}")
public Map<String, String> deleteRule(@PathVariable Long id) {
    ruleService.removeById(id);
    Map<String, String> response = new HashMap<>();
    response.put("message", "删除成功");
    return response;              // 没有校验 id 是否存在，删不存在的 id 也返回成功
}

@PutMapping("/{id}/toggle")
public Rule toggleRule(@PathVariable Long id) {
    Rule rule = ruleService.getById(id);
    if (rule != null) {
        rule.setEnabled(!rule.getEnabled());
        ruleService.updateById(rule);
        rule.syncToFrontend();
    }
    return rule;      // id 不存在时返回 null → HTTP 200 空 body
}

@PutMapping("/batch/enable")
public Map<String, String> batchEnable(@RequestBody List<Long> ids) {
    ruleService.update().eq("enabled", false).in("id", ids).set("enabled", true).update();
    ...
}
```

`toggle` 是"读-改-写"，并发下会丢更新（两个人同时点，状态可能只翻一次）。批量接口直接用 `LambdaUpdateWrapper` 拼字符串列名，绕过了类型安全。

---

## 7. `RiskResultController`

基路径 `/api/risks`（`README.md` 写的 `/api/risk-results` 是错的）。

```java
@GetMapping
public Map<String, Object> getRiskResults(
        @RequestParam(defaultValue = "1") int page,
        @RequestParam(defaultValue = "10") int pageSize,
        @RequestParam(required = false) String userId,
        @RequestParam(required = false) String eventType,
        @RequestParam(required = false) String riskLevel,
        @RequestParam(required = false) String action) {
    ...
    if (userId != null && !userId.isEmpty()) wrapper.like(RiskResult::getUserId, userId);
    ...
    wrapper.orderByDesc(RiskResult::getCreateTime);
    Page<RiskResult> result = riskResultService.page(new Page<>(page, pageSize), wrapper);
    // 与 rules 相同结构的 {data,total,page,pageSize}
}
```

风险记录是**只读**的（没有 POST/PUT/DELETE），因为数据由 job 的 MySQL Sink 写入。`userId` 用 `like` 是为了支持"输入片段搜人"。

分页没有上限保护：`pageSize=100000` 会直接打满内存。生产应加 `Math.min(pageSize, 200)`。

---

## 8. `StatsController`：三个统计接口怎么做出来的

### 8.1 `/api/stats/overview`（6 条 SQL）

```java
long totalRules   = ruleService.count();                                   // 1
long enabledRules = ruleService.count(enabledWrapper);                      // 2
LocalDateTime todayStart = LocalDateTime.of(LocalDate.now(), LocalTime.MIN);
long todayRisks     = riskResultService.count(todayWrapper);                // 3
long todayHighRisks = riskResultService.count(highWrapper);                 // 4  (riskLevel in HIGH,CRITICAL)
Map<String, Long> ruleTypeDistribution = ruleService.list()                 // 5
        .stream().collect(Collectors.groupingBy(r -> inferRuleType(r.getName()), Collectors.counting()));
List<RiskResult> todayResults = riskResultService.list(todayWrapper);        // 6
// riskLevelDistribution / eventTypeDistribution 在内存里对 todayResults 分组
```

返回：

```json
{
  "totalRules": 15, "enabledRules": 14,
  "todayRisks": 128, "todayHighRisks": 37,
  "ruleTypeDistribution": {"LOGIN":4,"ORDER":5,"ACTIVITY":4,"OTHER":2},
  "riskLevelDistribution": {"LOW":40,"MEDIUM":51,"HIGH":30,"CRITICAL":7},
  "eventTypeDistribution": {"LOGIN":60,"ORDER":40,"ACTIVITY":28}
}
```

两个实现细节：

1. **规则类型是"猜"出来的**：`inferRuleType(name)` 用中文/英文关键词匹配（`登录|login` → LOGIN，`订单|order` → ORDER，`活动|activity|优惠券|coupon` → ACTIVITY，其余 OTHER）。因为 `rule_config` 没有 `type` 列。改规则名会改变统计口径。
2. **分布是内存分组**：今日结果全量 `list` 后 stream 分组。今日数据量大时（百万级）会 OOM，应改成 `GROUP BY` 聚合查询。

### 8.2 `/api/stats/trend`（14 条 SQL）

```java
for (int i = 6; i >= 0; i--) {
    LocalDate date = LocalDate.now().minusDays(i);
    LambdaQueryWrapper<RiskResult> wrapper = new LambdaQueryWrapper<>();
    wrapper.ge(RiskResult::getCreateTime, LocalDateTime.of(date, LocalTime.MIN));
    wrapper.le(RiskResult::getCreateTime, LocalDateTime.of(date, LocalTime.MAX));
    long total = riskResultService.count(wrapper);

    LambdaQueryWrapper<RiskResult> highWrapper = wrapper.clone();   // 复制条件再叠加
    highWrapper.in(RiskResult::getRiskLevel, "HIGH", "CRITICAL");
    long high = riskResultService.count(highWrapper);
    ...
}
```

- 7 天 × 2 = 14 条 count，接口 P99 会随数据量线性变差；
- `wrapper.clone()` 是关键：直接复用同一对象会污染条件；
- 返回 `{dailyData:[{date,total,high}...]}`，前端 Dashboard 直接渲染表格。

### 8.3 `/api/stats/top-rules`（1 条 list + 内存解析）

```java
List<RiskResult> todayResults = riskResultService.list(todayWrapper);
Map<String, Long> ruleHitCount = new LinkedHashMap<>();
for (RiskResult r : todayResults) {
    String ruleName = extractRuleName(r.getDetails());     // 解析 JSON 取 matchedRuleName
    if (ruleName != null) ruleHitCount.merge(ruleName, 1L, Long::sum);
}
// 按次数降序取前 10
```

`extractRuleName` 用静态 `ObjectMapper` 解析 `details` 字符串：

```java
JsonNode node = MAPPER.readTree(details);
JsonNode nameNode = node.get("matchedRuleName");
```

**这依赖 `RiskActionResolver` 把 `matchedRuleName` 写进 details**——一旦"未命中"（不写 `matchedRuleName`）或改了字段名，排行榜会直接空掉。这也解释了为什么看板上"今日命中规则数"经常是 0：只有命中规则的事件才有这个字段。

---

## 9. 基础设施：分页、CORS、异常处理、日志

### 9.1 `WebConfig`

```java
@Bean
public MybatisPlusInterceptor mybatisPlusInterceptor() {
    MybatisPlusInterceptor interceptor = new MybatisPlusInterceptor();
    interceptor.addInnerInterceptor(new PaginationInnerInterceptor(DbType.MYSQL));
    return interceptor;
}
```

**没有这个 Bean，`page(new Page<>(1,10), wrapper)` 不会拼 `LIMIT`**，会把全表查出来再在内存里切——新手最常踩的分页坑。

```java
@Bean
public CorsFilter corsFilter() {
    config.setAllowCredentials(true);
    config.addAllowedOriginPattern("*");     // 注意是 Pattern，不是 addAllowedOrigin
    ...
}
```

`allowCredentials(true)` 与 `addAllowedOrigin("*")` 不能共存，必须用 `addAllowedOriginPattern`。开发期前端 3000 → 后端 8080 依赖它（Vite 也配了 proxy，两条路都能通）。

### 9.2 `GlobalExceptionHandler`

```java
@RestControllerAdvice
public class GlobalExceptionHandler {
    @ExceptionHandler(Exception.class)
    public Map<String, Object> handleException(Exception e) {
        response.put("status", 500);
        response.put("error", e.getClass().getSimpleName());
        response.put("message", e.getMessage());
        response.put("stackTrace", sw.toString());     // ⚠ 完整堆栈返回给客户端
        return response;
    }
}
```

问题：

- 方法没有 `@ResponseStatus`，**HTTP 状态码仍是 200**，前端 axios 不会进 reject 分支，必须自己判断 body 里的 `status`；
- 把服务器堆栈返回给浏览器，存在信息泄露风险；
- 所有异常（包括 404、参数错误）都被统一成 500。

### 9.3 日志：`log4j2-spring.xml`

| Appender | 文件 | 说明 |
|----------|------|------|
| Console | — | 控制台 |
| AppFile | `logs/app.log` | 按天 + 100MB 滚动，保留 30 个 |
| ErrorFile | `logs/error.log` | 仅 ERROR |
| SqlFile | `logs/sql.log` | `com.qinyadan.risk.web.mapper` 与 mybatis-plus 的 DEBUG SQL |

注意 `application.yml` 里又写了一份 `logging.level`——因为是 Log4j2 配置文件主导，`logging.level` 只对部分场景生效，排查日志级别时以 `log4j2-spring.xml` 为准。

---

## 10. 与 Flink 侧的协作边界

```
web 改规则 ──▶ UPDATE rule_config ──▶ (≤30s) ──▶ job 轮询 ──▶ 广播 ──▶ 生效
job 写风险 ──▶ INSERT risk_result ──▶ web 分页/统计 ──▶ 前端展示
```

**唯一的耦合是表结构**。因此改字段时必须三处同步：

1. `sql/init.sql`（DDL）；
2. `RuleSourceFunction` 的 SELECT 列与 `RuleConfig` 字段；
3. web 的 `Rule` 实体 + `sync*` 方法。

历史上已经因此踩坑：`docs/ARCHITECTURE.md` 里的 `rule_type`、`rule_name`、`rule_expression` 三列在真实表里**不存在**，照文档建表会让 job 报 `Unknown column`。

---

## 11. 测试现状

只有一个类：

```java
@SpringBootTest(classes = RiskWebApplication.class)
@AutoConfigureMockMvc
public class RuleControllerTest {
    @Autowired private MockMvc mockMvc;

    @Test
    public void testGetRules() throws Exception {
        mockMvc.perform(get("/api/rules?page=1&pageSize=10")).andDo(print());
    }
}
```

- 没有断言，只把响应打印出来 → 永远绿色，不能防回归；
- `@SpringBootTest` 会真实初始化 `DataSource`，且测试里的 GET 会真正查库，**本地没有 MySQL（或表不存在）时会报错**，不会自动跳过；
- 没有 `application-test.yml`、没有 H2/Testcontainers、没有 Mockito。

运行：

```bash
mvn test -pl flink-risk-web -am
mvn test -pl flink-risk-web -am -Dtest=RuleControllerTest#testGetRules
```

想让它变成真测试，最小改动是加 `@AutoConfigureMockMvc` + `andExpect(status().isOk())` + `andExpect(jsonPath("$.total").isNumber())`，并用 Testcontainers MySQL 或 H2（MySQL 兼容模式）替代真实库。

---

## 12. 已知问题与练习

| # | 问题 | 位置 | 建议 |
|---|------|------|------|
| 1 | 默认库名 `risk_control` 与 job 的 `flink_risk` 不一致 | `application.yml` | 统一，或全部走环境变量 |
| 2 | 明文密码写在配置里 | 同上 | 用环境变量/密钥管理，配置文件只留占位符 |
| 3 | 异常堆栈返回客户端且 HTTP 200 | `GlobalExceptionHandler` | 记日志 + 返回标准错误码 |
| 4 | `ruleType` 存入即丢 | `Rule` + `syncFromFrontend` | 加列或删除该字段 |
| 5 | 无表达式/枚举/必填校验 | `RuleController` | 加 `@Valid` + 自定义校验（含 Aviator 试编译） |
| 6 | `pageSize` 无上限 | 两个列表接口 | `Math.min(pageSize, 200)` |
| 7 | 统计接口 N+1 / 全量 list | `StatsController` | 改 `GROUP BY` 聚合 |
| 8 | 规则类型靠名称猜 | `inferRuleType` | 加 `type` 列 |
| 9 | toggle 并发丢更新 | `toggleRule` | 用 `UPDATE ... SET enabled = !enabled` 单语句 |
| 10 | 无鉴权 | 全局 | 加 Spring Security / 网关层 |

### 动手练习

- **练习 1**：给 `POST /api/rules` 加表达式试编译校验，非法表达式返回 400 而非 500。
- **练习 2**：把 `RuleControllerTest` 改成有断言的测试，并用 Testcontainers 启动 MySQL。
- **练习 3**：把 `/api/stats/trend` 改成一条 `SELECT DATE(create_time) d, COUNT(*) ... GROUP BY d` 的聚合查询（提示：需要自定义 Mapper 方法或 `@Select`）。
- **练习 4**：让 `updateRule` 支持"清空描述"（提示：`updateById` 忽略 null，需要 `LambdaUpdateWrapper.set`）。
- **练习 5**：统一库名并验证：web 新建规则 → 30 秒内 job 日志出现新的 `Rule registered: ...`。

---

下一个模块：[04 · frontend](04-frontend.md)
