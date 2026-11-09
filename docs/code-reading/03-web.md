# 代码精读（详解版）· flink-risk-web

> 本文件逐类、逐方法精读 Spring Boot 管理后端。阅读前提：了解 Spring Boot 基本分层与 MyBatis-Plus。
>
> 全模块 12 个 Java 文件 + 2 个配置文件，拆成 **7 个部分、22 讲**。顺序遵循"启动 → 基础设施 → 数据访问 → 实体 → 接口 → 统计 → 验证"，
> 也就是**从下往上**读：先看它怎么连上数据库，再看接口怎么暴露数据。
>
> 相关文档：[common 详解](01-common.md) · [job 详解](02-job.md) · [学习总览](../LEARNING-GUIDE.md)

---

## 阅读地图

```
第 1 部分  启动与配置      RiskWebApplication · pom.xml · application.yml · log4j2-spring.xml
                │  先确认"它怎么起来、连哪个库、日志去哪"
第 2 部分  基础设施        WebConfig（分页 + CORS）· GlobalExceptionHandler
                │  决定了所有接口的"默认行为"
第 3 部分  数据访问        RuleMapper · RiskResultMapper · RuleService · RiskResultService
                │  空壳但能力巨大：ServiceImpl 提供了全部 CRUD
第 4 部分  实体与映射      Rule（含影子字段）· RiskResult（JSON 列）
                │  字段名契约，联调 bug 的高发区
第 5 部分  接口层          RuleController（9 个端点）· RiskResultController（2 个端点）
                │  对外契约
第 6 部分  统计层          StatsController（3 个端点）
                │  最复杂的业务逻辑，也是性能瓶颈
第 7 部分  验证与改造      测试现状 · 与 job 的契约 · 改造清单
```

### 部分划分与顺序理由

| 部分 | 讲号 | 文件 | 为什么这个顺序 |
|------|------|------|----------------|
| 1 启动配置 | 1-4 | `RiskWebApplication`、pom、yml、log4j2 | 不先搞清连哪个库，后面所有 SQL 都是空中楼阁 |
| 2 基础设施 | 5-6 | `WebConfig`、`GlobalExceptionHandler` | 分页插件不注册，所有 `page()` 都会退化成全表查 |
| 3 数据访问 | 7-9 | Mapper、Service | 弄清"空壳类为什么能 CRUD" |
| 4 实体 | 10-12 | `Rule`、`RiskResult` | 字段别名技巧 + JSON 列处理 |
| 5 接口 | 13-16 | 两个 Controller | 依赖前 4 部分才能读懂每个端点 |
| 6 统计 | 17-19 | `StatsController` | 建立在实体与查询之上，单独成章 |
| 7 验证改造 | 20-22 | 测试、契约、清单 | 收口 |

### 前置准备

```bash
mvn -q -pl flink-risk-web -am compile      # 需要 common 先装好
mvn -pl flink-risk-web spring-boot:run     # 需要 MySQL
```

如果只想读代码不跑：**第 3 部分关于 `ServiceImpl` 能力的表格必须记住**，否则会以为 Service 里"什么都没写"是漏了实现。

---

# 第 1 部分 · 启动与配置（讲 1-4）

## 讲 1 · `RiskWebApplication`

文件：`flink-risk-web/src/main/java/com/qinyadan/risk/RiskWebApplication.java`

```java
package com.qinyadan.risk;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RiskWebApplication {
    public static void main(String[] args) {
        SpringApplication.run(RiskWebApplication.class, args);
    }
}
```

### 1.1 包位置决定扫描范围

`@SpringBootApplication` 隐含 `@ComponentScan(basePackages = "com.qinyadan.risk")`。因为启动类在 `com.qinyadan.risk`（**不是** `com.qinyadan.risk.web`），所以：

```
com.qinyadan.risk
├── RiskWebApplication          ← 启动类
└── web
    ├── config      ✅ 被扫描
    ├── controller  ✅
    ├── service     ✅
    └── mapper      ✅（@Mapper 也会被 MyBatis 扫描）
```

如果把启动类移动到 `com.qinyadan.risk.web`，扫描范围会缩小到 `web` 包，**当前仍能工作**，但将来在 `com.qinyadan.risk.xxx` 下加组件就扫不到。这是"启动类放最外层"的通用约定。

### 1.2 一句话启动

`SpringApplication.run` 做的事（按顺序）：

1. 创建 `ApplicationContext`（Servlet 环境）；
2. 加载 `application.yml`（含环境变量占位符解析）；
3. 自动配置：`DataSource`、`MybatisPlusAutoConfiguration`、`DispatcherServlet`、`Jackson`、`Log4j2`；
4. 扫描并实例化 `@RestController` / `@Service` / `@Mapper` / `@Configuration`；
5. 启动内嵌 Tomcat（默认 8080）。

**最容易失败的三个点**：

| 失败点 | 报错 | 原因 |
|--------|------|------|
| 数据源 | `Failed to configure a DataSource` | 缺 `spring.datasource.url` 或 MySQL 驱动 |
| MyBatis-Plus | `Invalid bound statement (not found)` | 用错 starter（Boot2 版）或 `@Mapper` 未扫描 |
| 日志 | `SLF4J: Class path contains multiple SLF4J bindings` | 没排除 `spring-boot-starter-logging` |

## 讲 2 · `pom.xml` 依赖逐项

文件：`flink-risk-web/pom.xml`

```xml
<properties>
    <java.version>17</java.version>
    <spring.boot.version>3.2.0</spring.boot.version>
    <mybatis.plus.version>3.5.5</mybatis.plus.version>
    <mysql.version>8.1.0</mysql.version>
</properties>
```

| 依赖 | 版本 | 作用 | 关键配置 |
|------|------|------|----------|
| `flink-risk-common` | 2.0.1-SNAPSHOT | **声明了但代码里没用**（见 2.3） | 排除 `slf4j-api` |
| `spring-boot-starter-web` | 3.2.0 | Tomcat + MVC + Jackson | 排除 `starter-logging`（改用 Log4j2） |
| `spring-boot-starter-log4j2` | 3.2.0 | Log4j2 绑定 | — |
| `spring-boot-starter-data-redis` | 3.2.0 | 预留（无代码使用） | 排除 `starter-logging` |
| `mybatis-plus-spring-boot3-starter` | 3.5.5 | ORM | 只排除 logback，**不能排除 autoconfigure/jdbc** |
| `spring-boot-starter-jdbc` | 3.2.0 | `DataSource` + `JdbcTemplate` | 必须保留 |
| `mysql-connector-j` | 8.1.0 | JDBC 驱动 | 注意是 `com.mysql` 新坐标 |
| `fastjson2` / `guava` | 继承父 POM | 未使用 | 可删 |
| `spring-boot-starter-test` | 3.2.0 | JUnit5 + MockMvc | test scope |

### 2.1 为什么排除 Logback 要写三遍

`spring-boot-starter-web`、`starter-data-redis`、`mybatis-plus-spring-boot3-starter` 都会传递引入 `spring-boot-starter-logging`（内含 Logback）。必须在每个引入点排除，否则 classpath 上 Logback 与 Log4j2 共存，SLF4J 会选择先找到的实现，导致"部分日志丢失"或"配置不生效"。

### 2.2 为什么是 `mybatis-plus-spring-boot3-starter`

| starter | 适配 | 用在 Boot 3 的后果 |
|---------|------|--------------------|
| `mybatis-plus-boot-starter` | Boot 2（`javax.*`） | 启动时 `ClassNotFoundException: javax.servlet.*` 或 `NoClassDefFoundError` |
| `mybatis-plus-spring-boot3-starter` | Boot 3（`jakarta.*`） | ✅ |

注释里那句"只排除 logback，不要排除 autoconfigure、jdbc！"是踩坑记录：早期版本误排除了 `mybatis-plus` 自带的 `spring-boot-autoconfigure` 与 `spring-boot-starter-jdbc`，导致 `DataSource` 不被创建、Mapper 扫描失效。

### 2.3 一个真实冗余：`flink-risk-common`

```xml
<dependency>
    <groupId>com.qinyadan.risk</groupId>
    <artifactId>flink-risk-common</artifactId>
    <version>${project.version}</version>
    ...
</dependency>
```

但 web 模块**没有任何一个类 import `com.qinyadan.risk.rule/redis/event`**（全模块搜索为空）。后果：

- 编译期多余依赖；
- 传递引入 `aviator`、`jedis`、`snakeyaml`、`fastjson2`，增加包体积与潜在冲突；
- 唯一"好处"是保证 common 先构建（`mvn -pl flink-risk-web -am` 需要它）。

如果确认不用，可以删掉；若要保留（比如将来复用 `RiskLevel`），建议加注释说明。

### 2.4 打包

```xml
<plugin>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-maven-plugin</artifactId>
    <configuration>
        <mainClass>com.qinyadan.risk.RiskWebApplication</mainClass>
    </configuration>
</plugin>
```

`mvn package` 是否产出**可执行 fat-jar**，取决于 `repackage` goal 是否绑定到 `package` 阶段。本项目**没有继承 `spring-boot-starter-parent`**，插件也没有显式 `<executions>`，因此不要假定 `target/*.jar` 一定能 `java -jar` 运行。稳妥做法是显式绑定：

```xml
<plugin>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-maven-plugin</artifactId>
    <version>${spring.boot.version}</version>
    <executions>
        <execution>
            <goals><goal>repackage</goal></goals>
        </execution>
    </executions>
    <configuration>
        <mainClass>com.qinyadan.risk.RiskWebApplication</mainClass>
    </configuration>
</plugin>
```

本地开发用 `mvn spring-boot:run` 即可绕过这个问题。

## 讲 3 · `application.yml` 逐项解读

文件：`flink-risk-web/src/main/resources/application.yml`

```yaml
server:
  port: ${SERVER_PORT:8080}
```

`${ENV:default}` 是 Spring 的占位符语法：优先取环境变量 `SERVER_PORT`，没有则用 8080。**这种写法让同一个 jar 能在本地与容器里跑**。

### 3.1 数据源

```yaml
spring:
  datasource:
    url: jdbc:mysql://${MYSQL_HOST:localhost}:${MYSQL_PORT:3306}/risk_control?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true
    username: ${MYSQL_USER:root}
    password: ${MYSQL_PASSWORD:root123123}
    driver-class-name: com.mysql.cj.jdbc.Driver
```

逐参数：

| 参数 | 作用 | 坑 |
|------|------|----|
| `risk_control` | 库名 | **与 job 默认的 `flink_risk` 不一致**；compose 创建的是 `flink_risk` |
| `useUnicode=true&characterEncoding=utf-8` | 字符集 | 老参数，MySQL 8 可省略 |
| `useSSL=false` | 关闭 SSL | 本地方便，生产应开启 |
| `serverTimezone=Asia/Shanghai` | 时区 | 不设会导致时间差 8 小时（`create_time` 写入/读取错位） |
| `allowPublicKeyRetrieval=true` | 允许获取公钥 | MySQL 8 + `caching_sha2_password` 必需，否则 `Public Key Retrieval is not allowed` |
| `password` 默认 `root123123` | — | 与 compose 的 `root123` 不同；docker profile 由环境变量覆盖 |

**库名不一致的连锁反应**：web 启动不会报错（懒连接），第一次请求 `/api/rules` 才抛 `Table 'risk_control.rule_config' doesn't exist`，被 `GlobalExceptionHandler` 包成 500 但 HTTP 状态是 200，前端表现是"空表格 + 控制台 500 文案"。

### 3.2 Jackson

```yaml
spring:
  jackson:
    date-format: yyyy-MM-dd HH:mm:ss
    time-zone: Asia/Shanghai
```

`java.util.Date` 序列化为 `"2026-11-09 10:00:00"`，前端 `RiskRecord.createTime` 直接展示。若改成 `LocalDateTime`，此配置不生效，需要 `@JsonFormat`。

### 3.3 Redis（配了但没用）

```yaml
spring:
  data:
    redis:
      host: ${REDIS_HOST:localhost}
      port: ${REDIS_PORT:6379}
      password: ${REDIS_PASSWORD:}
      database: ${REDIS_DATABASE:0}
      timeout: 2000ms
```

- 键路径必须是 `spring.data.redis`（Boot 3），写成 `spring.redis` 会不生效（旧版路径）；
- 全模块没有 `RedisTemplate`/`StringRedisTemplate` 注入，**这段配置目前纯属预留**；
- `starter-data-redis` 因此也是冗余依赖。

### 3.4 MyBatis-Plus

```yaml
mybatis-plus:
  mapper-locations: classpath*:/mapper/**/*.xml
  configuration:
    map-underscore-to-camel-case: true
    log-impl: org.apache.ibatis.logging.stdout.StdOutImpl
  global-config:
    db-config:
      id-type: auto
```

| 配置 | 作用 | 现状 |
|------|------|------|
| `mapper-locations` | XML 映射文件位置 | **仓库里没有任何 XML**，全是注解/条件构造器 |
| `map-underscore-to-camel-case` | `risk_level` → `riskLevel` | **必须为 true**，否则所有驼峰字段为 null |
| `log-impl: StdOutImpl` | SQL 打印到标准输出 | 与 log4j2 的 SqlFile 双份输出，生产应关闭 |
| `id-type: auto` | 主键自增 | 与实体 `@TableId(type = IdType.AUTO)` 一致 |

### 3.5 日志级别

```yaml
logging:
  level:
    com.qinyadan.risk: DEBUG
```

**这份配置几乎不起作用**：项目用 Log4j2，且提供了 `log4j2-spring.xml`。Log4j2 的配置文件优先级高于 Spring 的 `logging.level`，最终级别以 XML 里的 `<Logger>` 为准（见讲 4）。

## 讲 4 · `log4j2-spring.xml` 逐 appender

文件：`src/main/resources/log4j2-spring.xml`（文件名带 `-spring` 才会被 Spring Boot 加载）

### 4.1 四个 Appender

```xml
<Property name="LOG_PATTERN" value="%d{yyyy-MM-dd HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"/>
<Property name="LOG_DIR" value="logs"/>
```

| Appender | 文件 | 过滤 | 滚动策略 |
|----------|------|------|----------|
| `Console` | 标准输出 | 无 | — |
| `AppFile` | `logs/app.log` | 无 | 按天 + 100MB，保留 30 个，gz 压缩 |
| `ErrorFile` | `logs/error.log` | `ThresholdFilter level=ERROR` | 同上 |
| `SqlFile` | `logs/sql.log` | 由 Logger 决定 | 同上 |

**双触发策略**：

```xml
<TimeBasedTriggeringPolicy interval="1" modulate="true" />
<SizeBasedTriggeringPolicy size="100MB" />
```

`modulate="true"` 让按天滚动对齐到 0 点（否则会按启动时间偏移）。`DefaultRolloverStrategy max="30"` 保留 30 个归档。

### 4.2 Logger 层级

```xml
<Root level="INFO">
    <AppenderRef ref="Console"/> <AppenderRef ref="AppFile"/> <AppenderRef ref="ErrorFile"/>
</Root>

<Logger name="org.springframework" level="INFO" additivity="false"> ... </Logger>

<Logger name="com.qinyadan.risk.web.mapper" level="DEBUG" additivity="false">
    <AppenderRef ref="Console"/> <AppenderRef ref="SqlFile"/>
</Logger>

<Logger name="com.baomidou.mybatisplus" level="DEBUG" additivity="false">
    <AppenderRef ref="SqlFile"/>
</Logger>

<Logger name="com.qinyadan.risk.web" level="DEBUG" additivity="false"> ... </Logger>

<Logger name="org.apache.kafka" level="WARN"/>
<Logger name="org.apache.zookeeper" level="WARN"/>
<Logger name="io.lettuce" level="INFO"/>
```

要点：

- `additivity="false"` 阻止向 Root 冒泡，避免同一条日志被 Console + AppFile 重复打印两次；
- `com.qinyadan.risk.web.mapper` 是 **Mapper 接口所在包**，MyBatis 把 SQL 日志打在这个 logger 下；
- `io.lettuce`（Redis 客户端）设为 INFO —— 说明作者预期会用 Redis，但代码没用到。

### 4.3 日志文件位置

`LOG_DIR=logs` 是**相对工作目录**。用 `mvn spring-boot:run` 时工作目录是 `flink-risk-web/`，日志在 `flink-risk-web/logs/`；用 `java -jar` 时取决于启动目录。仓库根目录的 `logs/` 是 job 用的，两者不要混。

---

# 第 2 部分 · 基础设施（讲 5-6）

## 讲 5 · `WebConfig`：分页插件与 CORS

文件：`web/config/WebConfig.java`

```java
@Configuration
public class WebConfig {

    @Bean
    public MybatisPlusInterceptor mybatisPlusInterceptor() {
        MybatisPlusInterceptor interceptor = new MybatisPlusInterceptor();
        interceptor.addInnerInterceptor(new PaginationInnerInterceptor(DbType.MYSQL));
        return interceptor;
    }

    @Bean
    public CorsFilter corsFilter() {
        CorsConfiguration config = new CorsConfiguration();
        config.setAllowCredentials(true);
        config.addAllowedOriginPattern("*");
        config.addAllowedHeader("*");
        config.addAllowedMethod("*");

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", config);
        return new CorsFilter(source);
    }
}
```

### 5.1 分页插件：没有它会发生什么

`ruleService.page(new Page<>(1, 10), wrapper)` 的执行路径：

```
有 PaginationInnerInterceptor
  → SELECT COUNT(*) FROM rule_config WHERE ...
  → SELECT id,name,... FROM rule_config WHERE ... ORDER BY update_time DESC LIMIT ?,?

没有（或未注册）
  → SELECT id,name,... FROM rule_config WHERE ... ORDER BY update_time DESC
  → MyBatis-Plus 在内存里截取第 1 页的 10 条
```

数据量小时看不出差别；10 万条规则时会全表加载 → OOM。**这也是新手接 MyBatis-Plus 最常见的"分页不生效"原因。**

### 5.2 `DbType.MYSQL` 的作用

不同数据库的分页语法不同（MySQL `LIMIT`, Oracle `ROWNUM`, SQLServer `OFFSET FETCH`）。`DbType.MYSQL` 让插件生成 `LIMIT ?,?`。

### 5.3 CORS 的两个必须配对

```java
config.setAllowCredentials(true);          // 允许携带 Cookie
config.addAllowedOriginPattern("*");       // 必须用 Pattern，不能用 addAllowedOrigin("*")
```

Spring 规定：`allowCredentials(true)` 时不能使用 `*` 作为 origin（浏览器会拒绝 `Access-Control-Allow-Origin: *` + `Allow-Credentials: true`）。`addAllowedOriginPattern` 会在响应里回显具体 origin，从而合规。

**安全提示**：`*` + credentials 等于允许任意站点携带凭据访问。生产应改为白名单：

```java
config.setAllowedOriginPatterns(Arrays.asList("https://admin.example.com"));
```

### 5.4 两个 Bean 与前端代理的关系

Vite 开发时 `/api` 被代理到 8080，浏览器看到的是同源请求，**根本不触发 CORS**。CORS 配置真正生效的场景是：前端独立部署（不同域名）直接用 axios 请求后端。两条路都通，属于"双保险"。

## 讲 6 · `GlobalExceptionHandler`

文件：`web/config/GlobalExceptionHandler.java`

```java
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(Exception.class)
    public Map<String, Object> handleException(Exception e) {
        Map<String, Object> response = new HashMap<>();
        response.put("status", 500);
        response.put("error", e.getClass().getSimpleName());
        response.put("message", e.getMessage());

        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        response.put("stackTrace", sw.toString());

        return response;
    }
}
```

### 6.1 行为分析

| 点 | 实际行为 | 问题 |
|----|----------|------|
| `@ExceptionHandler(Exception.class)` | 捕获**所有**异常（含 404、参数绑定失败） | 把客户端错误也报成 500 |
| 返回值是 `Map` | Spring 用 Jackson 序列化 | 没有统一响应结构 |
| **没有 `@ResponseStatus`** | HTTP 状态码仍是 **200** | axios 的 `catch` 不触发，前端只能自己判断 body |
| `stackTrace` 进响应体 | 完整堆栈返回给浏览器 | 信息泄露（表名、SQL、内网路径） |
| `message` 可能为 null | `{"message":null}` | 前端展示 `undefined` |

### 6.2 前端视角的后果

`utils/request.ts` 的拦截器：

```ts
(error) => {
  const msg = error.response?.data?.message || error.message || '请求失败';
  console.error('[API Error]', msg);
  return Promise.reject(error);
}
```

因为异常响应是 HTTP 200，**这个 error 分支永远不会走到**；`response` 分支返回的是一个 `{status:500,...}` 对象，页面把它当数据用：

- `ProTable` 的 `request` 里 `res.data` 为 `undefined` → `data: []`，表格空白；
- 用户看不到任何错误提示，只有浏览器控制台里那条 `api.interceptors.response` 没打的日志。

### 6.3 正确写法（改进方向）

```java
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)          // HTTP 500
    public Map<String, Object> handle(Exception e) {
        log.error("Unhandled exception", e);                    // 堆栈只进日志
        return Map.of(
                "status", 500,
                "error", e.getClass().getSimpleName(),
                "message", e.getMessage() == null ? "服务器内部错误" : e.getMessage()
        );
    }
}
```

进一步可以做：`@ExceptionHandler(MethodArgumentNotValidException.class)` → 400、`@ExceptionHandler(NoSuchElementException.class)` → 404。

---

# 第 3 部分 · 数据访问（讲 7-9）

## 讲 7 · Mapper 接口：一行代码的能力

文件：`web/mapper/RuleMapper.java`、`RiskResultMapper.java`

```java
@Mapper
public interface RuleMapper extends BaseMapper<Rule> {
}
```

`BaseMapper<T>` 提供的核心方法（MyBatis-Plus 3.5.5）：

| 方法 | SQL | 说明 |
|------|-----|------|
| `insert(T)` | `INSERT INTO ...` | 回填自增主键 |
| `deleteById(Serializable)` | `DELETE ... WHERE id = ?` | 物理删除 |
| `deleteByMap(Map)` | `DELETE ... WHERE k1=? AND k2=?` | |
| `delete(Wrapper)` | `DELETE ... WHERE ...` | |
| `updateById(T)` | `UPDATE ... SET 非null字段 ... WHERE id=?` | **null 字段被忽略** |
| `update(T, Wrapper)` | 带条件更新 | |
| `selectById` | `SELECT ... WHERE id=?` | |
| `selectList(Wrapper)` | `SELECT ... WHERE ...` | |
| `selectPage(Page, Wrapper)` | 见讲 5 | 需要分页插件 |
| `selectCount(Wrapper)` | `SELECT COUNT(*)` | |

**为什么接口不需要实现类**：MyBatis 用 JDK 动态代理 + `MybatisMapperProxy` 在运行期生成实现；`@Mapper` 让 Spring 注册代理 Bean。

`RiskResultMapper` 同构，这里没有一条手写 SQL——所有查询都靠 `LambdaQueryWrapper`。

## 讲 8 · Service：空壳背后的能力

文件：`web/service/RuleService.java`、`RiskResultService.java`

```java
@Service
public class RuleService extends ServiceImpl<RuleMapper, Rule> implements IService<Rule> {
}
```

`ServiceImpl<M, T>` 把 `BaseMapper<T>` 的方法**再包装一层**，并增加批量操作：

| 方法 | 等价操作 |
|------|----------|
| `save(T)` | `baseMapper.insert` |
| `saveBatch(Collection)` | 循环 insert（可配 batchSize） |
| `removeById(Serializable)` | `deleteById` |
| `updateById(T)` | 同 |
| `getById(Serializable)` | `selectById` |
| `list()` / `list(Wrapper)` | `selectList` |
| `page(Page, Wrapper)` | `selectPage` |
| `count()` / `count(Wrapper)` | `selectCount` |
| `update()` | 返回 `LambdaUpdateChainWrapper`，用于链式更新 |
| `getBaseMapper()` | 拿到 Mapper |

### 8.1 分层评价

当前 Controller 直接调 `ruleService.page(...)`、`ruleService.update().eq(...)`，业务逻辑（过滤条件、字段同步、返回结构）全在 Controller。Service 只起到"泛型绑定"作用。

优点：CRUD 零样板，加一个实体只需要 2 个空接口 + 1 个空类。
缺点：一旦要加校验、审计、缓存、事务，Controller 会迅速膨胀；且 `RuleService` 无法被单测 mock（没有业务方法可 mock）。

**重构方向**：把"分页查询 + 字段同步"下沉为 `RuleService.pageRules(...)`，"创建 + 表达式校验"下沉为 `RuleService.createRule(...)`，Controller 只做参数绑定。

## 讲 9 · 从方法调用到 SQL

以 `GET /api/rules?page=2&pageSize=10&riskLevel=HIGH` 为例，完整链路：

```
RuleController.getRules(2, 10, null, "HIGH", null, null)
  → wrapper.like(...) 跳过（name 为空）
  → wrapper.eq(Rule::getRiskLevel, "HIGH")
  → wrapper.orderByDesc(Rule::getUpdateTime)
  → ruleService.page(new Page<>(2,10), wrapper)
       → PaginationInnerInterceptor 拦截
           ① SELECT COUNT(*) FROM rule_config WHERE risk_level = 'HIGH'
           ② SELECT id,name,expression,risk_level,action,weight,enabled,
                     description,create_time,update_time
              FROM rule_config
              WHERE risk_level = 'HIGH'
              ORDER BY update_time DESC
              LIMIT 10,10
  → result.getRecords().forEach(Rule::syncToFrontend)
  → 组装 {data,total,page,pageSize}
```

### 9.1 两个必须知道的细节

1. **`Page` 的 `page` 从 1 开始**，`LIMIT` 的 offset = `(page-1)*pageSize`。前端 `ProTable` 的 `current` 也是从 1 开始，正好对齐。
2. **`orderByDesc` 用的是数据库列名**：`LambdaQueryWrapper` 通过 `Rule::getUpdateTime` 的 lambda 反解出 `update_time`（依赖 `@TableField` 或驼峰转换）。如果实体字段改名而没改表，运行期报 `Unknown column`。

### 9.2 条件构造器的类型安全边界

| 写法 | 类型安全 | 说明 |
|------|----------|------|
| `wrapper.eq(Rule::getRiskLevel, v)` | ✅ 编译期 | 字段名由 lambda 推导 |
| `wrapper.eq("risk_level", v)` | ❌ 运行期 | 字符串写错要到执行时才报错 |
| `ruleService.update().eq("enabled", false).in("id", ids)` | ❌ | `batchEnable` 就是这种写法 |

`batchEnable`/`batchDisable` 为了少写一个实体，退化成了字符串列名——两个方法里的列名硬编码了 3 次（`enabled` 两次、`id` 一次），改表名容易漏改。

---

# 第 4 部分 · 实体与映射（讲 10-12）

## 讲 10 · `Rule` 全字段与影子字段

文件：`web/entity/Rule.java`

```java
@TableName("rule_config")
public class Rule {
    @TableId(type = IdType.AUTO)
    private Long id;
    private String name;
    private String expression;
    private String riskLevel;
    private String action;
    private Integer weight;
    private Boolean enabled;
    private String description;
    private Date createTime;
    private Date updateTime;

    @TableField(exist = false) private String ruleType;
    @TableField(exist = false) private String ruleExpression;
    @TableField(exist = false) private String ruleName;
```

### 10.1 两类字段

| 类别 | 字段 | 是否落库 | 来源/去向 |
|------|------|----------|-----------|
| DB 字段 | `id` `name` `expression` `riskLevel` `action` `weight` `enabled` `description` `createTime` `updateTime` | ✅ | `rule_config` 表 |
| 影子字段 | `ruleName` `ruleExpression` | ❌ | 前端别名，靠 `sync*` 手工搬运 |
| 影子字段 | `ruleType` | ❌ | **没人搬运**，永远为 null |

### 10.2 为什么需要影子字段

前端（`types/index.ts`、`services/rule.ts`、`Rule/List.tsx`）用的是 `ruleName` / `ruleExpression`，而后端表列是 `name` / `expression`。三种解决方式：

| 方案 | 代价 |
|------|------|
| 改前端字段名 | 要改 5+ 个文件，且破坏既有 API |
| `@JsonProperty("ruleName")` 映射 | 序列化/反序列化两个方向都要处理，且影响所有接口 |
| 影子字段 + 手工 `sync*`（当前） | 需要在每个端点显式调用，漏一处就出 bug |

选第三种的原因：改动最小、对 Jackson 零侵入，且能同时兼容两套命名（返回的 JSON 里 `name` 和 `ruleName` 都有值）。

### 10.3 字段类型的两处不一致

| 字段 | Java | MySQL | 风险 |
|------|------|-------|------|
| `weight` | `Integer` | `INT DEFAULT 1` | 库里为 NULL 时 Java 得到 null，`RuleConfig.weight` 是 `int` → 拆箱 NPE |
| `enabled` | `Boolean` | `TINYINT(1)` | 同上，`RuleConfig.enabled` 是 `boolean` |

实际上 `init.sql` 两列都有默认值且非空，所以不会出现 null；但如果有人手工插入 NULL，job 侧在 `rs.getInt` 时会得到 0，反而安全些。

## 讲 11 · `syncFromFrontend` / `syncToFrontend` 的调用时序

```java
public void syncFromFrontend() {          // 前端 → DB 字段
    if (ruleName != null) this.name = ruleName;
    if (ruleExpression != null) this.expression = ruleExpression;
}

public void syncToFrontend() {            // DB → 前端字段
    this.ruleName = this.name;
    this.ruleExpression = this.expression;
}
```

### 11.1 调用点全景

| 端点 | 调用 | 位置 |
|------|------|------|
| `GET /api/rules` | `syncToFrontend`（每条） | 分页结果组装前 |
| `GET /api/rules/enabled` | `syncToFrontend`（每条） | 返回前 |
| `GET /api/rules/{id}` | `syncToFrontend` | 判空之后 |
| `POST /api/rules` | `syncFromFrontend` → save → `syncToFrontend` | 写库前 + 返回前 |
| `PUT /api/rules/{id}` | `syncFromFrontend` → update → `syncToFrontend` | 同上 |
| `PUT /api/rules/{id}/toggle` | `syncToFrontend` | 返回前 |
| `DELETE` | 无（返回 message） | — |
| `batch/enable`、`batch/disable` | 无（返回 message） | — |

### 11.2 三个具体 bug 场景

**场景 A：只传 `ruleExpression` 更新**

```json
PUT /api/rules/1  {"ruleExpression": "failCount >= 5"}
```

`syncFromFrontend` 后 `expression = "failCount >= 5"`，`name` 仍为 null → `updateById` 忽略 null → 只更新 expression。**符合预期。**

**场景 B：想清空描述**

```json
PUT /api/rules/1  {"description": null}
```

`updateById` 默认策略是 `NOT_NULL`，null 被忽略 → **清空失败**。要支持清空必须用 `LambdaUpdateWrapper.set(Rule::getDescription, null)`。

**场景 C：新建时不传表达式**

```json
POST /api/rules  {"ruleName": "新规则", "riskLevel": "HIGH", "action": "BLOCK"}
```

`syncFromFrontend` 后 `expression = null` → 插入撞 `expression NOT NULL` 约束 → `DataIntegrityViolationException` → 被全局处理器包成 500（HTTP 200）。**前端表单里 `ruleExpression` 没有 `rules={[{required:true}]}`，能提交空值**——这是一个真实可复现的 bug。

### 11.3 `ruleType` 的完整路径

```
前端表单：没有 ruleType 输入项
  ↓
ProTable 列：没有 ruleType 列
  ↓
types.Rule：有 ruleType?: string（可选）
  ↓
Rule 实体：@TableField(exist=false)
  ↓
syncFromFrontend：不拷贝 ruleType
  ↓
数据库：没有该列
```

结论：**这条字段链从头到尾没有落点**。`StatsController` 的规则类型分布只能靠 `inferRuleType(name)` 猜。要么删掉这个字段，要么加列并补全链路。

## 讲 12 · `RiskResult` 实体与 JSON 列

文件：`web/entity/RiskResult.java`

```java
@TableName("risk_result")
public class RiskResult {
    @TableId(type = IdType.AUTO)
    private Long id;
    private String userId;
    private String eventId;
    private String eventType;
    private String riskLevel;
    private Integer riskScore;
    private String action;
    private String details;      // MySQL JSON 列
    private Date createTime;
}
```

### 12.1 与 common 的 `RiskResult` 是两个类

| | `com.qinyadan.risk.redis.RiskResult`（common） | `com.qinyadan.risk.web.entity.RiskResult`（web） |
|---|---|---|
| 用途 | 流处理载体 | 数据库映射 |
| `riskLevel` | `RiskLevel` 枚举 | `String` |
| `details` | `Map<String,Object>` | `String`（JSON 文本） |
| `ttl`/`timestamp` | 有 | 无 |
| `createTime` | 无 | `Date` |

**同名不同包**，读代码时看 import 判断。web 版本不含 `ttl`，因为 TTL 只对 Redis 有意义。

### 12.2 为什么 `details` 映射成 String

`risk_result.details` 是 MySQL `JSON` 类型。JDBC 驱动把它当字符串返回；MyBatis 用 `StringTypeHandler` 读取即可，**不需要自定义 TypeHandler**。

写入方向（job 的 `MysqlSinkFunction`）也是字符串：

```java
ps.setString(7, objectMapper.writeValueAsString(result.getDetails()));
```

MySQL 会做 JSON 合法性校验：如果写入的不是合法 JSON，会报 `Invalid JSON text`。这是一个隐性保护——但 `details` 里的 `BigDecimal` 会被 Jackson 序列化成数字，合法。

### 12.3 前端拿到的 `details` 是字符串

```ts
export interface RiskRecord {
  details: string;   // ← 是一段 JSON 文本，不是对象
}
```

所以前端若要展示，必须 `JSON.parse(record.details)`（当前页面没展示，见前端文档）。

### 12.4 表上的索引与查询匹配度

`sql/init.sql` 中：

```sql
INDEX idx_user_id (user_id),
INDEX idx_event_type (event_type),
INDEX idx_create_time (create_time)
```

| 查询 | 用到的索引 |
|------|-----------|
| `WHERE user_id LIKE '%x%'` | ❌ 前导通配符导致全表扫描 |
| `WHERE event_type = ?` | ✅ |
| `WHERE risk_level = ?` | ❌ 无索引 |
| `WHERE create_time >= ? AND <= ?`（趋势） | ✅ |
| `ORDER BY create_time DESC` | ✅（可走索引逆序） |

`riskLevel` 是高频筛选条件却没有索引；`userId` 用了 `like` 导致索引失效。优化：给 `risk_level` 加索引，`userId` 改前缀匹配或精确匹配。

---

# 第 5 部分 · 接口层（讲 13-16）

## 讲 13 · `RuleController` 分页查询

文件：`web/controller/RuleController.java`，基路径 `/api/rules`

```java
@GetMapping
public Map<String, Object> getRules(
        @RequestParam(name = "page", defaultValue = "1") int page,
        @RequestParam(name = "pageSize", defaultValue = "10") int pageSize,
        @RequestParam(name = "name", required = false) String name,
        @RequestParam(name = "riskLevel", required = false) String riskLevel,
        @RequestParam(name = "action", required = false) String action,
        @RequestParam(name = "enabled", required = false) Boolean enabled) {
```

### 13.1 参数表

| 参数 | 类型 | 默认 | 匹配方式 | 备注 |
|------|------|------|----------|------|
| `page` | int | 1 | — | 从 1 开始 |
| `pageSize` | int | 10 | — | **无上限**，传 100000 会全表加载 |
| `name` | String | null | `LIKE '%v%'` | 前端 `ruleName` 映射而来 |
| `riskLevel` | String | null | `=` | 大小写需与库里一致 |
| `action` | String | null | `=` | 前端规则页**没有传**（见前端文档） |
| `enabled` | Boolean | null | `=` | `true` → 1 |

### 13.2 组装逻辑

```java
LambdaQueryWrapper<Rule> wrapper = new LambdaQueryWrapper<>();
if (name != null && !name.isEmpty())        wrapper.like(Rule::getName, name);
if (riskLevel != null && !riskLevel.isEmpty()) wrapper.eq(Rule::getRiskLevel, riskLevel);
if (action != null && !action.isEmpty())    wrapper.eq(Rule::getAction, action);
if (enabled != null)                        wrapper.eq(Rule::getEnabled, enabled);
wrapper.orderByDesc(Rule::getUpdateTime);

Page<Rule> result = ruleService.page(new Page<>(page, pageSize), wrapper);
result.getRecords().forEach(Rule::syncToFrontend);
```

**空字符串判断**：`!name.isEmpty()` 避免 `?name=` 时生成 `LIKE '%%'`（全表，但结果正确）。`enabled` 用 `Boolean`，`?enabled=` 空串会由 Spring 转成 null（可接受）。

### 13.3 返回结构

```java
Map<String, Object> response = new HashMap<>();
response.put("data", result.getRecords());
response.put("total", result.getTotal());
response.put("page", result.getCurrent());
response.put("pageSize", result.getSize());
return response;
```

- `result.getSize()` 是**每页大小**，不是"当前页条数"；
- 没有统一包装（无 `code`/`message`），与 `GlobalExceptionHandler` 的 `{status,error,message}` 结构完全不同，前端要分别处理。

## 讲 14 · 其余 8 个规则端点

### 14.1 `GET /api/rules/enabled`

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

- 返回**数组**（不是分页对象）；
- 无数量限制，规则多时响应大；
- 注释说"供 Flink Job 拉取"，实际 job 直连 MySQL，不调用它。

### 14.2 `GET /api/rules/{id}`

```java
@GetMapping("/{id}")
public Rule getRule(@PathVariable Long id) {
    Rule rule = ruleService.getById(id);
    if (rule != null) rule.syncToFrontend();
    return rule;
}
```

id 不存在时返回 `null` → HTTP 200 + 空 body。前端 axios 拿到 `""`，`JSON.parse` 不涉及（axios 会返回空字符串），调用方容易踩空。

### 14.3 `POST /api/rules`

```java
@PostMapping
public Rule createRule(@RequestBody Rule rule) {
    rule.syncFromFrontend();
    ruleService.save(rule);
    rule.syncToFrontend();
    return rule;
}
```

- `id` 由数据库自增回填（`@TableId(type = IdType.AUTO)`）；
- `createTime`/`updateTime` 若前端没传，由数据库 `DEFAULT CURRENT_TIMESTAMP` 填充，**但返回的对象里这两个字段是 null**（MyBatis-Plus 不会回查）。前端若拿返回对象直接插入表格，时间列为空；
- 无表达式校验（见讲 16）。

### 14.4 `PUT /api/rules/{id}`

```java
@PutMapping("/{id}")
public Rule updateRule(@PathVariable Long id, @RequestBody Rule rule) {
    rule.setId(id);
    rule.syncFromFrontend();
    ruleService.updateById(rule);
    rule.syncToFrontend();
    return rule;
}
```

- `id` 以 path 为准（body 里的 id 被覆盖）；
- `updateById` 只更新非 null 字段；
- 返回值是**请求体 + id**，不是库里的最新状态（`updateTime` 为 null）。

### 14.5 `DELETE /api/rules/{id}`

```java
@DeleteMapping("/{id}")
public Map<String, String> deleteRule(@PathVariable Long id) {
    ruleService.removeById(id);
    Map<String, String> response = new HashMap<>();
    response.put("message", "删除成功");
    return response;
}
```

- `removeById` 返回 `boolean`（是否删到行），**被忽略**；
- 删不存在的 id 也返回"删除成功"；
- 物理删除，无软删除、无审计。

### 14.6 `PUT /api/rules/{id}/toggle`

```java
@PutMapping("/{id}/toggle")
public Rule toggleRule(@PathVariable Long id) {
    Rule rule = ruleService.getById(id);
    if (rule != null) {
        rule.setEnabled(!rule.getEnabled());
        ruleService.updateById(rule);
        rule.syncToFrontend();
    }
    return rule;
}
```

**读-改-写**的三步。并发问题：

```
T1: getById(1) → enabled=true
T2: getById(1) → enabled=true
T1: setEnabled(false) → update
T2: setEnabled(false) → update     ← 期望是"再翻一次变 true"，实际仍是 false
```

正确的幂等写法是"前端传目标状态"，或单语句：

```java
ruleService.update().eq("id", id).setSql("enabled = 1 - enabled").update();
```

另外 `rule.getEnabled()` 是 `Boolean`，库里为 NULL 时 `!null` 抛 NPE。

### 14.7 批量启停

```java
@PutMapping("/batch/enable")
public Map<String, String> batchEnable(@RequestBody List<Long> ids) {
    ruleService.update().eq("enabled", false).in("id", ids).set("enabled", true).update();
    Map<String, String> response = new HashMap<>();
    response.put("message", "批量启用成功");
    return response;
}
```

- 请求体是**裸数组** `[1,2,3]`（不是 `{"ids":[...]}`），前端必须对齐；
- `eq("enabled", false)` 只更新当前禁用的，天然幂等；
- `ids` 为空列表时，`in` 生成的 SQL 取决于 MyBatis-Plus 版本：部分版本会产出 `IN ()` 直接语法错误，部分版本会跳过该条件（变成只按 `enabled = false` 更新全表！）。**入口必须判空**：`if (ids == null || ids.isEmpty()) return;`
- 返回体不含"实际更新了多少行"。

### 14.8 端点总览

| # | 方法 | 路径 | 返回类型 | 是否分页 |
|---|------|------|----------|----------|
| 1 | GET | `/api/rules` | `Map` | ✅ |
| 2 | GET | `/api/rules/enabled` | `List<Rule>` | ❌ |
| 3 | GET | `/api/rules/{id}` | `Rule`（可能 null） | ❌ |
| 4 | POST | `/api/rules` | `Rule` | ❌ |
| 5 | PUT | `/api/rules/{id}` | `Rule` | ❌ |
| 6 | DELETE | `/api/rules/{id}` | `Map` | ❌ |
| 7 | PUT | `/api/rules/{id}/toggle` | `Rule`（可能 null） | ❌ |
| 8 | PUT | `/api/rules/batch/enable` | `Map` | ❌ |
| 9 | PUT | `/api/rules/batch/disable` | `Map` | ❌ |

**路径优先级注意**：`/api/rules/{id}` 与 `/api/rules/enabled` 共存时，Spring 会优先匹配**更具体的字面量路径** `enabled`，所以 `GET /api/rules/enabled` 不会被当成 id=enabled 解析。但如果将来加 `/api/rules/{id}/xxx` 与 `/api/rules/batch/xxx`，要注意字面量优先规则。

## 讲 15 · `RiskResultController`

文件：`web/controller/RiskResultController.java`，基路径 `/api/risks`

```java
@RestController
@RequestMapping("/api/risks")
public class RiskResultController {

    @GetMapping
    public Map<String, Object> getRiskResults(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "10") int pageSize,
            @RequestParam(required = false) String userId,
            @RequestParam(required = false) String eventType,
            @RequestParam(required = false) String riskLevel,
            @RequestParam(required = false) String action) {

        LambdaQueryWrapper<RiskResult> wrapper = new LambdaQueryWrapper<>();
        if (userId != null && !userId.isEmpty())     wrapper.like(RiskResult::getUserId, userId);
        if (eventType != null && !eventType.isEmpty()) wrapper.eq(RiskResult::getEventType, eventType);
        if (riskLevel != null && !riskLevel.isEmpty()) wrapper.eq(RiskResult::getRiskLevel, riskLevel);
        if (action != null && !action.isEmpty())     wrapper.eq(RiskResult::getAction, action);
        wrapper.orderByDesc(RiskResult::getCreateTime);

        Page<RiskResult> result = riskResultService.page(new Page<>(page, pageSize), wrapper);
        ...
    }

    @GetMapping("/{id}")
    public RiskResult getRiskResult(@PathVariable Long id) {
        return riskResultService.getById(id);
    }
}
```

要点：

- **与规则接口对称**（同样的分页返回结构），但**没有 `sync*`**（实体无影子字段）；
- `userId` 用 `like` → 索引失效（讲 12.4）；
- 只读，数据由 job 写入；
- 没有按时间范围筛选（看板统计在服务端做，列表页做不了"查昨天"）；
- **`details` 原样返回字符串**，前端要自己解析。

### 15.1 一个容易误解的点

`riskLevel` 过滤是**精确匹配字符串**。库里存的是 `HIGH`，传 `high` 查不到。前端的 `valueEnum` 用大写枚举，正好匹配。

## 讲 16 · 参数校验的全面缺失

三个 Controller 都**没有任何** `@Valid` / `@Validated` / 手工校验。逐个列风险：

| 端点 | 缺失校验 | 后果 |
|------|----------|------|
| `GET /api/rules` | `pageSize` 上限 | 内存/带宽打爆 |
| `GET /api/rules` | `page <= 0` | `LIMIT` 负 offset，MySQL 报语法错 → 500 |
| `POST /api/rules` | `name` 非空、`expression` 非空且可编译、`riskLevel`/`action` 枚举合法 | 脏数据入库 → job 侧整轮加载失败 |
| `POST /api/rules` | 重复规则 | 同一策略多条命中，`matchedRuleCount` 膨胀 |
| `PUT /api/rules/{id}` | 同 POST | 同上 |
| `DELETE /api/rules/{id}` | id 存在性 | 假成功 |
| `PUT /api/rules/{id}/toggle` | 同上 | 返回 null |
| `batch/*` | `ids` 非空 | `IN ()` 边界行为依赖版本 |

### 16.1 最小可行的校验补丁

```java
// 1) 表达式试编译（最重要）
try {
    AviatorEvaluator.compile(rule.getExpression());
} catch (Exception e) {
    throw new IllegalArgumentException("表达式不合法: " + e.getMessage());
}

// 2) 枚举校验
if (!Set.of("LOW","MEDIUM","HIGH","CRITICAL").contains(rule.getRiskLevel())) {
    throw new IllegalArgumentException("riskLevel 非法");
}

// 3) 分页上限
pageSize = Math.min(Math.max(pageSize, 1), 200);
page = Math.max(page, 1);
```

配合讲 6 的 `@ResponseStatus(HttpStatus.BAD_REQUEST)`，前端才能正确提示。

---

# 第 6 部分 · 统计层（讲 17-19）

## 讲 17 · `/api/stats/overview`：6 条 SQL

```java
@GetMapping("/overview")
public Map<String, Object> getOverview() {
    Map<String, Object> stats = new HashMap<>();

    long totalRules = ruleService.count();                                   // SQL 1
    LambdaQueryWrapper<Rule> enabledWrapper = new LambdaQueryWrapper<>();
    enabledWrapper.eq(Rule::getEnabled, true);
    long enabledRules = ruleService.count(enabledWrapper);                   // SQL 2

    LocalDateTime todayStart = LocalDateTime.of(LocalDate.now(), LocalTime.MIN);
    LambdaQueryWrapper<RiskResult> todayWrapper = new LambdaQueryWrapper<>();
    todayWrapper.ge(RiskResult::getCreateTime, todayStart);
    long todayRisks = riskResultService.count(todayWrapper);                 // SQL 3

    LambdaQueryWrapper<RiskResult> highWrapper = new LambdaQueryWrapper<>();
    highWrapper.ge(RiskResult::getCreateTime, todayStart);
    highWrapper.in(RiskResult::getRiskLevel, "HIGH", "CRITICAL");
    long todayHighRisks = riskResultService.count(highWrapper);              // SQL 4

    List<Rule> allRules = ruleService.list();                                // SQL 5
    Map<String, Long> ruleTypeDistribution = allRules.stream()
            .collect(Collectors.groupingBy(r -> inferRuleType(r.getName()), Collectors.counting()));

    List<RiskResult> todayResults = riskResultService.list(todayWrapper);    // SQL 6
    ...
}
```

### 17.1 "今天"的边界

```java
LocalDateTime todayStart = LocalDateTime.of(LocalDate.now(), LocalTime.MIN);   // 00:00:00
```

- 没有上限（`< 明天 0 点`），如果数据库里有未来时间的数据会被算进来（极少）；
- `LocalDate.now()` 用的是**JVM 默认时区**，而 JDBC URL 里 `serverTimezone=Asia/Shanghai`。若服务器时区不是东八区，会出现"今天"错位。正确做法是显式 `ZoneId.of("Asia/Shanghai")`。

### 17.2 风险等级分布的初始化技巧

```java
Map<String, Long> riskLevelDistribution = new LinkedHashMap<>();
riskLevelDistribution.put("LOW", 0L);
riskLevelDistribution.put("MEDIUM", 0L);
riskLevelDistribution.put("HIGH", 0L);
riskLevelDistribution.put("CRITICAL", 0L);

for (RiskResult r : todayResults) {
    String level = r.getRiskLevel();
    if (riskLevelDistribution.containsKey(level)) {
        riskLevelDistribution.put(level, riskLevelDistribution.get(level) + 1);
    }
}
```

**为什么先放 4 个 0**：前端 `Object.entries(RISK_LEVEL_CONFIG).map(...)` 会遍历四个等级取数；如果某个等级今天没有事件，缺 key 会导致卡片显示 `undefined`。预先置 0 保证结构稳定。

**为什么用 `LinkedHashMap`**：保证 JSON 字段顺序固定（LOW→MEDIUM→HIGH→CRITICAL），便于对比与调试。

### 17.3 事件类型分布没有预置

```java
Map<String, Long> eventTypeDistribution = todayResults.stream()
        .collect(Collectors.groupingBy(RiskResult::getEventType, Collectors.counting()));
```

这里用的是 `HashMap`（groupingBy 默认），且没有预置 0。前端 Home 页并没有渲染 eventTypeDistribution，所以暂时无害；但如果将来展示，注意缺 key。

### 17.4 规则类型靠"猜"

```java
private String inferRuleType(String name) {
    if (name == null) return "OTHER";
    if (name.contains("登录") || name.contains("login") || name.contains("Login")) return "LOGIN";
    if (name.contains("订单") || name.contains("order") || name.contains("Order")) return "ORDER";
    if (name.contains("活动") || name.contains("activity") || name.contains("Activity")
            || name.contains("优惠券") || name.contains("coupon")) return "ACTIVITY";
    return "OTHER";
}
```

问题：

| 问题 | 例子 |
|------|------|
| 依赖中文/英文关键词 | 规则名改成"账号保护策略" → 归入 OTHER |
| 大小写只覆盖三种写法 | `LOGIN`（全大写）不匹配任何分支？`contains("login")` 对 `"LOGIN"` 是 false（区分大小写），`contains("Login")` 也是 false → 归 OTHER |
| 优先级固定 | 名字同时含"登录"和"订单" → 归 LOGIN |
| 每次请求全量扫描规则 | 规则多时 CPU 浪费 |

`init.sql` 里的规则名都是中文（"登录失败次数过多"等），所以当前能正确分类。**但 `LOGIN` 全大写这种写法会漏**——上面第 2 行就是活生生的例子，值得记住。

### 17.5 性能

6 条 SQL，其中 SQL 6 是**全量 list 今天的所有风险记录**（可能几十万行）到内存里分组。峰值内存 = 今天记录数 × 对象大小。100 万条 ≈ 数百 MB，会拖慢接口甚至 OOM。

优化方案：

```sql
-- 一条 SQL 同时得到等级分布
SELECT risk_level, COUNT(*) FROM risk_result
WHERE create_time >= #{todayStart} GROUP BY risk_level;

-- 一条 SQL 得到事件类型分布
SELECT event_type, COUNT(*) FROM risk_result
WHERE create_time >= #{todayStart} GROUP BY event_type;
```

## 讲 18 · `/api/stats/trend`：14 条 SQL

```java
@GetMapping("/trend")
public Map<String, Object> getTrend() {
    Map<String, Object> trend = new HashMap<>();
    List<Map<String, Object>> dailyData = new ArrayList<>();

    for (int i = 6; i >= 0; i--) {
        LocalDate date = LocalDate.now().minusDays(i);
        LocalDateTime dayStart = LocalDateTime.of(date, LocalTime.MIN);
        LocalDateTime dayEnd = LocalDateTime.of(date, LocalTime.MAX);

        LambdaQueryWrapper<RiskResult> wrapper = new LambdaQueryWrapper<>();
        wrapper.ge(RiskResult::getCreateTime, dayStart);
        wrapper.le(RiskResult::getCreateTime, dayEnd);
        long total = riskResultService.count(wrapper);                 // SQL 1

        LambdaQueryWrapper<RiskResult> highWrapper = wrapper.clone();   // ①
        highWrapper.in(RiskResult::getRiskLevel, "HIGH", "CRITICAL");
        long high = riskResultService.count(highWrapper);               // SQL 2

        Map<String, Object> dayData = new HashMap<>();
        dayData.put("date", date.toString());
        dayData.put("total", total);
        dayData.put("high", high);
        dailyData.add(dayData);
    }

    trend.put("dailyData", dailyData);
    return trend;
}
```

### 18.1 逐点

- **① `wrapper.clone()`**：`LambdaQueryWrapper` 提供 `clone()`（浅拷贝条件列表），复制后再叠加 `in`。**不能直接复用同一个 wrapper**——那会把 `high` 条件带到后续查询（虽然这里每次循环都新建，但 clone 仍是必要习惯）。
- **`LocalTime.MAX`** 是 `23:59:59.999999999`，MySQL `DATETIME` 精度到秒，实际写入最大 `23:59:59`，`<=` 能覆盖。
- **7 天 × 2 = 14 条 SQL**，接口耗时 ≈ 14 × 单次 count（1~10ms）= 14~140ms；数据量大时更慢。
- **没有补零逻辑**：某天没有数据，`total=0` 仍会返回（因为循环固定 7 次），这点做对了。
- **返回顺序按日期升序**（`i` 从 6 递减到 0），前端 Dashboard 又 `sort` 成降序。两边都能看，但约定不统一。

### 18.2 一条 SQL 的等价写法

```sql
SELECT DATE(create_time) AS d,
       COUNT(*) AS total,
       SUM(CASE WHEN risk_level IN ('HIGH','CRITICAL') THEN 1 ELSE 0 END) AS high
FROM risk_result
WHERE create_time >= #{sevenDaysAgo}
GROUP BY DATE(create_time)
```

再在 Java 里把缺失日期补 0。

## 讲 19 · `/api/stats/top-rules`：解析 JSON 字段

```java
@GetMapping("/top-rules")
public Map<String, Object> getTopRules() {
    LocalDateTime todayStart = LocalDateTime.of(LocalDate.now(), LocalTime.MIN);
    LambdaQueryWrapper<RiskResult> wrapper = new LambdaQueryWrapper<>();
    wrapper.ge(RiskResult::getCreateTime, todayStart);
    List<RiskResult> todayResults = riskResultService.list(wrapper);       // 1 条 list

    Map<String, Long> ruleHitCount = new LinkedHashMap<>();
    for (RiskResult r : todayResults) {
        String ruleName = extractRuleName(r.getDetails());
        if (ruleName != null) {
            ruleHitCount.merge(ruleName, 1L, Long::sum);
        }
    }

    List<Map<String, Object>> topRules = ruleHitCount.entrySet().stream()
            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
            .limit(10)
            .map(entry -> {
                Map<String, Object> item = new HashMap<>();
                item.put("ruleName", entry.getKey());
                item.put("hitCount", entry.getValue());
                return item;
            })
            .collect(Collectors.toList());

    result.put("topRules", topRules);
    return result;
}
```

### 19.1 `extractRuleName`

```java
private static final ObjectMapper MAPPER = new ObjectMapper();

private String extractRuleName(String details) {
    if (details == null || details.isEmpty()) return null;
    try {
        JsonNode node = MAPPER.readTree(details);
        JsonNode nameNode = node.get("matchedRuleName");
        if (nameNode != null && !nameNode.isNull()) {
            return nameNode.asText();
        }
    } catch (Exception e) {
        // JSON 解析失败，回退到 eventType + riskLevel 组合
    }
    return null;
}
```

- `MAPPER` 静态复用，线程安全；
- **catch 块是空的**，注释说"回退到 eventType + riskLevel 组合"，但代码里**没有实现回退**，直接返回 null；
- 依赖 `details.matchedRuleName` 存在 → 只有"命中规则"的记录才计入排行；未命中记录（`{"reason":"No rule matched"}`）被跳过，符合语义。

### 19.2 排行榜的隐性依赖链

```
RiskActionResolver 写入 matchedRuleName
  → MysqlSinkFunction 序列化 details
    → risk_result.details (JSON)
      → extractRuleName 解析
        → 排行榜
```

链路上任何一环改名（`matchedRuleName` → `ruleName`），排行榜直接空掉，且**没有告警**。建议把字段名提取为常量共享。

### 19.3 性能与正确性

| 问题 | 说明 |
|------|------|
| 全量 list 今日记录 | 与 overview 相同的内存风险 |
| 单线程解析 JSON | 10 万条记录 = 10 万次 `readTree`，CPU 可观 |
| 平局顺序不稳定 | `sorted` 是稳定排序，但 `LinkedHashMap` 的插入顺序取决于遍历顺序（`list` 的顺序是 `create_time DESC`），所以平局按"最新出现的先"排列 |
| Top 10 硬编码 | 无法查 Top 20 |

优化：把统计结果物化到一张 `risk_stat_daily` 表（job 侧或定时任务写入），接口直接读聚合表。

---

# 第 7 部分 · 验证与改造（讲 20-22）

## 讲 20 · 测试现状逐行

文件：`src/test/java/com/qinyadan/risk/web/controller/RuleControllerTest.java`

```java
@SpringBootTest(classes = RiskWebApplication.class)
@AutoConfigureMockMvc
public class RuleControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void testGetRules() throws Exception {
        mockMvc.perform(get("/api/rules?page=1&pageSize=10"))
                .andDo(print());
    }
}
```

### 20.1 逐注解

| 注解 | 作用 | 本项目行为 |
|------|------|-----------|
| `@SpringBootTest(classes = ...)` | 启动完整 Spring 容器 | 会创建 `DataSource`、MyBatis 会话工厂 |
| `@AutoConfigureMockMvc` | 注入 `MockMvc`，不走真实端口 | 只测 MVC 层，不起 Tomcat |
| `.andDo(print())` | 打印请求/响应 | **没有断言** |

### 20.2 为什么它会失败（而不是跳过）

`GET /api/rules` 会真正执行 SQL。如果：

- MySQL 没启动 → `Communications link failure`；
- 库不存在 → `Unknown database 'risk_control'`；
- 表不存在 → `Table 'risk_control.rule_config' doesn't exist`。

测试上下文会抛异常，测试失败。**它没有 `@ActiveProfiles("test")`、没有 H2、没有 Testcontainers、没有 `@MockBean`**，所以是一个"必须连真库"的测试。

### 20.3 让它变成真测试的最小改动

```java
@SpringBootTest(classes = RiskWebApplication.class)
@AutoConfigureMockMvc
class RuleControllerTest {

    @Autowired MockMvc mockMvc;

    @Test
    void 分页查询应返回统一结构() throws Exception {
        mockMvc.perform(get("/api/rules?page=1&pageSize=10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").isNumber())
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.page").value(1))
                .andExpect(jsonPath("$.pageSize").value(10));
    }

    @Test
    void 非法pageSize应被限制() throws Exception {
        mockMvc.perform(get("/api/rules?pageSize=100000"))
                .andExpect(jsonPath("$.pageSize").value(lessThanOrEqualTo(200)));
    }
}
```

进一步用 `@SpringBootTest` + Testcontainers MySQL，或把 Controller 层改成 `@WebMvcTest` + `@MockBean RuleService`（更快，不依赖数据库）。

### 20.4 测试覆盖缺口

| 层 | 有测试 | 建议补 |
|----|--------|--------|
| Controller | 1 个（无断言） | 参数校验、异常响应、分页边界 |
| Service | 0 | 基本无逻辑，可跳过 |
| Mapper | 0 | 复杂查询（统计聚合）值得测 |
| 表达式校验 | 0 | `POST` 加校验后必须有 |

## 讲 21 · 与 job 的契约（表结构与枚举）

### 21.1 两张表：谁写谁读

```
        web                           MySQL                        job
  POST/PUT/toggle  ───────write──────▶ rule_config ──────read──────▶ RuleSourceFunction
  GET 分页/统计     ◀──────read─────── risk_result ◀──────write───── MysqlSinkFunction
```

### 21.2 `rule_config` 字段级契约

| 列 | 类型 | web 读 | web 写 | job 读 | 约束 |
|----|------|--------|--------|--------|------|
| `id` | BIGINT PK | ✅ | 自增 | ✅ `getLong` → String | — |
| `name` | VARCHAR(100) | ✅ | ✅ | ✅ | NOT NULL |
| `expression` | TEXT | ✅ | ✅ | ✅ | NOT NULL（空值会 500） |
| `risk_level` | VARCHAR(20) | ✅ | ✅ | ✅ `valueOf` | 必须大写枚举名 |
| `action` | VARCHAR(20) | ✅ | ✅ | ✅ 原样 | 自由字符串，但前端映射表有限 |
| `weight` | INT | ✅ | ✅ | ✅ | 默认 1 |
| `enabled` | TINYINT(1) | ✅ | ✅ | 只读 `=1` | 布尔 |
| `description` | VARCHAR(500) | ✅ | ✅ | ✅ | 可空 |
| `create_time` | DATETIME | ✅ | 不写（DB 默认） | ❌ 不查 | — |
| `update_time` | DATETIME | ✅ | 不写（DB 自动更新） | ❌ 不查 | — |

**三处最容易破坏契约的操作**：

1. `riskLevel` 写成小写 → job 的 `RiskLevel.valueOf` 抛异常 → 整轮规则加载失败；
2. `expression` 写成非法语法 → 该条规则被跳过（有日志）；
3. `action` 写前端不认识的取值 → 列表显示原始英文，统计口径不受影响。

### 21.3 `risk_result` 字段级契约

| 列 | 来源（job） | web 读 |
|----|-------------|--------|
| `user_id` | `RiskResult.userId` | ✅ |
| `event_id` | UUID（CEP 生成） | ✅ |
| `event_type` | `LOGIN/ORDER/ACTIVITY` | ✅ |
| `risk_level` | `RiskLevel.name()` | ✅ |
| `risk_score` | 等级分（10/50/80/100） | ✅ |
| `action` | 最高危规则的 action | ✅ |
| `details` | JSON 字符串 | ✅（String） |
| `create_time` | `NOW()` | ✅ |

### 21.4 库名矩阵（务必统一）

| 位置 | 库名 |
|------|------|
| `docker-compose.yml` `MYSQL_DATABASE` | `flink_risk` |
| `sql/init.sql` | 不建库，只建表 |
| job 默认 `--mysql.url` | `flink_risk` |
| web `application.yml` | `risk_control` |
| compose 的 `risk-web` 环境变量 | 只覆盖 host/port/password，**不覆盖库名** |

**结论**：`docker compose up` 之后，web 默认连不上库。要么把 web 的库名改成 `flink_risk`，要么在 compose 里加 `MYSQL_DATABASE` 建 `risk_control`，要么统一用环境变量。

## 讲 22 · 改造清单

### 22.1 正确性（P0）

| # | 问题 | 位置 | 修法 |
|---|------|------|------|
| 1 | 库名不一致 | `application.yml` | 统一为 `flink_risk` 或加环境变量 |
| 2 | 异常响应 HTTP 200 + 堆栈外泄 | `GlobalExceptionHandler` | 加 `@ResponseStatus`，堆栈只进日志 |
| 3 | 无表达式/枚举校验 | `RuleController` | 加 `AviatorEvaluator.compile` 试编译 |
| 4 | 新建可提交空 expression | `Rule/List.tsx` 表单 + 后端 | 前端 required + 后端校验 |
| 5 | `ruleType` 全链路丢失 | `Rule` + 前端 | 加列或删字段 |
| 6 | `toggle` 并发丢更新 | `RuleController.toggleRule` | 单语句 `SET enabled = 1 - enabled` |
| 7 | 删除/更新不校验存在性 | 同上 | 判 boolean 返回值，返回 404 |
| 8 | `pageSize` 无上限 | 两个列表接口 | `Math.min(pageSize, 200)` |

### 22.2 性能（P1）

| # | 问题 | 修法 |
|---|------|------|
| 1 | overview 6 条 SQL + 全量 list | 改 `GROUP BY` 聚合，或物化统计表 |
| 2 | trend 14 条 SQL | 一条 `GROUP BY DATE(create_time)` |
| 3 | top-rules 全量 list + 逐条 parse JSON | 在 job 侧写 `matched_rule_name` 独立列，直接 `GROUP BY` |
| 4 | `user_id LIKE '%x%'` 索引失效 | 改前缀匹配或精确匹配 |
| 5 | `risk_level` 无索引 | 加索引 |
| 6 | `log-impl: StdOutImpl` 双份 SQL 日志 | 生产关闭 |

### 22.3 结构（P2）

| # | 问题 | 修法 |
|---|------|------|
| 1 | 业务逻辑全在 Controller | 下沉到 `RuleService` / `RiskResultService` |
| 2 | 响应结构不统一 | 引入 `Result<T>{code,message,data}` |
| 3 | `flink-risk-common` 与 `starter-data-redis` 未使用 | 删除依赖或启用 |
| 4 | 无鉴权 | Spring Security + 角色（只读/管理员） |
| 5 | 无审计日志 | 规则变更写 `rule_audit` 表 |
| 6 | `RiskResult.details` 无解析端点 | 加 `GET /api/risks/{id}/detail` 返回结构化 JSON |

### 22.4 动手练习

1. **修库名并跑通**：把 `application.yml` 改为 `flink_risk`，启动后 `curl /api/rules` 应返回 15 条。
2. **补表达式校验**：`POST /api/rules` 传 `failCount >=`，应返回 400 而非 500（需先修 `GlobalExceptionHandler`）。
3. **重写 toggle**：改成单 SQL 并写并发测试（两个线程同时 toggle，断言最终状态正确）。
4. **优化 trend**：自定义 Mapper 方法用 `@Select` 写聚合 SQL，对比接口耗时。
5. **补 Controller 测试**：用 `@WebMvcTest` + `@MockBean RuleService` 覆盖分页、创建、删除三条路径。
6. **字段契约实验**：把 `init.sql` 中一条规则的 `risk_level` 改成小写，重启 job，观察日志中整轮加载失败的表现，理解"一条脏数据毁一批"。

---

## 附录 A · 类清单

| 类 | 行数级 | 角色 | 状态 |
|----|--------|------|------|
| `RiskWebApplication` | 10 | 启动类 | ✅ |
| `WebConfig` | 40 | 分页 + CORS | ✅ |
| `GlobalExceptionHandler` | 27 | 全局异常 | ⚠️ 需改 |
| `RuleController` | 155 | 9 个端点 | ✅（缺校验） |
| `RiskResultController` | 64 | 2 个端点 | ✅ |
| `StatsController` | 199 | 3 个统计 | ⚠️ 性能 |
| `Rule` | 78 | 实体 + 影子字段 | ⚠️ ruleType 无效 |
| `RiskResult` | 40 | 实体 | ✅ |
| `RuleMapper` / `RiskResultMapper` | 9 | BaseMapper | ✅ |
| `RuleService` / `RiskResultService` | 11 | ServiceImpl | ✅（空壳） |
| `RuleControllerTest` | 25 | 冒烟测试 | ⚠️ 无断言 |

## 附录 B · 读懂标准

能不看代码回答：

1. 启动类为什么放在 `com.qinyadan.risk` 而不是 `com.qinyadan.risk.web`？
2. 为什么排除 Logback 要写三遍？用错 MyBatis-Plus starter 会怎样？
3. `map-underscore-to-camel-case` 关掉后，哪些字段会变成 null？
4. 不注册 `PaginationInnerInterceptor` 会发生什么？
5. `allowCredentials(true)` 为什么必须配 `addAllowedOriginPattern`？
6. `ServiceImpl` 的 `page()` 最终执行几条 SQL？分别是什么？
7. `syncFromFrontend` 与 `syncToFrontend` 分别在哪些端点被调用？漏掉会怎样？
8. `updateById` 为什么无法清空字段？
9. `POST /api/rules` 不传 expression 会发生什么？前端为什么能提交空值？
10. overview / trend / top-rules 各执行几条 SQL？哪条最可能 OOM？
11. 为什么 `matchedRuleName` 一改，排行榜就空？
12. 为什么 web 连不上 job 同款数据库？怎么排查？

---

下一篇：无（web 为最后一篇）。返回 [common 详解](01-common.md) 或 [学习总览](../LEARNING-GUIDE.md)。
