# CODEBUDDY.md This file provides guidance to CodeBuddy when working with code in this repository.

## 项目概览

基于 Flink 2.2.1 + CEP 的实时风控系统。Kafka 三路事件 topic → Flink CEP 模式匹配 → Aviator 动态规则评估 → Redis/MySQL/Kafka 三路输出；Spring Boot 3.2 + React 18 提供规则管理与风险查询后台。源码包名统一为 `com.qinyadan.risk`（文档中出现的 `com.risk` 是历史包名，已废弃）。

## 常用命令

构建：

```bash
mvn clean package -DskipTests                            # 全量构建
mvn clean package -pl flink-risk-job -am -DskipTests     # 单模块（-am 必须先编译 common）
mvn clean package -pl flink-risk-web -am -DskipTests
```

`flink-risk-job` 用 `maven-shade-plugin` 打出含 mainClass 的 fat-jar；Flink 核心依赖为 `provided`。

测试：

```bash
mvn test                                        # 全部单元测试
mvn test -pl flink-risk-web -am                 # 单模块
mvn test -pl flink-risk-web -am -Dtest=RuleControllerTest#testGetRules -DfailIfNoTests=false   # 单个测试方法
```

当前仅有一个测试：`RuleControllerTest`（MockMvc 冒烟，无断言）；`@SpringBootTest` 需要本地 MySQL 可达，无依赖时会启动失败。集成测试约定命名 `*IT.java`，用 `-Dtest=*IT` 运行。

运行：

```bash
cd docker && docker compose up -d               # MySQL/Redis/Kafka/Kafka-UI/Adminer（另有 flink jobmanager+taskmanager）
cd scripts && ./docker-start.sh start           # 或脚本方式（另建网络/建 topic/初始化库）
./scripts/event-simulator.sh login_failure user_001   # 造登录失败数据；all / batch N / stress / cleanup
cd flink-risk-web && mvn spring-boot:run        # Web 后端 :8080
cd frontend && npm install && npm run dev       # 前端 :3000（/api 代理到 :8080）
cd flink-risk-job && mvn exec:java -Dexec.mainClass=com.qinyadan.risk.RiskControlApplication
```

前端：`npm run build`（`tsc && vite build`，输出 `dist/`）、`npm run lint`（eslint）。

提交到 Flink 集群：

```bash
flink run -c com.qinyadan.risk.RiskControlApplication target/flink-risk-job-2.0.1-SNAPSHOT.jar \
  --kafka.bootstrap.servers kafka:9092 --mysql.url "jdbc:mysql://mysql:3306/flink_risk?useSSL=false&serverTimezone=UTC" \
  --mysql.username root --mysql.password root123 --redis.host redis --redis.port 6379 --redis.db 0 --rule.poll.interval 30000
```

集群侧需手动往 JobManager/TaskManager 的 `lib/` 补 `flink-cep_2.12-2.2.1.jar`、`flink-connector-kafka-5.0.0-2.2.jar`、`kafka-clients`，详见 `docs/DEPLOY.md`。

## 架构要点

### 模块与依赖方向

- `flink-risk-common`：事件模型（`Event` + `LoginEvent`/`OrderEvent`/`ActivityEvent`）、`AviatorRuleEngine`、`RiskActionResolver`、`RiskResult`、Jedis 封装、Kafka 反序列化器。Java 8。
- `flink-risk-job`：Flink 作业与 Sink/Source 工厂。Java 8。
- `flink-risk-web`：Spring Boot + MyBatis-Plus CRUD 与统计接口。Java 17。
- `frontend`：React + Ant Design（ProComponents）管理界面，`src/services/*.ts` 是唯一 API 层。
- web/job 都依赖 common；job 与 web 之间**没有**代码依赖，二者只通过 MySQL `rule_config` 表间接耦合。

### 运行时数据流（`RiskControlApplication.main`）

1. `RuleSourceFunction`（parallelism=1）每 `--rule.poll.interval`（默认 30s）用原生 JDBC 查 `rule_config WHERE enabled=1`，发一份完整 `List<RuleConfig>` 快照。
2. 该流 `broadcast(RuleEvaluationFunction.RULE_STATE_DESCRIPTOR)`：`MapStateDescriptor<Void, List<RuleConfig>>`，约定用 `null` 作为唯一 key。
3. 三个 `KafkaSourceFactory.createXxxSource()` 经 `env.fromSource` 接入，`WatermarkStrategy.forBoundedOutOfOrderness(5s)` + `event.getTimestamp()` 作事件时间。
4. `keyBy(userId)` 后交给 `LoginCepJob` / `OrderCepJob` / `ActivityCepJob`，各自 `union` 多条 Pattern 的命中结果，输出 `PatternMatchResult`（`patternType` + `context` + `details`）。
5. 三路匹配流 `union` 后 `connect(ruleBroadcastStream).process(new RuleEvaluationFunction())`：从 `ReadOnlyBroadcastState` 取规则快照 → `RiskActionResolver.resolve()` → `RiskResult`。
6. `RiskResult` 三路下发：`RedisSinkFunction`（Flink 2.x `Sink`/`SinkWriter` API，key `risk:{eventType}:{userId}:{eventId}`，TTL 3600s）、`AsyncDataStream.orderedWait` + `MysqlSinkFunction`（写 `risk_result` 表）、`KafkaSinkFactory`（`risk-results` topic）。

### 规则热更新与评估语义

- 广播侧 `processBroadcastElement` **全量覆盖**规则快照，并 `new AviatorRuleEngine(...)` + `registerRules()` 后整体替换 `volatile` 字段（不在旧实例上增量改），所以引擎内部无需加锁。
- 规则未就绪（快照为 null/空）时走"失败开放"：直接产出 `LOW`/`ALLOW`/score 0 的 `RiskResult`，不阻塞数据流。
- `AviatorRuleEngine` 注册前先 `AviatorEvaluator.compile` 试编译，非法表达式跳过；执行时异常吞掉返回 false；表达式结果非 `Boolean` 一律视为未命中。编译结果走 Guava Cache（`new AviatorRuleEngine(1000, 3600)`）。
- 多规则命中时 `RiskActionResolver` "就高不就低"：先比 `RiskLevel.score`（LOW 10 / MEDIUM 50 / HIGH 80 / CRITICAL 100），再比 `weight`；最终 `riskScore` 取命中规则的等级分（不是加权平均）。
- Redis 侧 key 前缀与文档里的 `rule_engine:risk:` 不一致，以 `RiskResult.getRedisKey()` 的 `risk:` 为准。

### 规则上下文契约（改 Pattern 或规则时必须同步）

规则表达式只能访问 CepJob 写入 `context` 的变量，且缺失变量会让表达式抛错（被吞成"未命中"）：

- LOGIN：`eventType` `patternType` `userId` `failCount` `ip` `deviceId` `success` `location` `browser`（`LOCATION_CHANGE` 额外有 `prevIp`/`currIp`）
- ORDER：`eventType` `patternType` `userId` `orderCount` `amount` `productId` `paymentMethod`（`ORDER_FREQUENT` 额外 `totalAmount`，`ORDER_RAPID` 额外 `intervalMs`）
- ACTIVITY：`eventType` `patternType` `userId` `participationCount` `activityId` `activityName` `actionType` `channel` `source`

`PatternFactory` 是 Pattern 的唯一来源：6 个 pattern，均用 `followedBy` + `within(Duration)`。`within` 依赖 Watermark 推进，把 Watermark 改回 `noWatermarks()` 会导致 CEP 永不触发（见 `CHANGELOG.md` v2.3）。

### Web 层

- `Rule` 实体映射 `rule_config`，另有 `@TableField(exist=false)` 的 `ruleName`/`ruleExpression`/`ruleType` 别名；`syncFromFrontend()`/`syncToFrontend()` 在 Controller 中显式调用做 DB 字段 ↔ 前端字段转换。新增字段时要同时落库字段与别名。
- 路由：`/api/rules`（含 `/enabled`、`/{id}`、`/{id}/toggle`、`/batch/enable|disable`）、`/api/risks`（**不是** README 写的 `/api/risk-results`）、`/api/stats/overview|trend|top-rules`。
- 分页统一返回 `{data, total, page, pageSize}`，前端 `PageResult<T>` 与之对应。
- CORS 在 `WebConfig` 全放开；MyBatis-Plus 分页插件也在其中注册。

## 环境与配置注意事项

- 数据库名两处不一致：job 默认 `jdbc:mysql://localhost:3306/flink_risk`，web `application.yml` 默认 `/risk_control`。本地跑通前先确认二者指向同一库（`sql/init.sql` 只建表、不建库）。
- 默认口令：MySQL `root/root123`（web 默认回退值是 `root123123`，可用 `MYSQL_PASSWORD` 覆盖）、Redis `redis123`（job 默认密码为空，需显式传 `--redis.password` 时注意 `RiskControlApplication` 目前并未解析该参数）。
- Kafka 地址：宿主机用 `localhost:9092`，容器互访用 `kafka:29092`；job 的 `application.properties` 当前是 `127.0.0.1:29092`，实际参数以 `--kafka.bootstrap.servers` 为准（`application.properties` 并未被代码读取，全是 `getArg` 硬编码默认值）。
- `docker-compose.yml` 中 mysql 的 `./sql/init.sql` 挂载路径相对 `docker/` 目录，仓库里的 sql 在根目录，需修正或手工导入。
- `RuleLoader`（YAML 规则加载）与 `flink-risk-job/src/main/resources/rules.yaml` 目前未接入主流程，规则以 MySQL 为唯一真源。
- 文档 `docs/ARCHITECTURE.md` 部分内容滞后（包名、Redis 前缀、表结构、Java 版本），以代码为准。

## 代码约定

- 4 空格缩进，单行 ≤120 字符；日志统一 SLF4J（`RiskControlApplication` 里的 `System.out.println` 是既有遗留，新代码不要照抄）。
- `Closeable` 一律 try-with-resources；异常不得静默吞掉（至少在 `logger.error` 中带上参数上下文）。
- 新增 CEP 场景的固定套路：`PatternFactory` 加 pattern → 对应 `XxxCepJob.process()` 里 `CEP.pattern(...).select()` 组装 `context`/`details` → `sql/init.sql` 补规则 → 必要时同步 README/规则变量表。
- 分支从 `main` 拉出，命名 `feature/<desc>` 或 `fix/<issue>-<desc>`；提交信息用 `type(scope): subject`（feat/fix/docs/refactor/test/chore），正文列要点。
