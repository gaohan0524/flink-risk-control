# Flink 规则引擎风控系统

<p align="center">
  <a href="https://flink.apache.org/"><img src="https://img.shields.io/badge/Apache%20Flink-2.2.1-E6526C?logo=apacheflink" alt="Flink 2.2.1"></a>
  <a href="https://spring.io/"><img src="https://img.shields.io/badge/Spring%20Boot-3.2.0-6DB33F?logo=springboot" alt="Spring Boot 3.2.0"></a>
  <a href="https://react.dev/"><img src="https://img.shields.io/badge/React-18.3-61DAFB?logo=react" alt="React 18.3"></a>
  <a href="https://kafka.apache.org/"><img src="https://img.shields.io/badge/Apache%20Kafka-4.1-231F20?logo=apachekafka" alt="Apache Kafka 4.1"></a>
  <a href="https://redis.io/"><img src="https://img.shields.io/badge/Redis-8.8-DC382D?logo=redis" alt="Redis 8.8"></a>
  <a href="https://www.mysql.com/"><img src="https://img.shields.io/badge/MySQL-8.0-4479A1?logo=mysql" alt="MySQL 8.0"></a>
  <br>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="MIT License"></a>
</p>

> 基于 Apache Flink 2.2.1 + CEP 的实时风控系统：Kafka 事件流 → CEP 模式匹配 → Aviator 动态规则评估 → Redis / MySQL / Kafka 三路输出，配套 Spring Boot 管理后端与 React 管理界面。

## 项目简介

这是一个面向电商/金融场景的**实时风控引擎**。它把"用户行为事件流"转换成"风险处置结果"，核心是一条两段式流水线：

```
行为事件 ──CEP──▶ 命中的异常模式（LOGIN_FAILURE / ORDER_RAPID …）──规则引擎──▶ 处置结论（BLOCK / VERIFY / ALLOW …）
```

- **第一段（代码）**：判断"事件序列像不像异常"，例如同一用户 60 秒内连续 3 次登录失败；
- **第二段（配置）**：判断"这种异常在你的业务里算多严重、该怎么处置"，规则存在 MySQL，改库 30 秒内生效，无需重启作业。

### 核心能力

- **实时事件接入** —— 三个 Kafka Topic 分别接收登录、下单、活动事件，使用 Flink 2.x 新版 `KafkaSource` API，事件时间语义 + 5 秒乱序容忍
- **CEP 模式匹配** —— 内置 6 类模式：登录失败风暴、异地登录（实为两次成功登录）、频繁下单、快速下单、频繁参与活动、重复领券
- **动态规则引擎** —— 规则存 MySQL，`RuleSourceFunction` 每 30 秒拉取全量快照，经 `BroadcastState` 广播到所有并行实例；规则变更、禁用、删除均自动生效
- **多路结果输出** —— Redis（实时查询，TTL 1 小时）、MySQL（审计与后台查询）、Kafka（`risk-results`，供下游告警/数仓消费）
- **可视化管理后台** —— Spring Boot 3.2 + React 18 + Ant Design 5，支持规则 CRUD/启停/批量操作、风险记录查询、看板统计

### 技术亮点

| 特性 | 实现方式 |
|------|----------|
| 流处理引擎 | Apache Flink 2.2.1，EventTime + Watermark（`forBoundedOutOfOrderness(5s)`） |
| 事件匹配 | Flink CEP `Pattern` API，`followedBy` 宽松连续 + `within(Duration)`（30~60 秒） |
| 规则热更新 | `BroadcastStream` + `MapStateDescriptor<Void, List<RuleConfig>>`，每个 subtask 本地持有只读副本 |
| 表达式引擎 | Aviator 5.4.3，注册期试编译 + Guava 编译缓存 + 求值异常降级 |
| 决策口径 | 多规则命中"就高不就低"：先比风险等级分，再比权重 |
| 消息队列 | Kafka 4.1（`confluentinc/cp-kafka:8.1.4`，KRaft 模式，无 Zookeeper） |
| 缓存 | Redis 8.8 + Jedis 5.2.0 |
| 容错 | Checkpoint 每 60 秒；Kafka 侧 offset 随 checkpoint 提交 |

### 适用场景

- 电商营销风控：薅羊毛、刷单、优惠券滥用
- 账户安全：暴力破解、异地登录告警
- 支付风控：高频下单、快速连续下单

---

## 目录

- [项目简介](#项目简介)
- [系统架构](#系统架构)
- [模块结构](#模块结构)
- [系统截图](#系统截图)
- [技术栈](#技术栈)
- [快速启动](#快速启动)
- [CEP 风控场景](#cep-风控场景)
- [规则引擎](#规则引擎)
- [REST API](#rest-api)
- [数据库表](#数据库表)
- [Kafka Topic 格式](#kafka-topic-格式)
- [输出与存储](#输出与存储)
- [脚本工具](#脚本工具)
- [日志配置](#日志配置)
- [学习文档](#学习文档)
- [已知限制](#已知限制)
- [许可证](#许可证)
- [关于作者](#关于作者)

---

## 系统架构

```
Kafka Topics                    Flink 流处理引擎（flink-risk-job）                输出
┌──────────────┐    ┌──────────────────────────────────────────────┐    ┌──────────────┐
│ login-events │───▶│ KafkaSource (p=2) → keyBy(userId)            │    │ Redis        │
│ order-events │    │        ↓                                     │    │ (实时查询)   │
│ activity-    │    │ CEP Pattern × 6（within 30~60s）             │───▶│ MySQL        │
│   events     │    │        ↓                                     │    │ (审计/后台)  │
└──────────────┘    │ PatternMatchResult（patternType + context）  │    │ Kafka        │
                    │        ↓                                     │    │ risk-results │
┌──────────────┐    │ union → connect(BroadcastStream)             │    └──────────────┘
│ MySQL        │───▶│        ↓                                     │
│ rule_config  │    │ RuleEvaluationFunction（Aviator）             │
└──────────────┘    │        ↓                                     │
  ↑ 每 30s 轮询      │ RiskResult                                   │
  │ (p=1)            └──────────────────────────────────────────────┘
┌──────────────┐
│ risk-web     │  规则 CRUD / 风险查询 / 统计（读写同一套 MySQL 表）
└──────────────┘
```

**核心流程**：Kafka 接入 → 按 `userId` 分组 → CEP 模式匹配 → 上下文组装 → 广播规则评估 → 三路输出。

### 两条链路

| 链路 | 方向 | 关键类 |
|------|------|--------|
| 事件链路 | Kafka → CEP → 规则评估 → 三 Sink | `KafkaSourceFactory`、`PatternFactory`、`LoginCepJob`/`OrderCepJob`/`ActivityCepJob`、`RuleEvaluationFunction`、三个 Sink |
| 规则链路 | MySQL → 广播流 → 每个 subtask 的本地引擎 | `RuleSourceFunction`、`AviatorRuleEngine`、`RiskActionResolver` |

两条链路只在 `RuleEvaluationFunction` 汇合：事件侧读广播状态里的规则快照，广播侧全量替换快照并重建引擎。

---

## 模块结构

| 模块 | 说明 | 源文件 | 编译级别 | 打包 |
|------|------|--------|----------|------|
| `flink-risk-common` | 事件模型、Kafka 反序列化、Aviator 规则引擎、`RiskResult`、Redis 客户端 | 16 个 Java | Java 8 | jar（`provided` Flink） |
| `flink-risk-job` | Flink 作业：Kafka Source → CEP → 广播规则评估 → 三 Sink | 12 个 Java | Java 8 | jar（shade fat-jar） |
| `flink-risk-web` | Spring Boot 管理后端：规则 CRUD、风险查询、统计 | 12 个 Java + 1 个测试 | Java 17 | jar |
| `frontend` | React 管理界面：概览、规则管理、风险记录、数据看板 | TS/TSX | — | Vite 静态资源 |

依赖方向：`job`、`web` 均依赖 `common`；`job` 与 `web` 之间**没有代码依赖**，只通过 MySQL 的 `rule_config` / `risk_result` 两张表协作。

技术栈再补充两点：`flink-risk-job` 的 Flink 依赖（core/streaming/cep/connector-kafka/connector-base/runtime-web）全部是 `provided`，集群 `lib/` 必须补齐 CEP 与 Kafka 连接器（见 [docs/DEPLOY.md](docs/DEPLOY.md)）；`flink-risk-web` 使用 `mybatis-plus-spring-boot3-starter` 并排除了 Logback。

---

## 系统截图

### 系统概览

<img src="docs/capture/img.png" width="800" alt="系统概览">

### 规则管理

<img src="docs/capture/img_1.png" width="800" alt="规则管理">

### 风险记录

<img src="docs/capture/img_2.png" width="800" alt="风险记录">

### 数据看板

<img src="docs/capture/img_3.png" width="800" alt="数据看板">

### Flink Job 运行图

<img src="docs/capture/img_4.png" width="800" alt="Flink Job">

---

## 技术栈

| 层级 | 技术 | 版本 |
|------|------|------|
| 流处理 | Apache Flink | 2.2.1 |
| CEP | flink-cep | 2.2.1 |
| Kafka 连接器 | flink-connector-kafka | 5.0.0-2.2 |
| 规则引擎 | Aviator | 5.4.3 |
| 消息队列 | Kafka（KRaft 模式，cp-kafka） | 8.1.4 镜像 |
| 缓存 / 客户端 | Redis / Jedis | 8.8 / 5.2.0 |
| 数据库 | MySQL | 8.0 |
| Web 后端 | Spring Boot | 3.2.0（Java 17） |
| ORM | MyBatis-Plus | 3.5.5 |
| 前端 | React + Ant Design + ProComponents | 18.3 / 5.22 / 2.8 |
| 构建 | Maven + Vite | 3.8+ / 5.4 |
| JSON | Jackson / FastJSON2 | 2.17.2 / 2.0.43 |

---

## 快速启动

> 脚本为 Bash 脚本，Windows 建议用 Git Bash / WSL 执行。Java 版本要求：`common`/`job` 编译到 Java 8，`web` 需要 Java 17。

### 1. 启动 Docker 依赖

```bash
cd docker
docker compose up -d
```

组件与端口：

| 服务 | 端口 | 说明 |
|------|------|------|
| MySQL | 3306 | 用户 `root` / 密码 `root123`，数据库 `flink_risk` |
| Redis | 6379 | 密码 `redis123` |
| Kafka | 9092（宿主机）/ 29092（容器内） | KRaft 模式 |
| Kafka UI | 9090 | http://localhost:9090 |
| Flink UI | 8081 | JobManager |
| Adminer | 8082 | http://localhost:8082 |
| risk-web | 8080 | 由 compose 用 Maven 启动（见下方说明） |

**初始化数据库**：`docker/docker-compose.yml` 里 `./sql/init.sql` 是相对 `docker/` 目录解析的，而 SQL 在仓库根目录，挂载不会生效。请手工导入：

```bash
# 仓库根目录执行
mysql -h 127.0.0.1 -u root -proot123 flink_risk < sql/init.sql
```

### 2. 启动 Web 管理后端

```bash
mvn -q -DskipTests install          # 先把 common 装进本地仓库
cd flink-risk-web
mvn spring-boot:run                 # http://localhost:8080
```

默认数据源是 `jdbc:mysql://localhost:3306/risk_control`，**与 compose 创建的 `flink_risk` 不一致**；且 URL 里的库名是硬编码的，环境变量只能覆盖 host/port/user/password（`MYSQL_HOST` `MYSQL_PORT` `MYSQL_USER` `MYSQL_PASSWORD`），**改不了库名**。二选一：

```bash
# 方式 A：不改代码，建一个同名库并导表
mysql -h 127.0.0.1 -u root -proot123 -e "CREATE DATABASE IF NOT EXISTS risk_control"
mysql -h 127.0.0.1 -u root -proot123 risk_control < sql/init.sql

# 方式 B：把 application.yml 里的 risk_control 改成 flink_risk（推荐，与 job 统一）
```

另外注意默认密码是 `root123123`，而 compose 里是 `root123`，启动时需要 `MYSQL_PASSWORD=root123`。

### 3. 启动前端

```bash
cd frontend
npm install
npm run dev          # http://localhost:3000，/api 代理到 :8080
```

### 4. 启动 Flink Job

推荐用 IDEA 直接运行 `com.qinyadan.risk.RiskControlApplication`（`provided` 依赖在 IDE 的模块 classpath 里可用）。

命令行方式需要显式带上 `provided` 依赖：

```bash
cd flink-risk-job
mvn exec:java -Dexec.mainClass=com.qinyadan.risk.RiskControlApplication -Dexec.classpathScope=compile
```

完整参数写法（参数以 `--key value` 成对传给 `main`）：

```bash
mvn exec:java \
  -Dexec.mainClass=com.qinyadan.risk.RiskControlApplication \
  -Dexec.classpathScope=compile \
  -Dexec.args="--kafka.bootstrap.servers localhost:9092 \
               --mysql.url jdbc:mysql://localhost:3306/flink_risk?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true \
               --mysql.username root --mysql.password root123 \
               --redis.host localhost --redis.port 6379 --redis.db 0 \
               --rule.poll.interval 30000"
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--kafka.bootstrap.servers` | `localhost:9092` | 容器内为 `kafka:29092` |
| `--mysql.url` | `...localhost:3306/flink_risk...` | 库名以 URL 为准 |
| `--mysql.username` / `--mysql.password` | `root` / `root123123` | compose 的密码是 `root123` |
| `--redis.host` / `--redis.port` / `--redis.db` | `localhost` / `6379` / `0` | 无 `--redis.password` 参数 |
| `--rule.poll.interval` | `30000` | 规则轮询间隔（毫秒） |

> 注意：`flink-risk-job/src/main/resources/application.properties` **不会被代码读取**，所有配置以命令行参数（`main` 里的 `getArg` 默认值）为准。

### 5. 造数验证

```bash
cd scripts
./event-simulator.sh login_failure user_001     # 3 条失败登录
./event-simulator.sh order_frequent user_002    # 3 笔订单
./event-simulator.sh activity_frequent user_003 # 4 次参与
./event-simulator.sh coupon_repeat user_004     # 3 次领券
./event-simulator.sh all user_001               # 全部场景
./event-simulator.sh stress                     # 压测
```

判据：Job 控制台依次出现 `[SOURCE]` → `[CEP] Pattern matched` → `[EVAL]` → `[RULE] RiskResult` → `[SINK-*]`，随后 `risk_result` 表出现记录。

---

## CEP 风控场景

| 场景 | Pattern 条件 | 窗口 | patternType | 规则侧变量 |
|------|-------------|------|-------------|-----------|
| 登录失败风暴 | 连续 3 次 `success=false` | 60s | `LOGIN_FAILURE` | `failCount>=3` |
| 异地登录 | 连续 2 次 `success=true`（**不比较 IP**） | 60s | `LOCATION_CHANGE` | `prevIp` / `currIp` |
| 频繁下单 | 连续 3 笔 `amount>0` | 60s | `ORDER_FREQUENT` | `orderCount>=3`、`totalAmount` |
| 快速下单 | 连续 2 笔 `amount>100` | 30s | `ORDER_RAPID` | `intervalMs` |
| 频繁参与活动 | 连续 4 次 `actionType=PARTICIPATE` | 60s | `ACTIVITY_FREQUENT` | `participationCount>=4` |
| 重复领券 | 连续 3 次 `actionType=CLAIM_COUPON` | 60s | `COUPON_REPEAT` | `participationCount>=3` |

匹配语义：全部使用 `followedBy`（宽松连续，中间允许无关事件），不是严格连续；`within` 依赖 Watermark 推进，因此 `timestamp` 必须是**毫秒**且事件时间不能落后太多。

---

## 规则引擎

规则存于 MySQL `rule_config`，Job 每 30 秒轮询一次（`--rule.poll.interval`），通过 `BroadcastStream` 下发到每个并行实例。规则更新机制：

1. `RuleSourceFunction` 查 `enabled=1` 的规则，发一份 `List<RuleConfig>` 全量快照；
2. 广播侧**全量覆盖** `BroadcastState`；
3. 每个 subtask 用 `new AviatorRuleEngine(...) + registerRules(...)` 重建引擎，并以 `volatile` 写替换引用；
4. 注册期试编译，语法错误的单条规则被跳过并记 error 日志。

### 表达式可用变量

| 变量 | 类型 | 来源 / 说明 |
|------|------|-------------|
| `eventType` | String | `LOGIN` / `ORDER` / `ACTIVITY` |
| `patternType` | String | 上表 6 种模式之一 |
| `userId` | String | 用户 ID |
| `failCount` | int | 登录场景 = 命中事件数（`LOCATION_CHANGE` 下为 1） |
| `ip` / `deviceId` / `success` / `location` / `browser` | String / boolean | 登录场景最后一笔事件 |
| `prevIp` / `currIp` | String | 仅 `LOCATION_CHANGE` |
| `orderCount` | int | 下单场景 = 命中事件数（`ORDER_RAPID` 下为 **1**） |
| `amount` | double | 最后一笔订单金额 |
| `totalAmount` | double | `ORDER_FREQUENT` 的订单金额合计 |
| `intervalMs` | double | `ORDER_RAPID` 两笔订单时间间隔 |
| `productId` / `paymentMethod` | String | 下单场景 |
| `participationCount` | int | 活动场景 = 命中事件数（**不是**事件里的同名字段） |
| `activityId` / `activityName` / `actionType` / `channel` / `source` | String | 活动场景 |

表达式为 AviatorScript：支持 `== != > >= < <= && ||`，正则必须用字面量 `=~ /pattern/`（写成 `=~ "pattern"` 会在运行期抛异常，规则静默失效），取子串可用 `string.contains(s, "x")`。

### 决策逻辑

- 多条规则命中时取"风险最高"的一条：`RiskLevel.score` 降序（LOW 10 / MEDIUM 50 / HIGH 80 / CRITICAL 100），同分比 `weight`；
- 最终 `riskScore` 直接取该等级的固定分，不做加权平均；
- 未命中任何规则，或规则尚未加载时，输出 `LOW` / `score=0` / `ALLOW`（失败开放）。

预置 15 条规则，覆盖登录（4）、下单（5）、活动（4）、通用（2），见 `sql/init.sql`。

---

## REST API

### 规则管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/rules?page=1&pageSize=10&name=&riskLevel=&action=&enabled=` | 分页查询（`name` 模糊） |
| GET | `/api/rules/enabled` | 全部启用规则（数组） |
| GET | `/api/rules/{id}` | 查询单条 |
| POST | `/api/rules` | 创建规则 |
| PUT | `/api/rules/{id}` | 更新规则（只更新非 null 字段） |
| DELETE | `/api/rules/{id}` | 删除规则 |
| PUT | `/api/rules/{id}/toggle` | 启用/停用切换 |
| PUT | `/api/rules/batch/enable` | 批量启用，请求体为 `[1,2,3]` |
| PUT | `/api/rules/batch/disable` | 批量停用，请求体为 `[1,2,3]` |

### 风险记录（只读）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/risks?page=1&pageSize=10&userId=&eventType=&riskLevel=&action=` | 分页查询 |
| GET | `/api/risks/{id}` | 查询单条 |

### 统计

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/stats/overview` | 规则数、今日风险数、等级/类型分布 |
| GET | `/api/stats/trend` | 近 7 天风险趋势 |
| GET | `/api/stats/top-rules` | 今日规则命中 Top 10 |

分页统一返回 `{data, total, page, pageSize}`。注意：旧文档里的 `/api/risk-results` 不存在，实际路径是 `/api/risks`。

---

## 数据库表

### rule_config（规则配置）

| 字段 | 类型 | 说明 |
|------|------|------|
| id | BIGINT PK | 自增主键 |
| name | VARCHAR(100) | 规则名称 |
| expression | TEXT | Aviator 表达式（NOT NULL） |
| risk_level | VARCHAR(20) | `LOW` / `MEDIUM` / `HIGH` / `CRITICAL`（**必须大写**） |
| action | VARCHAR(20) | `ALLOW` / `VERIFY` / `REVIEW` / `WARN` / `BLOCK` 等 |
| weight | INT | 权重，默认 1 |
| enabled | TINYINT(1) | 是否启用，Job 只加载 `enabled=1` |
| description | VARCHAR(500) | 描述 |
| create_time | DATETIME | 创建时间 |
| update_time | DATETIME | 更新时间（自动） |

### risk_result（风险结果）

| 字段 | 类型 | 说明 |
|------|------|------|
| id | BIGINT PK | 自增主键 |
| user_id | VARCHAR(50) | 用户 ID |
| event_id | VARCHAR(50) | CEP 命中时生成的 UUID |
| event_type | VARCHAR(20) | `LOGIN` / `ORDER` / `ACTIVITY` |
| risk_level | VARCHAR(20) | 风险等级 |
| risk_score | INT | 10 / 50 / 80 / 100 |
| action | VARCHAR(20) | 处置动作 |
| details | JSON | 命中规则信息 + CEP 上下文（`matchedRuleName`、`failCount` 等） |
| create_time | DATETIME | 写入时间 |

索引：`idx_user_id`、`idx_event_type`、`idx_create_time`。

---

## Kafka Topic 格式

### login-events

```json
{
  "userId": "user_001",
  "eventId": "login_xxx",
  "eventType": "LOGIN",
  "timestamp": 1710000000000,
  "ip": "192.168.1.1",
  "deviceId": "dev_001",
  "success": false,
  "failureReason": "密码错误",
  "location": "北京",
  "browser": "Chrome"
}
```

### order-events

```json
{
  "userId": "user_001",
  "eventId": "order_xxx",
  "eventType": "ORDER",
  "timestamp": 1710000000000,
  "orderId": "ORD001",
  "amount": 50000.00,
  "productId": "SKU001",
  "productName": "异常商品",
  "quantity": 30,
  "paymentMethod": "可疑支付",
  "deliveryAddress": "异常地址",
  "processTime": 0
}
```

### activity-events

```json
{
  "userId": "user_001",
  "eventId": "act_xxx",
  "eventType": "ACTIVITY",
  "timestamp": 1710000000000,
  "activityId": "ACT001",
  "activityName": "双11大促",
  "couponCode": "CPN001",
  "couponValue": 50.00,
  "actionType": "PARTICIPATE",
  "participationCount": 5,
  "channel": "app",
  "source": "push"
}
```

反序列化使用裸 `ObjectMapper`：**JSON 里多出未定义字段会导致 `UnrecognizedPropertyException`**（默认严格模式），生产端加字段需同步改 POJO 或关闭严格模式。`timestamp` 必须是毫秒。

---

## 输出与存储

| 目标 | 形态 | 语义 |
|------|------|------|
| Redis | key `risk:{eventType}:{userId}:{eventId}`，value 为 JSON，TTL 3600s | 同步写，异常仅记日志（不影响作业） |
| MySQL | `INSERT INTO risk_result (...)`，异步 `orderedWait`（60s 超时 / 100 并发） | 写失败会 `completeExceptionally`，可能触发作业重启；无唯一键，重放可能重复 |
| Kafka | topic `risk-results`，key = `userId`（同用户同分区） | `acks=all` + 幂等生产者 + lz4 |

注意：当前 Redis 写入的 JSON 中 `riskLevel` 是枚举的 `toString()` 结果（形如 `RiskLevel{score=80, ...}`），且不含 `details`；Kafka 侧是 Jackson 全字段序列化（`"riskLevel":"HIGH"`）。两条路径格式不一致，下游需分别适配。

---

## 脚本工具

### docker-start.sh

```bash
cd scripts
./docker-start.sh start     # 启动依赖组件（另建网络、建 Topic、初始化库）
./docker-start.sh stop      # 停止并移除
./docker-start.sh restart
./docker-start.sh status
```

### event-simulator.sh

```bash
cd scripts
./event-simulator.sh login_failure user_001
./event-simulator.sh order_frequent user_002
./event-simulator.sh order_rapid user_002
./event-simulator.sh location_change user_003
./event-simulator.sh activity_frequent user_004
./event-simulator.sh coupon_repeat user_005
./event-simulator.sh all user_001
./event-simulator.sh batch 10
./event-simulator.sh stress
./event-simulator.sh cleanup
```

脚本依赖 `docker exec` 进入 `flink-risk-kafka` 容器，因此需先启动 Kafka 且容器名一致。

---

## 日志配置

### Flink Job（Log4j2 properties）

`flink-risk-job/src/main/resources/log4j2.properties`：

- 控制台 + `./logs/flink-risk-job.log`（相对启动目录）
- 按天 + 100MB 滚动，保留 30 个
- `com.qinyadan.risk` 为 DEBUG；`org.apache.kafka` / `org.apache.flink` 为 WARN

关键日志标记：`[SOURCE]` `[CEP]` `[CEP-MATCH]` `[EVAL]` `[RULE]` `[SINK-REDIS]` `[SINK-MYSQL]` `[SINK-KAFKA]`。另外 `RuleSourceFunction` 会打印 `Broadcast rules snapshot: N rules`，可作为"规则是否加载成功"的判据。

### Web 后端（Log4j2 XML）

`flink-risk-web/src/main/resources/log4j2-spring.xml`：

- `logs/app.log`（全量）、`logs/error.log`（ERROR）、`logs/sql.log`（MyBatis SQL）
- 每个文件按天 + 100MB 滚动，保留 30 个

---

## 学习文档

| 文档 | 内容 |
|------|------|
| [docs/LEARNING-GUIDE.md](docs/LEARNING-GUIDE.md) | 全局图景、数据契约、调试手段、15 条已知问题、精读路线 |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | 架构设计说明（模块、数据流、决策口径、存储、扩展） |
| [docs/DEPLOY.md](docs/DEPLOY.md) | 集群部署依赖与排查 |
| [docs/code-reading/01-common.md](docs/code-reading/01-common.md) | common 模块逐行精读（7 部分 24 讲） |
| [docs/code-reading/02-job.md](docs/code-reading/02-job.md) | job 模块逐行精读（7 层 22 讲） |
| [docs/code-reading/03-web.md](docs/code-reading/03-web.md) | web 模块逐行精读（7 部分 22 讲） |
| [CODEBUDDY.md](CODEBUDDY.md) | 仓库速览（命令、架构要点） |

---

## 已知限制

1. **数据库名不统一**：Job 默认 `flink_risk`，Web 默认 `risk_control`，`sql/init.sql` 只建表不建库。
2. **Redis 不支持密码**：`RedisClient` 构造 `JedisPool` 时未传 password，而 compose 里的 Redis 带 `requirepass redis123`，写入会 `NOAUTH`（仅日志可见）。
3. **Redis 值格式有缺陷**：`riskLevel` 被序列化成枚举 `toString()`，`details` 未写入。
4. **部分预置规则表达式非法**：`sql/init.sql` 中 `deviceId =~ "abnormal"`、`source =~ "脚本"` 等写法在 Aviator 里运行期抛异常，规则静默失效；应改为 `=~ /abnormal/` 或 `string.contains(...)`。
5. **`application.properties` 未生效**：所有配置以命令行参数为准，文件里的 `flink.state.backend=rocksdb` 等不会应用。
6. **生产化缺口**：算子未设置 `.uid()`、未配置状态后端与 checkpoint 目录、无显式重启策略；MySQL Sink 每条记录新建连接且无幂等键。
7. **`LOCATION_CHANGE` 名不副实**：只匹配"60 秒内两次成功登录"，不比较 IP，误报率高。
8. **`ruleType` 未落库**：Web 实体的 `ruleType` 是 `@TableField(exist=false)`，规则类型统计靠规则名猜测。

上述问题的定位与修复建议见 [docs/LEARNING-GUIDE.md 第 9 章](docs/LEARNING-GUIDE.md#9-已知问题与代码痛点) 及各模块精读文档。

---

## 许可证

本项目基于 [MIT License](LICENSE) 开源。

| 组件 | 许可证 |
|------|--------|
| [Apache Flink](https://flink.apache.org/) | Apache-2.0 |
| [Flink CEP](https://nightlies.apache.org/flink/flink-docs-stable/docs/libs/cep/) | Apache-2.0 |
| [Spring Boot](https://spring.io/projects/spring-boot) | Apache-2.0 |
| [React](https://react.dev/) | MIT |
| [Ant Design](https://ant.design/) | MIT |
| [Apache Kafka](https://kafka.apache.org/) | Apache-2.0 |
| [Redis](https://redis.io/) | BSD-3-Clause |
| [MySQL](https://www.mysql.com/) | GPL-2.0 |
| [Aviator](https://github.com/killme2008/aviatorscript) | LGPL-3.0 |
| [MyBatis Plus](https://baomidou.com/) | Apache-2.0 |
| [Jackson](https://github.com/FasterXML/jackson) | Apache-2.0 |
| [FastJSON2](https://github.com/alibaba/fastjson2) | Apache-2.0 |
| [JUnit 5](https://junit.org/junit5/) | EPL-2.0 |
| [Log4j2](https://logging.apache.org/log4j/2.x/) | Apache-2.0 |

---

## 关于作者

本项目由 [刘志敏](https://www.qinyadan.com/) 开发维护。

- **公众号**：关注「刘志敏」公众号，获取大数据、实时计算、风控系统等领域的技术文章与实战分享。
- **小程序**：搜索「刘志敏」小程序，随时随地浏览技术博客、项目案例与最新动态。

欢迎访问官网 [https://www.qinyadan.com/](https://www.qinyadan.com/) 了解更多内容。

> **开源不易，如果本项目对你有帮助，欢迎请我喝杯咖啡 ☕**
>
> <img src="docs/paycode.jpg" width="240" alt="微信支付">
