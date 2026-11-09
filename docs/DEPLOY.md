# Flink 风控 Job 部署说明

> 适用版本：`2.0.1-SNAPSHOT`（Flink 2.2.1）
> 目标：把 `flink-risk-job` 提交到一个可运行的 Flink 集群，并让它正确连上 Kafka / MySQL / Redis。
>
> 相关文档：[README](../README.md) · [架构说明](ARCHITECTURE.md) · [学习总览](LEARNING-GUIDE.md) · [job 精读](code-reading/02-job.md)

---

## 目录

1. [部署前必须知道的四件事](#1-部署前必须知道的四件事)
2. [依赖分两类](#2-依赖分两类)
3. [集群 lib/ 需要补哪些 JAR](#3-集群-lib-需要补哪些-jar)
4. [fat-jar 内置了什么](#4-fat-jar-内置了什么)
5. [完整部署步骤](#5-完整部署步骤)
6. [提交参数与运行配置](#6-提交参数与运行配置)
7. [生产化必改项](#7-生产化必改项)
8. [常见问题](#8-常见问题)
9. [部署检查清单](#9-部署检查清单)

---

## 1. 部署前必须知道的四件事

| # | 事实 | 影响 |
|---|------|------|
| 1 | `flink-risk-job` 的 Flink 依赖全部是 `provided` | 集群 `lib/` 必须补齐 CEP 与 Kafka 连接器，否则 `ClassNotFoundException` |
| 2 | 代码编译级别是 **Java 8**（父 POM `maven.compiler.target=8`） | 集群镜像需 Java 8+；本仓库 compose 用的是 `java8` 镜像 |
| 3 | 所有配置都由 `main` 里的 `getArg` 解析命令行参数 | `resources/application.properties` **不会生效**，不要在那里改配置 |
| 4 | 作业没有设置 `.uid()`、状态后端、checkpoint 目录 | 能从 checkpoint 重启，但**难以从 savepoint 恢复**；生产必须补配置 |

---

## 2. 依赖分两类

| 分类 | 说明 | 来源 |
|------|------|------|
| 集群级 JAR | Flink 框架与连接器，所有作业共享 | 放入 `$FLINK_HOME/lib` |
| 作业级 JAR | 业务代码 + 第三方库，随作业提交 | `mvn package` 打进 fat-jar |

判断方法：看 `flink-risk-job/pom.xml` 的 `scope`。

```xml
<!-- provided → 集群提供，不打进 fat-jar -->
flink-core / flink-streaming-java / flink-cep
flink-connector-kafka / flink-connector-base / flink-runtime-web

<!-- 默认 compile → 打进 fat-jar -->
jackson-databind / log4j2 / slf4j / mysql-connector-j
```

---

## 3. 集群 lib/ 需要补哪些 JAR

Flink 官方镜像的 `lib/` 已包含 `flink-core`、`flink-streaming-java`、`flink-runtime`、`flink-connector-base`、`flink-runtime-web` 等。
**不在镜像 lib/ 中**、必须补的有三个：

| JAR | 说明 |
|-----|------|
| `flink-cep-*.jar` | CEP 库。官方镜像通常只放在 `opt/`，需复制到 `lib/` |
| `flink-connector-kafka-5.0.0-2.2.jar` | Kafka Source/Sink 连接器（Flink 2.x 新 API） |
| `kafka-clients-*.jar` | 连接器的传递依赖（与连接器版本匹配） |

### 3.1 先确认镜像里实际的 JAR 名字

Flink 2.x 与 1.x 的构件命名不同（1.x 带 Scala 后缀，如 `flink-cep_2.12-1.18.1.jar`），**不要直接照抄旧文档的文件名**：

```bash
# 看 opt/ 里有什么（CEP 通常在这里）
docker exec flink-risk-jobmanager ls /opt/flink/opt | grep -i cep

# 看 lib/ 里已有什么
docker exec flink-risk-jobmanager ls /opt/flink/lib
docker exec flink-risk-taskmanager ls /opt/flink/lib

# 确认连接器本地仓库里的实际版本
find ~/.m2/repository/org/apache/flink/flink-connector-kafka -name "*.jar"
find ~/.m2/repository/org/apache/kafka/kafka-clients -name "*.jar"
```

### 3.2 复制 CEP

```bash
# 用上面 ls 查到的真实文件名替换 <CEP_JAR>
docker exec flink-risk-jobmanager cp /opt/flink/opt/<CEP_JAR> /opt/flink/lib/
docker exec flink-risk-taskmanager cp /opt/flink/opt/<CEP_JAR> /opt/flink/lib/
```

### 3.3 获取 Kafka 连接器与客户端

方式一（推荐）：直接从本地 Maven 仓库取（构建过一次就有）：

```bash
mvn -q -pl flink-risk-job -am dependency:go-offline

CONNECTOR=$(find ~/.m2/repository/org/apache/flink/flink-connector-kafka/5.0.0-2.2 -name "*.jar" | head -1)
CLIENTS=$(find ~/.m2/repository/org/apache/kafka/kafka-clients -name "*.jar" | sort | tail -1)

docker cp "$CONNECTOR" flink-risk-jobmanager:/opt/flink/lib/
docker cp "$CONNECTOR" flink-risk-taskmanager:/opt/flink/lib/
docker cp "$CLIENTS"   flink-risk-jobmanager:/opt/flink/lib/
docker cp "$CLIENTS"   flink-risk-taskmanager:/opt/flink/lib/
```

方式二：从 Maven 中央仓库下载（版本号必须与 `pom.xml` 一致）。

### 3.4 lib/ 目标清单（示例）

```
/opt/flink/lib/
├── flink-cep-2.2.1.jar                     # 新增（来自 opt/，实际文件名以 ls 为准）
├── flink-connector-kafka-5.0.0-2.2.jar     # 新增
├── kafka-clients-x.y.z.jar                 # 新增（与连接器匹配）
├── flink-core-2.2.1.jar                    # 镜像内置
├── flink-streaming-java-2.2.1.jar          # 镜像内置
├── flink-runtime-2.2.1.jar                 # 镜像内置
├── flink-connector-base-2.2.1.jar          # 镜像内置
├── flink-runtime-web-2.2.1.jar             # 镜像内置
└── log4j-*.jar                             # 镜像内置
```

> **JobManager 与 TaskManager 的 `lib/` 都必须放**，只放一边会出现"提交成功但运行期类找不到"。
> 修改 `lib/` 后必须**重启**两个组件才能生效。

---

## 4. fat-jar 内置了什么

`flink-risk-job` 使用 `maven-shade-plugin` 打包，`mainClass` 已写入 Manifest，第三方依赖（`common` 模块、Aviator、Jedis、FastJSON2、Gson、Jackson、SnakeYAML、Guava、Commons-Lang3、Log4j2、MySQL 驱动）都在 jar 内。

验证打包结果：

```bash
# 确认主类
unzip -p flink-risk-job/target/flink-risk-job-2.0.1-SNAPSHOT.jar META-INF/MANIFEST.MF

# 确认关键类在包里
unzip -l flink-risk-job/target/flink-risk-job-2.0.1-SNAPSHOT.jar | grep -E "AviatorRuleEngine|RiskControlApplication|MysqlSinkFunction"

# 确认 Flink 核心没被误打进去（provided 生效）
unzip -l flink-risk-job/target/flink-risk-job-2.0.1-SNAPSHOT.jar | grep "org/apache/flink/streaming/api/environment/StreamExecutionEnvironment.class" || echo "OK: flink 未打进 fat-jar"
```

**不要**把 Flink 核心 JAR 打进 fat-jar（会与集群版本冲突，典型症状是 `LinkageError` / `NoSuchMethodError`）。

---

## 5. 完整部署步骤

### 5.1 编译打包

```bash
cd flink-risk-control

# 只打 job 及其依赖（common），跳过测试
mvn clean package -pl flink-risk-job -am -DskipTests

# 产物
ls flink-risk-job/target/flink-risk-job-2.0.1-SNAPSHOT.jar
```

### 5.2 修正 compose 的挂载路径（重要）

`docker/docker-compose.yml` 中的挂载路径是相对 **compose 文件所在目录**（`docker/`）解析的，而实际资源在仓库根目录，因此以下三处当前不会生效：

| 现状 | 实际解析到 | 应改为 |
|------|-----------|--------|
| `./sql/init.sql:/docker-entrypoint-initdb.d/init.sql` | `docker/sql/init.sql` | `../sql/init.sql` |
| `./flink-risk-job/target:/opt/flink/usrlib` | `docker/flink-risk-job/target` | `../flink-risk-job/target` |
| `./flink-risk-web:/app`（risk-web 服务） | `docker/flink-risk-web` | `../flink-risk-web` |

未修正时的表现：

- MySQL 容器启动后**没有建表**（init.sql 未挂载）；
- Flink UI 的 "Submit New Job" 里看不到作业 jar；
- `risk-web` 容器启动失败（`/app` 为空，`mvn spring-boot:run` 找不到 pom）。

调试期可以先用仓库根目录执行：

```bash
docker compose -f docker/docker-compose.yml up -d
```

但挂载路径问题仍需按上表修正，或改用下面的手工方式。

### 5.3 启动依赖组件

```bash
cd docker
docker compose up -d mysql redis kafka kafka-ui adminer
```

若 `init.sql` 未生效，手工导入：

```bash
cd ..   # 回到仓库根目录
mysql -h 127.0.0.1 -u root -proot123 flink_risk < sql/init.sql
```

验证：

```bash
mysql -h 127.0.0.1 -u root -proot123 -e "SELECT COUNT(*) FROM flink_risk.rule_config"
# 期望 15
```

### 5.4 启动 Flink 集群

```bash
cd docker
docker compose up -d flink-jobmanager flink-taskmanager
docker ps | grep flink-risk
```

访问 http://localhost:8081 确认 TaskManager 已注册、有 4 个 slot。

### 5.5 放入作业 jar

```bash
# 方式一：复制到容器的 usrlib（与 compose 的挂载点一致）
docker cp flink-risk-job/target/flink-risk-job-2.0.1-SNAPSHOT.jar \
  flink-risk-jobmanager:/opt/flink/usrlib/

# 方式二：通过 Web UI 上传（Submit New Job）
```

### 5.6 提交作业

```bash
docker exec flink-risk-jobmanager flink run -d \
  -c com.qinyadan.risk.RiskControlApplication \
  /opt/flink/usrlib/flink-risk-job-2.0.1-SNAPSHOT.jar \
  --kafka.bootstrap.servers kafka:29092 \
  --mysql.url "jdbc:mysql://mysql:3306/flink_risk?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true" \
  --mysql.username root \
  --mysql.password root123 \
  --redis.host redis \
  --redis.port 6379 \
  --redis.db 0 \
  --rule.poll.interval 30000
```

> `-d` 表示 detached。不带 `-d` 可在前台看到提交日志，便于首次排错。

### 5.7 启动管理后端

compose 的 `risk-web` 服务会用 Maven 镜像跑 `mvn spring-boot:run`，但存在两个前提：挂载路径正确（见 5.2）、MySQL 已就绪。本地开发更推荐：

```bash
mvn -q -DskipTests install
cd flink-risk-web
mvn spring-boot:run
```

容器内启动时注意库名：`application.yml` 默认 `risk_control`，而 compose 创建的是 `flink_risk`（详见 [第 8 章 Q2](#q2-table-flink_riskrule_config-doesnt-exist--unknown-database)）。

---

## 6. 提交参数与运行配置

### 6.1 全部可用参数

| 参数 | 默认值 | 容器内建议值 |
|------|--------|-------------|
| `--kafka.bootstrap.servers` | `localhost:9092` | `kafka:29092` |
| `--mysql.url` | `jdbc:mysql://localhost:3306/flink_risk?...` | `jdbc:mysql://mysql:3306/flink_risk?...` |
| `--mysql.username` | `root` | `root` |
| `--mysql.password` | `root123123` | `root123` |
| `--redis.host` | `localhost` | `redis` |
| `--redis.port` | `6379` | `6379` |
| `--redis.db` | `0` | `0` |
| `--rule.poll.interval` | `30000` | `30000` |

参数解析约定：`--key value` 成对出现；未匹配到则用默认值；**最后一个参数必须有值**（`getArg` 循环到 `length-1`）。

**没有 `--redis.password`**：`RedisClient` 不支持密码。若目标 Redis 开启了 `requirepass`，Redis Sink 会持续打印 `NOAUTH` 且写入全部失败。三种处理方式：

1. 部署一个无密码的 Redis（仅限内网/测试）；
2. 修改 `RedisClient`，给 `JedisPool` 增加 password 参数（并给入口加 `--redis.password`）；
3. 暂时接受 Redis 一路失效（MySQL/Kafka 仍正常）。

### 6.2 环境变量

`application.properties` 不生效；`main` 也不读环境变量。需要改配置只能通过命令行参数。

### 6.3 并行度与资源

| 配置 | 代码中的值 | 位置 |
|------|-----------|------|
| 全局并行度 | 4 | `env.setParallelism(4)` |
| Kafka Source | 2 | `.setParallelism(2)` |
| 规则源 | 1 | `.setParallelism(1)`（**不可改大**） |
| Checkpoint 间隔 | 60s | `env.enableCheckpointing(60000)` |

TaskManager 的 slot 数需 ≥ 4（compose 已配 `taskmanager.numberOfTaskSlots: 4`）。

---

## 7. 生产化必改项

按优先级排列，全部是"代码里没有、部署时必须补"的能力：

| # | 缺口 | 后果 | 建议 |
|---|------|------|------|
| 1 | 无 `.uid()` | 改 DAG 后无法从 savepoint 恢复 | 给每个算子加稳定 uid |
| 2 | 无状态后端 / checkpoint 目录 | 默认存 JobManager 内存，重启即丢；状态大时 checkpoint 失败 | `EmbeddedRocksDBStateBackend` + HDFS/S3 |
| 3 | 无重启策略 | 默认固定延迟、无限次重启，故障时可能风暴 | `RestartStrategies.fixedDelayRestart(n, delay)` |
| 4 | MySQL Sink 每条新建连接 | 高 QPS 打满 `max_connections` | 改连接池（HikariCP），或在算子 `open()` 中维护连接 |
| 5 | MySQL 写失败 `completeExceptionally` | 单条脏数据触发作业重启 | 改 `complete(emptyList())` + 告警，或加死信队列 |
| 6 | `risk_result` 无唯一键 | checkpoint 重放导致重复插入 | 加 `(event_id, rule_id)` 唯一索引或改 upsert |
| 7 | Redis 不支持密码 | 无法对接受保护的 Redis | 扩展 `RedisClient` |
| 8 | 无监控指标 | 只能看日志 | 接入 Flink metrics + Prometheus |

---

## 8. 常见问题

### Q1: `ClassNotFoundException: org.apache.flink.cep.pattern.Pattern`

**原因**：`flink-cep` 未放入 `lib/`，或只放了一边（JM/TM）。

**解决**：按 3.2 复制到两个容器的 `lib/`，然后 `docker restart flink-risk-jobmanager flink-risk-taskmanager`。

### Q2: `Table 'flink_risk.rule_config' doesn't exist` / `Unknown database`

**原因**：库名或初始化脚本问题。

- compose 创建库 `flink_risk`，但 `init.sql` 未挂载成功 → 只有空库没有表；
- web 默认连 `risk_control` → 库不存在。

**解决**：

```bash
mysql -h 127.0.0.1 -u root -proot123 -e "SHOW DATABASES"
mysql -h 127.0.0.1 -u root -proot123 flink_risk < sql/init.sql
mysql -h 127.0.0.1 -u root -proot123 -e "SELECT COUNT(*) FROM flink_risk.rule_config"
```

Job 侧连库名以 `--mysql.url` 为准；web 侧要改 `application.yml` 或建同名库。

### Q3: `ClassNotFoundException: org.apache.flink.connector.kafka.source.KafkaSource`

**原因**：`flink-connector-kafka` 未放入 `lib/`。

**解决**：按 3.3 复制连接器，确认版本是 `5.0.0-2.2`。

### Q4: `NoClassDefFoundError: org/apache/kafka/clients/consumer/KafkaConsumer`

**原因**：`kafka-clients` 缺失（连接器的传递依赖，`provided` 不会进 fat-jar）。

**解决**：把与连接器匹配的 `kafka-clients` 放入两个容器的 `lib/`。

### Q5: 修改 `lib/` 后作业仍报同样的类找不到

**原因**：Flink 在进程启动时加载 `lib/`。

**解决**：重启 JobManager 与 TaskManager，再重新提交作业（旧作业的 classloader 不会更新）。

### Q6: 作业起来了，但 Kafka 收不到数据 / `[CEP]` 永不出现

排查顺序：

1. `--kafka.bootstrap.servers` 是否用了容器内地址（`kafka:29092`，不是 `localhost:9092`）；
2. topic 是否存在、是否有数据（http://localhost:9090）；
3. 日志里是否有 `[SOURCE]`（没有则消费侧问题）；
4. 有 `[SOURCE]` 无 `[CEP]`：检查事件 `timestamp` 是否为**毫秒**、`within` 是否过短、Watermark 是否推进。

### Q7: Redis 写入失败，日志 `NOAUTH Authentication required`

**原因**：Redis 有密码但客户端不支持（见 6.1）。

**解决**：用无密码 Redis，或修改 `RedisClient`/`RedisSinkFunction` 支持密码。

### Q8: 容器重启后 `lib/` 里的 JAR 消失

**原因**：容器可写层是临时的。

**解决**（三选一）：

1. 构建自定义镜像（推荐生产）；
2. 用 volume 挂载 `extra-lib`，并在启动命令里 `cp` 到 `lib/`：

```yaml
flink-jobmanager:
  image: apache/flink:2.2.1-scala_2.12-java8
  volumes:
    - ../flink-extra-lib:/opt/flink/extra-lib
  command: >
    bash -c "cp /opt/flink/extra-lib/*.jar /opt/flink/lib/ 2>/dev/null || true; jobmanager"
```

3. 每次重启后重新 `docker cp`。

### Q9: 作业频繁重启

按日志定位：

| 日志 | 原因 |
|------|------|
| `Failed to write risk result to MySQL` + `completeExceptionally` | MySQL 连接/字段问题，触发 failover |
| `Expression runtime error (check =~ syntax...)` | 规则表达式非法（`=~ "str"`），但**不会**导致重启，只会规则失效 |
| `Communications link failure` | MySQL 不可达或连接数耗尽 |
| checkpoint 超时/失败 | 状态过大或存储不可用 |

### Q10: 想从 savepoint 恢复但报状态不匹配

**原因**：算子没有 `.uid()`，拓扑一变 ID 就变。

**解决**：先在生产版本补 uid（一次性的破坏性变更），之后才能稳定使用 savepoint。

---

## 9. 部署检查清单

```text
□ mvn clean package -pl flink-risk-job -am -DskipTests 成功
□ fat-jar 的 MANIFEST 里 Main-Class = com.qinyadan.risk.RiskControlApplication
□ fat-jar 内不含 Flink 核心类
□ JobManager/TaskManager 的 lib/ 都含 cep + connector-kafka + kafka-clients
□ 重启过 JM/TM
□ MySQL 可连，flink_risk.rule_config 有 15 条、enabled=1 的规则
□ Kafka 三个 topic 存在且有数据
□ Redis 可连（若带密码，确认已处理 NOAUTH）
□ TaskManager slot ≥ 4
□ 提交命令里的地址全部是容器内地址（kafka:29092 / mysql:3306 / redis:6379）
□ Flink UI 里作业状态 RUNNING，checkpoint 有成功记录
□ 日志出现 Broadcast rules snapshot: 15 rules
□ 造数后 risk_result 表有新记录
```

---

*文档版本：v3.0 ｜ 更新日期：2026-11-09 ｜ 对应代码：2.0.1-SNAPSHOT*
