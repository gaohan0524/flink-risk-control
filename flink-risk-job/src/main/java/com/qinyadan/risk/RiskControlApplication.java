package com.qinyadan.risk;

import com.qinyadan.risk.cep.*;
import com.qinyadan.risk.event.ActivityEvent;
import com.qinyadan.risk.event.LoginEvent;
import com.qinyadan.risk.event.OrderEvent;
import com.qinyadan.risk.kafka.KafkaSourceFactory;
import com.qinyadan.risk.redis.RiskResult;
import com.qinyadan.risk.rule.RuleConfig;
import com.qinyadan.risk.sink.KafkaSinkFactory;
import com.qinyadan.risk.sink.MysqlSinkFunction;
import com.qinyadan.risk.sink.RedisSinkFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 风控主应用 - Flink 2.2.1
 *
 * 架构：Kafka 事件源 -> CEP 模式匹配 -> 广播规则评估 -> 三路输出（Redis + MySQL + Kafka）
 *
 * 启动命令:
 *   flink run -c com.qinyadan.risk.RiskControlApplication flink-risk-job-2.0.1-SNAPSHOT.jar \
 *     --kafka.bootstrap.servers kafka:9092 \
 *     --mysql.url jdbc:mysql://mysql:3306/risk_control?useSSL=false&serverTimezone=UTC \
 *     --mysql.username root --mysql.password root123 \
 *     --redis.host redis --redis.port 6379 --redis.db 0 \
 *     --rule.poll.interval 30000
 *
 * 流程:
 *   1. KafkaSource 消费 login-events / order-events / activity-events
 *   2. 规则广播流从 MySQL 定时拉取（parallelism=1）
 *   3. CEP 模式匹配产生 PatternMatchResult 中间结果
 *   4. RuleEvaluationFunction 连接广播规则，通过 AviatorRuleEngine 动态评估
 *   5. RiskResult 三路输出：
 *      - Redis：实时缓存（供实时查询）
 *      - MySQL：异步持久化（供历史分析、Web 后台查询）
 *      - Kafka：异步发送到 risk-results topic（供下游系统消费）
 */
public class RiskControlApplication {
    /**
     * 程序入口：编排并执行完整的风控 DAG
     *
     * 依次完成：解析参数 -> 创建执行环境 -> 构建规则广播流 -> 构建三路 Kafka 事件源
     * -> CEP 模式匹配 -> 广播规则评估 -> 三路输出（Redis/MySQL/Kafka）
     *
     * @param args 命令行参数（--kafka.bootstrap.servers / --mysql.url / --redis.host 等）
     */
    public static void main(String[] args) throws Exception {
        // ========== 1. 读取配置 ==========
        String kafkaBootstrapServers = getArg(args, "--kafka.bootstrap.servers", "localhost:9092");
        String mysqlUrl = getArg(args, "--mysql.url", "jdbc:mysql://localhost:3306/flink_risk?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true");
        String mysqlUsername = getArg(args, "--mysql.username", "root");
        String mysqlPassword = getArg(args, "--mysql.password", "root123123");
        String redisHost = getArg(args, "--redis.host", "localhost");
        int redisPort = Integer.parseInt(getArg(args, "--redis.port", "6379"));
        int redisDb = Integer.parseInt(getArg(args, "--redis.db", "0"));
        long rulePollInterval = Long.parseLong(getArg(args, "--rule.poll.interval", "30000"));

        // ========== 2. 创建 Flink 执行环境 ==========
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 全局默认并行度设为 4（各算子未单独指定时沿用此值）
        env.setParallelism(4);
        // 开启 Checkpoint：每 60s 做一次状态快照，用于故障恢复与 Exactly-Once 保证
        env.enableCheckpointing(60000);

        System.out.println("Flink Risk Control Job Started. Flink version: 2.2.1");
        System.out.println("Kafka: " + kafkaBootstrapServers);
        System.out.println("MySQL: " + mysqlUrl);
        System.out.println("Redis: " + redisHost + ":" + redisPort + "/" + redisDb);
        System.out.println("Rule poll interval: " + rulePollInterval + "ms");

        // ========== 3. 规则广播流（parallelism=1，避免重复查库） ==========
        SingleOutputStreamOperator<List<RuleConfig>> ruleStream = env
                .addSource(new RuleSourceFunction(mysqlUrl, mysqlUsername, mysqlPassword, rulePollInterval))
                .setParallelism(1)
                .name("mysql-rule-source");

        // 将规则流广播到所有下游并行实例，形成 Broadcast State（规则热更新的载体）
        BroadcastStream<List<RuleConfig>> ruleBroadcastStream = ruleStream
                .broadcast(RuleEvaluationFunction.RULE_STATE_DESCRIPTOR);

        // ========== 4. Kafka 事件源（新 Source API） ==========
        // 注意：必须指定 WatermarkStrategy，否则 CEP within() 时间窗口无法推进
        DataStream<LoginEvent> loginStream = env
                .fromSource(
                        KafkaSourceFactory.createLoginSource(kafkaBootstrapServers),
                        WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(java.time.Duration.ofSeconds(5))
                                .withTimestampAssigner((event, ts) -> event.getTimestamp()),
                        "kafka-login-source")
                .name("login-events")
                .setParallelism(2)
                .map(event -> {
                    System.out.println("[SOURCE] LoginEvent received: user=" + event.getUserId()
                            + ", success=" + event.isSuccess() + ", ip=" + event.getIp()
                            + ", timestamp=" + event.getTimestamp());
                    return event;
                });

        DataStream<OrderEvent> orderStream = env
                .fromSource(
                        KafkaSourceFactory.createOrderSource(kafkaBootstrapServers),
                        WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(java.time.Duration.ofSeconds(5))
                                .withTimestampAssigner((event, ts) -> event.getTimestamp()),
                        "kafka-order-source")
                .name("order-events")
                .setParallelism(2)
                .map(event -> {
                    System.out.println("[SOURCE] OrderEvent received: user=" + event.getUserId()
                            + ", amount=" + event.getAmount() + ", orderId=" + event.getOrderId()
                            + ", timestamp=" + event.getTimestamp());
                    return event;
                });

        DataStream<ActivityEvent> activityStream = env
                .fromSource(
                        KafkaSourceFactory.createActivitySource(kafkaBootstrapServers),
                        WatermarkStrategy.<ActivityEvent>forBoundedOutOfOrderness(java.time.Duration.ofSeconds(5))
                                .withTimestampAssigner((event, ts) -> event.getTimestamp()),
                        "kafka-activity-source")
                .name("activity-events")
                .setParallelism(2)
                .map(event -> {
                    System.out.println("[SOURCE] ActivityEvent received: user=" + event.getUserId()
                            + ", action=" + event.getActionType() + ", activity=" + event.getActivityId()
                            + ", timestamp=" + event.getTimestamp());
                    return event;
                });

        // ========== 5. CEP 模式匹配（按 userId 分组，确保同一用户的事件序列检测） ==========
        DataStream<PatternMatchResult> loginMatches = LoginCepJob.process(
                loginStream.keyBy((org.apache.flink.api.java.functions.KeySelector<LoginEvent, String>) event -> event.getUserId()));
        DataStream<PatternMatchResult> orderMatches = OrderCepJob.process(
                orderStream.keyBy((org.apache.flink.api.java.functions.KeySelector<OrderEvent, String>) event -> event.getUserId()));
        DataStream<PatternMatchResult> activityMatches = ActivityCepJob.process(
                activityStream.keyBy((org.apache.flink.api.java.functions.KeySelector<ActivityEvent, String>) event -> event.getUserId()));

        DataStream<PatternMatchResult> allMatches = loginMatches.union(orderMatches, activityMatches)
                .map(result -> {
                    System.out.println("[CEP] Pattern matched: user=" + result.getUserId()
                            + ", eventType=" + result.getEventType()
                            + ", pattern=" + result.getPatternType()
                            + ", context=" + result.getContext());
                    return result;
                });

        // ========== 6. 连接广播流，动态规则评估 ==========
        DataStream<RiskResult> riskResults = allMatches
                .connect(ruleBroadcastStream)
                .process(new RuleEvaluationFunction())
                .name("rule-evaluation")
                .map(result -> {
                    System.out.println("[RULE] RiskResult: user=" + result.getUserId()
                            + ", event=" + result.getEventType()
                            + ", level=" + result.getRiskLevel()
                            + ", action=" + result.getAction()
                            + ", score=" + result.getRiskScore()
                            + ", details=" + result.getDetails());
                    return result;
                });

        // ========== 7. 三路输出 ==========

        // 7a. Redis Sink（实时缓存，供实时查询）
        riskResults
                .map(result -> {
                    System.out.println("[SINK-REDIS] Writing to Redis: user=" + result.getUserId()
                            + ", key=" + result.getRedisKey());
                    return result;
                })
                .sinkTo(new RedisSinkFunction(redisHost, redisPort, redisDb))
                .name("redis-sink");

        // 7b. MySQL Sink（异步持久化，供历史分析和 Web 后台查询）
        // 异步写入 MySQL：orderedWait 保持输入顺序，超时 60s，最多并发 100 条
        // 通过异步 IO 隔离慢数据库，避免阻塞主数据流
        AsyncDataStream.orderedWait(
                riskResults.map(result -> {
                    System.out.println("[SINK-MYSQL] Writing to MySQL: user=" + result.getUserId()
                            + ", eventId=" + result.getEventId());
                    return result;
                }),
                new MysqlSinkFunction(mysqlUrl, mysqlUsername, mysqlPassword),
                60, TimeUnit.SECONDS,
                100)
                .name("mysql-async-sink");

        // 7c. Kafka Sink（异步发送到 risk-results topic，供下游系统消费）
        riskResults
                .map(result -> {
                    System.out.println("[SINK-KAFKA] Writing to Kafka: user=" + result.getUserId()
                            + ", level=" + result.getRiskLevel() + ", action=" + result.getAction());
                    return result;
                })
                .sinkTo(KafkaSinkFactory.createRiskResultSink(kafkaBootstrapServers))
                .name("kafka-risk-result-sink");

        env.execute("flink-risk-control-job");
    }

    /**
     * 从命令行参数中解析指定 key 对应的值（约定 --key value 成对出现）
     *
     * @param args         命令行参数数组
     * @param key          参数名（含 -- 前缀，如 --kafka.bootstrap.servers）
     * @param defaultValue 未找到时使用的默认值
     * @return 解析到的参数值；未找到则返回默认值
     */
    private static String getArg(String[] args, String key, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(key)) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }
}
