package com.qinyadan.risk.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.qinyadan.risk.redis.RiskResult;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Kafka Sink 工厂
 * 将 RiskResult 序列化为 JSON 后发送到 Kafka topic
 *
 * 下游消费者（如 Web 后端、告警系统、数据仓库）可订阅此 topic 获取实时风险结果
 */
public class KafkaSinkFactory {

    private static final String RISK_RESULT_TOPIC = "risk-results";  // 风险结果输出 topic
    private static final ObjectMapper objectMapper = new ObjectMapper();  // JSON 序列化器

    /**
     * 创建风险结果 KafkaSink
     *
     * @param bootstrapServers Kafka 集群地址
     * @return KafkaSink<RiskResult>
     */
    public static KafkaSink<RiskResult> createRiskResultSink(String bootstrapServers) {
        Properties producerProps = new Properties();
        // 配置 Kafka Producer 属性，在可靠性与吞吐之间取平衡
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");                // 所有副本确认后才算成功
        producerProps.put(ProducerConfig.RETRIES_CONFIG, "3");               // 发送失败重试 3 次
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // 开启幂等，避免重复投递
        producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");    // lz4 压缩，降低网络开销
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");        // 批量发送大小（字节）
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "5");             // 延迟 5ms 攒批，提高吞吐

        return KafkaSink.<RiskResult>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<RiskResult>builder()
                                .setTopic(RISK_RESULT_TOPIC)
                                // 以 userId 作为消息 key，保证同一用户的结果进入同一分区，维持顺序
                                .setKeySerializationSchema(
                                        (SerializationSchema<RiskResult>) value ->
                                                (value.getUserId() != null ? value.getUserId() : "").getBytes(StandardCharsets.UTF_8))
                                .setValueSerializationSchema(new RiskResultSerializationSchema())
                                .build()
                )
                .setKafkaProducerConfig(producerProps)
                .build();
    }

    /**
     * RiskResult JSON 序列化（Flink SerializationSchema）
     */
    public static class RiskResultSerializationSchema implements SerializationSchema<RiskResult> {
        private final ObjectMapper mapper = new ObjectMapper();

        /**
         * 将 RiskResult 序列化为 JSON 字节数组，作为 Kafka 消息体
         *
         * @param element 待序列化的风险结果
         * @return JSON 字节数组
         */
        @Override
        public byte[] serialize(RiskResult element) {
            try {
                return mapper.writeValueAsBytes(element);
            } catch (Exception e) {
                // 序列化失败直接抛运行时异常，交由 Flink 任务处理
                throw new RuntimeException("Failed to serialize RiskResult", e);
            }
        }

        /**
         * 初始化回调：此处无需额外初始化资源
         */
        @Override
        public void open(InitializationContext context) throws Exception {
            // no-op
        }
    }
}
