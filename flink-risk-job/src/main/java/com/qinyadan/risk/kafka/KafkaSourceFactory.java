package com.qinyadan.risk.kafka;

import com.qinyadan.risk.event.ActivityEvent;
import com.qinyadan.risk.event.Event;
import com.qinyadan.risk.event.LoginEvent;
import com.qinyadan.risk.event.OrderEvent;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

/**
 * KafkaSource 工厂类 - Flink 2.x 新 API
 *
 * 使用方式:
 *   KafkaSource<LoginEvent> loginSource = KafkaSourceFactory.createLoginSource("localhost:9092");
 *   DataStream<LoginEvent> loginStream = env.fromSource(loginSource, WatermarkStrategy.noWatermarks(), "login-source");
 */
public class KafkaSourceFactory {

    private static final String LOGIN_TOPIC = "login-events";        // 登录事件 topic
    private static final String ORDER_TOPIC = "order-events";        // 订单事件 topic
    private static final String ACTIVITY_TOPIC = "activity-events";  // 活动事件 topic
    private static final String GROUP_ID = "flink-risk-control-group";  // 消费者组 ID

    /**
     * 创建登录事件 KafkaSource
     * 从 login-events topic 消费，使用 LoginEventDeserializer 反序列化，从最早 offset 开始
     */
    public static KafkaSource<LoginEvent> createLoginSource(String bootstrapServers) {
        return KafkaSource.<LoginEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(LOGIN_TOPIC)
                .setGroupId(GROUP_ID)
                .setValueOnlyDeserializer(new LoginEventDeserializer())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();
    }

    /**
     * 创建订单事件 KafkaSource
     * 从 order-events topic 消费，使用 OrderEventDeserializer 反序列化，从最早 offset 开始
     */
    public static KafkaSource<OrderEvent> createOrderSource(String bootstrapServers) {
        return KafkaSource.<OrderEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(ORDER_TOPIC)
                .setGroupId(GROUP_ID)
                .setValueOnlyDeserializer(new OrderEventDeserializer())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();
    }

    /**
     * 创建活动事件 KafkaSource
     * 从 activity-events topic 消费，使用 ActivityEventDeserializer 反序列化，从最早 offset 开始
     */
    public static KafkaSource<ActivityEvent> createActivitySource(String bootstrapServers) {
        return KafkaSource.<ActivityEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(ACTIVITY_TOPIC)
                .setGroupId(GROUP_ID)
                .setValueOnlyDeserializer(new ActivityEventDeserializer())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();
    }

    /**
     * 创建所有事件类型的统一 KafkaSource（单 Topic 多事件类型）
     * 返回 DataStream<Event> 基类
     *
     * 中文说明：同时订阅三个 topic，使用 KafkaEventDeserializer 按 eventType 动态反序列化到对应子类
     */
    public static KafkaSource<Event> createUnifiedSource(String bootstrapServers) {
        return KafkaSource.<Event>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(LOGIN_TOPIC, ORDER_TOPIC, ACTIVITY_TOPIC)
                .setGroupId(GROUP_ID)
                .setValueOnlyDeserializer(new KafkaEventDeserializer())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();
    }
}
