package com.qinyadan.risk.sink;

import com.qinyadan.risk.redis.RiskResult;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Redis Sink（Flink 2.x 新 Sink API）
 * 将 RiskResult 写入 Redis，key 格式：risk:{eventType}:{userId}:{eventId}
 *
 * 使用方式：
 *   riskResults.sinkTo(new RedisSinkFunction(host, port, db));
 */
public class RedisSinkFunction implements Sink<RiskResult> {

    private final String redisHost;      // Redis 主机地址
    private final int redisPort;         // Redis 端口
    private final int redisDatabase;     // Redis 数据库索引
    private final int timeout;           // 连接超时时间（毫秒）

    /**
     * 便捷构造：默认超时时间 2000ms
     */
    public RedisSinkFunction(String redisHost, int redisPort, int redisDatabase) {
        this(redisHost, redisPort, redisDatabase, 2000);
    }

    /**
     * 完整构造
     *
     * @param redisHost     Redis 主机地址
     * @param redisPort     Redis 端口
     * @param redisDatabase Redis 数据库索引
     * @param timeout       连接超时时间（毫秒）
     */
    public RedisSinkFunction(String redisHost, int redisPort, int redisDatabase, int timeout) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisDatabase = redisDatabase;
        this.timeout = timeout;
    }

    /**
     * 创建 Redis SinkWriter 实例（每个并行子任务调用一次）
     * SinkWriter 内部持有独立的 Redis 连接池
     *
     * @param context Writer 初始化上下文
     * @return 新的 RedisSinkWriter
     */
    @Override
    public SinkWriter<RiskResult> createWriter(WriterInitContext context) throws IOException {
        return new RedisSinkWriter(redisHost, redisPort, redisDatabase, timeout);
    }

    // ==================== SinkWriter 实现 ====================

    /**
     * Redis SinkWriter 实现：负责将 RiskResult 写入 Redis
     */
    static class RedisSinkWriter implements SinkWriter<RiskResult> {
        private static final Logger logger = LoggerFactory.getLogger(RedisSinkWriter.class);

        private final com.qinyadan.risk.redis.RedisClient redisClient;  // Redis 客户端（内部封装连接池）

        /**
         * 构造：创建 Redis 客户端连接池
         */
        RedisSinkWriter(String redisHost, int redisPort, int redisDatabase, int timeout) {
            this.redisClient = new com.qinyadan.risk.redis.RedisClient(redisHost, redisPort, redisDatabase, timeout);
            logger.info("RedisSinkWriter opened: {}:{}", redisHost, redisPort);
        }

        /**
         * 写入一条风险结果：以 risk:{eventType}:{userId}:{eventId} 为 key，带 TTL 写入
         * 写入异常仅记录日志，不抛出，避免影响实时数据流
         *
         * @param value   待写入的风险结果
         * @param context 写入上下文
         */
        @Override
        public void write(RiskResult value, Context context) throws IOException {
            try {
                String key = value.getRedisKey();           // 生成 Redis key
                String jsonValue = value.toJsonString();    // 序列化为 JSON 字符串
                redisClient.setWithTTL(key, jsonValue, value.getTtl());  // 带过期时间写入
                if (logger.isDebugEnabled()) {
                    logger.debug("Risk result written to Redis: {}", key);
                }
            } catch (Exception e) {
                logger.error("Error writing risk result to Redis: user={}, event={}",
                        value.getUserId(), value.getEventType(), e);
            }
        }

        /**
         * 刷新缓冲区：Redis 为逐条同步写入，flush 无需额外操作
         *
         * @param endOfInput 是否已到输入结束
         */
        @Override
        public void flush(boolean endOfInput) throws IOException {
            // Redis 是逐条写入的，flush 无需额外操作
        }

        /**
         * 关闭 Writer：释放 Redis 连接池资源
         */
        @Override
        public void close() throws Exception {
            if (redisClient != null) {
                redisClient.close();
            }
            logger.info("RedisSinkWriter closed");
        }
    }
}
