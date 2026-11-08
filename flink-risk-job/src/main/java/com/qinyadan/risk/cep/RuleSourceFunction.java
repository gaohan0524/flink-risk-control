package com.qinyadan.risk.cep;

import com.qinyadan.risk.rule.RuleConfig;
import com.qinyadan.risk.rule.RiskLevel;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 规则数据源
 * 从 MySQL rule_config 表定时轮询拉取规则，作为广播流的数据源
 *
 * 工作方式：
 *   - 只有一个并行度（parallelism=1），避免重复查询数据库
 *   - 启动时立即拉取一次
 *   - 之后按固定间隔轮询
 *   - 通过 AtomicReference 持有最新规则快照
 */
public class RuleSourceFunction implements SourceFunction<List<RuleConfig>> {

    private static final Logger logger = LoggerFactory.getLogger(RuleSourceFunction.class);

    /** 规则快照，供外部读取 */
    private final AtomicReference<List<RuleConfig>> ruleSnapshot = new AtomicReference<>(new ArrayList<>());

    private final String jdbcUrl;   // MySQL JDBC 连接地址
    private final String username;   // MySQL 用户名
    private final String password;   // MySQL 密码
    private final long intervalMs;   // 规则轮询间隔（毫秒）

    private volatile boolean running = true;  // 运行标志，cancel 时置 false 停止主循环

    /**
     * 构造规则数据源
     *
     * @param jdbcUrl    MySQL JDBC 连接地址
     * @param username   MySQL 用户名
     * @param password   MySQL 密码
     * @param intervalMs 规则轮询间隔（毫秒）
     */
    public RuleSourceFunction(String jdbcUrl, String username, String password, long intervalMs) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.intervalMs = intervalMs;
    }

    /**
     * 数据源主循环：启动即拉取一次规则，之后按固定间隔轮询
     * 每次拉取结果通过 ctx.collect 发射到下游；拉取异常仅记录日志，不中断循环，等待下次重试
     *
     * @param ctx Flink 源上下文，用于向下游发射规则快照
     */
    @Override
    public void run(SourceContext<List<RuleConfig>> ctx) throws Exception {
        while (running) {
            try {
                List<RuleConfig> rules = fetchRulesFromDB();
                ruleSnapshot.set(rules);
                ctx.collect(rules);
                logger.info("Broadcast rules snapshot: {} rules", rules.size());
            } catch (Exception e) {
                logger.error("Failed to fetch rules from MySQL, will retry in {}ms", intervalMs, e);
            }

            // 等待下一次轮询
            Thread.sleep(intervalMs);
        }
    }

    /**
     * 取消数据源：将运行标志置为 false，使 run 主循环退出
     */
    @Override
    public void cancel() {
        running = false;
    }

    /**
     * 获取当前规则快照（供非广播场景使用）
     * 返回 AtomicReference 中保存的最新规则列表
     */
    public List<RuleConfig> getRuleSnapshot() {
        return ruleSnapshot.get();
    }

    /**
     * 从 MySQL 查询所有启用的规则
     *
     * 中文说明：查询 rule_config 表中 enabled=1 的记录并映射为 RuleConfig 列表，
     * 使用 try-with-resources 自动关闭 Connection/Statement/ResultSet
     */
    private List<RuleConfig> fetchRulesFromDB() throws SQLException {
        List<RuleConfig> rules = new ArrayList<>();

        // 仅查询启用状态的规则（enabled=1），查询字段与 RuleConfig 属性一一对应
        String sql = "SELECT id, name, expression, risk_level, action, weight, enabled, description " +
                     "FROM rule_config WHERE enabled = 1";

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                RuleConfig config = new RuleConfig();
                config.setId(String.valueOf(rs.getLong("id")));
                config.setName(rs.getString("name"));
                config.setExpression(rs.getString("expression"));
                config.setRiskLevel(RiskLevel.valueOf(rs.getString("risk_level")));
                config.setAction(rs.getString("action"));
                config.setWeight(rs.getInt("weight"));
                config.setEnabled(rs.getBoolean("enabled"));
                config.setDescription(rs.getString("description"));
                rules.add(config);
            }
        }

        return rules;
    }
}
