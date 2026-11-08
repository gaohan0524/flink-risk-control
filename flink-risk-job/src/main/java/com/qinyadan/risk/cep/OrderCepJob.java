package com.qinyadan.risk.cep;

import com.qinyadan.risk.event.OrderEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

/**
 * 下单风控 CEP 处理任务（Flink 2.x 兼容）
 * 只负责 CEP 模式匹配，输出 PatternMatchResult 中间结果
 * 动态规则解析由下游 RuleEvaluationFunction（广播处理）完成
 */
public class OrderCepJob {
    private static final Logger logger = LoggerFactory.getLogger(OrderCepJob.class);

    /**
     * 下单风控 CEP 处理入口
     * 对已按 userId 分组的订单事件流执行两种模式匹配（频繁下单、快速连续下单），
     * 输出 PatternMatchResult 中间结果供下游规则引擎评估
     *
     * @param input 按 userId 分组的订单事件流
     * @return 两种模式命中结果的并集（union 后的流）
     */
    public static DataStream<PatternMatchResult> process(KeyedStream<OrderEvent, String> input) {

        // 模式1: 频繁下单
        Pattern<OrderEvent, ?> frequentPattern = PatternFactory.createOrderFrequentPattern();
        DataStream<PatternMatchResult> frequentRisks = CEP.pattern(input, frequentPattern)
                // 命中"频繁下单"模式后的回调：聚合事件 -> 汇总金额 -> 构建上下文与详情
                .select(new PatternSelectFunction<OrderEvent, PatternMatchResult>() {
                    @Override
                    public PatternMatchResult select(Map<String, List<OrderEvent>> pattern) {
                        // 收集所有匹配的事件
                        List<OrderEvent> allEvents = new ArrayList<>();
                        allEvents.addAll(pattern.getOrDefault("first", Collections.emptyList()));
                        allEvents.addAll(pattern.getOrDefault("second", Collections.emptyList()));
                        allEvents.addAll(pattern.getOrDefault("third", Collections.emptyList()));

                        if (allEvents.isEmpty()) {
                            return null;
                        }

                        OrderEvent firstEvent = allEvents.get(0);

                        // 汇总命中订单的总金额（BigDecimal 精确累加，避免浮点误差）
                        BigDecimal totalAmount = BigDecimal.ZERO;
                        for (OrderEvent event : allEvents) {
                            if (event.getAmount() != null) {
                                totalAmount = totalAmount.add(event.getAmount());
                            }
                        }

                        Map<String, Object> context = buildOrderContext(allEvents, "ORDER_FREQUENT");
                        context.put("totalAmount", totalAmount.doubleValue());

                        Map<String, Object> details = new HashMap<>();
                        details.put("orderCount", allEvents.size());
                        details.put("totalAmount", totalAmount);

                        PatternMatchResult result = new PatternMatchResult(
                                firstEvent.getUserId(), "ORDER", "ORDER_FREQUENT");
                        result.setEventId(UUID.randomUUID().toString());
                        result.setContext(context);
                        result.setDetails(details);

                        logger.info("[CEP-MATCH] Frequent order detected: user={}, count={}, amount={}",
                                firstEvent.getUserId(), allEvents.size(), totalAmount);
                        return result;
                    }
                });

        // 模式2: 快速连续下单
        Pattern<OrderEvent, ?> rapidPattern = PatternFactory.createOrderRapidPattern();
        DataStream<PatternMatchResult> rapidRisks = CEP.pattern(input, rapidPattern)
                // 命中"快速连续下单"模式后的回调：计算两笔订单时间间隔 -> 构建上下文与详情
                .select(new PatternSelectFunction<OrderEvent, PatternMatchResult>() {
                    @Override
                    public PatternMatchResult select(Map<String, List<OrderEvent>> pattern) {
                        List<OrderEvent> firstList = pattern.getOrDefault("first", Collections.emptyList());
                        List<OrderEvent> secondList = pattern.getOrDefault("second", Collections.emptyList());

                        if (firstList.isEmpty() || secondList.isEmpty()) {
                            return null;
                        }

                        OrderEvent firstEvent = firstList.get(0);
                        OrderEvent secondEvent = secondList.get(0);

                        // 计算两笔订单的时间间隔（毫秒），作为快速下单判定依据
                        long interval = secondEvent.getTimestamp() - firstEvent.getTimestamp();

                        Map<String, Object> context = buildOrderContext(secondList, "ORDER_RAPID");
                        context.put("intervalMs", (double) interval);

                        Map<String, Object> details = new HashMap<>();
                        details.put("reason", "Rapid order submission detected");
                        details.put("intervalMs", interval);
                        details.put("firstOrderId", firstEvent.getOrderId());
                        details.put("secondOrderId", secondEvent.getOrderId());

                        PatternMatchResult result = new PatternMatchResult(
                                firstEvent.getUserId(), "ORDER", "ORDER_RAPID");
                        result.setEventId(UUID.randomUUID().toString());
                        result.setContext(context);
                        result.setDetails(details);

                        logger.info("[CEP-MATCH] Rapid order detected: user={}, interval={}ms",
                                firstEvent.getUserId(), interval);
                        return result;
                    }
                });

        // 将两种模式的命中结果流合并为一个流
        return frequentRisks.union(rapidRisks);
    }

    /**
     * 构建订单事件的规则评估上下文
     * 将命中事件的最新字段（amount/productId/paymentMethod 等）写入 Map，供 Aviator 表达式引用
     *
     * @param events      命中的订单事件列表
     * @param patternType 命中的模式类型
     * @return 规则评估上下文 Map
     */
    private static Map<String, Object> buildOrderContext(List<OrderEvent> events, String patternType) {
        Map<String, Object> context = new HashMap<>();
        if (events != null && !events.isEmpty()) {
            OrderEvent latest = events.get(events.size() - 1);
            context.put("eventType", "ORDER");
            context.put("patternType", patternType);
            context.put("userId", latest.getUserId());
            context.put("orderCount", events.size());
            context.put("amount", latest.getAmount() != null ? latest.getAmount().doubleValue() : 0);
            context.put("productId", latest.getProductId());
            context.put("paymentMethod", latest.getPaymentMethod());
        }
        return context;
    }
}
