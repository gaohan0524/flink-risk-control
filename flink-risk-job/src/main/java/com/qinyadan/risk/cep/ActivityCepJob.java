package com.qinyadan.risk.cep;

import com.qinyadan.risk.event.ActivityEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 活动风控 CEP 处理任务（Flink 2.x 兼容）
 * 只负责 CEP 模式匹配，输出 PatternMatchResult 中间结果
 * 动态规则解析由下游 RuleEvaluationFunction（广播处理）完成
 */
public class ActivityCepJob {
    private static final Logger logger = LoggerFactory.getLogger(ActivityCepJob.class);

    /**
     * 活动风控 CEP 处理入口
     * 对已按 userId 分组的活动事件流执行两种模式匹配（频繁参与活动、重复领取优惠券），
     * 输出 PatternMatchResult 中间结果供下游规则引擎评估
     *
     * @param input 按 userId 分组的活动事件流
     * @return 两种模式命中结果的并集（union 后的流）
     */
    public static DataStream<PatternMatchResult> process(KeyedStream<ActivityEvent, String> input) {

        // 模式1: 频繁参与活动
        Pattern<ActivityEvent, ?> frequentPattern = PatternFactory.createActivityFrequentPattern();
        DataStream<PatternMatchResult> frequentRisks = CEP.pattern(input, frequentPattern)
                // 命中"频繁参与活动"模式后的回调：聚合事件 -> 构建上下文与详情 -> 产出中间结果
                .select(new PatternSelectFunction<ActivityEvent, PatternMatchResult>() {
                    @Override
                    public PatternMatchResult select(Map<String, List<ActivityEvent>> pattern) {
                        // 收集所有匹配的事件
                        List<ActivityEvent> allEvents = new ArrayList<>();
                        allEvents.addAll(pattern.getOrDefault("first", Collections.emptyList()));
                        allEvents.addAll(pattern.getOrDefault("second", Collections.emptyList()));
                        allEvents.addAll(pattern.getOrDefault("third", Collections.emptyList()));
                        allEvents.addAll(pattern.getOrDefault("fourth", Collections.emptyList()));

                        if (allEvents.isEmpty()) {
                            return null;
                        }

                        ActivityEvent firstEvent = allEvents.get(0);

                        Map<String, Object> context = buildActivityContext(allEvents, "ACTIVITY_FREQUENT");

                        Map<String, Object> details = new HashMap<>();
                        details.put("participationCount", allEvents.size());
                        details.put("reason", "Abnormal participation pattern detected");

                        PatternMatchResult result = new PatternMatchResult(
                                firstEvent.getUserId(), "ACTIVITY", "ACTIVITY_FREQUENT");
                        result.setEventId(UUID.randomUUID().toString());
                        result.setContext(context);
                        result.setDetails(details);

                        logger.info("[CEP-MATCH] Abnormal activity detected: user={}, count={}",
                                firstEvent.getUserId(), allEvents.size());
                        return result;
                    }
                });

        // 模式2: 重复领取优惠券
        Pattern<ActivityEvent, ?> couponPattern = PatternFactory.createCouponRepeatPattern();
        DataStream<PatternMatchResult> couponRisks = CEP.pattern(input, couponPattern)
                // 命中"重复领取优惠券"模式后的回调：聚合事件 -> 构建上下文与详情 -> 产出中间结果
                .select(new PatternSelectFunction<ActivityEvent, PatternMatchResult>() {
                    @Override
                    public PatternMatchResult select(Map<String, List<ActivityEvent>> pattern) {
                        List<ActivityEvent> allEvents = new ArrayList<>();
                        allEvents.addAll(pattern.getOrDefault("first", Collections.emptyList()));
                        allEvents.addAll(pattern.getOrDefault("second", Collections.emptyList()));
                        allEvents.addAll(pattern.getOrDefault("third", Collections.emptyList()));

                        if (allEvents.isEmpty()) {
                            return null;
                        }

                        ActivityEvent firstEvent = allEvents.get(0);

                        Map<String, Object> context = buildActivityContext(allEvents, "COUPON_REPEAT");

                        Map<String, Object> details = new HashMap<>();
                        details.put("reason", "Repeated coupon claim detected");
                        details.put("couponClaimCount", allEvents.size());
                        if (!allEvents.isEmpty()) {
                            details.put("couponCode", allEvents.get(0).getCouponCode());
                        }

                        PatternMatchResult result = new PatternMatchResult(
                                firstEvent.getUserId(), "ACTIVITY", "COUPON_REPEAT");
                        result.setEventId(UUID.randomUUID().toString());
                        result.setContext(context);
                        result.setDetails(details);

                        logger.info("[CEP-MATCH] Repeated coupon claim detected: user={}",
                                firstEvent.getUserId());
                        return result;
                    }
                });

        // 将两种模式的命中结果流合并为一个流（union 要求两个流元素类型一致）
        return frequentRisks.union(couponRisks);
    }

    /**
     * 构建活动事件的规则评估上下文
     * 将命中事件的最新字段（activityId/actionType/channel/source 等）写入 Map，
     * 供下游 Aviator 规则表达式引用
     *
     * @param events      命中的活动事件列表
     * @param patternType 命中的模式类型
     * @return 规则评估上下文 Map
     */
    private static Map<String, Object> buildActivityContext(List<ActivityEvent> events, String patternType) {
        Map<String, Object> context = new HashMap<>();
        if (events != null && !events.isEmpty()) {
            ActivityEvent latest = events.get(events.size() - 1);
            context.put("eventType", "ACTIVITY");
            context.put("patternType", patternType);
            context.put("userId", latest.getUserId());
            context.put("participationCount", events.size());
            context.put("activityId", latest.getActivityId());
            context.put("activityName", latest.getActivityName());
            context.put("actionType", latest.getActionType());
            context.put("channel", latest.getChannel());
            context.put("source", latest.getSource());
        }
        return context;
    }
}
