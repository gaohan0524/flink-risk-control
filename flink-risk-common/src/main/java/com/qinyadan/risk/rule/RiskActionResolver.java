package com.qinyadan.risk.rule;

import com.qinyadan.risk.redis.RiskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * 风险动作解析器
 * CEP 模式匹配后，将事件上下文交给规则引擎执行，
 * 根据匹配到的规则动态决定 action、riskLevel、riskScore
 *
 * 中文详细说明：本类是"规则引擎"与"对外风险结果"之间的适配层，
 * 屏蔽 Aviator 表达式细节，把"命中哪些规则"翻译成业务上的
 * 风险等级（RiskLevel）、风险分（riskScore）、处置动作（action）。
 * 决策口径为"就高不就低"：多条规则命中时取风险等级最高的一条作为最终结果。
 */
public class RiskActionResolver {
    private static final Logger logger = LoggerFactory.getLogger(RiskActionResolver.class);

    // 规则引擎：负责实际的表达式求值与命中规则计算
    private final AviatorRuleEngine ruleEngine;

    /**
     * 构造解析器
     *
     * @param ruleEngine 规则引擎实例（由调用方保证其规则已注册完毕）
     */
    public RiskActionResolver(AviatorRuleEngine ruleEngine) {
        this.ruleEngine = ruleEngine;
    }

    /**
     * 根据事件上下文，通过规则引擎动态解析风险结果
     *
     * @param context   事件上下文变量（供 Aviator 表达式引用）
     * @param userId    用户ID
     * @param eventId   事件ID
     * @param eventType 事件类型
     * @param details   额外详情
     * @return 填充了动态 action/riskLevel/riskScore 的 RiskResult
     */
    public RiskResult resolve(Map<String, Object> context,
                              String userId,
                              String eventId,
                              String eventType,
                              Map<String, Object> details) {
        // 先构造基础结果（带 userId/eventId/eventType 与默认 TTL）
        RiskResult result = new RiskResult(userId, eventId, eventType);

        // 通过规则引擎执行所有匹配规则（返回值可能为 0~N 条）
        List<RuleConfig> matchedRules = ruleEngine.executeRules(context);

        if (matchedRules.isEmpty()) {
            // 分支一：没有任何规则命中 —— 按最低风险处理，直接放行
            // 无规则匹配时，使用默认低风险
            result.setRiskLevel(RiskLevel.LOW);
            result.setRiskScore(0);
            result.setAction("ALLOW");
            result.getDetails().put("reason", "No rule matched");
            logger.debug("No rule matched for user: {} event: {}", userId, eventType);
        } else {
            // 分支二：有规则命中 —— 从命中集合中挑选"风险最高"的一条作为最终决策依据
            // 取优先级最高的规则（风险评分最高的）
            RuleConfig topRule = selectTopPriorityRule(matchedRules);

            // 以该规则的风险等级/处置动作作为最终结果
            result.setRiskLevel(topRule.getRiskLevel());
            result.setRiskScore(topRule.getRiskLevel().getScore());
            result.setAction(topRule.getAction());
            // 记录命中的规则信息，便于排查与前端展示
            result.getDetails().put("matchedRuleId", topRule.getId());
            result.getDetails().put("matchedRuleName", topRule.getName());
            result.getDetails().put("matchedRuleCount", matchedRules.size());

            // 合并 CEP 侧传入的额外详情（如 failCount、intervalMs 等）
            if (details != null) {
                result.getDetails().putAll(details);
            }

            logger.info("Rule engine resolved action for user: {} event: {} -> rule: {} action: {} level: {}",
                    userId, eventType, topRule.getName(), topRule.getAction(), topRule.getRiskLevel());
        }

        return result;
    }

    /**
     * 从匹配的规则中选择优先级最高的规则
     * 优先级：按 riskLevel 的 score 降序，score 相同按 weight 降序
     *
     * 中文说明：用 Stream.max 配合自定义比较器实现"就高不就低"；
     * 比较器先比风险等级分，分数相同再比权重，权重也相同时由 max 保留先遇到的一条。
     * 末尾 orElse 在理论上不会触发（调用前已确保非空），仅作防御性兜底。
     *
     * @param matchedRules 命中的规则列表（非空）
     * @return 优先级最高的规则
     */
    private RuleConfig selectTopPriorityRule(List<RuleConfig> matchedRules) {
        return matchedRules.stream()
                .max((a, b) -> {
                    // 先比较风险等级分（分数高的优先）
                    int scoreCompare = Integer.compare(
                            a.getRiskLevel().getScore(), b.getRiskLevel().getScore());
                    if (scoreCompare != 0) return scoreCompare;
                    // 等级分相同再比较权重（权重高的优先）
                    return Integer.compare(a.getWeight(), b.getWeight());
                })
                .orElse(matchedRules.get(0));
    }

    /**
     * 获取规则引擎（供外部查询规则数量等）
     *
     * @return 当前解析器使用的规则引擎
     */
    public AviatorRuleEngine getRuleEngine() {
        return ruleEngine;
    }
}
