package com.qinyadan.risk.rule;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * AviatorScript 规则引擎
 * 支持动态规则加载、编译缓存、规则执行
 *
 * 中文详细说明：
 * 1) 职责：以"规则集合 + 事件上下文"为输入，输出命中的规则列表，并可进一步计算加权风险评分与风险等级。
 * 2) 规则以 RuleConfig 形式保存在内存 Map 中（key=规则ID），表达式为 AviatorScript 语法，
 *    可引用上下文变量（如 eventType / patternType / failCount / orderCount / intervalMs 等）。
 * 3) 表达式编译结果用 Guava Cache 缓存，避免同一条表达式每次执行都重新编译，提升吞吐。
 * 4) 线程模型：本类内部未做同步，设计为"单线程持有"使用；在 Flink 中由每个并行子任务在规则热更新时
 *    整体重建新实例并替换 volatile 引用，而非在运行中并发修改同一实例，从而规避锁开销。
 */
public class AviatorRuleEngine {
    private static final Logger logger = LoggerFactory.getLogger(AviatorRuleEngine.class);

    // 表达式编译缓存：key=AviatorScript 表达式原文，value=编译后的 Expression
    // maximumSize 限制条目数，expireAfterAccess 控制"空闲多久后过期"，防止规则频繁变更导致缓存无界膨胀
    private final Cache<String, Expression> expressionCache;
    // 已注册规则表：key=规则ID，value=规则配置；用于遍历执行以及按 ID 增删
    private final Map<String, RuleConfig> ruleConfigs;

    /**
     * 构造规则引擎
     *
     * @param cacheSize       表达式编译缓存的最大条目数
     * @param cacheTTLSeconds 缓存条目"最后一次访问后"的存活秒数（每次访问会续期）
     */
    public AviatorRuleEngine(int cacheSize, int cacheTTLSeconds) {
        this.expressionCache = CacheBuilder.newBuilder()
                .maximumSize(cacheSize)                                // 最多缓存 cacheSize 条编译结果
                .expireAfterAccess(cacheTTLSeconds, TimeUnit.SECONDS)  // 空闲超过 TTL 后自动失效
                .build();
        this.ruleConfigs = new HashMap<>();
    }

    /**
     * 注册规则（注册时校验表达式合法性）
     *
     * 中文详细说明：注册前先用 AviatorEvaluator.compile 试编译一次表达式：
     * 一是提前拦截语法错误的规则，避免运行时反复报错；
     * 二是让编译结果进入 Aviator 内部缓存，后续执行可直接复用。
     * 编译失败的规则会被跳过并记录错误，不影响其它规则注册；被禁用的规则直接忽略。
     *
     * @param ruleConfig 待注册的规则配置
     */
    public void registerRule(RuleConfig ruleConfig) {
        if (!ruleConfig.isEnabled()) {
            return;
        }
        // 注册前先编译表达式，不合法则拒绝注册，避免运行时反复报错
        try {
            AviatorEvaluator.compile(ruleConfig.getExpression());
        } catch (Exception e) {
            logger.error("Rule expression compilation failed, SKIP register: {} - {}, expression: [{}], error: {}",
                    ruleConfig.getId(), ruleConfig.getName(), ruleConfig.getExpression(), e.getMessage());
            return;
        }
        ruleConfigs.put(ruleConfig.getId(), ruleConfig);
        logger.info("Rule registered: {} - {}", ruleConfig.getId(), ruleConfig.getName());
    }

    /**
     * 批量注册规则
     *
     * 中文说明：逐个调用 registerRule，单条失败不会中断整体注册流程
     *
     * @param rules 规则列表
     */
    public void registerRules(List<RuleConfig> rules) {
        for (RuleConfig rule : rules) {
            registerRule(rule);
        }
    }

    /**
     * 执行规则，返回匹配的规则
     *
     * 中文详细说明：遍历内存中所有规则，逐条用事件上下文求值；表达式返回布尔 true 即视为命中。
     * 单条规则执行异常会被捕获并记录，不影响其它规则，保证"一条坏规则不会拖垮整批评估"。
     *
     * @param context 事件上下文变量表（供表达式引用）
     * @return 命中的规则列表（可能为空，也可能多条）
     */
    public List<RuleConfig> executeRules(Map<String, Object> context) {
        List<RuleConfig> matchedRules = new ArrayList<>();

        // 遍历全部已注册规则，逐条求值
        for (RuleConfig rule : ruleConfigs.values()) {
            // 双重保险：跳过被禁用的规则（注册时已过滤，此处再判一次以防规则被动态禁用）
            if (!rule.isEnabled()) {
                continue;
            }

            try {
                boolean result = evaluateExpression(rule.getExpression(), context);
                if (result) {
                    matchedRules.add(rule);
                    logger.debug("Rule matched: {}", rule.getId());
                }
            } catch (Exception e) {
                logger.error("Error executing rule: {}", rule.getId(), e);
            }
        }

        return matchedRules;
    }

    /**
     * 评估单个表达式
     * Aviator 的 =~ 操作符要求右操作数为正则字面量（如 /脚本/），不支持字符串字面量（如 "脚本"）
     * 若表达式中使用了错误的 =~ "字符串" 语法，执行时会产生 ExpressionRuntimeException
     *
     * 中文说明：表达式结果必须是 Boolean，非布尔结果一律按 false（未命中）处理；
     * 执行过程中的异常全部吞掉并记日志，返回 false，绝不向上抛出，避免单条规则异常拖垮数据流。
     *
     * @param expression 规则表达式
     * @param context    事件上下文
     * @return 表达式求值结果，仅当返回布尔 true 时视为命中
     */
    public boolean evaluateExpression(String expression, Map<String, Object> context) {
        try {
            // 先从缓存取编译结果，未命中再编译并回填缓存
            Expression compiledExpression = getOrCompileExpression(expression);
            Object result = compiledExpression.execute(context);
            // 只接受布尔结果，其它类型（null/数字/字符串等）一律视为未命中
            return result instanceof Boolean ? (Boolean) result : false;
        } catch (com.googlecode.aviator.exception.ExpressionRuntimeException e) {
            // 常见原因：=~ 操作符右操作数使用了字符串而非正则字面量
            logger.error("Expression runtime error (check =~ syntax, use /pattern/ not \"pattern\"): {}, contextKeys={}, tip: use 'source =~ /脚本/' or 'string.contains(source,\"脚本\")'",
                    expression, context.keySet());
            return false;
        } catch (Exception e) {
            logger.error("Error evaluating expression: {}, error: {}, contextKeys={}", expression, e.getMessage(), context.keySet());
            return false;
        }
    }

    /**
     * 计算风险评分
     *
     * 中文详细说明：采用"加权平均"口径——每条命中规则的贡献为其风险等级分 × 权重，
     * 最终得分 = Σ(等级分 × 权重) / Σ权重，结果落在 0~100 区间；无命中规则时返回 0。
     *
     * 注意：该方法会重新执行一次全部规则；若同一上下文还需用到结果，建议外部缓存返回值避免重复计算。
     *
     * @param context 事件上下文
     * @return 加权平均后的风险评分（0~100）
     */
    public int calculateRiskScore(Map<String, Object> context) {
        List<RuleConfig> matchedRules = executeRules(context);
        int totalScore = 0;   // 分子：Σ(等级分 × 权重)
        int totalWeight = 0;  // 分母：Σ权重

        for (RuleConfig rule : matchedRules) {
            totalScore += rule.getRiskLevel().getScore() * rule.getWeight();
            totalWeight += rule.getWeight();
        }

        // 无任何规则命中，评分记为 0
        if (totalWeight == 0) {
            return 0;
        }

        return totalScore / totalWeight;
    }

    /**
     * 获取风险等级
     *
     * 中文说明：先算出加权评分，再按 RiskLevel.fromScore 的阈值映射为风险等级
     *
     * @param context 事件上下文
     * @return 对应的风险等级
     */
    public RiskLevel getRiskLevel(Map<String, Object> context) {
        int score = calculateRiskScore(context);
        return RiskLevel.fromScore(score);
    }

    /**
     * 获取或编译表达式
     *
     * 中文说明：以表达式原文为 key 查缓存，未命中时通过 Guava 的 CacheLoader 回调编译并写回缓存。
     * 编译失败会包装为 RuntimeException 抛出（交由上层 evaluateExpression 捕获并记日志）。
     *
     * @param expression 表达式原文
     * @return 编译后的 Aviator Expression
     */
    private Expression getOrCompileExpression(String expression) {
        try {
            return expressionCache.get(expression, () -> AviatorEvaluator.compile(expression));
        } catch (ExecutionException e) {
            logger.error("Failed to compile expression: {}", expression, e);
            throw new RuntimeException("Expression compilation failed", e);
        }
    }

    /**
     * 清空规则
     * 中文说明：移除内存中的全部规则（不会清空表达式编译缓存）
     */
    public void clearRules() {
        ruleConfigs.clear();
        logger.info("All rules cleared");
    }

    /**
     * 移除规则
     *
     * @param ruleId 规则ID
     */
    public void removeRule(String ruleId) {
        ruleConfigs.remove(ruleId);
        logger.info("Rule removed: {}", ruleId);
    }

    /**
     * 获取规则数量
     *
     * @return 当前已注册的规则条数
     */
    public int getRuleCount() {
        return ruleConfigs.size();
    }

    /**
     * 获取所有规则
     *
     * @return 已注册规则的集合视图
     */
    public Collection<RuleConfig> getAllRules() {
        return ruleConfigs.values();
    }
}
