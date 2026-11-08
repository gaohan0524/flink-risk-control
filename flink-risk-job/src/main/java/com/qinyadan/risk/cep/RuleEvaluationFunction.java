package com.qinyadan.risk.cep;

import com.qinyadan.risk.redis.RiskResult;
import com.qinyadan.risk.rule.AviatorRuleEngine;
import com.qinyadan.risk.rule.RiskActionResolver;
import com.qinyadan.risk.rule.RuleConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 规则评估广播处理函数
 *
 * 双流连接：
 *   - 主输入流：PatternMatchResult（CEP 模式匹配命中后的中间结果）
 *   - 广播流：List<RuleConfig>（从 MySQL 定时拉取的规则快照）
 *
 * 工作方式：
 *   1. processElement：收到模式匹配结果时，从 BroadcastState 读取最新规则，
 *      通过 AviatorRuleEngine 动态评估，输出 RiskResult
 *   2. processBroadcastElement：收到新的规则快照时，全量替换 BroadcastState，
 *      重建 AviatorRuleEngine 实例
 *
 * 中文详细说明（为什么用 BroadcastProcessFunction）：
 *   - 规则需要"全局一致 + 可动态更新"，故采用广播状态模式：规则流广播到每个并行子任务，
 *     各子任务在本地保存一份只读规则副本，处理事件时无需远程调用即可评估。
 *   - 主输入流侧拿到的是 ReadOnlyBroadcastState，只能读不能写；
 *     只有广播流侧（processBroadcastElement）才持有可写的 BroadcastState。
 *   - 规则以"整份快照"形式下发，每次到达做全量替换；同时重建本地 AviatorRuleEngine，
 *     从而在不重启 Job 的情况下完成规则热更新。
 *   - 广播状态参与 Checkpoint，故障恢复后规则不会丢失。
 */
public class RuleEvaluationFunction
        extends BroadcastProcessFunction<PatternMatchResult, List<RuleConfig>, RiskResult> {

    private static final Logger logger = LoggerFactory.getLogger(RuleEvaluationFunction.class);

    /** 广播状态 Key */
    public static final String RULE_STATE_NAME = "rule-config-state";
    /**
     * 广播状态描述符：定义 BroadcastState 的名称与类型
     * Key 使用 Void 类型（实际以 null 作为唯一键），Value 为规则列表 List<RuleConfig>
     */
    public static final MapStateDescriptor<Void, List<RuleConfig>> RULE_STATE_DESCRIPTOR =
            new MapStateDescriptor<>(
                    RULE_STATE_NAME,
                    // 用 Void 作为 Key 类型：该广播状态只需保存"一份规则快照"，
                    // 因此约定用 null 作为唯一 Key，避免无意义的键空间
                    BasicTypeInfo.VOID_TYPE_INFO,
                    // Value 为 List<RuleConfig>，通过 TypeHint 匿名子类保留泛型信息，供 Flink 类型推断
                    TypeInformation.of(new TypeHint<List<RuleConfig>>() {})
            );

    /** 每个并行实例持有的规则引擎 */
    // 使用 volatile 修饰，确保规则热更新后各线程能立即看到最新引擎实例
    private transient volatile AviatorRuleEngine ruleEngine;
    // 风险动作解析器：基于规则引擎把事件上下文解析成 RiskResult
    private transient volatile RiskActionResolver resolver;

    /**
     * 算子实例初始化：创建空的规则引擎与动作解析器
     * 真正加载规则要等到第一次广播规则到达（processBroadcastElement 回调）
     */
    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        // 初始化空引擎，等待第一次广播规则到达
        this.ruleEngine = new AviatorRuleEngine(1000, 3600);
        this.resolver = new RiskActionResolver(ruleEngine);
        logger.info("RuleEvaluationFunction opened, waiting for rule broadcast...");
    }

    /**
     * 处理主输入流：CEP 模式匹配结果
     * 从广播状态获取最新规则，通过引擎动态评估 action
     *
     * 中文说明：收到 PatternMatchResult 后，从 BroadcastState 读取最新规则快照，
     * 交给 RiskActionResolver 动态评估出 RiskResult；规则尚未就绪时输出默认放行（ALLOW）
     */
    @Override
    public void processElement(
            PatternMatchResult matchResult,
            ReadOnlyContext ctx,
            Collector<RiskResult> out) throws Exception {

        logger.info("[EVAL] Processing match: user={}, eventType={}, pattern={}, context={}",
                matchResult.getUserId(), matchResult.getEventType(),
                matchResult.getPatternType(), matchResult.getContext());

        // ReadOnlyContext.getBroadcastState 返回 ReadOnlyBroadcastState（主输入流侧只读）
        ReadOnlyBroadcastState<Void, List<RuleConfig>> broadcastState =
                ctx.getBroadcastState(RULE_STATE_DESCRIPTOR);

        // Void 无法实例化，用 null 作为广播状态的唯一 Key，取出当前最新规则快照
        List<RuleConfig> rules = broadcastState.get(null);

        if (rules == null || rules.isEmpty()) {
            // 广播规则尚未到达（作业刚启动、规则源首次拉取未完成）：
            // 采取"失败开放"策略，输出默认低风险并放行，避免因规则未就绪而误伤全部事件
            // 广播规则尚未到达，输出默认低风险结果
            RiskResult result = new RiskResult(
                    matchResult.getUserId(),
                    matchResult.getEventId(),
                    matchResult.getEventType()
            );
            result.setRiskLevel(com.qinyadan.risk.rule.RiskLevel.LOW);
            result.setRiskScore(0);
            result.setAction("ALLOW");
            result.getDetails().put("reason", "No rules loaded yet");
            result.getDetails().putAll(matchResult.getDetails());
            out.collect(result);
            logger.warn("[EVAL] No rules in broadcast state, output ALLOW for user: {}", matchResult.getUserId());
            return;
        }

        logger.info("[EVAL] {} rules available, evaluating context against rules...", rules.size());

        // 通过规则引擎动态解析：把 CEP 上下文 + 规则集 => 风险等级/评分/处置动作
        RiskResult result = resolver.resolve(
                matchResult.getContext(),
                matchResult.getUserId(),
                matchResult.getEventId(),
                matchResult.getEventType(),
                matchResult.getDetails()
        );

        logger.info("[EVAL] Result: user={}, action={}, level={}, score={}, details={}",
                result.getUserId(), result.getAction(),
                result.getRiskLevel(), result.getRiskScore(), result.getDetails());

        out.collect(result);
    }

    /**
     * 处理广播流：规则快照更新
     * 全量替换广播状态，并重建本地规则引擎
     *
     * 中文说明：收到新的规则列表后，全量覆盖 BroadcastState，并在每个并行实例上
     * 重建 AviatorRuleEngine，实现规则热更新（无需重启 Job）
     */
    @Override
    public void processBroadcastElement(
            List<RuleConfig> rules,
            Context ctx,
            Collector<RiskResult> out) throws Exception {

        // 更新广播状态：以 null 为唯一 Key，整份快照覆盖写入（全量替换）
        BroadcastState<Void, List<RuleConfig>> broadcastState =
                ctx.getBroadcastState(RULE_STATE_DESCRIPTOR);
        broadcastState.put(null, rules);

        // 重建本地规则引擎：不在旧实例上增量修改，而是"新建 + 整体替换"，
        // 避免运行中修改共享状态带来的并发问题；通过 volatile 写保证新引擎对各线程可见
        AviatorRuleEngine newEngine = new AviatorRuleEngine(1000, 3600);
        newEngine.registerRules(rules);          // 注册时逐条预编译，非法规则被跳过
        this.ruleEngine = newEngine;             // volatile 写：发布新引擎
        this.resolver = new RiskActionResolver(newEngine);  // 同步替换解析器

        logger.info("Rule engine refreshed in this task: {} rules loaded", newEngine.getRuleCount());
    }
}
