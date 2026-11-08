package com.qinyadan.risk.cep;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * CEP 模式匹配中间结果
 * CepJob 在模式匹配命中后输出此对象，
 * 携带事件上下文，由下游广播处理函数连接规则引擎后动态解析 action
 */
public class PatternMatchResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private String userId;                 // 用户ID
    private String eventId;                // 事件ID（本次匹配生成的唯一标识）
    private String eventType;              // 事件类型（LOGIN/ORDER/ACTIVITY）
    private String patternType;            // 命中的模式类型（如 LOGIN_FAILURE / ORDER_RAPID）
    private Map<String, Object> context;   // 规则评估上下文（供 Aviator 表达式引用）
    private Map<String, Object> details;   // 附加详情（供持久化/展示）

    /**
     * 无参构造：初始化 context 与 details 两个 Map，避免后续使用时出现空指针
     */
    public PatternMatchResult() {
        this.context = new HashMap<>();
        this.details = new HashMap<>();
    }

    /**
     * 便捷构造：直接指定用户、事件类型与命中模式
     *
     * @param userId      用户ID
     * @param eventType   事件类型
     * @param patternType 命中的模式类型
     */
    public PatternMatchResult(String userId, String eventType, String patternType) {
        this();
        this.userId = userId;
        this.eventType = eventType;
        this.patternType = patternType;
    }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public String getPatternType() { return patternType; }
    public void setPatternType(String patternType) { this.patternType = patternType; }

    public Map<String, Object> getContext() { return context; }
    public void setContext(Map<String, Object> context) { this.context = context; }

    public Map<String, Object> getDetails() { return details; }
    public void setDetails(Map<String, Object> details) { this.details = details; }

    /**
     * 便于日志打印的字符串表示（仅输出核心字段，不含 details）
     */
    @Override
    public String toString() {
        return "PatternMatchResult{" +
                "userId='" + userId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", patternType='" + patternType + '\'' +
                ", context=" + context +
                '}';
    }
}
