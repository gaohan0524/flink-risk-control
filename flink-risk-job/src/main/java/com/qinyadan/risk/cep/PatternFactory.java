package com.qinyadan.risk.cep;

import com.qinyadan.risk.event.ActivityEvent;
import com.qinyadan.risk.event.LoginEvent;
import com.qinyadan.risk.event.OrderEvent;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import java.time.Duration;

/**
 * CEP 模式工厂（Flink 2.x - 带时间窗口）
 */
public class PatternFactory {

    /**
     * 登录失败多次模式：同一用户 60 秒内连续 3 次登录失败
     * 用于识别暴力破解 / 撞库攻击
     */
    public static Pattern<LoginEvent, ?> createLoginFailurePattern() {
        return Pattern.<LoginEvent>begin("first")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return !event.isSuccess();
                    }
                })
                .followedBy("second")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return !event.isSuccess();
                    }
                })
                .followedBy("third")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return !event.isSuccess();
                    }
                })
                .within(Duration.ofSeconds(60));
    }

    /**
     * 异地登录模式：同一用户 60 秒内出现两次成功登录
     * 仅负责匹配"两次成功登录"，具体是否异地由下游规则对比 IP/地点判断
     */
    public static Pattern<LoginEvent, ?> createLoginLocationChangePattern() {
        return Pattern.<LoginEvent>begin("first")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return event.isSuccess();
                    }
                })
                .followedBy("second")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return event.isSuccess();
                    }
                })
                .within(Duration.ofSeconds(60));
    }

    /**
     * 频繁下单模式：同一用户 60 秒内连续 3 笔金额大于 0 的订单
     * 用于识别刷单 / 异常下单行为
     */
    public static Pattern<OrderEvent, ?> createOrderFrequentPattern() {
        return Pattern.<OrderEvent>begin("first")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent event) {
                        return event.getAmount() != null && event.getAmount().compareTo(java.math.BigDecimal.ZERO) > 0;
                    }
                })
                .followedBy("second")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent event) {
                        return event.getAmount() != null && event.getAmount().compareTo(java.math.BigDecimal.ZERO) > 0;
                    }
                })
                .followedBy("third")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent event) {
                        return event.getAmount() != null && event.getAmount().compareTo(java.math.BigDecimal.ZERO) > 0;
                    }
                })
                .within(Duration.ofSeconds(60));
    }

    /**
     * 快速连续下单模式：同一用户 30 秒内连续 2 笔金额大于 100 的订单
     * 用于识别机器人 / 脚本下单
     */
    public static Pattern<OrderEvent, ?> createOrderRapidPattern() {
        return Pattern.<OrderEvent>begin("first")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent event) {
                        return event.getAmount() != null && event.getAmount().compareTo(new java.math.BigDecimal("100")) > 0;
                    }
                })
                .followedBy("second")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent event) {
                        return event.getAmount() != null && event.getAmount().compareTo(new java.math.BigDecimal("100")) > 0;
                    }
                })
                .within(Duration.ofSeconds(30));
    }

    /**
     * 频繁参与活动模式：同一用户 60 秒内连续 4 次参与（PARTICIPATE）活动
     * 用于识别薅羊毛行为
     */
    public static Pattern<ActivityEvent, ?> createActivityFrequentPattern() {
        return Pattern.<ActivityEvent>begin("first")
                .where(new SimpleCondition<ActivityEvent>() {
                    @Override
                    public boolean filter(ActivityEvent event) {
                        return "PARTICIPATE".equals(event.getActionType());
                    }
                })
                .followedBy("second")
                .where(new SimpleCondition<ActivityEvent>() {
                    @Override
                    public boolean filter(ActivityEvent event) {
                        return "PARTICIPATE".equals(event.getActionType());
                    }
                })
                .followedBy("third")
                .where(new SimpleCondition<ActivityEvent>() {
                    @Override
                    public boolean filter(ActivityEvent event) {
                        return "PARTICIPATE".equals(event.getActionType());
                    }
                })
                .followedBy("fourth")
                .where(new SimpleCondition<ActivityEvent>() {
                    @Override
                    public boolean filter(ActivityEvent event) {
                        return "PARTICIPATE".equals(event.getActionType());
                    }
                })
                .within(Duration.ofSeconds(60));
    }

    /**
     * 重复领券模式：同一用户 60 秒内连续 3 次领取优惠券（CLAIM_COUPON）
     * 用于识别羊毛党重复领取
     */
    public static Pattern<ActivityEvent, ?> createCouponRepeatPattern() {
        return Pattern.<ActivityEvent>begin("first")
                .where(new SimpleCondition<ActivityEvent>() {
                    @Override
                    public boolean filter(ActivityEvent event) {
                        return "CLAIM_COUPON".equals(event.getActionType());
                    }
                })
                .followedBy("second")
                .where(new SimpleCondition<ActivityEvent>() {
                    @Override
                    public boolean filter(ActivityEvent event) {
                        return "CLAIM_COUPON".equals(event.getActionType());
                    }
                })
                .followedBy("third")
                .where(new SimpleCondition<ActivityEvent>() {
                    @Override
                    public boolean filter(ActivityEvent event) {
                        return "CLAIM_COUPON".equals(event.getActionType());
                    }
                })
                .within(Duration.ofSeconds(60));
    }
}
