package com.zhangbao.portrait.entity;

import lombok.Data;

/**
 * @author zhangbao
 * @date 2020/12/30 21:53
 **/
@Data
public class ConsumptionLevel {
    private String userId;
    private String consumptionType;//消费水平，高水平1，中水平2，低水平3
    private String totalAmount;
    private Long count;//数量
    private String groupField;
}
