package com.zhangbao.portrait.entity;

import cn.hutool.core.date.DateUtil;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;

/**
 * @author zhangbao
 * @date 2020/11/19 23:11
 **/
@Data
public class UserGroupInfo implements Comparator<UserGroupInfo> {
    private String userId;
    private String createTime;
    private String amount;
    private String payType;
    private String payTime;
    private String payStatus;//支付状态,0未支付，1已支付，2已退款
    private String couponAmount;//优惠券金额
    private String totalAmount;
    private String refundAmount;
    private Long count;//数量
    private String productType;//消费类目
    private String groupField;

    private List<UserGroupInfo> list;//一个用户所有的消费信息

    //平均消费金额，消费最大金额，消费频次，消费类目，消费时间点
    private double avrAmount;
    private double maxAmount;
    private int days;
    private Long buyType1;//消费类目1数量
    private Long buyType2;//消费类目2数量
    private Long buyType3;//消费类目3数量
    private Long buyTime1;//消费时间点1数量
    private Long buyTime2;//消费时间点2数量
    private Long buyTime3;//消费时间点3数量
    private Long buyTime4;//消费时间点4数量



    @Override
    public int compare(UserGroupInfo o1, UserGroupInfo o2) {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime time1 = now;
        LocalDateTime time2 = now;

        time1 = DateUtil.parseLocalDateTime(o1.getCreateTime(), "yyyyMMdd HHmmss");
        time2 = DateUtil.parseLocalDateTime(o2.getCreateTime(), "yyyyMMdd HHmmss");
        return time1.compareTo(time2);
    }
}
