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
public class BaiJiaInfo implements Comparator<BaiJiaInfo> {
    private String baiJiaType;//败家指数区段：0-20，20-50，50-70，70-80，80-90，90-100
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
    private String groupField;

    private List<BaiJiaInfo> list;//对每个用户的订单聚合信息


    @Override
    public int compare(BaiJiaInfo o1, BaiJiaInfo o2) {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime time1 = now;
        LocalDateTime time2 = now;

        time1 = DateUtil.parseLocalDateTime(o1.getCreateTime(), "yyyyMMdd HHmmss");
        time2 = DateUtil.parseLocalDateTime(o2.getCreateTime(), "yyyyMMdd HHmmss");
        return time1.compareTo(time2);
    }
}
