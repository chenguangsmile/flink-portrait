package com.zhangbao.portrait.map;

import com.zhangbao.portrait.entity.ConsumptionLevel;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author zhangbao
 * @date 2020/11/15 21:24
 **/
public class ConsumptionLevelMap implements MapFunction<String, ConsumptionLevel> {
    @Override
    public ConsumptionLevel map(String s) throws Exception {
        if(StringUtils.isBlank(s)){
            return null;
        }
        String[] split = s.split(",");
        String id = split[0];
        String userId = split[1];
        String productId = split[2];
        String productType = split[3];
        String productNum = split[4];
        String createTime = split[5];
        String amount = split[6];
        String payType = split[7];
        String payTime = split[8];
        String payStatus = split[9];
        String couponAmount = split[10];
        String totalAmount = split[11];
        String refundAmount = split[12];

        ConsumptionLevel consumptionLevel = new ConsumptionLevel();
        consumptionLevel.setUserId(userId);
        consumptionLevel.setTotalAmount(amount);
        consumptionLevel.setGroupField("=consumptionLevel="+userId);
        return consumptionLevel;
    }
}
