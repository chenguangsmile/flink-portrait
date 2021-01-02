package com.zhangbao.portrait.map;

import com.zhangbao.portrait.entity.UserGroupInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangbao
 * @date 2020/11/15 21:24
 **/
public class UserGroupMap implements MapFunction<String, UserGroupInfo> {
    @Override
    public UserGroupInfo map(String s) throws Exception {
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

        UserGroupInfo userGroupInfo = new UserGroupInfo();

        userGroupInfo.setUserId(userId);
        userGroupInfo.setCreateTime(createTime);
        userGroupInfo.setAmount(amount);
        userGroupInfo.setPayType(payType);
        userGroupInfo.setPayTime(payTime);
        userGroupInfo.setPayStatus(payStatus);
        userGroupInfo.setCouponAmount(couponAmount);
        userGroupInfo.setTotalAmount(totalAmount);
        userGroupInfo.setRefundAmount(refundAmount);
        userGroupInfo.setProductType(productType);
        userGroupInfo.setCount(Long.valueOf(productNum));
        userGroupInfo.setGroupField("userGroupInfo=="+userId);
        List<UserGroupInfo> list = new ArrayList<>();
        list.add(userGroupInfo);
        userGroupInfo.setList(list);
        return userGroupInfo;
    }

}
