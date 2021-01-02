package com.zhangbao.portrait.reduce;

import com.zhangbao.portrait.entity.UserTypeInfo;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author zhangbao
 * @date 2020/11/23 23:56
 **/
public class UserTypeReduce implements ReduceFunction<UserTypeInfo> {

    @Override
    public UserTypeInfo reduce(UserTypeInfo userTypeInfo, UserTypeInfo t1) throws Exception {
        UserTypeInfo result = new UserTypeInfo();
        result.setUserType(userTypeInfo.getUserType());
        result.setCount(userTypeInfo.getCount()+t1.getCount());
        return result;
    }
}
