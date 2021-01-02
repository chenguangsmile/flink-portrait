package com.zhangbao.portrait.reduce;


import com.zhangbao.portrait.entity.UserGroupInfo;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangbao
 * @date 2020/11/18 23:19
 **/
public class UserGroupReduce implements ReduceFunction<UserGroupInfo> {

    @Override
    public UserGroupInfo reduce(UserGroupInfo userGroupInfo, UserGroupInfo t1) throws Exception {
        List<UserGroupInfo> list1 = userGroupInfo.getList();
        List<UserGroupInfo> list2 = t1.getList();
        UserGroupInfo finalGroupInfo = new UserGroupInfo();
        List<UserGroupInfo> finalList = new ArrayList<>();
        finalList.addAll(list1);
        finalList.addAll(list2);
        finalGroupInfo.setList(finalList);
        return finalGroupInfo;
    }
}
