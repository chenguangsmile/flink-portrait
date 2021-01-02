package com.zhangbao.portrait.reduce;

import com.zhangbao.portrait.entity.ChaoManWomenInfo;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.List;

/**
 * @author zhangbao
 * @date 2020/12/27 18:46
 **/
public class ChaoManAndWomenReduce implements ReduceFunction<ChaoManWomenInfo> {
    @Override
    public ChaoManWomenInfo reduce(ChaoManWomenInfo t1, ChaoManWomenInfo t2) throws Exception {
        String userId = t1.getUserId();
        List<ChaoManWomenInfo> list1 = t1.getList();
        List<ChaoManWomenInfo> list2 = t2.getList();
        list1.addAll(list2);
        ChaoManWomenInfo finalInfo = new ChaoManWomenInfo();
        finalInfo.setUserId(userId);
        finalInfo.setList(list1);
        return finalInfo;
    }
}
