package com.zhangbao.portrait.reduce;

import com.zhangbao.portrait.entity.ChaoManWomenInfo;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author zhangbao
 * @date 2020/12/27 18:31
 **/
public class ChaoManAndWomenFinalReduce implements ReduceFunction<ChaoManWomenInfo> {
    @Override
    public ChaoManWomenInfo reduce(ChaoManWomenInfo chaoManWomenInfo, ChaoManWomenInfo t1) throws Exception {
        Long count1 = chaoManWomenInfo.getCount();
        Long count2 = t1.getCount();

        ChaoManWomenInfo finalInfo = new ChaoManWomenInfo();

        finalInfo.setChaoType(chaoManWomenInfo.getChaoType());
        finalInfo.setCount(count1 + count2);
        return finalInfo;
    }
}
