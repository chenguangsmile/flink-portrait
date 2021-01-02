package com.zhangbao.portrait.reduce;


import com.google.common.collect.Lists;
import com.zhangbao.portrait.entity.BaiJiaInfo;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.List;

/**
 * @author zhangbao
 * @date 2020/11/18 23:19
 **/
public class BaiJiaReduce implements ReduceFunction<BaiJiaInfo> {

    @Override
    public BaiJiaInfo reduce(BaiJiaInfo baiJiaInfo, BaiJiaInfo t1) throws Exception {
        List<BaiJiaInfo> list = Lists.newArrayList();
        list.addAll(baiJiaInfo.getList());
        list.addAll(t1.getList());

        BaiJiaInfo result= new BaiJiaInfo();
        result.setUserId(baiJiaInfo.getUserId());
        result.setList(list);
        return result;
    }
}
