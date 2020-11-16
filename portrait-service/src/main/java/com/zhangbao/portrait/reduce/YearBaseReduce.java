package com.zhangbao.portrait.reduce;


import com.zhangbao.portrait.entity.YearBase;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author zhangbao
 * @date 2020/11/15 23:48
 **/
public class YearBaseReduce implements ReduceFunction<YearBase> {
    @Override
    public YearBase reduce(YearBase yearBase, YearBase t1) throws Exception {
        YearBase finalYearBase = new YearBase();
        finalYearBase.setYearType(yearBase.getYearType());
        finalYearBase.setCount(yearBase.getCount() + t1.getCount());
        return finalYearBase;
    }
}
