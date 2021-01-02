package com.zhangbao.portrait.reduce;


import com.zhangbao.portrait.entity.ConsumptionLevel;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author zhangbao
 * @date 2020/11/15 23:48
 **/
public class ConsumptionLevelFinalReduce implements ReduceFunction<ConsumptionLevel> {
    @Override
    public ConsumptionLevel reduce(ConsumptionLevel consumptionLevel1, ConsumptionLevel t1) throws Exception {
        ConsumptionLevel consumptionLevel = new ConsumptionLevel();
        consumptionLevel.setConsumptionType(consumptionLevel1.getConsumptionType());

        consumptionLevel1.setCount(consumptionLevel1.getCount() + t1.getCount());
        return consumptionLevel1;
    }
}
