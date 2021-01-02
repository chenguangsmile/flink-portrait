package com.zhangbao.portrait.reduce;

import cn.hutool.core.util.StrUtil;
import com.zhangbao.portrait.entity.ConsumptionLevel;
import com.zhangbao.portrait.utils.HBaseUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author zhangbao
 * @date 2020/12/7 22:48
 **/
public class ConsumptionLevelReduce implements GroupReduceFunction<ConsumptionLevel, ConsumptionLevel> {
    @Override
    public void reduce(Iterable<ConsumptionLevel> iterable, Collector<ConsumptionLevel> collector) throws Exception {
        Iterator<ConsumptionLevel> iterator = iterable.iterator();
        int size = 0;
        Double sum = 0D;
        String userId = "-1";
        while (iterator.hasNext()){
            ConsumptionLevel next = iterator.next();
            userId = next.getUserId();
            sum += Double.valueOf(next.getTotalAmount());
            size++;
        }
        Double avrAmount = sum/size;//高消费5000 ，中水平1000-5000，低水平1000
        String flag = "low";
        if(avrAmount >=1000 && avrAmount < 5000){
            flag = "middle";
        }else if(avrAmount >=5000){
            flag = "high";
        }
        String tableName = "userFlagInfo";
        String row = userId+"";
        String columnFamily = "consumptionInfo";
        String column = "consumptionLevel";
        String data = HBaseUtils.getdata(tableName, row, columnFamily, column);
        if(StrUtil.isBlank(data)){
            ConsumptionLevel consumptionLevel = new ConsumptionLevel();
            consumptionLevel.setConsumptionType(flag);
            consumptionLevel.setGroupField("=consumptionType"+flag);
            consumptionLevel.setCount(1L);
            collector.collect(consumptionLevel);
        }else if(!data.equals(flag)){
            ConsumptionLevel consumptionLevel1 = new ConsumptionLevel();
            consumptionLevel1.setConsumptionType(data);
            consumptionLevel1.setCount(-1L);
            consumptionLevel1.setGroupField("=consumptionType"+data);
            collector.collect(consumptionLevel1);

            ConsumptionLevel consumptionLevel2 = new ConsumptionLevel();
            consumptionLevel2.setConsumptionType(flag);
            consumptionLevel2.setGroupField("=consumptionType"+flag);
            consumptionLevel2.setCount(1L);
            collector.collect(consumptionLevel2);
        }
        //将消费水平存入HBase中
        HBaseUtils.putData(tableName,row,columnFamily,column,flag);
    }
}
