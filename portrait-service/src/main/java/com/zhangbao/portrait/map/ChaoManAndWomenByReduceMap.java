package com.zhangbao.portrait.map;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.zhangbao.portrait.entity.ChaoManWomenInfo;
import com.zhangbao.portrait.utils.HBaseUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.Set;

/**
 * @author zhangbao
 * @date 2020/12/27 19:03
 **/
public class ChaoManAndWomenByReduceMap implements FlatMapFunction<ChaoManWomenInfo,ChaoManWomenInfo> {
    @Override
    public void flatMap(ChaoManWomenInfo chaoManWomenInfo, Collector<ChaoManWomenInfo> collector) throws Exception {
        Map<String,Long> pre = Maps.newHashMap();

        String rowKey = chaoManWomenInfo.getUserId();
        String chaoType = chaoManWomenInfo.getChaoType();
        long count = pre.get(chaoType)==null?0L:pre.get(chaoType);
        pre.put(chaoType,count+chaoManWomenInfo.getCount());
        String tableName = "userFlagInfo";
        String columnFamily = "userBehavior";
        String column = "chaoManAndWomen";
        String data = HBaseUtils.getdata(tableName, rowKey, columnFamily, column);
        if(StrUtil.isNotBlank(data)){
            Map<String, Long> dataMap = JSONObject.parseObject(data, Map.class);
            Set<String> strings = pre.keySet();
            for (String key : strings) {
                Long preCount = dataMap.get(key)==null?0L:dataMap.get(key);
                pre.put(key,pre.get(key) + preCount);
            }
        }
        if(!pre.isEmpty()){
            String chaoString = JSONObject.toJSONString(pre);
            HBaseUtils.putData(tableName,rowKey,columnFamily,column,chaoString);

            Long man = pre.get("1");
            Long women = pre.get("2");
            String flag = "women";
            Long finalCount = women;
            if(man>women){
                flag = "man";
                finalCount = man;
            }
            if(finalCount > 2000){
                column = "chaoType";
                ChaoManWomenInfo chaoManWomen = new ChaoManWomenInfo();
                chaoManWomen.setChaoType(flag);
                chaoManWomen.setCount(1L);
                chaoManWomen.setGroupField("chaoType=="+flag);
                String type = HBaseUtils.getdata(tableName, rowKey, columnFamily, column);
                if(StrUtil.isNotBlank(type) && !type.equals(flag)){
                    chaoManWomenInfo.setChaoType(type);
                    chaoManWomenInfo.setCount(-1L);
                    chaoManWomen.setGroupField("chaoType=="+type);
                }
                HBaseUtils.putData(tableName, rowKey, columnFamily, column,flag);
                collector.collect(chaoManWomen);
            }
        }
    }
}
