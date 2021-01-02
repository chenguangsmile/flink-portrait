package com.zhangbao.portrait.map;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.zhangbao.log.ScanProductLog;
import com.zhangbao.portrait.entity.BrandLike;
import com.zhangbao.portrait.kafka.KafkaEvent;
import com.zhangbao.portrait.utils.HBaseUtils;
import com.zhangbao.portrait.utils.MapUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @author zhangbao
 * @date 2020/11/23 23:03
 **/
public class BrandLikeMap implements FlatMapFunction<KafkaEvent, BrandLike> {
    @Override
    public void flatMap(KafkaEvent kafkaEvent, Collector<BrandLike> collector) throws Exception {
        String word = kafkaEvent.getWord();
        ScanProductLog scanProductLog = JSONObject.parseObject(word, ScanProductLog.class);
        int userId = scanProductLog.getUserId();
        String brand = scanProductLog.getBrand();

        String tableName = "userFlagInfo";
        String row = userId+"";
        String columnFamily = "userBehavior";
        String column = "brandList";//品牌浏览列表
        String dataMap = HBaseUtils.getdata(tableName, row, columnFamily, column);
        Map<String,Long> map = Maps.newHashMap();
        if(StrUtil.isNotBlank(dataMap)){
            map = JSONObject.parseObject(dataMap, Map.class);
        }
        //获取之前品牌偏好
        String preMaxBrand = MapUtils.getMaxBrandLike(map);
        Long oldCount = map.get(brand)==null?0L:map.get(brand);
        map.put(brand,oldCount + 1);
        HBaseUtils.putData(tableName,row,columnFamily,column,JSONObject.toJSONString(map));

        //获取最大品牌偏好
        String maxBrand = MapUtils.getMaxBrandLike(map);
        if(StrUtil.isNotBlank(preMaxBrand) && !preMaxBrand.equals(maxBrand)){
            BrandLike brandLike = new BrandLike();
            brandLike.setBrand(preMaxBrand);
            brandLike.setCount(-1L);
            brandLike.setGroupField("=groupField="+preMaxBrand);
            collector.collect(brandLike);
        }
        BrandLike brandLike = new BrandLike();
        brandLike.setBrand(maxBrand);
        brandLike.setCount(1L);
        brandLike.setGroupField("=groupField="+maxBrand);
        collector.collect(brandLike);
        column = "brandLike";//品牌偏好
        HBaseUtils.putData(tableName, row, columnFamily, column,maxBrand);
    }
}
