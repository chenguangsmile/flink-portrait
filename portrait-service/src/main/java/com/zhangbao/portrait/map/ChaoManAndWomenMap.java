package com.zhangbao.portrait.map;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sun.imageio.plugins.common.ReaderUtil;
import com.typesafe.config.Config;
import com.zhangbao.log.ScanProductLog;
import com.zhangbao.portrait.entity.ChaoManWomenInfo;
import com.zhangbao.portrait.entity.UserTypeInfo;
import com.zhangbao.portrait.kafka.KafkaEvent;
import com.zhangbao.portrait.utils.HBaseUtils;
import com.zhangbao.portrait.utils.MapUtils;
import com.zhangbao.utils.ReadProperties;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * @author zhangbao
 * @date 2020/11/23 23:03
 **/
public class ChaoManAndWomenMap implements FlatMapFunction<KafkaEvent, ChaoManWomenInfo> {
    @Override
    public void flatMap(KafkaEvent kafkaEvent, Collector<ChaoManWomenInfo> collector) throws Exception {
        String word = kafkaEvent.getWord();
        ScanProductLog scanProductLog = JSONObject.parseObject(word, ScanProductLog.class);
        int userId = scanProductLog.getUserId();
        int productId = scanProductLog.getProductId();

        ChaoManWomenInfo chaoManWomenInfo = new ChaoManWomenInfo();
        chaoManWomenInfo.setUserId(userId+"");
        Config config = ReadProperties.Load("chaoType.properties");
        String chaoType = config.getString(productId + "");
        if(StrUtil.isNotBlank(chaoType)){
            chaoManWomenInfo.setChaoType(chaoType);
            chaoManWomenInfo.setCount(1L);
            chaoManWomenInfo.setGroupField("=groupField="+userId);
            List<ChaoManWomenInfo> list = Lists.newArrayList();
            list.add(chaoManWomenInfo);
            chaoManWomenInfo.setList(list);
            collector.collect(chaoManWomenInfo);
        }

    }
}
