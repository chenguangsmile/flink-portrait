package com.zhangbao.controller;

import com.alibaba.fastjson.JSONObject;
import com.zhangbao.entity.ResultData;
import com.zhangbao.log.AttentionProductLog;
import com.zhangbao.log.BuyCarProductLog;
import com.zhangbao.log.CollectProductLog;
import com.zhangbao.log.ScanProductLog;
import com.zhangbao.utils.IpUtils;
import com.zhangbao.utils.ReadProperties;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.Date;

/**
 * @author zhangbao
 * @date 2020/11/22 0:11
 **/
@RestController
@RequestMapping("/infoLog")
public class InfoController {

    private final String attentionProductLogTopic = ReadProperties.getKey("attentionProductLog");
    private final String buyCarProductLogTopic = ReadProperties.getKey("buyCarProductLog");
    private final String collectProductLogTopic = ReadProperties.getKey("collectProductLog");
    private final String scanProductLogTopic = ReadProperties.getKey("scanProductLog");

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @RequestMapping("/helloWorld")
    public ResultData helloWorld(HttpServletRequest request){
        String realIp = IpUtils.getRealIp(request);
        return ResultData.ok("hello:"+realIp);
    }

    /**
     * AttentionProductLog:{'productId':'','productType':''...}
     * BuyCarProductLog:{'productId':'','productType':''...}
     * CollectProductLog:{'productId':'','productType':''...}
     * ScanProductLog:{'productId':'','productType':''...}
     * @param receiveLog
     * @return
     */
    @PostMapping("/receiveLog")
    public ResultData receiveLog(String receiveLog,HttpServletRequest request){

        if(StringUtils.isBlank(receiveLog)){
            return ResultData.error("数据为空");
        }
        String[] split = receiveLog.split(":", 2);
        String className = split[0];
        String data = split[1];
        String msg = "";
        if ("AttentionProductLog".equals(className)){
            AttentionProductLog attentionProductLog = JSONObject.parseObject(data, AttentionProductLog.class);
            msg = JSONObject.toJSONString(attentionProductLog);
            kafkaTemplate.send(attentionProductLogTopic,msg+"##1##"+new Date().getTime());
        }else if("BuyCarProductLog".equals(className)) {
            BuyCarProductLog buyCarProductLog = JSONObject.parseObject(data, BuyCarProductLog.class);
            msg = JSONObject.toJSONString(buyCarProductLog);
            kafkaTemplate.send(buyCarProductLogTopic,msg+"##1##"+new Date().getTime());
        }else if("CollectProductLog".equals(className)) {
            CollectProductLog collectProductLog = JSONObject.parseObject(data, CollectProductLog.class);
            msg = JSONObject.toJSONString(collectProductLog);
            kafkaTemplate.send(collectProductLogTopic,msg+"##1##"+new Date().getTime());
        }else if("ScanProductLog".equals(className)) {
            ScanProductLog scanProductLog = JSONObject.parseObject(data, ScanProductLog.class);
            msg = JSONObject.toJSONString(scanProductLog);
            kafkaTemplate.send(scanProductLogTopic,msg+"##1##"+new Date().getTime());
        }

        return ResultData.ok(msg);
    }

}
