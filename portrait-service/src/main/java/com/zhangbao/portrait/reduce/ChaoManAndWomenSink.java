package com.zhangbao.portrait.reduce;

import com.zhangbao.portrait.entity.ChaoManWomenInfo;
import com.zhangbao.portrait.utils.MongoUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;


/**
 * @author zhangbao
 * @date 2020/12/27 18:49
 **/
public class ChaoManAndWomenSink implements SinkFunction<ChaoManWomenInfo> {
    @Override
    public void invoke(ChaoManWomenInfo value, Context context) throws Exception {
        String chaoType = value.getChaoType();
        long count = value.getCount();
        Document doc = MongoUtils.findoneby("brand-like-statics", "flink-portrait", chaoType);
        if(doc == null){
            doc = new Document();
            doc.put("info",chaoType);
            doc.put("count",count);
        }else {
            Long oldCount = doc.getLong("count");
            doc.put("count",oldCount + count);
        }
        MongoUtils.saveorupdatemongo("brand-like-statics", "flink-portrait",doc);
    }
}
