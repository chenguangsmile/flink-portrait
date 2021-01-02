package com.zhangbao.portrait.reduce;

import com.zhangbao.portrait.entity.BrandLike;
import com.zhangbao.portrait.utils.MongoUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;

/**
 * @author zhangbao
 * @date 2020/12/2 22:06
 **/
public class BrandLikeSink implements SinkFunction<BrandLike> {
    @Override
    public void invoke(BrandLike value, Context context) throws Exception {
        String brand = value.getBrand();
        long count = value.getCount();
        Document doc = MongoUtils.findoneby("brand-like-statics", "flink-portrait", brand);
        if(doc == null){
            doc = new Document();
            doc.put("info",brand);
            doc.put("count",count);
        }else {
            Long oldCount = doc.getLong("count");
            doc.put("count",oldCount + count);
        }
        MongoUtils.saveorupdatemongo("brand-like-statics", "flink-portrait",doc);

    }
}
