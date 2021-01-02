package com.zhangbao.portrait.reduce;

import com.zhangbao.portrait.entity.BrandLike;
import com.zhangbao.portrait.entity.UserTypeInfo;
import com.zhangbao.portrait.utils.MongoUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;

/**
 * @author zhangbao
 * @date 2020/12/2 22:06
 **/
public class UserTypeSink implements SinkFunction<UserTypeInfo> {
    @Override
    public void invoke(UserTypeInfo value, Context context) throws Exception {
        String userType = value.getUserType();
        long count = value.getCount();
        Document doc = MongoUtils.findoneby("user-type-statics", "flink-portrait", userType);
        if(doc == null){
            doc = new Document();
            doc.put("info",userType);
            doc.put("count",count);
        }else {
            Long oldCount = doc.getLong("count");
            doc.put("count",oldCount + count);
        }
        MongoUtils.saveorupdatemongo("user-type-statics", "flink-portrait",doc);

    }
}
