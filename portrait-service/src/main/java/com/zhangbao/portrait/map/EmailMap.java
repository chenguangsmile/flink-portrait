package com.zhangbao.portrait.map;

import com.zhangbao.portrait.entity.CarrierInfo;
import com.zhangbao.portrait.entity.EmailInfo;
import com.zhangbao.portrait.utils.CarrierUtils;
import com.zhangbao.portrait.utils.EmailUtils;
import com.zhangbao.portrait.utils.HBaseUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author zhangbao
 * @date 2020/11/15 21:24
 **/
public class EmailMap implements MapFunction<String, EmailInfo> {
    @Override
    public EmailInfo map(String s) throws Exception {
        if(StringUtils.isBlank(s)){
            return null;
        }
        String[] split = s.split(",");
        String userId = split[0];
        String userName = split[1];
        String password = split[2];
        String sex = split[3];
        String telphone = split[4];
        String email = split[5];
        String age = split[6];
        String registerTime = split[7];
        String userType = split[8];//用户类型，0：pc，1：移动，2：小程序
        String tableName = "userFlagInfo";
        String row = userId;
        String columnFamily = "baseInfo";
        String column = "emailInfo";

        String emailType = EmailUtils.getEmailtypeBy(email);//邮箱类型

        //将用户年代信息存入HBase中
        HBaseUtils.putData(tableName,row,columnFamily,column,emailType);
        EmailInfo emailInfo = new EmailInfo();
        emailInfo.setCount(1L);
        emailInfo.setEmailType(emailType);
        emailInfo.setGroupField("emailInfo==" + emailType);
        return emailInfo;
    }

}
