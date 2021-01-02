package com.zhangbao.portrait.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * HBase工具类
 * @author zhangbao
 * @date 2020/11/15 22:10
 **/
public class HBaseUtils {
    private static Logger logger = Logger.getLogger(HBaseUtils.class);
    private static Configuration conf;
    private static Connection conn;
    private static Properties prop;
    static {
        conf = HBaseConfiguration.create();
        prop = new Properties();
        try {
            prop.load(HBaseUtils.class.getResourceAsStream("/hbase.properties"));
            conf.set("hbase.rootdir",prop.getProperty("hbase_rootdir"));
            conf.set("hbase.zookeeper.quorum", prop.getProperty("quorum"));
            conf.set("hbase.client.scanner.timeout.period",prop.getProperty("timeout_period"));
            conf.set("hbase.rpc.timeout", prop.getProperty("hbase_rpc_timeout"));
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getdata(String tablename, String rowkey, String famliyname,String colum) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tablename));
        // 将字符串转换成byte[]
        byte[] rowkeybyte = Bytes.toBytes(rowkey);
        Get get = new Get(rowkeybyte);
        Result result =table.get(get);
        byte[] resultbytes = result.getValue(famliyname.getBytes(),colum.getBytes());
        if(resultbytes == null){
            return null;
        }

        return new String(resultbytes);
    }

    public static void putData(String tableName, String row, String columnFamily, String column, String data)
            throws IOException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf(tableName));
        try {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
            table.put(put);
        } finally {
            table.close();
            conn.close();
        }
    }


    /**
     * 插入数据，create "userflaginfo,"baseinfo"
     * create "tfidfdata,"baseinfo"
     */
    public static void put(String tablename, String rowkey, String famliyname, Map<String,String> datamap) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tablename));
        // 将字符串转换成byte[]
        byte[] rowkeybyte = Bytes.toBytes(rowkey);
        Put put = new Put(rowkeybyte);
        if(datamap != null){
            Set<Map.Entry<String,String>> set = datamap.entrySet();
            for(Map.Entry<String,String> entry : set){
                String key = entry.getKey();
                Object value = entry.getValue();
                put.addColumn(Bytes.toBytes(famliyname), Bytes.toBytes(key), Bytes.toBytes(value+""));
            }
        }
        table.put(put);
        table.close();
        System.out.println("ok");
    }
}
