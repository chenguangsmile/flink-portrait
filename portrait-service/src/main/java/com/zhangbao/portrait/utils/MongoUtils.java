package com.zhangbao.portrait.utils;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

/**
 * @author zhangbao
 * @date 2020/11/16 23:36
 **/
public class MongoUtils {

    private static MongoClient mongoClient = new MongoClient("192.168.88.71",27017);


    /**
     * @param tablename 数据库表
     * @param database  数据库
     * @param yearbasetype  数据
     * @return
     */
    public static Document findoneby(String tablename, String database, String yearbasetype){
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        MongoCollection mongoCollection = mongoDatabase.getCollection(tablename);
        Document  doc = new Document();
        doc.put("info", yearbasetype);
        FindIterable<Document> itrer = mongoCollection.find(doc);
        MongoCursor<Document> mongocursor = itrer.iterator();
        if(mongocursor.hasNext()){
            return mongocursor.next();
        }else{
            return null;
        }
    }


    public static void saveorupdatemongo(String tablename,String database,Document doc) {
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        MongoCollection<Document> mongocollection = mongoDatabase.getCollection(tablename);
        if(!doc.containsKey("_id")){
            ObjectId objectid = new ObjectId();
            doc.put("_id", objectid);
            mongocollection.insertOne(doc);
            return;
        }
        Document matchDocument = new Document();
        String objectid = doc.get("_id").toString();
        matchDocument.put("_id", new ObjectId(objectid));
        FindIterable<Document> findIterable =  mongocollection.find(matchDocument);
        if(findIterable.iterator().hasNext()){
            mongocollection.updateOne(matchDocument, new Document("$set",doc));
            try {
                System.out.println("come into saveorupdatemongo ---- update---"+ JSONObject.toJSONString(doc));
            } catch (Exception e) {
// TODO Auto-generated catch block
                e.printStackTrace();
            }
        }else{
            mongocollection.insertOne(doc);
            try {
                System.out.println("come into saveorupdatemongo ---- insert---"+JSONObject.toJSONString(doc));
            }catch (Exception e) {
// TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

}
