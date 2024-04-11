package org.example;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.bson.Document;

public class MongoDBExample {
    public static void main(String[] args) {
        // 连接到 MongoDB 服务器
        try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {
            // 选择数据库
            MongoDatabase database = mongoClient.getDatabase("student");

            // 获取集合
            MongoCollection<Document> collection = database.getCollection("students");

            // 向集合中添加记录
            Document document = new Document("name", "scofield")
                    .append("score", new Document("English", 45)
                            .append("Math", 89)
                            .append("Computer", 100));
            collection.insertOne(document);

            // 获取 Scofield 的所有成绩信息
            Document scofieldDocument = collection.find(Filters.eq("name", "scofield"))
                    .projection(Projections.fields(Projections.excludeId(), Projections.include("score")))
                    .first();
            System.out.println("Scofield's scores: " + scofieldDocument.get("score"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
