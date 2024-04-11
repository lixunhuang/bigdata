package org.example;

import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

public class RedisExample {
    public static void main(String[] args) {
        // 连接到 Redis 服务器
        Jedis jedis = new Jedis("localhost", 6379);


        // 向 Student 表中添加记录
        Map<String, String> scoreMap = new HashMap<>();
        scoreMap.put("English", "45");
        scoreMap.put("Math", "89");
        scoreMap.put("Computer", "100");
        jedis.hmset("scofield", scoreMap);

        // 获取 scofield 的 English 成绩信息
        String englishScore = jedis.hget("scofield", "English");
        System.out.println("Scofield's English Score: " + englishScore);

        // 关闭连接
        jedis.close();
    }
}
