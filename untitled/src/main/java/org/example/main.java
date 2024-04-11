package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;

public class main {
    public static void main(String[] args) {
        String hdfsUri = "hdfs://localhost:9000"; // HDFS 的本地地址
        String filePath = "/user/1.txt"; // 要读取的文件路径

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);

        try {
            FileSystem fs = FileSystem.get(conf);
            Path file = new Path(filePath);

            // 调用功能1：按行读取文件内容
            System.out.println("Calling function 1: Read lines from file");
            MyFSDataInputStream myStream1 = new MyFSDataInputStream(fs, file);
            String line;
            line = myStream1.readline();
            System.out.println(line);
            System.out.println("\nFunction 1 from cache: " + myStream1.isFromCache()); // 查询功能1是否来自缓存
            // System.out.println("\nFunction 1 from cache: " + myStream1.isFromCache()); // 查询功能2是否来自缓存
            myStream1.close();

            // 调用功能2：缓存数据
            System.out.println("\nCalling function 2: Cache data");
            MyFSDataInputStream myStream2 = new MyFSDataInputStream(fs, file);
            byte[] buffer = new byte[20];
            int bytesRead;
            int position = 0;
            while ((bytesRead = myStream2.read_s(buffer, 0, buffer.length, position++)) != -1) {
                // 在这里处理数据，例如缓存
                // 以下示例仅打印读取的内容
                String data = new String(buffer, 0, bytesRead);
                System.out.print(data);
            }

            bytesRead = myStream2.read_s(buffer, 0, buffer.length, 0);
            String data = new String(buffer, 0, bytesRead);
            System.out.println("\n\n");
            System.out.print(data);
            System.out.println("\nFunction 2 from cache: " + myStream2.isFromCache()); // 查询功能2是否来自缓存

            myStream2.close();

            // 获取缓存中的数据
            Map<Long, String> cacheData = myStream2.getCache();
            System.out.println("\nCached Data:");
            for (Map.Entry<Long, String> entry : cacheData.entrySet()) {
                System.out.println("Position: " + entry.getKey() + ", Data: " + entry.getValue());
            }

            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
