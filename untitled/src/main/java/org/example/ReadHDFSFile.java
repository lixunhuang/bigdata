package org.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

public class ReadHDFSFile {

    public static void main(String[] args) {
        // 注册Hadoop提供的URLStreamHandlerFactory
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

        String hdfsFile = "hdfs://localhost:9000/user/1.txt"; // 指定HDFS中的文件路径

        try {
            // 使用URL打开HDFS文件流
            URL url = new URL(hdfsFile);
            URLConnection connection = url.openConnection();
            InputStream inputStream = connection.getInputStream();

            // 从文件流中读取文本并输出到终端
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

            // 关闭流
            reader.close();
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
