import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.Queue;

public class MyFSDataInputStream extends FSDataInputStream {

    private BufferedReader reader;
    private Queue<String> cache;

    public MyFSDataInputStream(Path path, FSDataInputStream in) throws IOException {
        super(in);
        reader = new BufferedReader(new InputStreamReader(in));
        cache = new LinkedList<>();
    }

    public String readLine() throws IOException {
        // 如果缓存不为空，则先从缓存中读取数据
        if (!cache.isEmpty()) {
            return cache.poll();
        }

        // 从HDFS文件中读取一行数据
        String line = reader.readLine();

        // 如果读到文件末尾，则返回空
        if (line == null) {
            return null;
        }

        // 将读取的数据放入缓存
        cache.add(line);

        return line;
    }

    // 其他方法的实现留空或根据需要添加
}
