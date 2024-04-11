package org.example;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class MyFSDataInputStream extends FSDataInputStream {

    private BufferedReader reader;
    private Map<Long, String> cache;
    private long currentPosition;
    private boolean fromCache;

    public MyFSDataInputStream(FileSystem fs, Path p) throws IOException {
        super(fs.open(p));
        this.reader = new BufferedReader(new InputStreamReader(super.in));
        this.cache = new HashMap<>();
        this.currentPosition = 0;
        this.fromCache = false; // 默认从文件流中读取
    }

    public String readline() throws IOException {
        String line = reader.readLine();
        if (line != null) {
            cache.put(currentPosition, line);
            currentPosition = getPos();
            fromCache = false;
        }
        return line;
    }

    @Override
    public int read() throws IOException {
        // 首先查找缓存
        if (cache.containsKey(currentPosition)) {
            int data = cache.get(currentPosition).charAt(0); // 从缓存中读取数据
            currentPosition++; // 更新当前位置
            fromCache = true; // 标记为从缓存中读取
            return data;
        }

        int data = in.read();
        if (data != -1) {
            cache.put(currentPosition++, String.valueOf((char) data));
            fromCache = false; // 标记为从文件流中读取
        }
        return data;
    }

    public int read_s(byte[] b, int off, int len, long position) throws IOException {
        // 首先查找缓存
        if (cache.containsKey(position)) {
            String cachedData = cache.get(position);
            byte[] cachedBytes = cachedData.getBytes();
            int bytesToRead = Math.min(len, cachedBytes.length);
            System.arraycopy(cachedBytes, 0, b, off, bytesToRead);
            fromCache = true;
            return bytesToRead;
        }
        // 缓存中没有数据，则从输入流读取
        int bytesRead = super.read(b, off, len);
        if (bytesRead > 0) {
            String data = new String(b, off, bytesRead);
            cache.put(position, data);
            fromCache = false;
        }
        return bytesRead;
    }


    @Override
    public void seek(long pos) throws IOException {
        super.seek(pos);
        currentPosition = pos;
    }

    public boolean isFromCache() {
        return fromCache;
    }

    public String getCachedData(long pos) {
        return cache.get(pos);
    }
    public Map<Long, String> getCache() {
        return cache;
    }
}

