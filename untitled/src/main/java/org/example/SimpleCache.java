package org.example;

import java.util.HashMap;
import java.util.Map;

public class SimpleCache<K, V> {

    private final Map<K, V> cache;
    private final long defaultTTL; // 默认过期时间，单位：毫秒

    public SimpleCache() {
        this.cache = new HashMap<>();
        this.defaultTTL = 0; // 默认不设置过期时间
    }

    public SimpleCache(long defaultTTL) {
        this.cache = new HashMap<>();
        this.defaultTTL = defaultTTL;
    }

    public synchronized void put(K key, V value) {
        put(key, value, defaultTTL);
    }

    public synchronized void put(K key, V value, long ttl) {
        long expirationTime = System.currentTimeMillis() + ttl;
        cache.put(key, value);
        // 如果设置了过期时间，将其保存下来
        if (ttl > 0) {
            cache.put(getExpirationKey(key), (V) Long.valueOf(expirationTime));
        }
    }

    public synchronized V get(K key) {
        V value = cache.get(key);
        if (value != null) {
            Long expirationTime = (Long) cache.get(getExpirationKey(key));
            if (expirationTime == null || expirationTime >= System.currentTimeMillis()) {
                return value;
            } else {
                // 如果缓存已经过期，则删除该项
                cache.remove(key);
                cache.remove(getExpirationKey(key));
                return null;
            }
        }
        return null;
    }

    public synchronized void remove(K key) {
        cache.remove(key);
        cache.remove(getExpirationKey(key));
    }

    public synchronized void clear() {
        cache.clear();
    }

    private K getExpirationKey(K key) {
        return (K) ("__expiration__" + key);
    }
}
