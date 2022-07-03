package com.echoxxzhang.mydb.backend.common;

import com.echoxxzhang.mydb.common.Error;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * DM 的功能其实可以归纳为两点：上层模块和文件系统之间的一个抽象层，
 * 向下直接读写文件，向上提供数据的包装；还有日志功能。
 * 这里采用的是引用计数缓存框架
 */
public abstract class AbstractCache<T> {

    private HashMap<Long, T> cache;                // 实际缓存的数据
    private HashMap<Long, Integer> references;     // 资源的引用个数
    private HashMap<Long, Boolean> getting;        // 正在被获取的资源

    private int maxResource;       // 缓存的最大缓存资源数,如果超出这个数则报错
    private int count = 0;         // 缓存中元素的个数
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    //
    protected T get(long key) throws Exception {
        while (true) {
            lock.lock();
            if (getting.containsKey(key)) { // 如果请求的资源正在被其他线程获取，进行自旋
                lock.unlock();
                try { // 不断自旋
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            if (cache.containsKey(key)) {
                // 缓存命中，直接返回
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1); // 更新引用个数
                lock.unlock();
                return obj;
            }

            // 缓存不命中的情况，尝试获取

            if (maxResource > 0 && count == maxResource) { // 检查是否超出最大资源鼠
                lock.unlock();
                throw Error.CacheFullException;
            }
            count++;
            getting.put(key, true); // 注册 正在引用

            lock.unlock();
            break;

        }

        // 线程拿到了执行资格，由当前线程进行加载资源
        T obj = null;
        try {
            obj = getForCache(key);

        } catch (Exception e) {
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();

        return obj;
    }

    // 当资源不在缓存时的获取行为 todo
    protected abstract T getForCache(long key) throws Exception;

    // 当资源被驱逐时的写回行为 todo
    protected abstract void releaseForCache(T obj);

    // 释放一个引用
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            if (ref == 0) { // 当引用计数是0时，释放该对象
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count--;
            } else {
                references.put(key, ref); // 不为0时，更新资源
            }
        } finally {
            lock.unlock();
        }
    }

    // 关闭缓存，写回所有资源
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }

}
