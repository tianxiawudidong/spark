package com.ifchange.spark.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorFactory {

    public ExecutorFactory() {
    }

    public static ExecutorService newFixedThreadPool(int nThreads, int queue_size) {
        return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue(queue_size), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static ExecutorService newFixedThreadPool(int nThreads) {
        return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue(512), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static ExecutorService newSingleThreadExecutor(int queue_size) {
        return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue(queue_size), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static ExecutorService newSingleThreadExecutor() {
        return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue(50), new ThreadPoolExecutor.CallerRunsPolicy());
    }
}
