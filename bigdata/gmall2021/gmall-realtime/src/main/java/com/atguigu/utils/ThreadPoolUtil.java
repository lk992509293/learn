package com.atguigu.utils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {
    //声明java线程池
    private static ThreadPoolExecutor threadPoolExecutor;

    public ThreadPoolUtil() {
    }

    public static ThreadPoolExecutor getThreadPoolExecutor() {
        //单例模式，确保threadPoolExecutor不会被重复创建
        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(4,//线程池中的线程数量，它的数量决定了添加的任务是开辟新的线程去执行，还是放到workQueue任务队列中去
                            20,//指定了线程池中的最大线程数量，这个参数会根据你使用的workQueue任务队列的类型，决定线程池会开辟的最大线程数量；
                            60, //当线程池中空闲线程数量超过corePoolSize时，多余的线程会在多长时间内被销毁；
                            TimeUnit.SECONDS, //keepAliveTime的单位
                            new LinkedBlockingQueue<>() //任务队列，被添加到线程池中，但尚未被执行的任务
                    );
                }
            }
        }
        return threadPoolExecutor;
    }
}
