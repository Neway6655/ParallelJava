package com.neway6655.parallel.framework;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

/**
 * Created by neway on 11/7/15.
 */
public abstract class ParallelTask<V> implements Callable<V> {

    private String taskId;

    private CountDownLatch startLatch;

    public ParallelTask() {
        taskId = UUID.randomUUID().toString();
    }

    @Override
    public V call() throws Exception {
        startLatch.countDown();
        return process();
    }

    public void setStartLatch(CountDownLatch startLatch) {
        this.startLatch = startLatch;
    }

    public String getTaskId() {
        return taskId;
    }

    abstract protected V process() throws InterruptedException;

}
