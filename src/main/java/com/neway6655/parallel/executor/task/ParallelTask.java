package com.neway6655.parallel.executor.task;

import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Created by neway on 11/7/15.
 */
public abstract class ParallelTask<V> implements Callable<V> {

    private String taskId;

    public ParallelTask() {
        taskId = UUID.randomUUID().toString();
    }

    @Override
    public V call() throws Exception {
        return process();
    }

    public String getTaskId() {
        return taskId;
    }

    abstract protected V process() throws InterruptedException;

    /**
     * Created by neway on 12/7/15.
     */
    public static class TaskResult<T> {

        private String taskId;

        private T result;

        public TaskResult() {
        }

        public String getTaskId() {
            return taskId;
        }

        public void setTaskId(String taskId) {
            this.taskId = taskId;
        }

        public T getResult() {
            return result;
        }

        public void setResult(T result) {
            this.result = result;
        }
    }
}
