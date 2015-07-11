package com.neway6655.parallel.framework;

/**
 * Created by neway on 12/7/15.
 */
public class TaskResult<T> {

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
