package com.neway6655.parallel.forkjoin.task;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Func2;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.RecursiveTask;

/**
 * Created by neway on 3/8/15.
 */
public class ParallelTask<V> extends RecursiveTask<V>{

    private Logger logger = LoggerFactory.getLogger(ParallelTask.class);

    private List<Callable<V>> taskList;

    private Func2<V, V, V> reduceFunc;

    public ParallelTask(List<Callable<V>> taskList, Func2<V, V, V> reduceFunc){
        this.taskList = taskList;
        this.reduceFunc = reduceFunc;
    }

    @Override
    protected V compute() {
        if (taskList.size() == 1){
            try {
                System.out.println(Thread.currentThread().getName());
                return taskList.get(0).call();
            } catch (Exception e) {
                logger.error("Failed to call task.",e);
                return null;
            }
        }

        List<ParallelTask<V>> parallelTasks = Lists.newArrayList();
        for (Callable<V> task: taskList){
            ParallelTask parallelTask = new ParallelTask(Lists.newArrayList(task), null);
            parallelTask.fork();

            parallelTasks.add(parallelTask);

        }

        V result = null;
        for (ParallelTask<V> parallelTask : parallelTasks){
            V taskResult = parallelTask.join();
            result = reduceFunc.call(result, taskResult);
        }

        return result;
    }
}
