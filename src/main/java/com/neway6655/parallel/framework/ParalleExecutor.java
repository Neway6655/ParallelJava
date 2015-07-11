package com.neway6655.parallel.framework;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

/**
 * Created by neway on 12/7/15.
 */
public class ParalleExecutor<T> {

    private static final Logger logger = LoggerFactory.getLogger(ParalleExecutor.class);

    private List<ParallelTask> parallelTaskList = Lists.newArrayList();

    private List<TaskResult<T>> resultList = Lists.newArrayList();

    private ExecutorService executorService;

    private CountDownLatch taskDoneLatch;

    private CountDownLatch startLatch;

    private ExecutorService collectResultExecutorService;

    public ParalleExecutor(int parallelThreads) {
        executorService = Executors.newFixedThreadPool(parallelThreads);
        collectResultExecutorService = Executors.newFixedThreadPool(parallelThreads);
    }

    public void addTask(ParallelTask task) {
        parallelTaskList.add(task);
    }

    public List<TaskResult<T>> parallelProcess() {
        initCountDoneLatch();

        List<Future> taskFutureList = Lists.newArrayList();

        for (ParallelTask task : parallelTaskList) {
            task.setStartLatch(startLatch);
            Future<T> taskFuture = executorService.submit(task);
            taskFutureList.add(taskFuture);
        }

        try {
            startLatch.await(10, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        List<Future> collectResultFutureList = Lists.newArrayList();
        for (Future<T> future : taskFutureList) {
            collectResultFutureList.add(collectResultExecutorService.submit(new CollectTaskFutureResultTask(future, taskDoneLatch)));
        }

        try {
            taskDoneLatch.await(10, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (taskDoneLatch.getCount() == 0) {
            for (Future<T> future : collectResultFutureList) {
                TaskResult taskResult = new TaskResult();
                try {
                    taskResult.setResult(future.get(10, TimeUnit.MILLISECONDS));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    logger.error("Error occurred when execute task.", e);
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }
                resultList.add(taskResult);
            }
        }

        if (!isAllTaskCompleted()) {
            resultList.clear();
        }

        return resultList;
    }

    private boolean isAllTaskCompleted() {
        return resultList.size() == parallelTaskList.size();
    }

    private void initCountDoneLatch() {
        int taskCount = parallelTaskList.size();
        startLatch = new CountDownLatch(taskCount);
        taskDoneLatch = new CountDownLatch(taskCount);
    }

    private class CollectTaskFutureResultTask implements Callable<T> {

        private Future<T> future;

        private CountDownLatch countDownLatch;

        public CollectTaskFutureResultTask(Future<T> future, CountDownLatch countDownLatch) {
            this.future = future;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public T call() {
            try {
                countDownLatch.countDown();
                T result = future.get(1, TimeUnit.SECONDS);
                return result;
            } catch (InterruptedException e) {
                e.printStackTrace();
                return null;
            } catch (ExecutionException e) {
                e.printStackTrace();
                return null;
            } catch (TimeoutException e) {
                future.cancel(true);
                return null;
            }
        }
    }
}
