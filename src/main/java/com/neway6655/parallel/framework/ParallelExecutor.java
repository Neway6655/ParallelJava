package com.neway6655.parallel.framework;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

/**
 * Created by neway on 12/7/15.
 */
public class ParallelExecutor<T> {

    private static final Logger logger = LoggerFactory.getLogger(ParallelExecutor.class);

    public static final int DEFAULT_TIMEOUT_IN_SEC = 5;

    private List<ParallelTask> parallelTaskList = Lists.newArrayList();

    private List<TaskResult<T>> resultList = Lists.newArrayList();

    private ExecutorService executorService;

    private CountDownLatch taskDoneLatch;

    private CountDownLatch startLatch;

    private ExecutorService collectResultExecutorService;

    private long timeoutInMillSec;

    public ParallelExecutor(int parallelThreads){
        this(parallelThreads, DEFAULT_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
    }

    public ParallelExecutor(int parallelThreads, long timeout, TimeUnit timeUnit) {
        if (timeout <= 0) {
            throw new IllegalArgumentException("timeout must be a positive value.");
        }
        timeoutInMillSec = timeUnit.toMillis(timeout);
        executorService = Executors.newFixedThreadPool(parallelThreads);
        collectResultExecutorService = Executors.newFixedThreadPool(parallelThreads);
    }

    public void addTask(ParallelTask task) {
        parallelTaskList.add(task);
    }

    public List<TaskResult<T>> parallelProcess() {
        initCountDoneLatch();

        List<Future> taskResultFutureList = Lists.newArrayList();

        for (ParallelTask task : parallelTaskList) {
            task.setStartLatch(startLatch);
            Future<T> taskFuture = executorService.submit(task);
            taskResultFutureList.add(taskFuture);
        }

        try {
            boolean started = startLatch.await(10, TimeUnit.MILLISECONDS);
            if (!started) {
                logger.warn("Some task has not started yet.");
            }
        } catch (InterruptedException e) {
            // ignore interrupted exception.
        }

        List<Future> collectResultFutureList = Lists.newArrayList();
        for (Future<T> future : taskResultFutureList) {
            collectResultFutureList.add(collectResultExecutorService.submit(new CollectTaskFutureResultTask(future, taskDoneLatch)));
        }

        try {
            boolean collectionStarted = taskDoneLatch.await(10, TimeUnit.MILLISECONDS);
            if (!collectionStarted) {
                logger.warn("Some task future result has not started to be collected yet.");
            }
        } catch (InterruptedException e) {
            // ignore interrupted exception.
        }

        if (taskDoneLatch.getCount() == 0) {
            for (Future<T> future : collectResultFutureList) {
                TaskResult taskResult = new TaskResult();
                try {
                    taskResult.setResult(future.get(timeoutInMillSec, TimeUnit.MILLISECONDS));
                } catch (InterruptedException e) {
                    // ignore interrupted exception.
                } catch (ExecutionException e) {
                    logger.error("Error occurred when execute task.", e);
                } catch (TimeoutException e) {
                    logger.error("Timeout to fetch task's execution result.", e);
                }

                if (taskResult.getResult() != null) {
                    resultList.add(taskResult);
                }
            }
        } else {
            logger.warn("Some task's result has not been collected due to task operation timeout.");
        }

        if (!isAllTaskCompleted()) {
            logger.error("Some task's execution operation timeout, give up all task's execution result, please retry again.");
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
                T result = future.get(timeoutInMillSec, TimeUnit.MILLISECONDS);
                return result;
            } catch (InterruptedException e) {
                // ignore interrupted exception.
                return null;
            } catch (ExecutionException e) {
                logger.error("Error occurred when executing task.", e);
                return null;
            } catch (TimeoutException e) {
                future.cancel(true);
                logger.error("Failed to fetch task's execution result due to timeout.", e);
                return null;
            }
        }
    }
}