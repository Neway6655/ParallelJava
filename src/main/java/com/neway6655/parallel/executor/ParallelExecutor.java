package com.neway6655.parallel.executor;

import com.google.common.collect.Lists;
import com.neway6655.parallel.framework.ParallelTask;
import com.neway6655.parallel.framework.TaskResult;
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

    public static final int SHUTDOWN_TIMEOUT_IN_SEC = 10;

    private ExecutorService parallelExecutorService;

    private ExecutorService collectResultExecutorService;

    private CountDownLatch taskStartLatch;

    private CountDownLatch taskFinishLatch;

    private long timeoutInMillSec;

    public ParallelExecutor(int parallelThreads) {
        this(parallelThreads, DEFAULT_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
    }

    public ParallelExecutor(int parallelThreads, long timeout, TimeUnit timeUnit) {
        if (timeout <= 0) {
            throw new IllegalArgumentException("timeout must be a positive value.");
        }
        timeoutInMillSec = timeUnit.toMillis(timeout);
        parallelExecutorService = Executors.newFixedThreadPool(parallelThreads);
        collectResultExecutorService = Executors.newFixedThreadPool(parallelThreads);
    }

    public List<TaskResult<T>> parallelProcess(ParallelTask... parallelTasks) {
        List<TaskResult<T>> resultList = Lists.newArrayList();

        initCountDoneLatch(parallelTasks.length);

        List<Future> taskResultFutureList = startParallelTasks(parallelTasks);

        List<Future> collectResultFutureList = getTaskFutureResults(taskResultFutureList);

        if (taskFinishLatch.getCount() == 0) {
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

        if (!isAllTaskCompleted(resultList.size(), parallelTasks.length)) {
            logger.error("Some task's execution operation timeout, give up all task's execution result, please retry again.");
            resultList.clear();
        }

        return resultList;
    }

    public void shutdown() throws InterruptedException {
        parallelExecutorService.shutdown();
        if (!parallelExecutorService.awaitTermination(SHUTDOWN_TIMEOUT_IN_SEC, TimeUnit.SECONDS)) {
            parallelExecutorService.shutdownNow();
        }

        collectResultExecutorService.shutdown();
        if (!collectResultExecutorService.awaitTermination(SHUTDOWN_TIMEOUT_IN_SEC, TimeUnit.SECONDS)) {
            collectResultExecutorService.shutdownNow();
        }
    }


    private void initCountDoneLatch(int taskNum) {
        taskStartLatch = new CountDownLatch(taskNum);
        taskFinishLatch = new CountDownLatch(taskNum);
    }

    private List<Future> getTaskFutureResults(List<Future> taskResultFutureList) {
        List<Future> collectResultFutureList = Lists.newArrayList();
        for (Future<T> future : taskResultFutureList) {
            collectResultFutureList.add(collectResultExecutorService.submit(new CollectTaskFutureResultTask(future, taskFinishLatch)));
        }

        try {
            boolean collectionStarted = taskFinishLatch.await(10, TimeUnit.MILLISECONDS);
            if (!collectionStarted) {
                logger.warn("Some task future result has not started to be collected yet.");
            }
        } catch (InterruptedException e) {
            // ignore interrupted exception.
        }
        return collectResultFutureList;
    }

    private List<Future> startParallelTasks(ParallelTask... parallelTaskList) {
        List<Future> taskResultFutureList = Lists.newArrayList();

        for (ParallelTask task : parallelTaskList) {
            task.setStartLatch(taskStartLatch);
            Future<T> taskFuture = parallelExecutorService.submit(task);
            taskResultFutureList.add(taskFuture);
        }

        try {
            boolean started = taskStartLatch.await(10, TimeUnit.MILLISECONDS);
            if (!started) {
                logger.warn("Some task has not started yet.");
            }
        } catch (InterruptedException e) {
            // ignore interrupted exception.
        }

        return taskResultFutureList;
    }

    private boolean isAllTaskCompleted(int resultSize, int taskNum) {
        return resultSize == taskNum;
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
                future.cancel(true);
                return null;
            } catch (TimeoutException e) {
                future.cancel(true);
                logger.error("Failed to fetch task's execution result due to timeout.", e);
                return null;
            }
        }
    }
}
