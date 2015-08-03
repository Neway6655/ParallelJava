package com.neway6655.parallel.executor;

import com.google.common.collect.Lists;
import com.neway6655.parallel.executor.task.ParallelTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

/**
 * Created by neway on 12/7/15.
 */
public class ParallelExecutor {

	private static final Logger logger = LoggerFactory.getLogger(ParallelExecutor.class);

	public static final int DEFAULT_TIMEOUT_IN_SEC = 5;

	public static final int SHUTDOWN_TIMEOUT_IN_SEC = 10;

	private ExecutorService parallelExecutorService;

	private ExecutorService collectResultExecutorService;

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

	public <T> List<ParallelTask.TaskResult<T>> parallelProcess(ParallelTask... parallelTasks) {
		CountDownLatch fetchResultLatch = new CountDownLatch(parallelTasks.length);

		List<ParallelTask.TaskResult<T>> resultList = Lists.newArrayList();

		List<Future> taskResultFutureList = startParallelTasks(parallelTasks);

		List<Future> collectResultFutureList = getTaskFutureResults(taskResultFutureList, fetchResultLatch);

		boolean finished = false;
		try {
			finished = fetchResultLatch.await(timeoutInMillSec, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			// ignore interruptedException.
		}
		if (!finished) {
            logger.error("Failed to fetch result from some task due to timeout.");
			return resultList;
		}

		for (Future<T> future : collectResultFutureList) {
			ParallelTask.TaskResult taskResult = new ParallelTask.TaskResult();
			try {
				taskResult.setResult(future.get());
                resultList.add(taskResult);
			} catch (InterruptedException e) {
				// ignore interrupted exception.
			} catch (ExecutionException e) {
				logger.error("Error occurred when execute task.", e);
                return Lists.newArrayList();
			}
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

	private <T> List<Future> getTaskFutureResults(List<Future> taskResultFutureList, CountDownLatch fetchResultLatch) {
		List<Future> collectResultFutureList = Lists.newArrayList();
		for (Future<T> future : taskResultFutureList) {
			collectResultFutureList.add(collectResultExecutorService.submit(new CollectTaskFutureResultTask(future,
					fetchResultLatch)));
		}

		return collectResultFutureList;
	}

	private <T> List<Future> startParallelTasks(ParallelTask... parallelTaskList) {
		List<Future> taskResultFutureList = Lists.newArrayList();

		for (ParallelTask task : parallelTaskList) {
			Future<T> taskFuture = parallelExecutorService.submit(task);
			taskResultFutureList.add(taskFuture);
		}

		return taskResultFutureList;
	}

	private class CollectTaskFutureResultTask<T> implements Callable<T> {

		private Future<T> future;

		private CountDownLatch fetchResultLatch;

		public CollectTaskFutureResultTask(Future<T> future, CountDownLatch fetchResultLatch) {
			this.future = future;
			this.fetchResultLatch = fetchResultLatch;
		}

		@Override
		public T call() {
			try {
				T result = future.get(timeoutInMillSec, TimeUnit.MILLISECONDS);
				fetchResultLatch.countDown();
				return result;
			} catch (InterruptedException e) {
				// ignore interrupted exception.
				return null;
			} catch (ExecutionException e) {
				logger.error("Error occurred when executing task.", e);
				future.cancel(true);
				return null;
			} catch (TimeoutException e) {
				logger.error("Failed to fetch task's execution result due to timeout.", e);
				future.cancel(true);
				return null;
			}
		}
	}
}
