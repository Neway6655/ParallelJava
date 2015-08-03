package com.neway6655.parallel.forkjoin;

import com.neway6655.parallel.forkjoin.task.ParallelTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Func2;

import java.util.List;
import java.util.concurrent.*;

/**
 * Created by neway on 3/8/15.
 */
public class ParallelExecutor<V> {

	private static final Logger logger = LoggerFactory.getLogger(ParallelExecutor.class);

	public static final int DEFAULT_TIMEOUT_IN_SEC = 5;

	public static final int SHUTDOWN_TIMEOUT_IN_SEC = 10;

	private ForkJoinPool forkJoinPool = new ForkJoinPool();

	public V parallelProcess(List<Callable<V>> taskList) {
		Func2 reduceFunc = new Func2() {
			@Override
			public Object call(Object o, Object o2) {
				System.out.println("result: " + o2);
				return o;
			}
		};
		ParallelTask<V> parallelTask = new ParallelTask(taskList, reduceFunc);

		ForkJoinTask<V> result = forkJoinPool.submit(parallelTask);
		try {
			return result.get(DEFAULT_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			// ignore interrupted exception.
		} catch (ExecutionException e) {
			logger.error("Error when executing tasks.", e);
		} catch (TimeoutException e) {
			logger.error("Executing tasks timeout.", e);
		}

		return null;
	}

	public void shutdown() throws InterruptedException {
		forkJoinPool.shutdown();

		if (!forkJoinPool.awaitTermination(SHUTDOWN_TIMEOUT_IN_SEC, TimeUnit.SECONDS)) {
			forkJoinPool.shutdownNow();
		}
	}

}
