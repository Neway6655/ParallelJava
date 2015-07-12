package com.neway6655.parallel.framework;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;

public class ParallelExecutorTest {

    private ParallelExecutor<String> parallelExecutor;

    @Test
    public void testParalleExecutorSuccessfully() {
        ParallelTask<String> simpleTask1 = new ParallelTask<String>() {
            @Override
            protected String process() throws InterruptedException {
                return "1";
            }
        };

        ParallelTask<String> simpleTask2 = new ParallelTask<String>() {
            @Override
            protected String process() throws InterruptedException {
                return "2";
            }
        };

        parallelExecutor = new ParallelExecutor<String>(2);
        parallelExecutor.addTask(simpleTask1);
        parallelExecutor.addTask(simpleTask2);

        List<TaskResult<String>> taskResults = parallelExecutor.parallelProcess();

        assertEquals(2, taskResults.size());
        System.out.println(taskResults.get(0).getResult());
        System.out.println(taskResults.get(1).getResult());
    }

    @Test
    public void testParalleExecutorFailedByOneTaskTimeout() {
        ParallelTask<String> simpleTask1 = new ParallelTask<String>() {
            @Override
            protected String process() throws InterruptedException {
                Thread.sleep(20);
                return "1";
            }
        };

        ParallelTask<String> simpleTask2 = new ParallelTask<String>() {
            @Override
            protected String process() throws InterruptedException {
                return "2";
            }
        };

        parallelExecutor = new ParallelExecutor<String>(2, 10, TimeUnit.MILLISECONDS);
        parallelExecutor.addTask(simpleTask1);
        parallelExecutor.addTask(simpleTask2);

        List<TaskResult<String>> taskResults = parallelExecutor.parallelProcess();

        assertEquals(0, taskResults.size());
    }

    @Test
    public void testParallelExecutorSuccessfullWithSomeLongTask() {
        ParallelTask<String> simpleTask1 = new ParallelTask<String>() {
            @Override
            protected String process() throws InterruptedException {
                Thread.sleep(3000);
                return "1";
            }
        };

        ParallelTask<String> simpleTask2 = new ParallelTask<String>() {
            @Override
            protected String process() throws InterruptedException {
                return "2";
            }
        };

        parallelExecutor = new ParallelExecutor<String>(2, 5, TimeUnit.SECONDS);
        parallelExecutor.addTask(simpleTask1);
        parallelExecutor.addTask(simpleTask2);

        long startTime = System.currentTimeMillis();
        List<TaskResult<String>> taskResults = parallelExecutor.parallelProcess();

        long endTime = System.currentTimeMillis();
        System.out.println("Totally used: " + (endTime - startTime) + " in millseconds.");

        assertEquals(2, taskResults.size());
        System.out.println(taskResults.get(0).getResult());
        System.out.println(taskResults.get(1).getResult());
    }
}
