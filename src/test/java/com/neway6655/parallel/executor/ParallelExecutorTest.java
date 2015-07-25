package com.neway6655.parallel.executor;

import com.google.common.collect.Lists;
import com.neway6655.parallel.executor.task.ParallelTask;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;

public class ParallelExecutorTest {

    private static Random random = new SecureRandom();
    private ParallelExecutor parallelExecutor;
    private int taskNums = 100;

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

        parallelExecutor = new ParallelExecutor(2);

        List<ParallelTask.TaskResult<String>> taskResults = parallelExecutor.parallelProcess(simpleTask1, simpleTask2);

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

        parallelExecutor = new ParallelExecutor(2, 10, TimeUnit.MILLISECONDS);

        List<ParallelTask.TaskResult<String>> taskResults = parallelExecutor.parallelProcess(simpleTask1, simpleTask2);

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

        parallelExecutor = new ParallelExecutor(2, 5, TimeUnit.SECONDS);

        long startTime = System.currentTimeMillis();
        List<ParallelTask.TaskResult<String>> taskResults = parallelExecutor.parallelProcess(simpleTask1, simpleTask2);

        long endTime = System.currentTimeMillis();
        System.out.println("Totally used: " + (endTime - startTime) + " in millseconds.");

        assertEquals(2, taskResults.size());
        System.out.println(taskResults.get(0).getResult());
        System.out.println(taskResults.get(1).getResult());
    }

    @Test
    public void testLargeTasksParallelProcessing(){
        List<ParallelTask> taskList = Lists.newArrayList();

        for (int i = 0; i < taskNums; i ++){
            taskList.add(new ParallelTask() {
                @Override
                protected Object process() throws InterruptedException {
                    Thread.sleep(100);

                    return random.nextInt();
                }
            });
        }

        parallelExecutor = new ParallelExecutor(100, 5, TimeUnit.SECONDS);

        ParallelTask[] parallelTasks = taskList.toArray(new ParallelTask[]{});

        long startTime = System.currentTimeMillis();
        parallelExecutor.parallelProcess(parallelTasks);
        System.out.println("Time cost(ms): " + (System.currentTimeMillis() - startTime));
    }
}
