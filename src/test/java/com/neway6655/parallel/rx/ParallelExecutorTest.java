package com.neway6655.parallel.rx;

import com.google.common.collect.Lists;
import com.neway6655.parallel.rx.task.ParallelTask;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.List;
import java.util.Random;

import static junit.framework.Assert.assertEquals;

public class ParallelExecutorTest {

    private static Random random = new SecureRandom();

    private int taskNums = 100;

    @Test
    public void testParallelExecute(){
        ParallelTask task1 = new ParallelTask() {
            @Override
            protected Object process() {
                return "task1";
            }
        };

        ParallelTask task2 = new ParallelTask() {
            @Override
            protected Object process() {
                return "task2";
            }
        };

        List<Object> result = ParallelExecutor.parallelProcess(task1, task2);

        assertEquals(2, result.size());
    }

    @Test
    public void testTimeout(){
        ParallelTask longTask = new ParallelTask() {
            @Override
            protected Object process() throws InterruptedException {
                Thread.sleep(2000);
                return "long task";
            }
        };

        ParallelTask immediateTask = new ParallelTask() {
            @Override
            protected Object process() throws InterruptedException {
                return "immediate task";
            }
        };

        List<Object> result = ParallelExecutor.parallelProcess(1, longTask, immediateTask);

        assertEquals(0, result.size());
    }


    @Test
    public void testLargeTasksParallelProcessing(){
        List<ParallelTask> taskList = Lists.newArrayList();

        for(int i = 1; i< taskNums; i++){
            taskList.add(new ParallelTask() {
                @Override
                protected Object process() throws InterruptedException {
                    Thread.sleep(100);

                    return random.nextInt(taskNums);
                }
            });
        }

        ParallelTask[] tasks = taskList.toArray(new ParallelTask[]{});
        long startTime = System.currentTimeMillis();
        ParallelExecutor.parallelProcess(tasks);
        System.out.println("time cost(ms): " + (System.currentTimeMillis() - startTime));
    }

}
