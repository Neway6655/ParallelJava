package com.neway6655.parallel.rx;

import com.google.common.collect.Lists;
import com.neway6655.parallel.rx.task.ParallelTask;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.List;
import java.util.Random;

public class ParallelExecutorTest {

    private static Random random = new SecureRandom();

    private int taskNums = 100;

    @Test
    public void testParallelExecute(){
        ParallelTask task1 = new ParallelTask() {
            @Override
            protected Object process() {
                return 1;
            }
        };

        ParallelTask task2 = new ParallelTask() {
            @Override
            protected Object process() {
                return 2;
            }
        };

        ParallelExecutor.parallelProcess(task1, task2);
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
