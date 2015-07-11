package com.neway6655.parallel.framework;

import org.junit.Test;

import java.util.List;

import static junit.framework.Assert.assertEquals;

public class ParalleExecutorTest {

    private ParalleExecutor<String> paralleExecutor;

    @Test
    public void testParalleExecutorSuccessfully(){
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

        paralleExecutor = new ParalleExecutor<String>(2);
        paralleExecutor.addTask(simpleTask1);
        paralleExecutor.addTask(simpleTask2);

        List<TaskResult<String>> taskResults = paralleExecutor.parallelProcess();

        assertEquals(2, taskResults.size());
        System.out.println(taskResults.get(0).getResult());
        System.out.println(taskResults.get(1).getResult());
    }
}
