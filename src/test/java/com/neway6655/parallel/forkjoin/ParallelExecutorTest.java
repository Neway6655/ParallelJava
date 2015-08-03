package com.neway6655.parallel.forkjoin;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.concurrent.Callable;

public class ParallelExecutorTest {

    @Test
    public void testParallelExecutor(){
        Callable<String> task1 = new Callable<String>() {
            @Override
            public String call() throws Exception {
                return "task1";
            }
        };

        Callable<String> task2 = new Callable<String>() {
            @Override
            public String call() throws Exception {
                return "task2";
            }
        };

        ParallelExecutor<String> parallelExecutor = new ParallelExecutor<String>();

        parallelExecutor.parallelProcess(Lists.newArrayList(task1, task2));
    }

}
