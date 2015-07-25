package com.neway6655.parallel.rx.task;

import java.util.concurrent.Callable;

/**
 * Created by neway on 24/7/15.
 */
public abstract class ParallelTask implements Callable {

    @Override
    public Object call() throws Exception {
        return process();
    }

    protected abstract Object process() throws InterruptedException;
}
