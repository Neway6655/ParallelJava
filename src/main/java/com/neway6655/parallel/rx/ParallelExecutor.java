package com.neway6655.parallel.rx;

import com.google.common.collect.Lists;
import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

/**
 * Created by neway on 13/7/15.
 */
public class ParallelExecutor {

    private static final int TIMEOUT_IN_SEC = 3;

    public static void parallelProcess(Callable<Object>... tasks) {

        List<Observable<Object>> observables = Lists.newArrayList();

        for(Callable<Object> task: tasks){
            observables.add(getAsyncObservable(task));
        }

        List<Object> result = Observable.merge(observables).buffer(TIMEOUT_IN_SEC, TimeUnit.SECONDS).toBlocking().first();

        for (Object object : result){
            System.out.println(object);
        }
    }

    private static Observable<Object> getAsyncObservable(Callable<Object> task){
        return getSyncObservable(task).subscribeOn(Schedulers.io());
    }

    private static Observable<Object> getSyncObservable(Callable<Object> task){
        FutureTask<Object> future = new FutureTask<Object>(task);
        new Thread(future).start();
        return Observable.from(future);
    }

    public static Observable<Object> executeAsync(final Object object){
        return executeSync(object).subscribeOn(Schedulers.io());
    }

    private static Observable<Object> executeSync(final Object object){
        return Observable.create(new Observable.OnSubscribe<Object>() {
            @Override
            public void call(Subscriber<? super Object> subscriber) {
                if (((Integer) object) == 1){
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                System.out.println("execute sync method... " + object);
                subscriber.onNext(object);
                subscriber.onCompleted();
            }
        });
    }
}
