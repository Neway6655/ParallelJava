package com.neway6655.parallel.rx;

import com.google.common.collect.Lists;
import com.neway6655.parallel.rx.task.ParallelTask;
import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by neway on 13/7/15.
 */
public class ParallelExecutor {

    private static final int DEFAULT_TIMEOUT_IN_SEC = 3;

    public static List<Object> parallelProcess(ParallelTask... tasks) {
        return parallelProcess(DEFAULT_TIMEOUT_IN_SEC, tasks);
    }

    public static List<Object> parallelProcess(int timeoutInSec, ParallelTask... tasks) {

        List<Observable<Object>> observables = Lists.newArrayList();

        for(ParallelTask task: tasks){
            observables.add(getAsyncObservable(task));
        }

        // TODO: Neway, how to handle timeout situations.
        List<Object> result = Observable.merge(observables).take(timeoutInSec, TimeUnit.SECONDS).buffer(timeoutInSec, TimeUnit.SECONDS).toBlocking().first();

        if (result.size() != tasks.length){
            return Lists.newArrayList();
        }

        for (Object object : result){
            System.out.println(object);
        }

        return result;
    }

    // TODO: Neway, check how io and compute differs, how about the thread pool strategy between them.
    private static Observable<Object> getAsyncObservable(ParallelTask task){
        return getSyncObservable(task).subscribeOn(Schedulers.io());
    }

    private static Observable<Object> getSyncObservable(final ParallelTask task){
        return Observable.create(new Observable.OnSubscribe<Object>() {

            @Override
            public void call(Subscriber<? super Object> subscriber) {
                try {
                    System.out.println("Thread name: " + Thread.currentThread().getName());
                    Object result = task.call();
                    subscriber.onNext(result);
                    subscriber.onCompleted();
                } catch (Exception e) {
                    subscriber.onError(e);
                }
            }
        });
    }

}
