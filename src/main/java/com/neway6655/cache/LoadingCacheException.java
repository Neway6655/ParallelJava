package com.neway6655.cache;

/**
 * Created by neway on 6/12/2015.
 */
public class LoadingCacheException extends Exception{

    public LoadingCacheException(String message) {
        super(message);
    }

    public LoadingCacheException(String message, Throwable cause) {
        super(message, cause);
    }
}
