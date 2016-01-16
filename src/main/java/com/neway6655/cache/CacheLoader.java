package com.neway6655.cache;

/**
 * Created by neway on 15/11/2015.
 */
public abstract class CacheLoader<T extends CachedObject> {

    public abstract T load();
}
