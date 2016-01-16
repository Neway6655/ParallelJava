package com.neway6655.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.util.concurrent.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;

/**
 * RedisLoadingCache is aim to solve cache loading issues in highly concurrent situations, it is designed to avoid large
 * concurrent requests breaking through cache layer when cache data is not loaded or expired, which may cause the
 * layer(e.g. database) behind cache layer needs to handle too many requests concurrently, while the service in this
 * layer, such as database, or other remote service, are often unable to handle such kind of concurrent requests.
 *
 * Cache loading issues contains two cases: cache data expired; cache service are unavailable, such as redis connection
 * broken: For the first case, the solution is to auto refresh cache data before it expired in the background. For the
 * second case, the solution is to limit the concurrent requests by refusing some requests, to protect the backend
 * service layer.
 *
 * @param <T>
 */
public abstract class RedisLoadingCache<T extends CachedObject> {

	private static final Logger LOG = LoggerFactory.getLogger(RedisLoadingCache.class);

	private static final int ASYNC_REFRESH_TIMEOUT = 50;

	private static final int LOCKERS = 32;

	private static final int ASYNC_LOADING_THREADS = 5;

	private static final int DEFAULT_DIRECT_LOADING_THRESHOLD = 100;

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private RedisTemplate jedisTemplate;

	private Class<T> cachedObjectClass;

	private long refreshBeforeExpired;

	private int ttlInSec;

	private long directLoadingThreshold = DEFAULT_DIRECT_LOADING_THRESHOLD;

	private ListeningExecutorService listeningExecutorService;

	private ExecutorService executorService;

	private Striped<Lock> keyLocks = Striped.lazyWeakLock(LOCKERS);

	private RateLimiter rateLimiter = RateLimiter.create(directLoadingThreshold);

	public RedisLoadingCache(RedisTemplate redisTemplate, Class<T> cachedObjectClass, long refreshBeforeExpired,
			int ttlInSec, long directLoadingThreshold) {
		Assert.isTrue(ttlInSec > 0, "Cached object's ttlInSec must be a positive number.");
		this.jedisTemplate = redisTemplate;
		this.cachedObjectClass = cachedObjectClass;
		this.refreshBeforeExpired = refreshBeforeExpired;
		this.ttlInSec = ttlInSec;
		this.directLoadingThreshold = directLoadingThreshold;

		executorService = Executors.newFixedThreadPool(ASYNC_LOADING_THREADS,
				new ThreadFactoryBuilder().setNameFormat("async-refresh-cache-" + name()).build());
		listeningExecutorService = MoreExecutors.listeningDecorator(executorService);
	}

	public abstract T load(String key);

	public T get(String key) {
		T cachedObject = null;
		try {
			String value = (String) jedisTemplate.boundValueOps(key).get();

			if (StringUtils.isBlank(value)) {
				// try to load async first.
				asyncLoading(key);
				waitShortly();
				// try to get from redis after a short period, in case if cached was loaded.
				value = (String) jedisTemplate.boundValueOps(key).get();

				if (StringUtils.isBlank(value)) {
					// then load sync.
					cachedObject = protectedLoad(key);
					if (cachedObject == null) {
						return null;
					}

					set(key, cachedObject);
					return cachedObject;
				}

				try {
					cachedObject = MAPPER.readValue(value, cachedObjectClass);
				} catch (IOException e) {
					LOG.error("Error when read value for json: {}.", value, e);
					return null;
				}
				set(key, cachedObject);
				return cachedObject;
			}

			refreshCacheInBackgroundIfNeed(key, value);
		} catch (RedisConnectionFailureException e) {
			LOG.error("Cache service is unavailable with exception, try to load directly... ", e);
			return protectedLoad(key);
		}

		return cachedObject;

	}

	private T protectedLoad(String key) {
		if (rateLimiter.tryAcquire(100, TimeUnit.MILLISECONDS)) {
			return load(key);
		}

		LOG.warn("Failed to load value directly due to the traffic exceeds threshold: {}.", directLoadingThreshold);
		// if there are too much traffic which exceeds threshold, return null to protect the backend service.
		return null;
	}

	private void waitShortly() {
		try {
			Thread.sleep(ASYNC_REFRESH_TIMEOUT + ThreadLocalRandom.current().nextInt(1, ASYNC_REFRESH_TIMEOUT));
		} catch (InterruptedException e) {
			// ignore.
		}
	}

	private void refreshCacheInBackgroundIfNeed(String key, String value) {
		T cachedObject = null;
		try {
			cachedObject = MAPPER.readValue(value, cachedObjectClass);
		} catch (IOException e) {
			LOG.error("Failed to deserialize cached object from json string: {}.", value, e);
			return;
		}

		long remainingSec = cachedObject.getExpiredTime() - getCurrentTimeInSecond();

		if (remainingSec < refreshBeforeExpired) {
			asyncLoading(key);
		}
	}

	private void set(String key, T cachedObject) {
		cachedObject.setExpiredTime(getCurrentTimeInSecond() + ttlInSec);
		String value = null;
		try {
			value = MAPPER.writeValueAsString(cachedObject);
		} catch (JsonProcessingException e) {
			LOG.error("Failed to serialize object to json.", e);
		}
		if (value == null) {
			return;
		}
		jedisTemplate.boundValueOps(key).set(value, ttlInSec, TimeUnit.SECONDS);
	}

	private void asyncLoading(final String key) {
		// try to get refreshLock
		Lock refreshLock = keyLocks.get(key);

		if (refreshLock == null) {
			return;
		}

		boolean acquiredLock = refreshLock.tryLock();

		// if not acquired refreshLock, then do not load the key because it is loading by others.
		if (!acquiredLock) {
			return;
		}

		// if acquired refreshLock, then refresh the key's value.
		LOG.debug("Async loading cache of key: {}.", key);

		try {
			ListenableFuture<T> resultFuture = listeningExecutorService.submit(new AsyncRefreshCacheTask(jedisTemplate,
					key, ttlInSec));
			CheckedFuture<T, Exception> checkedResultFuture = Futures.makeChecked(resultFuture,
					new Function<Exception, Exception>() {
						@Override
						public Exception apply(Exception input) {
							return new LoadingCacheException("Error occurred when loading key: " + key, input);
						}
					});
			Futures.addCallback(checkedResultFuture, new FutureCallback<T>() {
				@Override
				public void onSuccess(T result) {
					set(key, result);
					LOG.debug("Refreshed Successfully for key: " + key);
				}

				@Override
				public void onFailure(Throwable t) {
					LOG.error("Failed to retrieve cached data.", t);
				}
			});
		} finally {
			refreshLock.unlock();
		}

		return;
	}

	private static Long getCurrentTimeInSecond() {
		return System.currentTimeMillis() / 1000;
	}

	protected abstract String name();

	private class AsyncRefreshCacheTask implements Callable<T> {

		private RedisTemplate jedisTemplate;

		private String key;

		private int ttlInSec;

		public AsyncRefreshCacheTask(RedisTemplate jedisTemplate, String key, int ttlInSec) {
			this.jedisTemplate = jedisTemplate;
			this.key = key;
			this.ttlInSec = ttlInSec;
		}

		@Override
		public T call() throws Exception {
			Assert.hasText(key, "Key should not be empty.");

			T cachedObject = null;
			try {
				cachedObject = protectedLoad(key);
				cachedObject.setExpiredTime(getCurrentTimeInSecond() + ttlInSec);
				String value = MAPPER.writeValueAsString(cachedObject);
				if (value == null) {
					return null;
				}
				jedisTemplate.boundValueOps(key).set(value, ttlInSec, TimeUnit.SECONDS);
			} catch (Exception e) {
				LOG.error("Failed to async load cached key: {}.", key, e);
			}

			return cachedObject;
		}

	}
}
