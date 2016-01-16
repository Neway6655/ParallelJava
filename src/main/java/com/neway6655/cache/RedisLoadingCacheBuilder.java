package com.neway6655.cache;

import org.apache.commons.lang3.StringUtils;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.Assert;

import java.util.concurrent.TimeUnit;

/**
 * Created by neway on 15/11/2015.
 */
public class RedisLoadingCacheBuilder<T extends CachedObject> {

	private static final int DEFAULT_DIRECT_LOADING_THRESHOLD = 100;

	private RedisTemplate jedisTemplate;

	private Class<T> cachedObjectClass;

	private long refreshBeforeExpired;

	private int ttl;

	private long directLoadingThreshold = DEFAULT_DIRECT_LOADING_THRESHOLD;

	private String name;

	private RedisLoadingCacheBuilder(RedisTemplate jedisTemplate, Class<T> cachedObjectClass) {
		this.jedisTemplate = jedisTemplate;
		this.cachedObjectClass = cachedObjectClass;
	}

	public static RedisLoadingCacheBuilder newBuilder(RedisTemplate jedisCluster, Class cachedObjectClass) {
		return new RedisLoadingCacheBuilder(jedisCluster, cachedObjectClass);
	}

	public RedisLoadingCacheBuilder refreshBeforeExpired(long period, TimeUnit timeUnit) {
		this.refreshBeforeExpired = timeUnit.toSeconds(period);
		return this;
	}

	public RedisLoadingCacheBuilder expired(long duration, TimeUnit timeUnit) {
		this.ttl = (int) timeUnit.toSeconds(duration);
		return this;
	}

	public RedisLoadingCacheBuilder loadingThreshold(long directLoadingThreshold) {
		this.directLoadingThreshold = directLoadingThreshold;
		return this;
	}

	public RedisLoadingCacheBuilder name(String name) {
		this.name = name;
		return this;
	}

	public RedisLoadingCache build(final CacheLoader loader) {
		Assert.isTrue(StringUtils.isNoneBlank(name), "Cache name should not be empty.");
		Assert.isTrue(ttl > 1, "Expired time must be larger than 1 second.");
		Assert.isTrue(refreshBeforeExpired > 0 && refreshBeforeExpired < ttl,
				"Refresh before expired period must be larger than 0 and smaller than expired time.");

		final String name = this.name;
		return new RedisLoadingCache(jedisTemplate, cachedObjectClass, refreshBeforeExpired, ttl, directLoadingThreshold) {
			@Override
			public CachedObject load(String key) {
				return loader.load();
			}

			@Override
			protected String name() {
				return name;
			}
		};
	}
}
