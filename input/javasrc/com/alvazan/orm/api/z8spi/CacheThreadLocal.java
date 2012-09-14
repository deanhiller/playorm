package com.alvazan.orm.api.z8spi;

public class CacheThreadLocal {

	private static ThreadLocal<Cache> cacheLocal = new ThreadLocal<Cache>();
	
	public static void setCache(Cache cache) {
		cacheLocal.set(cache);
	}
	public static Cache getCache() {
		return cacheLocal.get();
	}
}
