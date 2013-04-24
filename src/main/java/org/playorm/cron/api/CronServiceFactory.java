package org.playorm.cron.api;

import java.util.Map;

public abstract class CronServiceFactory {

	private static final String OUR_IMPL = "org.playorm.cron.bindings.CronServiceFactoryImpl";
	
	public static final String SCAN_RATE_MILLIS = "org.playorm.monitor.scanrate";
	public static final String HOST_UNIQUE_NAME = "org.playorm.monitor.host";	
	
	private volatile static CronService singleton;
	
	public static CronService getSingleton(Map<String, Object> properties) {
		//double checked locking is ONLY ok if you use the keyword volatile in java
		synchronized(CronServiceFactory.class) {
			if(singleton == null)
				singleton = create(properties);
		}
		return singleton;
	}
	
	public synchronized static CronService create(Map<String, Object> properties) {
		CronServiceFactory newInstance = createInstance(OUR_IMPL);
		return newInstance.createService(properties);
	}

	public static final String NOSQL_MGR_FACTORY = "org.playorm.monitor.factory";

	protected abstract CronService createService(Map<String, Object> properties);

	private static CronServiceFactory createInstance(String impl) {
		try {
			Class<?> clazz = Class.forName(impl);
			CronServiceFactory newInstance = (CronServiceFactory) clazz.newInstance();
			return newInstance;
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

}
