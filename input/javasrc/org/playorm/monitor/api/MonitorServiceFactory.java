package org.playorm.monitor.api;

import java.util.Map;

public abstract class MonitorServiceFactory {

	private static final String OUR_IMPL = "org.playorm.monitor.bindings.MonitorServiceFactoryImpl";
	
	public static final String SCAN_RATE_MILLIS = "org.playorm.monitor.scanrate";
	public static final String HOST_UNIQUE_NAME = "org.playorm.monitor.host";	
	
	public synchronized static MonitorService create(Map<String, Object> properties) {
		MonitorServiceFactory newInstance = createInstance(OUR_IMPL);
		return newInstance.createService(properties);
	}

	protected abstract MonitorService createService(Map<String, Object> properties);

	private static MonitorServiceFactory createInstance(String impl) {
		try {
			Class<?> clazz = Class.forName(impl);
			MonitorServiceFactory newInstance = (MonitorServiceFactory) clazz.newInstance();
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
