package com.alvazan.orm.api.base;

import java.util.Map;


public abstract class AbstractBootstrap {

	public synchronized static NoSqlEntityManagerFactory create(DbTypeEnum type, Map<String, String> properties) {
		return create(type, "com.alvazan.orm.impl.bindings.Bootstrap", properties);
	}
	
	public synchronized static NoSqlEntityManagerFactory create(DbTypeEnum type, String impl, Map<String, String> properties) {
		try {
			Class<?> clazz = Class.forName(impl);
			AbstractBootstrap newInstance = (AbstractBootstrap) clazz.newInstance();
			return newInstance.createInstance(type, properties);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	protected abstract NoSqlEntityManagerFactory createInstance(DbTypeEnum type, Map<String, String> properties);

}
