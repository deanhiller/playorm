package com.alvazan.orm.api;


public abstract class AbstractBootstrap {

	public synchronized static NoSqlEntityManagerFactory create(DbTypeEnum type) {
		return create(type, "com.alvazan.orm.impl.bindings.Bootstrap");
	}
	
	public synchronized static NoSqlEntityManagerFactory create(DbTypeEnum type, String impl) {
		try {
			Class<?> clazz = Class.forName(impl);
			AbstractBootstrap newInstance = (AbstractBootstrap) clazz.newInstance();
			return newInstance.createInstance(type);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	protected abstract NoSqlEntityManagerFactory createInstance(DbTypeEnum type);

}
