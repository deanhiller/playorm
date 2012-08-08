package com.alvazan.orm.api.base;

import java.util.Map;

import com.alvazan.orm.api.spi3.db.conv.Converter;

@SuppressWarnings("rawtypes")
public abstract class AbstractBootstrap {

	public synchronized static NoSqlEntityManagerFactory create(DbTypeEnum type, Map<String, String> properties, Map<Class, Converter> converters, ClassLoader cl) {
		return create(type, "com.alvazan.orm.impl.bindings.Bootstrap", properties, converters, cl);
	}
	
	public synchronized static NoSqlEntityManagerFactory create(DbTypeEnum type, String impl, Map<String, String> properties, Map<Class, Converter> converters, ClassLoader cl) {
		try {
			Class<?> clazz = Class.forName(impl);
			AbstractBootstrap newInstance = (AbstractBootstrap) clazz.newInstance();
			NoSqlEntityManagerFactory inst = newInstance.createInstance(type, properties, converters, cl);
			
			return inst;
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	protected abstract NoSqlEntityManagerFactory createInstance(DbTypeEnum type, Map<String, String> properties, Map<Class, Converter> converters, ClassLoader cl);

}
